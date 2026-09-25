# Printer activity log ("timeline")
#
# Records user actions and print events on this printer into the Moonraker
# database, exposes them over HTTP / JSON-RPC and pushes
# "notify_activity_changed" to websocket clients.
#
# Two tiers:
#   tier 1 - important, kept forever, collected by fleet_daemon (print state
#            changes, filament / nozzle changes, service events, klippy
#            shutdowns, emergency stop, prime confirmations, ...)
#   tier 2 - verbose, printer only, pruned by age (raw gcode, jogs, homing,
#            temperature commands, ...)
#
# This component is a core component (see server.CORE_COMPONENTS) and needs
# no [activity] section in moonraker.conf.  Settings live in the database.
#
# Copyright (C) 2026 Pantheon Design
#
# This file may be distributed under the terms of the GNU GPLv3 license.

from __future__ import annotations
import asyncio
import bisect
import collections
import ipaddress
import logging
import re
import socket
import time
import urllib.parse
import uuid
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
)

from ..common import (
    RequestType, JobEvent, KlippyState, WebRequest, current_api_request
)
from ..utils import json_wrapper as jsonw

if TYPE_CHECKING:
    from ..confighelper import ConfigHelper
    from ..eventloop import FlexTimer
    from .database import MoonrakerDatabase, NamespaceWrapper
    from .klippy_connection import KlippyConnection
    from .history import History

ACTIVITY_NS = "activity"
META_KEY = "activity.meta"
SETTINGS_KEY = "activity.settings"
META_VERSION = 1
KEY_WIDTH = 12

DEFAULT_SETTINGS: Dict[str, Any] = {
    "retention_days": 90,
    "record_gcode": True,
    "coalesce_window": 10.0,
}
TIER2_MAX = 20_000
TIER1_SAFETY_MAX = 250_000
PRUNE_INTERVAL = 3600.
PRUNE_FIRST_DELAY = 120.
PRUNE_CHUNK = 500
FLUSH_DELAY = .25
DAEMON_IP_REFRESH = 600.
RECENT_MAX = 200
SCRIPT_MAX_CHARS = 2000
SUMMARY_MAX = 160
DETAILS_MAX_BYTES = 4096
START_INTENT_WINDOW = 30.
PAUSE_INTENT_WINDOW = 15.
FILAMENT_DEDUPE_WINDOW = 30.
SHORT_DEDUPE_WINDOW = 10.
CONTINUATION_THRESHOLD = 30.
# Timestamps below this are from a clock that has not been set (no NTP yet)
CLOCK_SANE_TS = 1.6e9
LIST_LIMIT_DEFAULT = 100
LIST_LIMIT_MAX = 1000
SERVICE_TIME_FUTURE_SLACK = 300.

# The single source of truth for service-event types (id, label)
SERVICE_TYPES: List[Tuple[str, str]] = [
    ("nozzle_change", "Nozzle change"),
    ("hotend_service", "Hotend service"),
    ("extruder_feeder", "Extruder / feeder"),
    ("belts_motion", "Belts / motion"),
    ("bed_leveling", "Bed leveling"),
    ("lubrication", "Lubrication"),
    ("firmware_config", "Firmware / config"),
    ("repair", "Repair"),
    ("inspection", "Inspection"),
    ("other", "Other"),
]
SERVICE_TYPE_IDS = {sid for sid, _ in SERVICE_TYPES}
SERVICE_TYPE_LABELS = dict(SERVICE_TYPES)

TIER1_TYPES = frozenset({
    "print_started", "print_paused", "print_resumed", "print_completed",
    "print_cancelled", "print_error", "print_interrupted",
    "filament_set", "spool_loaded", "nozzle_set", "nozzle_life_reset",
    "service", "klippy_shutdown", "klippy_error", "emergency_stop",
    "firmware_restart", "prime_confirmed", "prime_reset",
    "fleet_worker_toggled",
})

CLIENT_EVENT_RE = re.compile(r"^client\.[a-z0-9_]{1,40}$")
GCODE_CMD_RE = re.compile(r"^[GM]\d+$")
JOG_CMDS = frozenset({"G0", "G1", "G90", "G91", "M82", "M83"})
TEMP_CMDS = {
    "M104": "extruder", "M109": "extruder",
    "M140": "heater_bed", "M190": "heater_bed",
    "M141": "chamber", "M191": "chamber",
}
SYSTEM_ORIGIN_SOURCE = "system"


@dataclass
class Origin:
    source: str
    client: Optional[str] = None
    ip: Optional[str] = None
    endpoint: Optional[str] = None


SYSTEM_ORIGIN = Origin(SYSTEM_ORIGIN_SOURCE)


class IdxEntry(NamedTuple):
    seq: int
    ts: float
    tier: int
    type: str
    source: str
    deleted: bool
    id: str
    job_id: Optional[str]


def _key(seq: int) -> str:
    return f"{seq:0{KEY_WIDTH}d}"


def _trim(text: Any, limit: int) -> str:
    text = "" if text is None else str(text)
    if len(text) <= limit:
        return text
    return text[:limit - 1] + "…"


class Activity:
    def __init__(self, config: ConfigHelper) -> None:
        self.server = config.get_server()
        self.eventloop = self.server.get_event_loop()
        self.db: MoonrakerDatabase = self.server.lookup_component("database")
        self.db.register_local_namespace(ACTIVITY_NS)
        self.ns: NamespaceWrapper = self.db.wrap_namespace(
            ACTIVITY_NS, parse_keys=False
        )
        meta = self.db.get_item("moonraker", META_KEY, None).result()
        self.meta: Dict[str, Any] = meta if isinstance(meta, dict) else {}
        stored = self.db.get_item("moonraker", SETTINGS_KEY, {}).result()
        self.settings: Dict[str, Any] = dict(DEFAULT_SETTINGS)
        if isinstance(stored, dict):
            self.settings.update(stored)

        self._ready = False
        self._index: List[IdxEntry] = []       # ascending seq
        self._seqs: List[int] = []             # parallel to _index
        self._by_id: Dict[str, int] = {}       # id -> seq
        self._tier2_count = 0
        self.next_seq = 1
        self._pending: Dict[str, Dict[str, Any]] = {}
        self._pending_deletes: Set[str] = set()
        self._flush_handle: Optional[asyncio.TimerHandle] = None
        self._recent: collections.deque = collections.deque(maxlen=RECENT_MAX)
        self._recent_by_seq: Dict[int, Dict[str, Any]] = {}
        self._bursts: Dict[str, int] = {}      # coalesce key -> seq
        self._intents: Dict[str, Tuple[float, Origin]] = {}
        self._daemon_ips: Set[Any] = set()
        self._daemon_ip_task: Optional[asyncio.Task] = None
        self._prune_timer: Optional[FlexTimer] = None
        self._service_lock = asyncio.Lock()
        self._last_404_log = 0.

        self.server.register_notification("activity:activity_changed")
        self.server.register_notification("activity:settings_changed")

        handlers: List[Tuple[str, Callable]] = [
            ("history:history_changed", self._on_history_changed),
            ("job_state:state_changed", self._on_job_state),
            ("klippy_connection:request_received", self._on_klippy_request),
            ("klippy_connection:prime_state_changed", self._on_prime_changed),
            ("klippy_connection:fleet_worker_changed", self._on_fleet_worker),
            ("database:hs3_item_changed", self._on_hs3_changed),
            ("spool_tracker:filament_changed", self._on_filament_changed),
            ("spool_tracker:spool_loaded", self._on_spool_loaded),
            ("spool_tracker:nozzle_life_changed", self._on_nozzle_life),
            ("spool_tracker:meters_changed", self._on_meters),
            ("fleet:download_requested", self._on_fleet_download),
            ("file_manager:filelist_changed", self._on_filelist),
            ("server:klippy_shutdown", self._on_klippy_shutdown),
            ("server:klippy_started", self._on_klippy_started),
            ("server:klippy_ready", self._on_klippy_ready),
        ]
        for name, cb in handlers:
            self.server.register_event_handler(name, cb)

        ep = self.server.register_endpoint
        ep("/server/activity/list", RequestType.GET, self._handle_list)
        ep("/server/activity/info", RequestType.GET, self._handle_info)
        ep("/server/activity/service", RequestType.POST | RequestType.DELETE,
           self._handle_service)
        ep("/server/activity/service/update", RequestType.POST,
           self._handle_service_update)
        ep("/server/activity/service_types", RequestType.GET,
           self._handle_service_types)
        ep("/server/activity/settings", RequestType.GET | RequestType.POST,
           self._handle_settings)
        ep("/server/activity/event", RequestType.POST,
           self._handle_client_event)
        ep("/server/activity/prune", RequestType.POST, self._handle_prune)

    # ------------------------------------------------------------------ init
    async def component_init(self) -> None:
        await self._load_index()
        self._prune_timer = self.eventloop.register_timer(self._prune_cb)
        self._prune_timer.start(delay=PRUNE_FIRST_DELAY)
        self.eventloop.register_callback(self._refresh_daemon_ips)
        self._ready = True
        open_job = self.meta.get("open_job_id")
        if open_job:
            # Moonraker died while a print was tracked (crash / power loss)
            self._add(
                "print_interrupted", 1,
                f"Print interrupted (moonraker crashed)",
                {"reason": "interrupted"},
                origin=SYSTEM_ORIGIN, job_id=str(open_job),
            )
            await self._set_meta(open_job_id=None)
        self._add(
            "moonraker_started", 2, "Moonraker started",
            {"version": self.server.get_app_args().get("software_version", "")},
            origin=SYSTEM_ORIGIN,
        )

    async def _load_index(self) -> None:
        items: List[Tuple[str, Any]] = await self.ns.items()
        by_id: Dict[str, Tuple[int, Dict[str, Any]]] = {}
        stale: List[str] = []
        for key, rec in items:
            try:
                seq = int(key)
            except (TypeError, ValueError):
                stale.append(key)
                continue
            if not isinstance(rec, dict) or "id" not in rec:
                stale.append(key)
                continue
            rid = str(rec["id"])
            prev = by_id.get(rid)
            if prev is not None:
                # Duplicate id (interrupted re-sequence): keep the higher seq
                if prev[0] > seq:
                    stale.append(key)
                    continue
                stale.append(_key(prev[0]))
            by_id[rid] = (seq, rec)
        entries = sorted(
            (self._entry(seq, rec) for seq, rec in by_id.values()),
            key=lambda e: e.seq
        )
        self._index = entries
        self._seqs = [e.seq for e in entries]
        self._by_id = {e.id: e.seq for e in entries}
        self._tier2_count = sum(1 for e in entries if e.tier == 2)
        self.next_seq = (entries[-1].seq + 1) if entries else 1
        if stale:
            logging.info(f"Activity: removing {len(stale)} stale records")
            for i in range(0, len(stale), PRUNE_CHUNK):
                await self.ns.delete_batch(stale[i:i + PRUNE_CHUNK])
        meta_changed = False
        if not entries or not self.meta.get("epoch"):
            self.meta["epoch"] = str(uuid.uuid4())
            meta_changed = True
        if self.meta.get("version") != META_VERSION:
            self.meta["version"] = META_VERSION
            meta_changed = True
        if meta_changed:
            await self._set_meta()
        logging.info(
            f"Activity log loaded: {len(entries)} events "
            f"({self._tier2_count} tier 2), next_seq={self.next_seq}, "
            f"epoch={self.meta['epoch']}"
        )

    @staticmethod
    def _entry(seq: int, rec: Dict[str, Any]) -> IdxEntry:
        return IdxEntry(
            seq, float(rec.get("ts") or 0.), int(rec.get("tier") or 2),
            str(rec.get("type") or "unknown"),
            str(rec.get("source") or SYSTEM_ORIGIN_SOURCE),
            bool(rec.get("deleted")), str(rec["id"]),
            rec.get("job_id") or None,
        )

    async def _set_meta(self, **changes: Any) -> None:
        self.meta.update(changes)
        try:
            await self.db.insert_item("moonraker", META_KEY, self.meta)
        except Exception:
            logging.exception("Activity: failed to persist meta")

    def _set_meta_sync(self, **changes: Any) -> None:
        self.meta.update(changes)
        try:
            self.db.insert_item("moonraker", META_KEY, self.meta)
        except Exception:
            logging.exception("Activity: failed to persist meta")

    # ------------------------------------------------------- attribution
    async def _refresh_daemon_ips(self) -> None:
        while True:
            fi = self.server.lookup_component("fleet_integration", None)
            url = getattr(fi, "fleet_url", None) if fi is not None else None
            if url:
                host = urllib.parse.urlparse(url).hostname
                if host:
                    try:
                        infos = await self.eventloop.run_in_thread(
                            socket.getaddrinfo, host, None
                        )
                        ips = set()
                        for info in infos:
                            try:
                                ips.add(ipaddress.ip_address(info[4][0]))
                            except ValueError:
                                pass
                        if ips != self._daemon_ips:
                            self._daemon_ips = ips
                            logging.info(
                                f"Activity: fleet_daemon resolved to "
                                f"{[str(i) for i in ips]}"
                            )
                    except Exception as e:
                        logging.debug(f"Activity: daemon resolve failed: {e}")
            await asyncio.sleep(DAEMON_IP_REFRESH)

    def resolve_origin(self, web_request: Optional[WebRequest]) -> Origin:
        wr = web_request
        if wr is None or (
            wr.get_client_connection() is None and wr.get_ip_address() is None
        ):
            outer = current_api_request.get(None)
            if outer is not None:
                wr = outer
        if wr is None:
            return SYSTEM_ORIGIN
        conn = wr.get_client_connection()
        ip = wr.get_ip_address()
        if ip is None and conn is not None:
            ip = getattr(conn, "ip_addr", None)
        ip_s = str(ip) if ip is not None else None
        ep = wr.get_endpoint()
        if conn is not None:
            name = ""
            try:
                name = str((conn.client_data or {}).get("name", "") or "")
            except Exception:
                pass
            if name:
                return Origin(name.lower(), name, ip_s, ep)
            return Origin(self._source_from_ip(ip, "websocket"), None, ip_s, ep)
        if ip is not None:
            return Origin(self._source_from_ip(ip, "http"), None, ip_s, ep)
        return Origin(SYSTEM_ORIGIN_SOURCE, None, None, ep)

    def _source_from_ip(self, ip: Any, default: str) -> str:
        if ip is None:
            return default
        try:
            if ip.is_loopback:
                return "klipperscreen"
        except AttributeError:
            return default
        if ip in self._daemon_ips:
            return "fleet_daemon"
        return default

    # ------------------------------------------------------------ writing
    def _add(
        self,
        etype: str,
        tier: int,
        summary: str,
        details: Optional[Dict[str, Any]] = None,
        *,
        origin: Optional[Origin] = None,
        web_request: Optional[WebRequest] = None,
        ts: Optional[float] = None,
        job_id: Optional[str] = None,
        filename: Optional[str] = None,
        coalesce_key: Optional[str] = None,
        merge: Optional[Callable[[Dict[str, Any], Dict[str, Any]], None]] = None,
        dedupe_key: Optional[str] = None,
        dedupe_window: float = 0.,
    ) -> Optional[Dict[str, Any]]:
        if not self._ready:
            return None
        now = time.time()
        if origin is None:
            origin = self.resolve_origin(web_request)
        details = self._cap_details(dict(details or {}))
        # 1) dedupe: an identical event was recorded moments ago
        if dedupe_key is not None and dedupe_window > 0:
            for rec in reversed(self._recent):
                if now - float(rec.get("recorded_at", 0.)) > dedupe_window:
                    break
                if (
                    rec.get("type") == etype and not rec.get("deleted") and
                    rec.get("details", {}).get(dedupe_key) ==
                    details.get(dedupe_key)
                ):
                    return rec
        # 2) coalesce: extend a burst in place
        if coalesce_key is not None:
            seq = self._bursts.get(coalesce_key)
            rec = self._recent_by_seq.get(seq) if seq is not None else None
            window = float(self.settings.get("coalesce_window") or 0.)
            if (
                rec is not None and not rec.get("deleted") and
                now - float(rec.get("updated_at", 0.)) <= window
            ):
                if merge is not None:
                    merge(rec, details)
                rec["summary"] = _trim(summary, SUMMARY_MAX) if summary else rec["summary"]
                rec["details"] = self._cap_details(rec.get("details", {}))
                rec["updated_at"] = now
                self._stage(rec)
                self._notify("updated", rec)
                return rec
        # 3) new record
        seq = self.next_seq
        self.next_seq += 1
        rec = {
            "id": str(uuid.uuid4()),
            "seq": seq,
            "ts": float(ts) if ts is not None else now,
            "recorded_at": now,
            "updated_at": now,
            "type": etype,
            "tier": 1 if tier == 1 else 2,
            "source": origin.source,
            "client": origin.client,
            "ip": origin.ip,
            "summary": _trim(summary, SUMMARY_MAX),
            "details": details,
            "job_id": job_id,
            "filename": filename,
            "deleted": False,
        }
        if rec["ts"] < CLOCK_SANE_TS:
            rec["details"]["clock_unsynced"] = True
        self._append_index(rec)
        if coalesce_key is not None:
            self._bursts[coalesce_key] = seq
        self._stage(rec)
        self._notify("added", rec)
        return rec

    def _append_index(self, rec: Dict[str, Any]) -> None:
        entry = self._entry(rec["seq"], rec)
        self._index.append(entry)
        self._seqs.append(entry.seq)
        self._by_id[entry.id] = entry.seq
        if entry.tier == 2:
            self._tier2_count += 1
        self._recent.append(rec)
        self._recent_by_seq[entry.seq] = rec
        if len(self._recent_by_seq) > RECENT_MAX * 2:
            keep = {r["seq"] for r in self._recent}
            self._recent_by_seq = {
                s: r for s, r in self._recent_by_seq.items() if s in keep
            }

    def _reseq(self, rec: Dict[str, Any], **changes: Any) -> Dict[str, Any]:
        """Apply changes to a tier-1 record and give it a fresh seq so
        incremental collectors (since_seq) pick the mutation up."""
        old_seq = int(rec["seq"])
        new_seq = self.next_seq
        self.next_seq += 1
        rec.update(changes)
        rec["seq"] = new_seq
        rec["updated_at"] = time.time()
        # Drop the old index entry
        pos = bisect.bisect_left(self._seqs, old_seq)
        if pos < len(self._seqs) and self._seqs[pos] == old_seq:
            old_entry = self._index[pos]
            if old_entry.tier == 2:
                self._tier2_count -= 1
            del self._index[pos]
            del self._seqs[pos]
        self._recent_by_seq.pop(old_seq, None)
        self._pending.pop(_key(old_seq), None)
        self._pending_deletes.add(_key(old_seq))
        self._append_index(rec)
        self._stage(rec)
        return rec

    def _stage(self, rec: Dict[str, Any]) -> None:
        self._pending[_key(int(rec["seq"]))] = rec
        if not self.server.is_running():
            self._flush_sync()
            return
        if self._flush_handle is None:
            self._flush_handle = self.eventloop.delay_callback(
                FLUSH_DELAY, self._flush
            )

    def _take_pending(
        self
    ) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
        pending, self._pending = self._pending, {}
        deletes = [k for k in self._pending_deletes if k not in pending]
        self._pending_deletes = set()
        return pending, deletes

    def _flush_sync(self) -> None:
        # Only valid while the server is not running (database calls are
        # executed synchronously then).
        self._flush_handle = None
        pending, deletes = self._take_pending()
        try:
            if pending:
                self.ns.insert_batch(pending)
            if deletes:
                self.ns.delete_batch(deletes)
        except Exception:
            logging.exception("Activity: synchronous flush failed")

    async def _flush(self) -> None:
        self._flush_handle = None
        pending, deletes = self._take_pending()
        if not pending and not deletes:
            return
        try:
            if pending:
                await self.ns.insert_batch(pending)
            if deletes:
                await self.ns.delete_batch(deletes)
        except Exception as e:
            if type(e).__name__ == "MapFullError":
                self.server.add_warning(
                    "Activity log: database map is full, pruning old "
                    "tier-2 events", warn_id="activity_map_full"
                )
                await self._emergency_prune()
                try:
                    if pending:
                        await self.ns.insert_batch(pending)
                    if deletes:
                        await self.ns.delete_batch(deletes)
                    return
                except Exception:
                    logging.exception("Activity: flush retry failed")
                # keep tier-1 rows pending, drop tier-2
                for k, rec in pending.items():
                    if rec.get("tier") == 1:
                        self._pending.setdefault(k, rec)
                self._pending_deletes.update(deletes)
            else:
                logging.exception("Activity: flush failed")
                self._pending.update(
                    {k: v for k, v in pending.items() if k not in self._pending}
                )
                self._pending_deletes.update(deletes)

    async def _flush_now(self) -> None:
        if self._flush_handle is not None:
            self._flush_handle.cancel()
            self._flush_handle = None
        await self._flush()

    def _notify(self, action: str, rec: Dict[str, Any]) -> None:
        self.server.send_event(
            "activity:activity_changed", {"action": action, "event": rec}
        )

    def _cap_details(self, details: Dict[str, Any]) -> Dict[str, Any]:
        for k in ("script", "last_script"):
            v = details.get(k)
            if isinstance(v, str) and len(v) > SCRIPT_MAX_CHARS:
                details[k] = v[:SCRIPT_MAX_CHARS]
                details["truncated"] = True
        try:
            if len(jsonw.dumps(details)) > DETAILS_MAX_BYTES:
                return {"truncated": True, "keys": sorted(details.keys())}
        except Exception:
            return {"truncated": True}
        return details

    async def _get_record(self, seq: int) -> Optional[Dict[str, Any]]:
        rec = self._pending.get(_key(seq))
        if rec is not None:
            return rec
        rec = self._recent_by_seq.get(seq)
        if rec is not None:
            return rec
        return await self.ns.get(_key(seq), None)

    async def _get_records(self, seqs: List[int]) -> List[Dict[str, Any]]:
        out: Dict[int, Dict[str, Any]] = {}
        missing: List[str] = []
        for seq in seqs:
            rec = self._pending.get(_key(seq)) or self._recent_by_seq.get(seq)
            if rec is not None:
                out[seq] = rec
            else:
                missing.append(_key(seq))
        if missing:
            fetched: Dict[str, Any] = await self.ns.get_batch(missing)
            for key, rec in fetched.items():
                if isinstance(rec, dict):
                    try:
                        out[int(key)] = rec
                    except ValueError:
                        pass
        return [out[s] for s in seqs if s in out]

    # ------------------------------------------------------------ pruning
    def _prune_cb(self, eventtime: float) -> float:
        self.eventloop.register_callback(self._prune)
        return eventtime + PRUNE_INTERVAL

    async def _prune(self) -> int:
        days = float(self.settings.get("retention_days") or 0.)
        cutoff = time.time() - days * 86400. if days > 0 else None
        victims: List[IdxEntry] = []
        if cutoff is not None:
            for e in self._index:
                if e.tier == 2 and CLOCK_SANE_TS < e.ts < cutoff:
                    victims.append(e)
        remaining_t2 = self._tier2_count - len(victims)
        if remaining_t2 > TIER2_MAX:
            excess = remaining_t2 - TIER2_MAX
            chosen = {e.seq for e in victims}
            for e in self._index:
                if excess <= 0:
                    break
                if e.tier == 2 and e.seq not in chosen:
                    victims.append(e)
                    excess -= 1
        tier1 = len(self._index) - self._tier2_count
        if tier1 > TIER1_SAFETY_MAX:
            self.server.add_warning(
                f"Activity log holds {tier1} tier-1 events; pruning the "
                "oldest beyond the safety cap", warn_id="activity_tier1_cap"
            )
            excess = tier1 - TIER1_SAFETY_MAX
            for e in self._index:
                if excess <= 0:
                    break
                if e.tier == 1:
                    victims.append(e)
                    excess -= 1
        if not victims:
            return 0
        return await self._delete_entries(victims)

    async def _emergency_prune(self) -> int:
        t2 = [e for e in self._index if e.tier == 2]
        victims = t2[:max(1, len(t2) // 4)]
        return await self._delete_entries(victims)

    async def _delete_entries(self, victims: List[IdxEntry]) -> int:
        seqs = {e.seq for e in victims}
        keys = [_key(s) for s in seqs]
        for i in range(0, len(keys), PRUNE_CHUNK):
            chunk = keys[i:i + PRUNE_CHUNK]
            try:
                await self.ns.delete_batch(chunk)
            except Exception:
                logging.exception("Activity: prune delete failed")
                return 0
        self._index = [e for e in self._index if e.seq not in seqs]
        self._seqs = [e.seq for e in self._index]
        self._by_id = {e.id: e.seq for e in self._index}
        self._tier2_count = sum(1 for e in self._index if e.tier == 2)
        for s in seqs:
            self._recent_by_seq.pop(s, None)
            self._pending.pop(_key(s), None)
        self._bursts = {k: v for k, v in self._bursts.items() if v not in seqs}
        logging.info(f"Activity: pruned {len(seqs)} events")
        return len(seqs)

    # ------------------------------------------------------- event hooks
    def _job_id(self) -> Optional[str]:
        hist: Optional[History] = self.server.lookup_component("history", None)
        if hist is None:
            return None
        return getattr(hist, "current_job_id", None)

    def _take_intent(self, kind: str, window: float) -> Optional[Origin]:
        item = self._intents.pop(kind, None)
        if item is None:
            return None
        stamp, origin = item
        if time.monotonic() - stamp > window:
            return None
        return origin

    def _set_intent(self, kind: str, origin: Origin) -> None:
        self._intents[kind] = (time.monotonic(), origin)

    def _on_history_changed(self, data: Dict[str, Any]) -> None:
        job = data.get("job") or {}
        action = data.get("action")
        job_id = job.get("job_id")
        job_id = str(job_id) if job_id is not None else None
        filename = job.get("filename")
        if action == "added":
            start_time = float(job.get("start_time") or 0.)
            continuation = (
                start_time > 0 and time.time() - start_time > CONTINUATION_THRESHOLD
            )
            origin = self._take_intent("start", START_INTENT_WINDOW)
            if continuation:
                origin = SYSTEM_ORIGIN
            self._add(
                "print_started", 1,
                f"Print {'resumed tracking' if continuation else 'started'}: "
                f"{filename}",
                {"start_time": start_time, "continuation": continuation},
                origin=origin or SYSTEM_ORIGIN, job_id=job_id, filename=filename,
            )
            self._set_meta_or_schedule(open_job_id=job_id)
        elif action == "finished":
            status = str(job.get("status") or "")
            details = {
                "status": status,
                "print_duration": job.get("print_duration"),
                "total_duration": job.get("total_duration"),
                "filament_used": job.get("filament_used"),
            }
            dur = self._fmt_duration(job.get("print_duration"))
            origin: Optional[Origin] = SYSTEM_ORIGIN
            if status == "completed":
                etype, summary = "print_completed", f"Print completed: {filename} ({dur})"
            elif status == "cancelled":
                etype, summary = "print_cancelled", f"Print cancelled: {filename} ({dur})"
                origin = self._take_intent("cancel", START_INTENT_WINDOW) or SYSTEM_ORIGIN
            elif status == "error":
                etype, summary = "print_error", f"Print error: {filename}"
                kconn: KlippyConnection = self.server.lookup_component("klippy_connection")
                details["message"] = kconn.state_message
            else:
                etype = "print_interrupted"
                summary = f"Print interrupted ({status}): {filename}"
                details["reason"] = status
            self._add(
                etype, 1, summary, details, origin=origin,
                job_id=job_id, filename=filename,
            )
            self._set_meta_or_schedule(open_job_id=None)

    def _set_meta_or_schedule(self, **changes: Any) -> None:
        if self.server.is_running():
            self.eventloop.register_callback(self._set_meta, **changes)
        else:
            self._set_meta_sync(**changes)

    @staticmethod
    def _fmt_duration(secs: Any) -> str:
        try:
            s = int(float(secs or 0))
        except (TypeError, ValueError):
            return "?"
        h, rem = divmod(s, 3600)
        m, s = divmod(rem, 60)
        if h:
            return f"{h}h {m:02d}m"
        if m:
            return f"{m}m {s:02d}s"
        return f"{s}s"

    def _on_job_state(
        self, job_event: JobEvent, prev_stats: Dict[str, Any],
        new_stats: Dict[str, Any]
    ) -> None:
        filename = new_stats.get("filename") or prev_stats.get("filename")
        job_id = self._job_id()
        if job_event == JobEvent.PAUSED:
            origin = self._take_intent("pause", PAUSE_INTENT_WINDOW)
            self._add(
                "print_paused", 1, f"Print paused: {filename}",
                {"print_duration": new_stats.get("print_duration"),
                 "total_duration": new_stats.get("total_duration")},
                origin=origin or SYSTEM_ORIGIN, job_id=job_id, filename=filename,
            )
        elif job_event == JobEvent.RESUMED:
            origin = self._take_intent("resume", PAUSE_INTENT_WINDOW)
            self._add(
                "print_resumed", 1, f"Print resumed: {filename}",
                {"print_duration": new_stats.get("print_duration"),
                 "total_duration": new_stats.get("total_duration")},
                origin=origin or SYSTEM_ORIGIN, job_id=job_id, filename=filename,
            )
        elif self.server.lookup_component("history", None) is None:
            # No history component: record start/finish from job_state only
            if job_event == JobEvent.STARTED:
                origin = self._take_intent("start", START_INTENT_WINDOW)
                self._add("print_started", 1, f"Print started: {filename}",
                          {"continuation": False}, origin=origin or SYSTEM_ORIGIN,
                          filename=filename)
            elif job_event == JobEvent.COMPLETE:
                self._add("print_completed", 1, f"Print completed: {filename}",
                          {"status": "completed"}, origin=SYSTEM_ORIGIN,
                          filename=filename)
            elif job_event == JobEvent.ERROR:
                self._add("print_error", 1, f"Print error: {filename}",
                          {"status": "error"}, origin=SYSTEM_ORIGIN,
                          filename=filename)
            elif job_event == JobEvent.CANCELLED:
                origin = self._take_intent("cancel", START_INTENT_WINDOW)
                self._add("print_cancelled", 1, f"Print cancelled: {filename}",
                          {"status": "cancelled"}, origin=origin or SYSTEM_ORIGIN,
                          filename=filename)

    def _on_klippy_request(self, rpc_method: str, web_request: WebRequest) -> None:
        origin = self.resolve_origin(web_request)
        if rpc_method == "gcode/script":
            script = web_request.get_str("script", "")
            if script:
                self._record_script(script, origin)
        elif rpc_method == "emergency_stop":
            self._add("emergency_stop", 1, "Emergency stop", {"via": "rpc"},
                      origin=origin)
        elif rpc_method == "pause_resume/pause":
            self._set_intent("pause", origin)
        elif rpc_method == "pause_resume/resume":
            self._set_intent("resume", origin)
        elif rpc_method == "pause_resume/cancel":
            self._set_intent("cancel", origin)
        elif rpc_method == "gcode/restart":
            self._add("klippy_restart", 2, "Klipper restart", {}, origin=origin)
        elif rpc_method == "gcode/firmware_restart":
            self._add("firmware_restart", 1, "Firmware restart", {},
                      origin=origin)

    def _record_script(self, script: str, origin: Origin) -> None:
        lines = [ln.split(";", 1)[0].strip() for ln in script.splitlines()]
        lines = [ln for ln in lines if ln]
        if not lines:
            return
        cmds = [ln.split()[0].upper() for ln in lines]
        first = cmds[0]
        first_args = lines[0].split()[1:]
        if "M112" in cmds:
            self._add("emergency_stop", 1, "Emergency stop (M112)",
                      {"via": "M112"}, origin=origin)
            return
        if "FIRMWARE_RESTART" in cmds:
            self._add("firmware_restart", 1, "Firmware restart", {},
                      origin=origin)
            return
        if "RESTART" in cmds:
            self._add("klippy_restart", 2, "Klipper restart", {},
                      origin=origin)
            return
        if first == "SDCARD_PRINT_FILE":
            self._set_intent("start", origin)
            return
        if first in ("PAUSE", "RESUME", "CANCEL_PRINT", "M24", "M25"):
            kind = {"PAUSE": "pause", "M25": "pause", "RESUME": "resume",
                    "M24": "resume", "CANCEL_PRINT": "cancel"}[first]
            self._set_intent(kind, origin)
        if all(c in JOG_CMDS for c in cmds) and any(c in ("G0", "G1") for c in cmds):
            axes: Set[str] = set()
            for ln, c in zip(lines, cmds):
                if c in ("G0", "G1"):
                    for tok in ln.split()[1:]:
                        if tok and tok[0].upper() in "XYZE":
                            axes.add(tok[0].upper())
            relative = "G91" in cmds
            key = f"jog:{origin.source}:{origin.client or ''}:{origin.ip or ''}"

            def merge(rec: Dict[str, Any], det: Dict[str, Any]) -> None:
                d = rec.setdefault("details", {})
                d["count"] = int(d.get("count", 1)) + 1
                d["axes"] = sorted(set(d.get("axes", [])) | set(det["axes"]))
                d["relative"] = det["relative"]
                d["last_script"] = det["last_script"]
                rec["summary"] = _trim(
                    f"Jog x{d['count']} ({', '.join(d['axes']) or '-'})",
                    SUMMARY_MAX
                )

            self._add(
                "jog", 2, f"Jog ({', '.join(sorted(axes)) or '-'})",
                {"count": 1, "axes": sorted(axes), "relative": relative,
                 "last_script": script},
                origin=origin, coalesce_key=key, merge=merge,
            )
            return
        if first == "G28":
            axes_s = "".join(
                t[0].upper() for t in first_args if t and t[0].upper() in "XYZ"
            ) or "XYZ"
            self._add("homing", 2, f"Home {axes_s}", {"axes": axes_s, "script": script},
                      origin=origin)
            return
        if first in TEMP_CMDS or first == "SET_HEATER_TEMPERATURE":
            heater = TEMP_CMDS.get(first, "")
            target: Optional[float] = None
            for tok in first_args:
                up = tok.upper()
                if first in TEMP_CMDS and up.startswith("S"):
                    try:
                        target = float(up[1:])
                    except ValueError:
                        pass
                elif up.startswith("HEATER="):
                    heater = tok.split("=", 1)[1]
                elif up.startswith("TARGET="):
                    try:
                        target = float(tok.split("=", 1)[1])
                    except ValueError:
                        pass
            key = f"temp:{origin.source}:{heater}"

            def merge_t(rec: Dict[str, Any], det: Dict[str, Any]) -> None:
                d = rec.setdefault("details", {})
                d["target"] = det["target"]
                d["count"] = int(d.get("count", 1)) + 1
                d["last_script"] = det["last_script"]

            tgt = f"{target:g}°C" if target is not None else "?"
            self._add(
                "temperature", 2, f"Set {heater or 'heater'} to {tgt}",
                {"heater": heater, "target": target, "count": 1,
                 "last_script": script},
                origin=origin, coalesce_key=key, merge=merge_t,
            )
            return
        if not self.settings.get("record_gcode", True):
            return
        if GCODE_CMD_RE.match(first):
            self._add("gcode", 2, lines[0], {"script": script, "lines": len(lines)},
                      origin=origin)
        else:
            self._add("macro", 2, lines[0],
                      {"name": first, "params": " ".join(first_args),
                       "script": script, "lines": len(lines)},
                      origin=origin)

    def _on_prime_changed(self, value: int, reason: str) -> None:
        origin: Optional[Origin] = None
        if reason == "request":
            origin = self.resolve_origin(None)
        else:
            origin = SYSTEM_ORIGIN
        if value:
            self._add("prime_confirmed", 1, "Bed clear confirmed",
                      {"reason": reason}, origin=origin)
        else:
            self._add("prime_reset", 1, f"Prime reset ({reason})",
                      {"reason": reason}, origin=origin)

    def _on_fleet_worker(self, value: int) -> None:
        self._add(
            "fleet_worker_toggled", 1,
            f"Fleet worker {'enabled' if value else 'disabled'}",
            {"enabled": bool(value)}, origin=Origin("fleet_daemon"),
            dedupe_key="enabled", dedupe_window=SHORT_DEDUPE_WINDOW,
        )

    def _on_hs3_changed(
        self, key: Any, val: Any, old_val: Any, web_request: WebRequest
    ) -> None:
        if not isinstance(key, str):
            return
        origin = self.resolve_origin(web_request)
        if key == "filament_type":
            if val == old_val:
                return
            self._add(
                "filament_set", 1, f"Filament set: {old_val} → {val}",
                {"old_type": old_val, "new_type": val},
                origin=origin, dedupe_key="new_type",
                dedupe_window=FILAMENT_DEDUPE_WINDOW,
            )
        elif key in ("nozzle_size", "nozzle_type"):
            field = key
            det = {
                "nozzle_size": None, "nozzle_type": None,
                "prev_size": None, "prev_type": None,
            }
            if key == "nozzle_size":
                det["nozzle_size"], det["prev_size"] = val, old_val
            else:
                det["nozzle_type"], det["prev_type"] = val, old_val

            def merge_n(rec: Dict[str, Any], d: Dict[str, Any]) -> None:
                rd = rec.setdefault("details", {})
                for k in ("nozzle_size", "nozzle_type", "prev_size", "prev_type"):
                    if d.get(k) is not None and rd.get(k) is None:
                        rd[k] = d[k]
                    elif d.get(k) is not None and k in ("nozzle_size", "nozzle_type"):
                        rd[k] = d[k]
                rec["summary"] = _trim(self._nozzle_summary(rd), SUMMARY_MAX)

            self._add(
                "nozzle_set", 1, self._nozzle_summary(det), det,
                origin=origin, coalesce_key=f"nozzle:{origin.source}",
                merge=merge_n,
            )
        elif key in ("nozzle_life", "remaining_nozzle_life"):
            try:
                fval = float(val)
            except (TypeError, ValueError):
                fval = None
            self._add(
                "nozzle_life_reset", 1,
                f"Nozzle life set ({key}={fval})",
                {"nozzle_life": fval if key == "nozzle_life" else None,
                 "remaining_nozzle_life": fval if key == "remaining_nozzle_life" else fval,
                 "reset": key == "remaining_nozzle_life"},
                origin=origin, dedupe_key="remaining_nozzle_life",
                dedupe_window=SHORT_DEDUPE_WINDOW,
            )
        elif key == "is_fleet_worker":
            try:
                enabled = bool(int(val))
            except (TypeError, ValueError):
                return
            self._add(
                "fleet_worker_toggled", 1,
                f"Fleet worker {'enabled' if enabled else 'disabled'}",
                {"enabled": enabled}, origin=origin,
                dedupe_key="enabled", dedupe_window=SHORT_DEDUPE_WINDOW,
            )

    @staticmethod
    def _nozzle_summary(d: Dict[str, Any]) -> str:
        parts = []
        if d.get("nozzle_size") is not None:
            parts.append(f"{d['nozzle_size']} mm")
        if d.get("nozzle_type") is not None:
            parts.append(str(d["nozzle_type"]))
        return "Nozzle set: " + (" ".join(parts) if parts else "?")

    def _on_filament_changed(self, data: Dict[str, Any]) -> None:
        old_t, new_t = data.get("old_type"), data.get("new_type")
        if old_t == new_t:
            return
        self._add(
            "filament_set", 1, f"Filament set: {old_t} → {new_t}",
            {"old_type": old_t, "new_type": new_t,
             "spool_qr_code": data.get("spool_qr_code")},
            origin=self.resolve_origin(None), dedupe_key="new_type",
            dedupe_window=FILAMENT_DEDUPE_WINDOW,
        )

    def _on_spool_loaded(self, data: Dict[str, Any]) -> None:
        w = data.get("weight")
        w_s = f"{float(w):.0f} g " if w is not None else ""
        qr = data.get("qr_code")
        qr_s = f" (QR {qr})" if qr else ""
        self._add(
            "spool_loaded", 1,
            f"Spool loaded: {w_s}{data.get('filament_type')}{qr_s}",
            {"filament_type": data.get("filament_type"), "weight": w,
             "qr_code": qr, "type_changed": bool(data.get("type_changed"))},
            origin=self.resolve_origin(None),
        )

    def _on_nozzle_life(self, data: Dict[str, Any]) -> None:
        life = data.get("nozzle_life")
        self._add(
            "nozzle_life_reset", 1,
            f"Nozzle life {'reset' if data.get('reset') else 'set'} ({life} kg)",
            {"nozzle_life": life,
             "remaining_nozzle_life": data.get("remaining_nozzle_life"),
             "reset": bool(data.get("reset"))},
            origin=self.resolve_origin(None), dedupe_key="remaining_nozzle_life",
            dedupe_window=SHORT_DEDUPE_WINDOW,
        )

    def _on_meters(self, data: Dict[str, Any]) -> None:
        origin = self.resolve_origin(None)
        odo = data.get("odometer") or {}
        trip = data.get("tripmeter_reset") or []
        if odo:
            self._add("odometer_set", 2,
                      "Odometer set: " + ", ".join(f"{k}={v}" for k, v in odo.items()),
                      {"odometer": odo}, origin=origin)
        if trip:
            self._add("tripmeter_reset", 2,
                      "Tripmeter reset: " + ", ".join(trip),
                      {"axes": trip}, origin=origin)

    def _on_fleet_download(
        self, filename: str, start_print: bool, cached: bool
    ) -> None:
        self._add(
            "fleet_download", 2,
            f"Fleet {'download & print' if start_print else 'download'}: {filename}",
            {"filename": filename, "start_print": bool(start_print),
             "cached": bool(cached)},
            origin=self.resolve_origin(None), filename=filename,
        )

    def _on_filelist(self, info: Dict[str, Any]) -> None:
        item = info.get("item") or {}
        if item.get("root") != "gcodes":
            return
        action = info.get("action")
        path = item.get("path")
        if action == "create_file":
            etype, summary = "file_uploaded", f"File added: {path}"
        elif action == "delete_file":
            etype, summary = "file_deleted", f"File deleted: {path}"
        elif action == "move_file":
            etype, summary = "file_moved", f"File moved: {path}"
        else:
            return
        details: Dict[str, Any] = {"path": path, "action": action}
        src = info.get("source_item")
        if src:
            details["source_path"] = src.get("path")
        self._add(etype, 2, summary, details, origin=self.resolve_origin(None),
                  filename=path)

    def _on_klippy_shutdown(self) -> None:
        kconn: KlippyConnection = self.server.lookup_component("klippy_connection")
        msg = kconn.state_message
        self._add("klippy_shutdown", 1, f"Klipper shutdown: {_trim(msg, 100)}",
                  {"message": msg}, origin=SYSTEM_ORIGIN)

    def _on_klippy_started(self, state: KlippyState) -> None:
        if state == KlippyState.ERROR:
            kconn: KlippyConnection = self.server.lookup_component(
                "klippy_connection")
            msg = kconn.state_message
            self._add("klippy_error", 1, f"Klipper error: {_trim(msg, 100)}",
                      {"message": msg}, origin=SYSTEM_ORIGIN)

    def _on_klippy_ready(self) -> None:
        self._add("klippy_ready", 2, "Klipper ready", {}, origin=SYSTEM_ORIGIN)

    # ---------------------------------------------------------- endpoints
    def _require_ready(self) -> None:
        if not self._ready:
            raise self.server.error("Activity log initializing", 503)

    @staticmethod
    def _csv(web_request: WebRequest, name: str) -> Optional[Set[str]]:
        raw = web_request.get(name, None)
        if raw is None:
            return None
        if isinstance(raw, (list, tuple)):
            vals = [str(v) for v in raw]
        else:
            vals = str(raw).split(",")
        out = {v.strip() for v in vals if v.strip()}
        return out or None

    async def _handle_list(self, web_request: WebRequest) -> Dict[str, Any]:
        self._require_ready()
        since_seq = web_request.get_int("since_seq", None)
        before_seq = web_request.get_int("before_seq", None)
        tiers_raw = self._csv(web_request, "tier")
        tiers: Optional[Set[int]] = None
        if tiers_raw:
            try:
                tiers = {int(t) for t in tiers_raw}
            except ValueError:
                raise self.server.error("Invalid 'tier'", 400)
        types = self._csv(web_request, "types")
        exclude = self._csv(web_request, "exclude_types")
        sources = self._csv(web_request, "source")
        job_id = web_request.get_str("job_id", None)
        after = web_request.get_float("after", None)
        before = web_request.get_float("before", None)
        sync_mode = since_seq is not None
        include_deleted = web_request.get_boolean("include_deleted", sync_mode)
        sort = web_request.get_str("sort", "seq").lower()
        order = web_request.get_str(
            "order", "asc" if sync_mode else "desc").lower()
        start = max(0, web_request.get_int("start", 0))
        limit = web_request.get_int("limit", LIST_LIMIT_DEFAULT)
        if limit <= 0 or limit > LIST_LIMIT_MAX:
            limit = LIST_LIMIT_MAX
        if sort not in ("seq", "ts") or order not in ("asc", "desc"):
            raise self.server.error("Invalid 'sort' or 'order'", 400)

        entries = self._index
        if since_seq is not None:
            entries = entries[bisect.bisect_right(self._seqs, since_seq):]
        if before_seq is not None:
            hi = bisect.bisect_left(self._seqs, before_seq)
            entries = entries[:hi] if since_seq is None else [
                e for e in entries if e.seq < before_seq
            ]
        filtered: List[IdxEntry] = []
        for e in entries:
            if tiers is not None and e.tier not in tiers:
                continue
            if types is not None and e.type not in types:
                continue
            if exclude is not None and e.type in exclude:
                continue
            if sources is not None and e.source not in sources:
                continue
            if job_id is not None and e.job_id != job_id:
                continue
            if after is not None and e.ts < after:
                continue
            if before is not None and e.ts >= before:
                continue
            if not include_deleted and e.deleted:
                continue
            filtered.append(e)
        if sort == "ts":
            filtered.sort(key=lambda e: (e.ts, e.seq))
        if order == "desc":
            filtered.reverse()
        total = len(filtered)
        page = filtered[start:start + limit]
        records = await self._get_records([e.seq for e in page])
        next_since = since_seq if since_seq is not None else None
        if sync_mode and page:
            next_since = max(e.seq for e in page)
        return {
            "events": records,
            "count": len(records),
            "total": total,
            "max_seq": self._seqs[-1] if self._seqs else 0,
            "min_seq": self._seqs[0] if self._seqs else 0,
            "epoch": self.meta.get("epoch"),
            "has_more": start + limit < total,
            "next_since_seq": next_since,
        }

    async def _handle_info(self, web_request: WebRequest) -> Dict[str, Any]:
        counts: Dict[str, int] = {"1": 0, "2": 0}
        for e in self._index:
            counts[str(e.tier)] = counts.get(str(e.tier), 0) + 1
        return {
            "ready": self._ready,
            "epoch": self.meta.get("epoch"),
            "max_seq": self._seqs[-1] if self._seqs else 0,
            "min_seq": self._seqs[0] if self._seqs else 0,
            "total": len(self._index),
            "tier_counts": counts,
            "settings": dict(self.settings),
            "service_types": [
                {"id": sid, "label": label} for sid, label in SERVICE_TYPES
            ],
            "daemon_ips": [str(i) for i in self._daemon_ips],
            "pending": len(self._pending),
            "open_job_id": self.meta.get("open_job_id"),
        }

    async def _handle_service_types(
        self, web_request: WebRequest
    ) -> Dict[str, Any]:
        return {
            "service_types": [
                {"id": sid, "label": label} for sid, label in SERVICE_TYPES
            ]
        }

    def _parse_service_fields(
        self, web_request: WebRequest, existing: Optional[Dict[str, Any]] = None
    ) -> Tuple[Dict[str, Any], float]:
        cur = dict((existing or {}).get("details", {}))
        stype = web_request.get_str("service_type", cur.get("service_type"))
        if stype not in SERVICE_TYPE_IDS:
            raise self.server.error(
                f"Invalid service_type '{stype}'; one of "
                f"{sorted(SERVICE_TYPE_IDS)}", 400)
        other = web_request.get_str(
            "service_type_other", cur.get("service_type_other") or "")
        other = _trim(other.strip(), 120)
        if stype == "other" and not other:
            raise self.server.error(
                "service_type_other is required when service_type is 'other'",
                400)
        if stype != "other":
            other = ""
        operator = _trim(
            web_request.get_str("operator", cur.get("operator") or "").strip(),
            80)
        comment = _trim(
            web_request.get_str("comment", cur.get("comment") or "").strip(),
            2000)
        now = time.time()
        default_ts = float((existing or {}).get("ts") or now)
        service_time = web_request.get_float("service_time", default_ts)
        if service_time > now + SERVICE_TIME_FUTURE_SLACK:
            raise self.server.error("service_time is in the future", 400)
        details = {
            "service_type": stype,
            "service_type_label": SERVICE_TYPE_LABELS[stype],
            "service_type_other": other,
            "operator": operator,
            "comment": comment,
        }
        return details, float(service_time)

    @staticmethod
    def _service_summary(details: Dict[str, Any]) -> str:
        label = details.get("service_type_label") or details.get("service_type")
        if details.get("service_type") == "other" and details.get("service_type_other"):
            label = details["service_type_other"]
        op = details.get("operator")
        return f"Service: {label}" + (f" - {op}" if op else "")

    async def _handle_service(self, web_request: WebRequest) -> Dict[str, Any]:
        self._require_ready()
        async with self._service_lock:
            if web_request.get_request_type() == RequestType.DELETE:
                rec = await self._find_service(web_request.get_str("id"))
                self._reseq(rec, deleted=True)
                await self._flush_now()
                self._notify("deleted", rec)
                return {"event": rec}
            details, service_time = self._parse_service_fields(web_request)
            rec = self._add(
                "service", 1, self._service_summary(details), details,
                web_request=web_request, ts=service_time,
            )
            await self._flush_now()
            return {"event": rec}

    async def _handle_service_update(
        self, web_request: WebRequest
    ) -> Dict[str, Any]:
        self._require_ready()
        async with self._service_lock:
            rec = await self._find_service(web_request.get_str("id"))
            details, service_time = self._parse_service_fields(web_request, rec)
            self._reseq(
                rec, details=details, ts=service_time,
                summary=_trim(self._service_summary(details), SUMMARY_MAX),
            )
            await self._flush_now()
            self._notify("updated", rec)
            return {"event": rec}

    async def _find_service(self, rid: str) -> Dict[str, Any]:
        seq = self._by_id.get(rid)
        rec = await self._get_record(seq) if seq is not None else None
        if rec is None or rec.get("type") != "service" or rec.get("deleted"):
            raise self.server.error(f"Service event '{rid}' not found", 404)
        # Make sure we mutate the cached instance
        self._recent_by_seq[seq] = rec  # type: ignore[index]
        return rec

    async def _handle_settings(self, web_request: WebRequest) -> Dict[str, Any]:
        if web_request.get_request_type() == RequestType.POST:
            new = dict(self.settings)
            rd = web_request.get_int("retention_days", None)
            if rd is not None:
                if rd < 1 or rd > 3650:
                    raise self.server.error("retention_days must be 1..3650", 400)
                new["retention_days"] = rd
            rg = web_request.get_boolean("record_gcode", None)
            if rg is not None:
                new["record_gcode"] = bool(rg)
            cw = web_request.get_float("coalesce_window", None)
            if cw is not None:
                if cw < 0 or cw > 60:
                    raise self.server.error("coalesce_window must be 0..60", 400)
                new["coalesce_window"] = float(cw)
            decreased = new["retention_days"] < self.settings["retention_days"]
            self.settings = new
            await self.db.insert_item("moonraker", SETTINGS_KEY, self.settings)
            self.server.send_event("activity:settings_changed", dict(self.settings))
            if decreased and self._ready:
                self.eventloop.register_callback(self._prune)
        return {"settings": dict(self.settings)}

    async def _handle_client_event(
        self, web_request: WebRequest
    ) -> Dict[str, Any]:
        self._require_ready()
        etype = web_request.get_str("type")
        if not CLIENT_EVENT_RE.match(etype):
            raise self.server.error(
                "type must match ^client\\.[a-z0-9_]{1,40}$", 400)
        summary = web_request.get_str("summary", etype)
        details = web_request.get("details", {})
        if not isinstance(details, dict):
            raise self.server.error("details must be an object", 400)
        ts = web_request.get_float("ts", None)
        rec = self._add(etype, 2, summary, details, web_request=web_request,
                        ts=ts)
        return {"event": rec}

    async def _handle_prune(self, web_request: WebRequest) -> Dict[str, Any]:
        self._require_ready()
        deleted = await self._prune()
        return {"deleted": deleted}

    # ------------------------------------------------------------- exit
    async def on_exit(self) -> None:
        self._ready = self._ready  # keep recording during shutdown
        self._add("moonraker_exit", 2, "Moonraker stopping", {},
                  origin=SYSTEM_ORIGIN)
        if self._prune_timer is not None:
            self._prune_timer.stop()
        if self._flush_handle is not None:
            self._flush_handle.cancel()
            self._flush_handle = None
        if self._pending or self._pending_deletes:
            if self.server.is_running():
                await self._flush()
            else:
                self._flush_sync()


def load_component(config: ConfigHelper) -> Activity:
    return Activity(config)
