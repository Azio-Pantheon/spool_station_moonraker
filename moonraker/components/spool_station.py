# Spool Registration Station Component
#
# Turns this Moonraker instance into a dedicated spool registration
# station.  A USB QR scanner (via KlipperScreen) delivers spool QR codes
# and a phone with the station Mainsail page delivers batch numbers; this
# component owns the small state machine between the two scans, talks to
# fleet_daemon (QR availability check, spool creation) and pushes
# `notify_spool_station_status` to every client (KlipperScreen station
# panel, station Mainsail page).
#
# fleet_daemon is authoritative for spools and filaments; the station only
# caches the filament list so the picker is instant.  The pending QR lives
# in memory only and expires after `qr_timeout` seconds.
#
# The station never depends on Klipper: no `server:klippy_ready` handler,
# no klippy_apis lookup.
#
# Configuration (moonraker.conf):
#   [spool_station]
#   fleet_daemon_url: http://pantheonfleet.local:8090
#   # station_hostname: auto (socket.gethostname() + .local)
#   qr_timeout: 120          # seconds a scanned QR waits for its batch
#   scan_debounce: 0.5       # same code+source within this window is ignored
#   fleet_poll_interval: 30  # seconds between filament list refreshes

from __future__ import annotations
import asyncio
import logging
import math
import socket
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from ..common import RequestType
from ..utils import json_wrapper as jsonw
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Tuple,
)

if TYPE_CHECKING:
    from ..confighelper import ConfigHelper
    from ..common import WebRequest
    from .database import MoonrakerDatabase
    from .http_client import HttpClient, HttpResponse

DB_NAMESPACE = "spool_station"
DB_KEY_FILAMENT_ID = "filament_id"
DB_KEY_FLEET_URL = "fleet_daemon_url"

DEFAULT_FLEET_URL = "http://pantheonfleet.local:8090"
HTTP_TIMEOUT = 10.

STATE_IDLE = "idle"
STATE_AWAITING_BATCH = "awaiting_batch"
STATE_REGISTERING = "registering"

SOURCE_SCANNER = "scanner"
SOURCE_PHONE = "phone"

RESULT_QR = "qr"
RESULT_BATCH = "batch"
RESULT_REGISTER = "register"

FILAMENT_KEYS = ("id", "name", "material", "vendor_name", "color_hex")


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SpoolStation:
    def __init__(self, config: ConfigHelper) -> None:
        self.server = config.get_server()
        self.eventloop = self.server.get_event_loop()

        # Configuration (every option read with a default so
        # validate_config never warns about a bare [spool_station] section)
        conf_url: str = config.get("fleet_daemon_url", DEFAULT_FLEET_URL)
        self._conf_fleet_url = conf_url.strip().rstrip("/") or DEFAULT_FLEET_URL
        hostname: str = config.get(
            "station_hostname", socket.gethostname()
        ).strip().lower()
        if not hostname.endswith(".local"):
            hostname += ".local"
        self.station_hostname = hostname
        self.qr_timeout = config.getfloat("qr_timeout", default=120., minval=5.)
        self.scan_debounce = config.getfloat(
            "scan_debounce", default=.5, minval=0.
        )
        self.fleet_poll_interval = config.getint(
            "fleet_poll_interval", default=30, minval=5
        )

        # Persisted state
        self.database: MoonrakerDatabase = self.server.lookup_component(
            "database"
        )
        self.fleet_url: str = self._conf_fleet_url
        self.fleet_url_source: str = "config"
        self._filament_id: Optional[int] = None
        self._load_from_database()

        # Runtime state
        self._state: str = STATE_IDLE
        self._pending_qr: Optional[str] = None
        self._qr_scanned_at: Optional[str] = None
        self._qr_expires_at: Optional[str] = None
        self._qr_expires_mono: Optional[float] = None
        self._last_result: Optional[Dict[str, Any]] = None
        self._filaments: List[Dict[str, Any]] = []
        self._fleet_connected = False
        self._last_scan: Optional[Tuple[str, str, float]] = None
        self._last_poll_state: Optional[Tuple[bool, int]] = None
        self._scan_lock = asyncio.Lock()
        self._expire_handle: Optional[asyncio.TimerHandle] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._is_closing = False

        # Endpoints.  Single-method paths become server.spool_station.<leaf>;
        # dual-method paths become server.spool_station.get_<leaf> /
        # post_<leaf>.
        self.server.register_endpoint(
            "/server/spool_station/status", RequestType.GET,
            self._handle_status
        )
        self.server.register_endpoint(
            "/server/spool_station/scan", RequestType.POST,
            self._handle_scan
        )
        self.server.register_endpoint(
            "/server/spool_station/cancel", RequestType.POST,
            self._handle_cancel
        )
        self.server.register_endpoint(
            "/server/spool_station/filament",
            RequestType.GET | RequestType.POST,
            self._handle_filament
        )
        self.server.register_endpoint(
            "/server/spool_station/filaments", RequestType.GET,
            self._handle_filaments
        )
        self.server.register_endpoint(
            "/server/spool_station/config",
            RequestType.GET | RequestType.POST,
            self._handle_config
        )

        # Notification: notify_spool_station_status, params[0] = status dict
        self.server.register_notification(
            "spool_station:status", "spool_station_status"
        )

    async def component_init(self) -> None:
        self.http_client: HttpClient = self.server.lookup_component(
            "http_client"
        )
        self._poll_task = self.eventloop.create_task(self._poll_loop())
        logging.info(
            f"[SpoolStation] Component initialized: {self.fleet_url} "
            f"({self.fleet_url_source}), hostname={self.station_hostname}, "
            f"filament_id={self._filament_id}, "
            f"qr_timeout={self.qr_timeout:g}s"
        )

    # ------------------------------------------------------------------
    # Persistence (Moonraker DB, namespace "spool_station")
    # ------------------------------------------------------------------

    def _load_from_database(self) -> None:
        try:
            url = self.database.get_item(
                DB_NAMESPACE, DB_KEY_FLEET_URL, None
            ).result()
            if isinstance(url, str):
                url = url.strip().rstrip("/")
                if url.startswith(("http://", "https://")):
                    self.fleet_url = url
                    self.fleet_url_source = "database"
        except Exception as e:
            logging.warning(
                f"[SpoolStation] Failed to load fleet_daemon_url from "
                f"database: {e}"
            )
        try:
            fid = self.database.get_item(
                DB_NAMESPACE, DB_KEY_FILAMENT_ID, None
            ).result()
            if isinstance(fid, int) and not isinstance(fid, bool):
                self._filament_id = fid
            elif isinstance(fid, str) and fid.strip().isdigit():
                self._filament_id = int(fid.strip())
        except Exception as e:
            logging.warning(
                f"[SpoolStation] Failed to load filament_id from database: {e}"
            )

    async def _persist(self, key: str, value: Any) -> None:
        try:
            await self.database.insert_item(DB_NAMESPACE, key, value)
        except Exception as e:
            logging.warning(f"[SpoolStation] Failed to persist '{key}': {e}")

    # ------------------------------------------------------------------
    # Payload shaping
    # ------------------------------------------------------------------

    def _find_filament(self, filament_id: Optional[int]) -> Optional[Dict[str, Any]]:
        if filament_id is None:
            return None
        for item in self._filaments:
            if item.get("id") == filament_id:
                return item
        return None

    def _filament_summary(self) -> Optional[Dict[str, Any]]:
        item = self._find_filament(self._filament_id)
        if item is None:
            return None
        return {key: item.get(key) for key in FILAMENT_KEYS}

    def _qr_remaining(self) -> int:
        if self._pending_qr is None or self._qr_expires_mono is None:
            return 0
        remaining = self._qr_expires_mono - time.monotonic()
        if remaining <= 0:
            return 0
        return int(math.ceil(remaining))

    def _status_payload(self) -> Dict[str, Any]:
        return {
            "state": self._state,
            "pending_qr": self._pending_qr,
            "qr_scanned_at": self._qr_scanned_at,
            "qr_expires_at": self._qr_expires_at,
            "qr_remaining": self._qr_remaining(),
            "qr_timeout": self.qr_timeout,
            "filament_id": self._filament_id,
            "filament": self._filament_summary(),
            "last_result": (
                dict(self._last_result) if self._last_result is not None else None
            ),
            "fleet_connected": self._fleet_connected,
            "fleet_daemon_url": self.fleet_url,
            "station_hostname": self.station_hostname,
            "filaments_count": len(self._filaments),
        }

    def _config_payload(self) -> Dict[str, Any]:
        return {
            "fleet_daemon_url": self.fleet_url,
            "fleet_daemon_url_source": self.fleet_url_source,
            "station_hostname": self.station_hostname,
            "qr_timeout": self.qr_timeout,
            "scan_debounce": self.scan_debounce,
            "fleet_poll_interval": self.fleet_poll_interval,
        }

    def _set_result(
        self,
        ok: bool,
        kind: str,
        message: str,
        spool: Optional[Dict[str, Any]] = None
    ) -> None:
        self._last_result = {
            "ok": ok,
            "kind": kind,
            "message": message,
            "spool": spool,
            "at": _utcnow_iso(),
        }
        level = logging.INFO if ok else logging.WARNING
        logging.log(level, f"[SpoolStation] {kind}: {message}")

    # ------------------------------------------------------------------
    # Notifications
    # ------------------------------------------------------------------

    def _notify_status(self) -> None:
        self._last_poll_state = (self._fleet_connected, len(self._filaments))
        self.server.send_event("spool_station:status", self._status_payload())

    def _notify_if_fleet_changed(self) -> None:
        """Poll-loop notification: only when fleet_connected or the
        filament count changed since the last push."""
        state = (self._fleet_connected, len(self._filaments))
        if state == self._last_poll_state:
            return
        self._notify_status()

    def _set_fleet_connected(self, connected: bool) -> None:
        if connected != self._fleet_connected:
            logging.info(
                f"[SpoolStation] fleet_daemon "
                f"{'reachable' if connected else 'unreachable'}"
            )
        self._fleet_connected = connected

    # ------------------------------------------------------------------
    # fleet_daemon HTTP
    # ------------------------------------------------------------------

    async def _fleet_request(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]] = None
    ) -> Optional[HttpResponse]:
        """Issue a request to fleet_daemon.  Returns None when the daemon
        could not be reached (connection / timeout / transport error);
        HTTP error statuses are returned to the caller.  Updates the
        `fleet_connected` flag."""
        url = f"{self.fleet_url}{path}"
        try:
            resp = await self.http_client.request(
                method, url, body=body, request_timeout=HTTP_TIMEOUT
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.debug(f"[SpoolStation] {method} {path} failed: {e}")
            self._set_fleet_connected(False)
            return None
        if resp.status_code >= 500 and resp.error is not None and not resp.content:
            # Tornado transport-level failure (connection refused, timeout)
            # surfaces as a synthetic 5xx with no body.
            logging.debug(
                f"[SpoolStation] {method} {path} unreachable: {resp.error}"
            )
            self._set_fleet_connected(False)
            return None
        self._set_fleet_connected(True)
        return resp

    def _error_detail(self, resp: HttpResponse, fallback: str) -> str:
        """Pull a human-readable message out of a fleet_daemon (FastAPI)
        error body: {"detail": "..."} or {"detail": {...}}."""
        try:
            data = resp.json()
        except Exception:
            text = resp.text.strip()
            return text or fallback
        if isinstance(data, dict):
            detail = data.get("detail", data.get("error", data.get("message")))
            if isinstance(detail, str) and detail:
                return detail
            if isinstance(detail, dict):
                msg = detail.get("message") or detail.get("detail")
                if isinstance(msg, str) and msg:
                    return msg
                return jsonw.dumps(detail).decode()
            if isinstance(detail, list) and detail:
                parts: List[str] = []
                for item in detail:
                    if isinstance(item, dict):
                        parts.append(str(item.get("msg", item)))
                    else:
                        parts.append(str(item))
                return "; ".join(parts)
        return fallback

    def _parse_response_json(self, resp: HttpResponse) -> Dict[str, Any]:
        try:
            data = resp.json()
        except Exception as e:
            raise self.server.error(
                f"Invalid response from fleet_daemon: {e}", 502
            )
        if not isinstance(data, dict):
            raise self.server.error(
                "Invalid response from fleet_daemon: expected an object", 502
            )
        return data

    # ------------------------------------------------------------------
    # Filament cache / poll loop
    # ------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        # Refresh right away so the picker is populated at boot.
        while not self._is_closing:
            try:
                await self._refresh_filaments()
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("[SpoolStation] Filament refresh failed")
            self._notify_if_fleet_changed()
            await asyncio.sleep(self.fleet_poll_interval)

    async def _refresh_filaments(self) -> bool:
        """GET /spool/filaments into the cache.  Returns True when
        fleet_daemon was reachable."""
        resp = await self._fleet_request("GET", "/spool/filaments")
        if resp is None:
            return False
        if resp.status_code != 200:
            logging.warning(
                f"[SpoolStation] Failed to fetch filaments: "
                f"HTTP {resp.status_code} {self._error_detail(resp, '')}"
            )
            return True
        try:
            data = resp.json()
        except Exception as e:
            logging.warning(f"[SpoolStation] Invalid filament response: {e}")
            return True
        raw_list: Any = data
        if isinstance(data, dict):
            raw_list = data.get("filaments", data.get("items", []))
        if not isinstance(raw_list, list):
            logging.warning("[SpoolStation] Invalid filament response: no list")
            return True
        filaments: List[Dict[str, Any]] = []
        for raw in raw_list:
            if not isinstance(raw, dict):
                continue
            fid = raw.get("id")
            if isinstance(fid, bool) or not isinstance(fid, int):
                continue
            filaments.append(dict(raw))
        if filaments != self._filaments:
            self._filaments = filaments
            logging.info(
                f"[SpoolStation] Filament cache refreshed: {len(filaments)} items"
            )
        return True

    # ------------------------------------------------------------------
    # Pending QR state machine
    # ------------------------------------------------------------------

    def _cancel_expiry(self) -> None:
        if self._expire_handle is not None:
            self._expire_handle.cancel()
            self._expire_handle = None

    def _schedule_expiry(self, delay: float, qr_code: str) -> None:
        self._cancel_expiry()
        if self._is_closing:
            return
        self._expire_handle = self.eventloop.delay_callback(
            max(delay, 0.), self._expire_pending, qr_code
        )

    def _arm_qr(self, qr_code: str) -> None:
        now = datetime.now(timezone.utc)
        expires = now + timedelta(seconds=self.qr_timeout)
        self._pending_qr = qr_code
        self._qr_scanned_at = now.isoformat()
        self._qr_expires_at = expires.isoformat()
        self._qr_expires_mono = time.monotonic() + self.qr_timeout
        self._state = STATE_AWAITING_BATCH
        self._schedule_expiry(self.qr_timeout, qr_code)

    def _clear_pending(self) -> None:
        self._cancel_expiry()
        self._pending_qr = None
        self._qr_scanned_at = None
        self._qr_expires_at = None
        self._qr_expires_mono = None
        self._state = STATE_IDLE

    async def _expire_pending(self, qr_code: str) -> None:
        async with self._scan_lock:
            self._expire_handle = None
            if (
                self._state != STATE_AWAITING_BATCH
                or self._pending_qr != qr_code
            ):
                return
            self._clear_pending()
            self._set_result(False, RESULT_QR, f"QR {qr_code} expired")
        self._notify_status()

    # ------------------------------------------------------------------
    # Scan processing
    # ------------------------------------------------------------------

    def _is_debounced(self, code: str, source: str) -> bool:
        now = time.monotonic()
        last = self._last_scan
        self._last_scan = (code, source, now)
        if last is None or self.scan_debounce <= 0:
            return False
        last_code, last_source, last_at = last
        return (
            last_code == code
            and last_source == source
            and (now - last_at) < self.scan_debounce
        )

    async def _process_qr(self, code: str) -> None:
        """source=scanner: the code is a spool QR.  Validate it against
        fleet_daemon and arm it (replacing any earlier pending QR)."""
        if not code:
            self._set_result(False, RESULT_QR, "Empty QR code")
            return
        if self._state == STATE_REGISTERING:
            self._set_result(False, RESULT_QR, "Registration in progress")
            return
        if self._filament_id is None:
            self._set_result(
                False, RESULT_QR, "Select a filament on the station page first"
            )
            return
        path = f"/spool/qr-available/{quote(code, safe='')}"
        resp = await self._fleet_request("GET", path)
        if resp is None:
            self._clear_pending()
            self._set_result(False, RESULT_QR, "fleet_daemon unreachable")
            return
        if resp.status_code != 200:
            self._clear_pending()
            detail = self._error_detail(
                resp, f"QR check failed: HTTP {resp.status_code}"
            )
            self._set_result(False, RESULT_QR, detail)
            return
        self._arm_qr(code)
        self._set_result(
            True, RESULT_QR,
            f"QR {code} armed, scan the batch number"
        )

    @staticmethod
    def _extract_batch(code: str) -> str:
        """Batch labels read `Batch No:12345`; keep the text after the
        first ':'.  Plain codes pass through."""
        text = code.strip()
        if ":" in text:
            text = text.split(":", 1)[1]
        return text.strip()

    async def _process_batch(self, code: str) -> None:
        """source=phone: the code is the batch number.  Register the
        pending QR with fleet_daemon."""
        lot_nr = self._extract_batch(code)
        if not lot_nr:
            self._set_result(False, RESULT_BATCH, "Empty batch number")
            return
        if self._state == STATE_REGISTERING:
            self._set_result(False, RESULT_BATCH, "Registration in progress")
            return
        qr_code = self._pending_qr
        if qr_code is None or self._state != STATE_AWAITING_BATCH:
            self._set_result(False, RESULT_BATCH, "Scan the spool QR first")
            return
        filament_id = self._filament_id
        if filament_id is None:
            self._set_result(
                False, RESULT_BATCH,
                "Select a filament on the station page first"
            )
            return
        self._state = STATE_REGISTERING
        self._notify_status()
        body: Dict[str, Any] = {
            "filament_id": filament_id,
            "qr_code": qr_code,
            "lot_nr": lot_nr,
            "used_weight": 0,
        }
        resp = await self._fleet_request("POST", "/spool/spools", body)
        if resp is None:
            # Keep the QR armed so the batch can simply be rescanned once
            # fleet_daemon is back.  Re-arm the expiry in case the timer
            # fired (as a no-op) while we were registering.
            self._state = STATE_AWAITING_BATCH
            remaining = self._qr_remaining()
            self._schedule_expiry(float(remaining), qr_code)
            self._set_result(
                False, RESULT_REGISTER,
                "fleet_daemon unreachable, scan the batch again"
            )
            return
        if resp.status_code not in (200, 201):
            self._clear_pending()
            detail = self._error_detail(
                resp, f"Registration failed: HTTP {resp.status_code}"
            )
            self._set_result(False, RESULT_REGISTER, detail)
            return
        spool: Optional[Dict[str, Any]] = None
        try:
            data = resp.json()
        except Exception as e:
            logging.warning(f"[SpoolStation] Invalid create_spool response: {e}")
        else:
            if isinstance(data, dict):
                spool = data
        spool_id: Any = spool.get("id") if spool is not None else None
        self._clear_pending()
        message = (
            f"Registered spool #{spool_id}" if spool_id is not None
            else "Registered spool"
        )
        self._set_result(True, RESULT_REGISTER, message, spool)

    async def _process_scan(self, code: str, source: str) -> bool:
        """Run one scan through the state machine.  Returns False when the
        scan was ignored (debounce)."""
        async with self._scan_lock:
            if self._is_debounced(code, source):
                logging.debug(
                    f"[SpoolStation] Debounced {source} scan: {code!r}"
                )
                return False
            if source == SOURCE_SCANNER:
                await self._process_qr(code)
            else:
                await self._process_batch(code)
        return True

    # ------------------------------------------------------------------
    # API handlers
    # ------------------------------------------------------------------

    async def _handle_status(self, web_request: WebRequest) -> Dict[str, Any]:
        return self._status_payload()

    async def _handle_scan(self, web_request: WebRequest) -> Dict[str, Any]:
        code = web_request.get_str("code").strip()
        source = web_request.get_str("source").strip().lower()
        if source not in (SOURCE_SCANNER, SOURCE_PHONE):
            raise self.server.error(
                f"Invalid source '{source}', expected "
                f"'{SOURCE_SCANNER}' or '{SOURCE_PHONE}'",
                400
            )
        if len(code) > 512:
            raise self.server.error("code is too long (max 512 chars)", 400)
        processed = await self._process_scan(code, source)
        if processed:
            self._notify_status()
        return self._status_payload()

    async def _handle_cancel(self, web_request: WebRequest) -> Dict[str, Any]:
        async with self._scan_lock:
            if self._state == STATE_REGISTERING:
                # The create request is in flight; it settles on its own.
                return self._status_payload()
            self._clear_pending()
            self._last_result = None
            logging.info("[SpoolStation] Pending QR cancelled")
        self._notify_status()
        return self._status_payload()

    async def _handle_filament(self, web_request: WebRequest) -> Dict[str, Any]:
        if web_request.get_action() != "POST":
            return {
                "filament_id": self._filament_id,
                "filament": self._filament_summary(),
            }
        raw: Any = web_request.get("filament_id")
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            self._filament_id = None
            await self._persist(DB_KEY_FILAMENT_ID, None)
            logging.info("[SpoolStation] Filament cleared")
            self._notify_status()
            return self._status_payload()
        if isinstance(raw, bool):
            raise self.server.error("filament_id must be an integer", 400)
        try:
            filament_id = int(raw)
        except (TypeError, ValueError):
            raise self.server.error(
                f"filament_id must be an integer, got {raw!r}", 400
            )
        if self._find_filament(filament_id) is None:
            await self._refresh_filaments()
        if self._find_filament(filament_id) is None:
            if not self._filaments:
                raise self.server.error(
                    "Filament list unavailable (fleet_daemon unreachable)", 503
                )
            raise self.server.error(f"Unknown filament id {filament_id}", 400)
        self._filament_id = filament_id
        await self._persist(DB_KEY_FILAMENT_ID, filament_id)
        logging.info(f"[SpoolStation] Filament selected: {filament_id}")
        self._notify_status()
        return self._status_payload()

    async def _handle_filaments(self, web_request: WebRequest) -> Dict[str, Any]:
        refresh = web_request.get_int("refresh", 0)
        if refresh:
            await self._refresh_filaments()
            self._notify_if_fleet_changed()
        return {
            "filaments": [dict(item) for item in self._filaments],
            "fleet_connected": self._fleet_connected,
            "count": len(self._filaments),
        }

    async def _handle_config(self, web_request: WebRequest) -> Dict[str, Any]:
        if web_request.get_action() != "POST":
            return self._config_payload()
        url = web_request.get_str("fleet_daemon_url").strip().rstrip("/")
        if not url.startswith(("http://", "https://")):
            raise self.server.error(
                "fleet_daemon_url must start with http:// or https://", 400
            )
        self.fleet_url = url
        self.fleet_url_source = "database"
        await self._persist(DB_KEY_FLEET_URL, url)
        logging.info(f"[SpoolStation] fleet_daemon_url set to {url}")
        # Re-probe the daemon so fleet_connected / the picker reflect the
        # new target right away.
        await self._refresh_filaments()
        self._notify_status()
        return self._config_payload()

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    async def close(self) -> None:
        self._is_closing = True
        self._cancel_expiry()
        if self._poll_task is not None:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except (asyncio.CancelledError, Exception):
                pass
            self._poll_task = None
        logging.info("[SpoolStation] Component shut down")


def load_component(config: ConfigHelper) -> SpoolStation:
    return SpoolStation(config)
