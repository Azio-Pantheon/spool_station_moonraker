# Fleet Daemon Integration
#
# Connects to a fleet_daemon instance to provide fleet-wide gcode
# file availability and download-and-print functionality.
#
# Configuration (moonraker.conf):
#   [fleet_integration]
#   fleet_daemon_url: http://fleet_daemon_host:8090
#   printer_hostname: this_printer.local
#   poll_interval: 300

from __future__ import annotations
import asyncio
import logging
import os
import re
from pathlib import Path
from ..common import RequestType
from ..utils import json_wrapper as jsonw
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Any,
    Optional,
    Union,
)

if TYPE_CHECKING:
    from ..confighelper import ConfigHelper
    from ..common import WebRequest
    from .http_client import HttpClient
    from .klippy_apis import KlippyAPI as APIComp
    from .file_manager.file_manager import FileManager
    from .klippy_connection import KlippyConnection

FLEET_SUBDIR = "fleet_gcodes"

class FleetIntegration:
    def __init__(self, config: ConfigHelper) -> None:
        self.server = config.get_server()
        self.eventloop = self.server.get_event_loop()

        # Configuration
        fleet_url = config.get("fleet_daemon_url")
        fleet_url = fleet_url.rstrip("/")
        self.fleet_url = fleet_url
        import socket as _socket
        default_hostname = _socket.gethostname().lower()
        hostname = config.get("printer_hostname", default_hostname).lower()
        if not hostname.endswith('.local'):
            hostname += '.local'
        self.printer_hostname = hostname
        self.poll_interval = config.getint("poll_interval", default=300)
        self.download_timeout = config.getint("download_timeout", default=600)

        # State
        self._fleet_files: List[Dict[str, Any]] = []
        self._connected = False
        self._ws_task: Optional[asyncio.Task] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._download_status: Optional[Dict[str, Any]] = None
        self._download_active = False
        self._is_closing = False

        # Register endpoints
        self.server.register_endpoint(
            "/server/fleet/files", RequestType.GET,
            self._handle_fleet_files
        )
        self.server.register_endpoint(
            "/server/fleet/status", RequestType.GET,
            self._handle_fleet_status
        )
        self.server.register_endpoint(
            "/server/fleet/download_and_print", RequestType.POST,
            self._handle_download_and_print
        )
        self.server.register_endpoint(
            "/server/fleet/download_file", RequestType.POST,
            self._handle_download_only
        )

        # Register notifications
        self.server.register_notification(
            "fleet:files_changed", "fleet_files_changed"
        )
        self.server.register_notification(
            "fleet:download_status", "fleet_download_status"
        )
        self.server.register_notification(
            "fleet:connection_status", "fleet_connection_status"
        )

        # Register event handlers
        self.server.register_event_handler(
            "server:klippy_ready", self._on_klippy_ready
        )
        self.server.register_event_handler(
            "file_manager:filelist_changed", self._on_local_filelist_changed
        )

    async def component_init(self) -> None:
        self.http_client: HttpClient = self.server.lookup_component(
            "http_client"
        )
        # Initial fetch — folders first so any later download has its parent in place
        await self._refresh_fleet_folders()
        await self._refresh_fleet_files()
        # Start WebSocket connection
        self._ws_task = self.eventloop.create_task(
            self._fleet_ws_connection()
        )
        # Start poll fallback
        self._poll_task = self.eventloop.create_task(
            self._poll_loop()
        )
        logging.info(
            f"[Fleet] Integration initialized: {self.fleet_url}, "
            f"hostname={self.printer_hostname}"
        )

    def _on_local_filelist_changed(self, result: Dict[str, Any]) -> None:
        """Called when local files change (upload, delete, move).
        Re-evaluate is_local flags on fleet files.
        On deletion, notify fleet_daemon to clear its cache entry."""
        import urllib.parse
        action = result.get("action", "")
        item = result.get("item", {})
        path = item.get("path", "")
        # Only care about changes in fleet_gcodes/
        if not path.startswith(f"{FLEET_SUBDIR}/") and path != FLEET_SUBDIR:
            return
        # Skip thumbnail files
        if "/.thumbs/" in path:
            return
        self._update_local_flags()
        # Notify fleet_daemon when a fleet file is deleted locally
        if action == "delete_file" and self.fleet_url:
            fleet_filename = path[len(FLEET_SUBDIR) + 1:]  # strip "fleet_gcodes/"
            # URL-decode: Moonraker sends URL-encoded paths, fleet_daemon uses literal
            fleet_filename = urllib.parse.unquote(fleet_filename)
            logging.info(f"[Fleet] Local delete detected: {fleet_filename}")
            self.eventloop.create_task(
                self._notify_fleet_cache_removed(fleet_filename)
            )

    def _update_local_flags(self, force_notify: bool = False) -> None:
        """Re-check is_local for all fleet files and notify clients if changed."""
        if not self._fleet_files:
            return
        fm: FileManager = self.server.lookup_component("file_manager")
        gcodes_path = fm.get_directory("gcodes")
        changed = False
        for f in self._fleet_files:
            if gcodes_path:
                local_path = os.path.join(
                    gcodes_path, FLEET_SUBDIR, f["filename"]
                )
                is_local = os.path.isfile(local_path)
            else:
                is_local = False
            was_local = f.get("is_local", False)
            if was_local != is_local:
                f["is_local"] = is_local
                changed = True
        if changed or force_notify:
            self.server.send_event(
                "fleet:files_changed",
                {"files": self._fleet_files}
            )
            if changed:
                logging.info("[Fleet] Local flags updated after file change")

    async def _notify_fleet_cache_removed(self, fleet_filename: str) -> None:
        """Tell fleet_daemon that a cached file was deleted from this printer."""
        url = f"{self.fleet_url}/gcodes/uncache"
        body = {
            "filename": fleet_filename,
            "printer_hostname": self.printer_hostname,
        }
        try:
            resp = await self.http_client.request(
                "POST", url, body=body, request_timeout=10.
            )
            if resp.status_code == 200:
                logging.info(
                    f"[Fleet] Notified fleet_daemon: cache removed "
                    f"{fleet_filename} from {self.printer_hostname}"
                )
            else:
                logging.warning(
                    f"[Fleet] Failed to notify cache removal: "
                    f"HTTP {resp.status_code}"
                )
        except Exception as e:
            logging.warning(f"[Fleet] Failed to notify cache removal: {e}")

    def _on_klippy_ready(self) -> None:
        # Ensure fleet_gcodes directory exists on printer
        fm: FileManager = self.server.lookup_component("file_manager")
        gcodes_path = fm.get_directory("gcodes")
        if gcodes_path:
            fleet_dir = os.path.join(gcodes_path, FLEET_SUBDIR)
            os.makedirs(fleet_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # Fleet daemon WebSocket connection
    # ------------------------------------------------------------------

    async def _fleet_ws_connection(self) -> None:
        """Persistent WebSocket connection to fleet_daemon for real-time
        file update notifications."""
        import tornado.websocket as tornado_ws

        ws_url = re.sub(r"^http", "ws", self.fleet_url) + "/ws"
        log_connect = True

        while not self._is_closing:
            if log_connect:
                logging.info(f"[Fleet] Connecting to fleet_daemon: {ws_url}")
                log_connect = False
            try:
                ws = await tornado_ws.websocket_connect(
                    ws_url,
                    connect_timeout=10.,
                    ping_interval=30.,
                    ping_timeout=60.
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.debug(f"[Fleet] WebSocket connection failed: {e}")
            else:
                self._connected = True
                logging.info("[Fleet] Connected to fleet_daemon")
                self._send_connection_notification()
                await self._refresh_worker_flag()
                await self._read_ws_messages(ws)
                log_connect = True

            was_connected = self._connected
            self._connected = False
            if was_connected:
                self._send_connection_notification()
            if not self._is_closing:
                await asyncio.sleep(5.)

    async def _read_ws_messages(self, ws) -> None:
        """Read messages from fleet_daemon WebSocket."""
        while True:
            message = await ws.read_message()
            if message is None:
                logging.info("[Fleet] fleet_daemon WebSocket disconnected")
                break
            if isinstance(message, str):
                if message in ("ping", "pong"):
                    continue
                try:
                    data = jsonw.loads(message)
                except Exception:
                    continue
                event = data.get("event")
                if event == "gcodes_updated":
                    logging.info("[Fleet] Received gcodes_updated, refreshing file list")
                    await self._refresh_fleet_folders()
                    await self._refresh_fleet_files()
                elif event == "workers_updated":
                    logging.info("[Fleet] Received workers_updated, refreshing worker flag")
                    await self._refresh_worker_flag()

    # ------------------------------------------------------------------
    # Fleet worker flag
    # ------------------------------------------------------------------

    async def _refresh_worker_flag(self) -> None:
        """Ask fleet_daemon whether this printer is an enabled fleet worker
        and mirror the answer into machine_state.is_fleet_worker.
        404 means the daemon does not know this printer (not a worker).
        On a network error the last known value is kept."""
        url = f"{self.fleet_url}/workers/{self.printer_hostname}"
        try:
            resp = await self.http_client.request(
                "GET", url, request_timeout=10.
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.debug(f"[Fleet] Failed to fetch worker flag: {e}")
            return
        if resp.status_code == 200:
            try:
                data = jsonw.loads(resp.content)
            except Exception as e:
                logging.debug(f"[Fleet] Invalid worker response: {e}")
                return
            value = 1 if data.get("enabled") else 0
        elif resp.status_code == 404:
            value = 0
        else:
            logging.debug(
                f"[Fleet] Failed to fetch worker flag: HTTP {resp.status_code}"
            )
            return
        kconn: KlippyConnection = self.server.lookup_component(
            "klippy_connection"
        )
        kconn.set_fleet_worker(value)

    def _get_worker_flag(self) -> int:
        kconn: KlippyConnection = self.server.lookup_component(
            "klippy_connection"
        )
        return int(kconn.shared_printer_config.is_fleet_worker)

    # ------------------------------------------------------------------
    # Poll fallback
    # ------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        """Fallback: periodically poll fleet_daemon for file list."""
        while not self._is_closing:
            await asyncio.sleep(self.poll_interval)
            if not self._connected:
                # Only poll when WebSocket is down
                await self._refresh_fleet_folders()
                await self._refresh_fleet_files()

    # ------------------------------------------------------------------
    # Fleet file list
    # ------------------------------------------------------------------

    async def _refresh_fleet_files(self) -> None:
        """Fetch the current file list from fleet_daemon."""
        url = f"{self.fleet_url}/gcodes/fleet-files"
        try:
            resp = await self.http_client.request(
                "GET", url, request_timeout=10.
            )
            if resp.status_code == 200:
                data = jsonw.loads(resp.content)
                self._fleet_files = data.get("files", [])
                # Set initial is_local flags and notify
                self._update_local_flags(force_notify=True)
                logging.info(
                    f"[Fleet] Refreshed file list: {len(self._fleet_files)} files"
                )
            else:
                logging.warning(
                    f"[Fleet] Failed to fetch files: HTTP {resp.status_code}"
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.debug(f"[Fleet] Failed to fetch files: {e}")

    async def _refresh_fleet_folders(self) -> None:
        """Fetch folder structure from fleet_daemon and create matching local subdirs."""
        url = f"{self.fleet_url}/gcodes/fleet-folders"
        try:
            resp = await self.http_client.request(
                "GET", url, request_timeout=10.
            )
            if resp.status_code != 200:
                logging.warning(
                    f"[Fleet] Failed to fetch folders: HTTP {resp.status_code}"
                )
                return
            data = jsonw.loads(resp.content)
            folders = data.get("folders", [])
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.debug(f"[Fleet] Failed to fetch folders: {e}")
            return

        fm: FileManager = self.server.lookup_component("file_manager")
        gcodes_path = fm.get_directory("gcodes")
        if not gcodes_path:
            return

        fleet_root = os.path.join(gcodes_path, FLEET_SUBDIR)
        created = 0
        for rel in folders:
            rel_norm = rel.strip("/").replace("\\", "/")
            if not rel_norm or ".." in rel_norm.split("/"):
                continue
            target = os.path.join(fleet_root, rel_norm)
            if not os.path.isdir(target):
                try:
                    os.makedirs(target, exist_ok=True)
                    created += 1
                except OSError as e:
                    logging.warning(f"[Fleet] mkdir failed for {target}: {e}")
        if created:
            logging.info(f"[Fleet] Created {created} fleet subdirectories from daemon")

    # ------------------------------------------------------------------
    # API Handlers
    # ------------------------------------------------------------------

    async def _handle_fleet_files(
        self, web_request: WebRequest
    ) -> Dict[str, Any]:
        """Return fleet file list, refreshing from fleet_daemon first."""
        await self._refresh_fleet_files()
        return {
            "files": self._fleet_files,
            "connected": self._connected,
        }

    async def _handle_fleet_status(
        self, web_request: WebRequest
    ) -> Dict[str, Any]:
        """Return fleet connection status and download info."""
        return {
            "connected": self._connected,
            "fleet_daemon_url": self.fleet_url,
            "printer_hostname": self.printer_hostname,
            "file_count": len(self._fleet_files),
            "download_status": self._download_status,
            "is_fleet_worker": self._get_worker_flag(),
        }

    async def _handle_download_and_print(
        self, web_request: WebRequest
    ) -> Dict[str, Any]:
        """Queue a fleet file download and print. Returns immediately;
        progress is reported via fleet:download_status notifications."""
        filename = web_request.get_str("filename")

        if self._download_active:
            raise self.server.error(
                f"Download already in progress: "
                f"{(self._download_status or {}).get('filename', '?')}"
            )

        fm: FileManager = self.server.lookup_component("file_manager")
        gcodes_path = fm.get_directory("gcodes")
        if not gcodes_path:
            raise self.server.error("Gcodes directory not configured")

        local_path = os.path.join(gcodes_path, FLEET_SUBDIR, filename)
        cached = os.path.isfile(local_path)
        # Activity log (additive)
        self.server.send_event(
            "fleet:download_requested", filename, True, cached)
        if cached:
            # File already cached — just start the print
            kapis: APIComp = self.server.lookup_component("klippy_apis")
            print_path = f"{FLEET_SUBDIR}/{filename}"
            await kapis.start_print(print_path)
            return {
                "status": "started",
                "filename": filename,
                "cached": True,
            }

        # Launch in background — return immediately
        self.eventloop.create_task(
            self._bg_download(filename, local_path, start_print=True)
        )
        return {
            "status": "queued",
            "filename": filename,
        }

    async def _handle_download_only(
        self, web_request: WebRequest
    ) -> Dict[str, Any]:
        """Queue a fleet file download without printing. Returns immediately."""
        filename = web_request.get_str("filename")

        if self._download_active:
            raise self.server.error(
                f"Download already in progress: "
                f"{(self._download_status or {}).get('filename', '?')}"
            )

        fm: FileManager = self.server.lookup_component("file_manager")
        gcodes_path = fm.get_directory("gcodes")
        if not gcodes_path:
            raise self.server.error("Gcodes directory not configured")

        local_path = os.path.join(gcodes_path, FLEET_SUBDIR, filename)
        self.server.send_event(
            "fleet:download_requested", filename, False,
            os.path.isfile(local_path))
        if os.path.isfile(local_path):
            return {
                "status": "already_local",
                "filename": filename,
            }

        # Launch in background — return immediately
        self.eventloop.create_task(
            self._bg_download(filename, local_path, start_print=False)
        )
        return {
            "status": "queued",
            "filename": filename,
        }

    async def _bg_download(
        self, filename: str, local_path: str, start_print: bool
    ) -> None:
        """Background task: download file, optionally start print.
        Updates status via notifications throughout."""
        self._download_active = True
        self._set_status(filename, "requesting")
        try:
            if start_print:
                await self._download_and_start_print(filename, local_path)
            else:
                await self._download_fleet_file(filename, local_path)
                self._set_status(filename, "complete")
                await self._refresh_fleet_files()
        except Exception as e:
            logging.exception(f"[Fleet] Background download failed: {filename}")
            self._set_status(filename, "error", str(e))
        finally:
            self._download_active = False
            await asyncio.sleep(3)
            self._download_status = None
            self._send_download_notification()

    async def _download_fleet_file(
        self, filename: str, local_path: str
    ) -> str:
        """Download a fleet file to this printer (no print).
        Returns the Moonraker-relative path for use with start_print."""
        # Step 1: Tell fleet_daemon to push the file to us
        url = f"{self.fleet_url}/gcodes/download"
        body = {
            "filename": filename,
            "printer_hostname": self.printer_hostname,
        }
        try:
            resp = await self.http_client.request(
                "POST", url, body=body, request_timeout=10.
            )
            if resp.status_code not in (200, 201):
                error_msg = resp.content.decode("utf-8", errors="replace")
                raise self.server.error(
                    f"Fleet download request failed: {error_msg}"
                )
        except asyncio.CancelledError:
            raise
        except self.server.error:
            raise
        except Exception as e:
            raise self.server.error(
                f"Failed to request download from fleet_daemon: {e}"
            )

        # Step 2: Wait for the file to appear locally
        # fleet_daemon uploads via Moonraker's upload API, which may
        # URL-encode the filename. Check both variants.
        import urllib.parse
        self._set_status(filename, "downloading")

        encoded_filename = urllib.parse.quote(filename, safe="/")
        local_path_encoded = os.path.join(
            os.path.dirname(local_path),
            urllib.parse.quote(os.path.basename(filename), safe="")
        )

        timeout = self.download_timeout
        poll_interval = 2
        elapsed = 0
        found_path = None
        while elapsed < timeout:
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
            if os.path.isfile(local_path):
                found_path = local_path
                break
            if local_path_encoded != local_path and os.path.isfile(local_path_encoded):
                found_path = local_path_encoded
                break
            # Also check via Moonraker's file manager metadata
            fm: FileManager = self.server.lookup_component("file_manager")
            for meta_path in (
                f"{FLEET_SUBDIR}/{filename}",
                f"{FLEET_SUBDIR}/{encoded_filename}",
            ):
                meta = fm.gcode_metadata.get(meta_path, None)
                if meta is not None:
                    logging.info(f"[Fleet] File detected via metadata: {meta_path}")
                    found_path = local_path
                    break
            if found_path:
                break

        if found_path is None:
            raise self.server.error(
                f"Download timed out waiting for {filename} "
                f"({timeout}s)"
            )

        logging.info(f"[Fleet] File arrived: {found_path}")

        # Step 3: Brief wait for Moonraker inotify to detect the file
        self._set_status(filename, "processing")
        await asyncio.sleep(2)

        # Determine the Moonraker-relative path for start_print
        # Check which metadata key the file is registered under
        fm2: FileManager = self.server.lookup_component("file_manager")
        for candidate in (
            f"{FLEET_SUBDIR}/{filename}",
            f"{FLEET_SUBDIR}/{encoded_filename}",
        ):
            meta = fm2.gcode_metadata.get(candidate, None)
            if meta is not None:
                logging.info(f"[Fleet] Metadata found at: {candidate}")
                return candidate

        # Fallback — metadata may still be parsing, use the raw path
        logging.info(f"[Fleet] Using fallback path: {FLEET_SUBDIR}/{filename}")
        return f"{FLEET_SUBDIR}/{filename}"

    async def _download_and_start_print(
        self, filename: str, local_path: str
    ) -> None:
        """Download a fleet file then start printing it."""
        print_path = await self._download_fleet_file(filename, local_path)

        self._set_status(filename, "starting_print")

        kapis: APIComp = self.server.lookup_component("klippy_apis")
        try:
            logging.info(f"[Fleet] Starting print: {print_path}")
            await kapis.start_print(print_path)
            logging.info(f"[Fleet] Print started: {print_path}")
        except Exception as e:
            logging.error(f"[Fleet] Failed to start print: {e}")
            raise self.server.error(
                f"Failed to start print after download: {e}"
            )

        self._set_status(filename, "complete")
        await self._refresh_fleet_files()

    def _set_status(
        self, filename: str, status: str, error: Optional[str] = None
    ) -> None:
        """Update download status and notify clients."""
        self._download_status = {"filename": filename, "status": status}
        if error:
            self._download_status["error"] = error
        self._send_download_notification()

    def _send_download_notification(self) -> None:
        """Broadcast download status to all connected clients."""
        self.server.send_event(
            "fleet:download_status",
            {"download_status": self._download_status}
        )

    def _send_connection_notification(self) -> None:
        """Broadcast fleet_daemon link state to all connected clients."""
        self.server.send_event(
            "fleet:connection_status",
            {
                "connected": self._connected,
                "fleet_url": self.fleet_url,
            }
        )

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    async def close(self) -> None:
        self._is_closing = True
        for task in [self._ws_task, self._poll_task]:
            if task is not None:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        logging.info("[Fleet] Integration shut down")


def load_component(config: ConfigHelper) -> FleetIntegration:
    return FleetIntegration(config)
