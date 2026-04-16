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
        self._download_lock = asyncio.Lock()
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

        # Register event handler
        self.server.register_event_handler(
            "server:klippy_ready", self._on_klippy_ready
        )

    async def component_init(self) -> None:
        self.http_client: HttpClient = self.server.lookup_component(
            "http_client"
        )
        # Initial fetch
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
                await self._read_ws_messages(ws)
                log_connect = True

            self._connected = False
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
                    await self._refresh_fleet_files()

    # ------------------------------------------------------------------
    # Poll fallback
    # ------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        """Fallback: periodically poll fleet_daemon for file list."""
        while not self._is_closing:
            await asyncio.sleep(self.poll_interval)
            if not self._connected:
                # Only poll when WebSocket is down
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

                # Mark which files are locally available
                fm: FileManager = self.server.lookup_component("file_manager")
                gcodes_path = fm.get_directory("gcodes")
                if gcodes_path:
                    for f in self._fleet_files:
                        local_path = os.path.join(
                            gcodes_path, FLEET_SUBDIR, f["filename"]
                        )
                        f["is_local"] = os.path.isfile(local_path)
                else:
                    for f in self._fleet_files:
                        f["is_local"] = False

                # Notify clients
                self.server.send_event(
                    "fleet:files_changed",
                    {"files": self._fleet_files}
                )
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
        }

    async def _handle_download_and_print(
        self, web_request: WebRequest
    ) -> Dict[str, Any]:
        """Download a fleet file to this printer and start printing it.

        Flow:
        1. POST to fleet_daemon's download queue
        2. fleet_daemon uploads file to this printer via Moonraker upload API
        3. Watch for file to appear in gcodes/fleet_gcodes/
        4. Wait for metadata processing
        5. Start print via klippy_apis
        """
        filename = web_request.get_str("filename")

        if self._download_lock.locked():
            raise self.server.error(
                f"Download already in progress: "
                f"{(self._download_status or {}).get('filename', '?')}"
            )

        fm: FileManager = self.server.lookup_component("file_manager")
        gcodes_path = fm.get_directory("gcodes")
        if not gcodes_path:
            raise self.server.error("Gcodes directory not configured")

        local_path = os.path.join(gcodes_path, FLEET_SUBDIR, filename)
        if os.path.isfile(local_path):
            # File already cached — just start the print
            kapis: APIComp = self.server.lookup_component("klippy_apis")
            print_path = f"{FLEET_SUBDIR}/{filename}"
            await kapis.start_print(print_path)
            return {
                "status": "started",
                "filename": filename,
                "cached": True,
            }

        async with self._download_lock:
            self._set_status(filename, "requesting")
            try:
                result = await self._download_and_start_print(
                    filename, local_path
                )
                return result
            except Exception as e:
                self._set_status(filename, "error", str(e))
                raise
            finally:
                # Clear status after a delay so UI can see the final state
                await asyncio.sleep(5)
                self._download_status = None
                self._send_download_notification()

    async def _handle_download_only(
        self, web_request: WebRequest
    ) -> Dict[str, Any]:
        """Download a fleet file to this printer without starting a print."""
        filename = web_request.get_str("filename")

        if self._download_lock.locked():
            raise self.server.error(
                f"Download already in progress: "
                f"{(self._download_status or {}).get('filename', '?')}"
            )

        fm: FileManager = self.server.lookup_component("file_manager")
        gcodes_path = fm.get_directory("gcodes")
        if not gcodes_path:
            raise self.server.error("Gcodes directory not configured")

        local_path = os.path.join(gcodes_path, FLEET_SUBDIR, filename)
        if os.path.isfile(local_path):
            return {
                "status": "already_local",
                "filename": filename,
            }

        async with self._download_lock:
            self._set_status(filename, "requesting")
            try:
                await self._download_fleet_file(filename, local_path)
                self._set_status(filename, "complete")
                await self._refresh_fleet_files()
                return {
                    "status": "downloaded",
                    "filename": filename,
                }
            except Exception as e:
                self._set_status(filename, "error", str(e))
                raise
            finally:
                await asyncio.sleep(5)
                self._download_status = None
                self._send_download_notification()

    async def _download_fleet_file(
        self, filename: str, local_path: str
    ) -> None:
        """Download a fleet file to this printer (no print)."""
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
        self._set_status(filename, "downloading")

        timeout = self.download_timeout
        poll_interval = 2
        elapsed = 0
        while elapsed < timeout:
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
            if os.path.isfile(local_path):
                break
        else:
            raise self.server.error(
                f"Download timed out waiting for {filename} "
                f"({timeout}s)"
            )

        # Step 3: Wait for metadata processing
        self._set_status(filename, "processing")
        await asyncio.sleep(3)

    async def _download_and_start_print(
        self, filename: str, local_path: str
    ) -> Dict[str, Any]:
        """Download a fleet file then start printing it."""
        await self._download_fleet_file(filename, local_path)

        self._set_status(filename, "starting_print")

        kapis: APIComp = self.server.lookup_component("klippy_apis")
        print_path = f"{FLEET_SUBDIR}/{filename}"
        try:
            await kapis.start_print(print_path)
        except Exception as e:
            raise self.server.error(
                f"Failed to start print after download: {e}"
            )

        self._set_status(filename, "complete")
        await self._refresh_fleet_files()

        return {
            "status": "started",
            "filename": filename,
            "cached": False,
        }

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
