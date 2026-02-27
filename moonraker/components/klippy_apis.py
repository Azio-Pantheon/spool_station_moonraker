# Helper for Moonraker to Klippy API calls.
#
# Copyright (C) 2020 Eric Callahan <arksine.code@gmail.com>
#
# This file may be distributed under the terms of the GNU GPLv3 license.

from __future__ import annotations
from ..utils import Sentinel
from ..common import WebRequest, APITransport, RequestType
import asyncio
from datetime import datetime
import logging
import os
import time



# Annotation imports
from typing import (
    TYPE_CHECKING,
    Any,
    Union,
    Optional,
    Dict,
    List,
    TypeVar,
    Mapping,
    Callable,
    Coroutine
)
if TYPE_CHECKING:
    from ..confighelper import ConfigHelper
    from .klippy_connection import KlippyConnection as Klippy
    Subscription = Dict[str, Optional[List[Any]]]
    SubCallback = Callable[[Dict[str, Dict[str, Any]], float], Optional[Coroutine]]
    _T = TypeVar("_T")

INFO_ENDPOINT = "info"
ESTOP_ENDPOINT = "emergency_stop"
LIST_EPS_ENDPOINT = "list_endpoints"
GC_OUTPUT_ENDPOINT = "gcode/subscribe_output"
GCODE_ENDPOINT = "gcode/script"
SUBSCRIPTION_ENDPOINT = "objects/subscribe"
STATUS_ENDPOINT = "objects/query"
OBJ_LIST_ENDPOINT = "objects/list"
REG_METHOD_ENDPOINT = "register_remote_method"

class KlippyAPI(APITransport):
    def __init__(self, config: ConfigHelper, shared_printer_config) -> None:
        self.server = config.get_server()
        self.klippy: Klippy = self.server.lookup_component("klippy_connection")
        self.eventloop = self.server.get_event_loop()
        app_args = self.server.get_app_args()
        self.version = app_args.get('software_version')
        # Maintain a subscription for all moonraker requests, as
        # we do not want to overwrite them
        self.host_subscription: Subscription = {}
        self.subscription_callbacks: List[SubCallback] = []

        # Register GCode Aliases
        self.server.register_endpoint(
            "/printer/print/pause", RequestType.POST, self._gcode_pause
        )
        self.server.register_endpoint(
            "/printer/print/resume", RequestType.POST, self._gcode_resume
        )
        self.server.register_endpoint(
            "/printer/print/cancel", RequestType.POST, self._gcode_cancel
        )
        self.server.register_endpoint(
            "/printer/print/start", RequestType.POST, self._gcode_start_print
        )
        self.server.register_endpoint(
            "/printer/restart", RequestType.POST, self._gcode_restart
        )
        self.server.register_endpoint(
            "/printer/firmware_restart", RequestType.POST, self._gcode_firmware_restart
        )
        self.server.register_event_handler(
            "server:klippy_disconnect", self._on_klippy_disconnect
        )

        self.shared_printer_config = shared_printer_config

    def _on_klippy_disconnect(self) -> None:
        self.host_subscription.clear()
        self.subscription_callbacks.clear()

    async def _gcode_pause(self, web_request: WebRequest) -> str:
        return await self.pause_print()

    async def _gcode_resume(self, web_request: WebRequest) -> str:
        return await self.resume_print()

    async def _gcode_cancel(self, web_request: WebRequest) -> str:
        return await self.cancel_print()

    async def _gcode_start_print(self, web_request: WebRequest) -> str:
        filename: str = web_request.get_str('filename')
        return await self.start_print(filename)

    async def _gcode_restart(self, web_request: WebRequest) -> str:
        return await self.do_restart("RESTART")

    async def _gcode_firmware_restart(self, web_request: WebRequest) -> str:
        return await self.do_restart("FIRMWARE_RESTART")

    async def _send_klippy_request(
        self,
        method: str,
        params: Dict[str, Any],
        default: Any = Sentinel.MISSING,
        transport: Optional[APITransport] = None
    ) -> Any:
        try:
            req = WebRequest(method, params, transport=transport or self)
            result = await self.klippy.request(req)
        except self.server.error:
            if default is Sentinel.MISSING:
                raise
            result = default
        return result

    async def run_gcode(self,
                        script: str,
                        default: Any = Sentinel.MISSING,
                        from_start_print: bool = False
                        ) -> str:
        if from_start_print == True:
            # Check the filament type and configure the appropriate pre_script
            filament = self.shared_printer_config.filament
            targeted_filament = False
            if filament == "PETG-CF":
                hotend_temp = 280
                targeted_filament = True
            elif filament in ["PA-CF", "PA-GF"]:
                hotend_temp = 300
                targeted_filament = True

            # Example timestamp (e.g., when the last job ended)
            last_job_end_time = self.shared_printer_config.last_print_time
            

            # Get the current time
            current_time = time.time()

            # Calculate the elapsed time in seconds
            elapsed_time_seconds = current_time - last_job_end_time

            # Convert seconds to hours
            elapsed_hours = elapsed_time_seconds / 3600

            print(f"Hours passed: {elapsed_hours:.2f}")

            if self.shared_printer_config.wet_filament_purge == 1 and elapsed_hours > 12 and targeted_filament:
                # Substitute variables into the template
                macro_script = f'WET_FILAMENT_PURGE HOTEND_TEMP={hotend_temp}'
                self.shared_printer_config.is_purging = 1
                self.shared_printer_config.last_print_time = time.time()
                database = self.server.lookup_component('database')
                await self._send_klippy_request(GCODE_ENDPOINT, {'script': macro_script}, default)
                self.shared_printer_config.is_purging = 0
                asyncio.create_task(self._async_insert_last_print_time(database, self.shared_printer_config.last_print_time))

            # --- Dribble Test ---
            try:
                await self._run_dribble_test(default, filament)
            except Exception as e:
                logging.warning(f"Dribble test failed (non-blocking): {e}")

        params = {'script': script}
        result = await self._send_klippy_request(
            GCODE_ENDPOINT, params, default)
        return result
    
    async def _async_insert_last_print_time(self, database, last_print_time: float) -> None:
        try:
            await database.insert_item(
                namespace="HS3",
                key="last_print_time",
                value=last_print_time
            )
        except Exception as e:
            print(f"Failed to insert last_print_time: {e}")

    async def _run_dribble_test(self, default: Any, filament: str) -> None:
        CAMERA_URL = "http://localhost:8080/?action=snapshot"
        CROP_ROI = (510,270,190,480)
        THRESHOLD = 20
        BLUR = 21

        logging.info("Dribble test: running DRIBBLE macro")
        await self._send_klippy_request(
            GCODE_ENDPOINT,
            {'script': f'DRIBBLE FILAMENT={filament}'},
            default
        )

        http_client = self.server.lookup_component("http_client")

        await asyncio.sleep(10)
        # Take 3 "before" snapshots (2s apart to handle focus changes)
        before_list = []
        logging.info("Dribble test: taking 2 'before' snapshots")
        for i in range(2):
            resp = await http_client.request("GET", CAMERA_URL)
            if resp.has_error():
                raise Exception(f"Failed to fetch before snapshot {i}: {resp.error}")
            before_list.append(resp.content)
            if i < 2:
                await asyncio.sleep(2)

        # Wait 30 seconds for dribble to form
        logging.info("Dribble test: waiting 30s for dribble to form")
        await asyncio.sleep(30)

        # Take 3 "after" snapshots (2s apart to handle focus changes)
        after_list = []
        logging.info("Dribble test: taking 2 'after' snapshots")
        for i in range(2):
            resp = await http_client.request("GET", CAMERA_URL)
            if resp.has_error():
                raise Exception(f"Failed to fetch after snapshot {i}: {resp.error}")
            after_list.append(resp.content)
            if i < 2:
                await asyncio.sleep(2)

        # Try all 9 before/after pairs, pick shortest dribble length
        def _dribble_worker():
            from .dribble_diff import measure_dribble
            LOG_DIR = "/home/hs3/printer_data/logs"
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            tmpdir = os.path.join(LOG_DIR, f"dribble_{timestamp}")
            os.makedirs(tmpdir, exist_ok=True)

            # Write all snapshots to disk
            for i, data in enumerate(before_list):
                with open(os.path.join(tmpdir, f"before_{i}.png"), "wb") as f:
                    f.write(data)
            for i, data in enumerate(after_list):
                with open(os.path.join(tmpdir, f"after_{i}.png"), "wb") as f:
                    f.write(data)

            # Evaluate all 4 combinations (no output images yet)
            best_result = None
            best_pair = (0, 0)
            for bi in range(2):
                for ai in range(2):
                    result = measure_dribble(
                        before_path=os.path.join(tmpdir, f"before_{bi}.png"),
                        after_path=os.path.join(tmpdir, f"after_{ai}.png"),
                        crop_roi=CROP_ROI,
                        threshold=THRESHOLD,
                        blur_strength=BLUR,
                    )
                    if best_result is None or result.dribble_length_px < best_result.dribble_length_px:
                        best_result = result
                        best_pair = (bi, ai)

            # Re-run the best pair with output images
            best_result = measure_dribble(
                before_path=os.path.join(tmpdir, f"before_{best_pair[0]}.png"),
                after_path=os.path.join(tmpdir, f"after_{best_pair[1]}.png"),
                crop_roi=CROP_ROI,
                threshold=THRESHOLD,
                blur_strength=BLUR,
                output_dir=tmpdir,
            )
            return tmpdir, best_pair, best_result

        tmpdir, best_pair, result = await self.eventloop.run_in_thread(_dribble_worker)
        logging.info(
            f"Dribble test: best pair=before_{best_pair[0]}/after_{best_pair[1]}, "
            f"length={result.dribble_length_px}px, "
            f"width={result.dribble_width_px}px, "
            f"area={result.dribble_area_px}px² "
            f"(images saved to {tmpdir})"
        )

    async def start_print(
        self, filename: str, wait_klippy_started: bool = False
    ) -> str:
        # WARNING: Do not call this method from within the following
        # event handlers when "wait_klippy_started" is set to True:
        # klippy_identified, klippy_started, klippy_ready, klippy_disconnect
        # Doing so will result in "wait_started" blocking for the specifed
        # timeout (default 20s) and returning False.
        # XXX - validate that file is on disk
        if filename[0] == '/':
            filename = filename[1:]
        # Escape existing double quotes in the file name
        filename = filename.replace("\"", "\\\"")
        script = f'SDCARD_PRINT_FILE FILENAME="{filename}"'
        if wait_klippy_started:
            await self.klippy.wait_started()
        return await self.run_gcode(script, Any, True)

    async def pause_print(
        self, default: Union[Sentinel, _T] = Sentinel.MISSING
    ) -> Union[_T, str]:
        self.server.send_event("klippy_apis:pause_requested")
        return await self._send_klippy_request(
            "pause_resume/pause", {}, default)

    async def resume_print(
        self, default: Union[Sentinel, _T] = Sentinel.MISSING
    ) -> Union[_T, str]:
        self.server.send_event("klippy_apis:resume_requested")
        return await self._send_klippy_request(
            "pause_resume/resume", {}, default)

    async def cancel_print(
        self, default: Union[Sentinel, _T] = Sentinel.MISSING
    ) -> Union[_T, str]:
        self.server.send_event("klippy_apis:cancel_requested")
        return await self._send_klippy_request(
            "pause_resume/cancel", {}, default)

    async def do_restart(
        self, gc: str, wait_klippy_started: bool = False
    ) -> str:
        # WARNING: Do not call this method from within the following
        # event handlers when "wait_klippy_started" is set to True:
        # klippy_identified, klippy_started, klippy_ready, klippy_disconnect
        # Doing so will result in "wait_started" blocking for the specifed
        # timeout (default 20s) and returning False.
        if wait_klippy_started:
            await self.klippy.wait_started()
        try:
            result = await self.run_gcode(gc)
        except self.server.error as e:
            if str(e) == "Klippy Disconnected":
                result = "ok"
            else:
                raise
        return result

    async def list_endpoints(self,
                             default: Union[Sentinel, _T] = Sentinel.MISSING
                             ) -> Union[_T, Dict[str, List[str]]]:
        return await self._send_klippy_request(
            LIST_EPS_ENDPOINT, {}, default)

    async def emergency_stop(self) -> str:
        return await self._send_klippy_request(ESTOP_ENDPOINT, {})

    async def get_klippy_info(self,
                              send_id: bool = False,
                              default: Union[Sentinel, _T] = Sentinel.MISSING
                              ) -> Union[_T, Dict[str, Any]]:
        params = {}
        if send_id:
            ver = self.version
            params = {'client_info': {'program': "Moonraker", 'version': ver}}
        return await self._send_klippy_request(INFO_ENDPOINT, params, default)

    async def get_object_list(self,
                              default: Union[Sentinel, _T] = Sentinel.MISSING
                              ) -> Union[_T, List[str]]:
        result = await self._send_klippy_request(
            OBJ_LIST_ENDPOINT, {}, default)
        if isinstance(result, dict) and 'objects' in result:
            return result['objects']
        if default is not Sentinel.MISSING:
            return default
        raise self.server.error("Invalid response received from Klippy", 500)

    async def query_objects(self,
                            objects: Mapping[str, Optional[List[str]]],
                            default: Union[Sentinel, _T] = Sentinel.MISSING
                            ) -> Union[_T, Dict[str, Any]]:
        params = {'objects': objects}
        result = await self._send_klippy_request(
            STATUS_ENDPOINT, params, default)
        if isinstance(result, dict) and "status" in result:
            return result["status"]
        if default is not Sentinel.MISSING:
            return default
        raise self.server.error("Invalid response received from Klippy", 500)

    async def subscribe_objects(
        self,
        objects: Mapping[str, Optional[List[str]]],
        callback: Optional[SubCallback] = None,
        default: Union[Sentinel, _T] = Sentinel.MISSING
    ) -> Union[_T, Dict[str, Any]]:
        # The host transport shares subscriptions amongst all components
        for obj, items in objects.items():
            if obj in self.host_subscription:
                prev = self.host_subscription[obj]
                if items is None or prev is None:
                    self.host_subscription[obj] = None
                else:
                    uitems = list(set(prev) | set(items))
                    self.host_subscription[obj] = uitems
            else:
                self.host_subscription[obj] = items
        params = {"objects": dict(self.host_subscription)}
        result = await self._send_klippy_request(SUBSCRIPTION_ENDPOINT, params, default)
        if isinstance(result, dict) and "status" in result:
            if callback is not None:
                self.subscription_callbacks.append(callback)
            return result["status"]
        if default is not Sentinel.MISSING:
            return default
        raise self.server.error("Invalid response received from Klippy", 500)

    async def subscribe_from_transport(
        self,
        objects: Mapping[str, Optional[List[str]]],
        transport: APITransport,
        default: Union[Sentinel, _T] = Sentinel.MISSING,
    ) -> Union[_T, Dict[str, Any]]:
        params = {"objects": dict(objects)}
        result = await self._send_klippy_request(
            SUBSCRIPTION_ENDPOINT, params, default, transport
        )
        if isinstance(result, dict) and "status" in result:
            return result["status"]
        if default is not Sentinel.MISSING:
            return default
        raise self.server.error("Invalid response received from Klippy", 500)

    async def subscribe_gcode_output(self) -> str:
        template = {'response_template':
                    {'method': "process_gcode_response"}}
        return await self._send_klippy_request(GC_OUTPUT_ENDPOINT, template)

    async def register_method(self, method_name: str) -> str:
        return await self._send_klippy_request(
            REG_METHOD_ENDPOINT,
            {'response_template': {"method": method_name},
             'remote_method': method_name})

    def send_status(
        self, status: Dict[str, Any], eventtime: float
    ) -> None:
        for cb in self.subscription_callbacks:
            self.eventloop.register_callback(cb, status, eventtime)
        self.server.send_event("server:status_update", status)

def load_component(config: ConfigHelper, shared_printer_config) -> KlippyAPI:
    return KlippyAPI(config, shared_printer_config)