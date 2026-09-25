# Spool Tracker Component for Moonraker
#
# Copyright (C) 2024 <Your Name>
#
# This file may be distributed under the terms of the GNU GPLv3 license.

from __future__ import annotations
import asyncio
import logging
import math
import time
import uuid
from datetime import datetime, date
from typing import (
    TYPE_CHECKING,
    Dict,
    Any,
    List,
    Optional,
    Union,
)

if TYPE_CHECKING:
    from ..confighelper import ConfigHelper
    from ..common import WebRequest
    from .klippy_apis import KlippyAPI as APIComp
    from .database import MoonrakerDatabase

# Hard-coded filament specifications
FILAMENT_TYPES = {
    "PA-CF": {
        "density": 1.16,  # g/cm³
        "diameter": 1.75,  # mm
        "name": "Carbon Fiber Nylon(PA-CF)"
    },
    "PETG-CF": {
        "density": 1.27,  # g/cm³
        "diameter": 1.75,  # mm
        "name": "Carbon Fiber Co-Polyester (PETG)"
    },
    "PA-GF": {
        "density": 1.16,  # g/cm³
        "diameter": 1.75,  # mm
        "name": "Glass Fiber Nylon (PA-GF)"
    },
    "TPU": {
        "density": 1.15,  # g/cm³
        "diameter": 1.75,  # mm
        "name": "95A Flex(TPU 95A)"
    },
}


# Daily service-tracker snapshots (odometers, tripmeters, nozzle life,
# filament usage) kept permanently in their own namespace, one record per
# local calendar day.  fleet_daemon collects them via
# GET /server/spool_tracker/snapshots?since=YYYY-MM-DD.
SNAPSHOT_NAMESPACE = "service_tracker_history"
SNAPSHOT_CHECK_INTERVAL = 600.
SNAPSHOT_FIRST_DELAY = 60.
SNAPSHOT_LIST_MAX = 2000


class SpoolTracker:
    def __init__(self, config: ConfigHelper):
        self.server = config.get_server()
        self.eventloop = self.server.get_event_loop()
        
        # Ready flag - set to True after initialization completes
        self._ready = False
        
        # Configuration
        self.sync_rate_seconds = config.getint("sync_rate", default=5, minval=1)
        
        # Custom filament types (runtime additions) - initialize empty dict first
        self.custom_filaments: Dict[str, Dict[str, Any]] = {}
        
        # Database and shared config references - MUST be set before loading custom filaments
        self.database: MoonrakerDatabase = self.server.lookup_component("database")
        self.shared_printer_config = self.server.shared_printer_config
        
        # Load custom filaments from database AFTER database reference is initialized
        self._load_custom_filaments_from_database()
        
        # Initialize filament state from shared config and database
        self._initialize_filament_state()
        
        # Usage tracking
        self.pending_usage_mm = 0.0  # Accumulated extrusion in mm
        self._pending_nozzle_e_mm = 0.0  # Forward extrusion for nozzle life (always active)
        self._highest_epos: float = 0.0
        self._current_extruder: str = "extruder"
        self._last_known_filament_type: str = self.current_filament_type
        
        # Nozzle life tracking delegated to shared_printer_config (authoritative source)
        self._initialize_nozzle_life_state()
        
        # odometer tracking for X, Y, Z, E movement
        self.odometer_x: float = 0.0
        self.odometer_y: float = 0.0
        self.odometer_z: float = 0.0
        self.odometer_e: float = 0.0
        self._last_x_pos: Optional[float] = None
        self._last_y_pos: Optional[float] = None
        self._last_z_pos: Optional[float] = None
        self._last_e_pos: Optional[float] = None
        self._initialize_odometer_state()
        
        # tripmeter tracking for X, Y, Z, E movement (resettable)
        self.tripmeter_x: float = 0.0
        self.tripmeter_y: float = 0.0
        self.tripmeter_z: float = 0.0
        self.tripmeter_e: float = 0.0
        self._initialize_tripmeter_state()
        
        # Timer for periodic reporting and database sync
        self.report_timer = self.eventloop.register_timer(self._report_usage)

        # Daily service-tracker snapshots (permanent)
        self.database.register_local_namespace(SNAPSHOT_NAMESPACE)
        self.snapshot_ns = self.database.wrap_namespace(
            SNAPSHOT_NAMESPACE, parse_keys=False
        )
        self.snapshot_timer = self.eventloop.register_timer(self._snapshot_check)
        
        # Component references
        self.klippy_apis: APIComp = self.server.lookup_component("klippy_apis")
        
        self._register_notifications()
        self._register_endpoints()
        
        # Mark component as ready
        self._ready = True
        
        logging.info(f"Spool Tracker initialized with {self.current_filament_type}, "
                    f"remaining weight: {self.get_remaining_weight():.1f}g")

    def _initialize_filament_state(self):
        """Initialize filament type and weight from shared config and database."""
        # Load filament type from shared printer config
        try:
            filament_type = self.database.get_item(
                "HS3", "filament_type", "N/A"
            ).result()
            self.current_filament_type = filament_type or "N/A"
        except Exception as e:
            logging.warning(f"Failed to load filament type from database: {e}")
            self.current_filament_type = "N/A"
        
        # Initialize timestamps
        self.first_used: Optional[datetime] = None
        self.last_used: Optional[datetime] = None
        
        # Load remaining weight from database, default to 0 if not found
        try:
            remaining_weight = self.database.get_item(
                "HS3", "remaining_filament_weight", 0.0
            ).result()
            self.remaining_weight = float(remaining_weight)
        except Exception as e:
            logging.warning(f"Failed to load remaining filament weight from database: {e}")
            self.remaining_weight = 0.0
        
        # Load initial weight from database
        try:
            initial_weight = self.database.get_item(
                "HS3", "initial_filament_weight", 0.0
            ).result()
            self.initial_weight = float(initial_weight)
        except Exception as e:
            logging.warning(f"Failed to load initial filament weight from database: {e}")
            self.initial_weight = 0.0
        
        # Load used weight from database
        try:
            used_weight = self.database.get_item(
                "HS3", "used_filament_weight", 0.0
            ).result()
            self.used_weight = float(used_weight)
        except Exception as e:
            logging.warning(f"Failed to load used filament weight from database: {e}")
            self.used_weight = 0.0
        
        # Load used length from database (in meters)
        try:
            used_length = self.database.get_item(
                "HS3", "used_filament_length", 0.0
            ).result()
            self.used_length = float(used_length)
        except Exception as e:
            logging.warning(f"Failed to load used filament length from database: {e}")
            self.used_length = 0.0
        
        # Load spool QR code from database
        try:
            spool_qr_code = self.database.get_item(
                "HS3", "spool_qr_code", ""
            ).result()
            self.spool_qr_code = str(spool_qr_code) if spool_qr_code else ""
        except Exception:
            self.spool_qr_code = ""

        # If filament type is invalid or N/A, stop tracking
        if not self._filament_exists(self.current_filament_type) or self.current_filament_type == "N/A":
            self.current_filament_type = "N/A"
            self.remaining_weight = 0.0
            self.spool_qr_code = ""
            logging.info("Spool Tracker: Invalid or missing filament type, tracking disabled")

    def _initialize_odometer_state(self):
        """Initialize odometer X, Y, Z values from database."""
        try:
            self.odometer_x = float(self.database.get_item(
                "HS3", "odometer_x", 0.0
            ).result())
        except Exception as e:
            logging.warning(f"Failed to load odometer_x from database: {e}")
            self.odometer_x = 0.0
        
        try:
            self.odometer_y = float(self.database.get_item(
                "HS3", "odometer_y", 0.0
            ).result())
        except Exception as e:
            logging.warning(f"Failed to load odometer_y from database: {e}")
            self.odometer_y = 0.0
        
        try:
            self.odometer_z = float(self.database.get_item(
                "HS3", "odometer_z", 0.0
            ).result())
        except Exception as e:
            logging.warning(f"Failed to load odometer_z from database: {e}")
            self.odometer_z = 0.0
        
        try:
            self.odometer_e = float(self.database.get_item(
                "HS3", "odometer_e", 0.0
            ).result())
        except Exception as e:
            logging.warning(f"Failed to load odometer_e from database: {e}")
            self.odometer_e = 0.0
        
        logging.info(f"odometer initialized: X={self.odometer_x:.2f}mm, "
                    f"Y={self.odometer_y:.2f}mm, Z={self.odometer_z:.2f}mm, "
                    f"E={self.odometer_e:.2f}mm")

    def _initialize_tripmeter_state(self):
        """Initialize tripmeter X, Y, Z values from database."""
        try:
            self.tripmeter_x = float(self.database.get_item(
                "HS3", "tripmeter_x", 0.0
            ).result())
        except Exception as e:
            logging.warning(f"Failed to load tripmeter_x from database: {e}")
            self.tripmeter_x = 0.0
        
        try:
            self.tripmeter_y = float(self.database.get_item(
                "HS3", "tripmeter_y", 0.0
            ).result())
        except Exception as e:
            logging.warning(f"Failed to load tripmeter_y from database: {e}")
            self.tripmeter_y = 0.0
        
        try:
            self.tripmeter_z = float(self.database.get_item(
                "HS3", "tripmeter_z", 0.0
            ).result())
        except Exception as e:
            logging.warning(f"Failed to load tripmeter_z from database: {e}")
            self.tripmeter_z = 0.0
        
        try:
            self.tripmeter_e = float(self.database.get_item(
                "HS3", "tripmeter_e", 0.0
            ).result())
        except Exception as e:
            logging.warning(f"Failed to load tripmeter_e from database: {e}")
            self.tripmeter_e = 0.0
        
        logging.info(f"tripmeter initialized: X={self.tripmeter_x:.2f}mm, "
                    f"Y={self.tripmeter_y:.2f}mm, Z={self.tripmeter_z:.2f}mm, "
                    f"E={self.tripmeter_e:.2f}mm")

    def _initialize_nozzle_life_state(self):
        """Log nozzle life values already loaded into shared_printer_config by database.py."""
        logging.info(
            f"Nozzle life initialized: "
            f"total={self.shared_printer_config.nozzle_life:.3f}kg, "
            f"remaining={self.shared_printer_config.remaining_nozzle_life:.3f}kg"
        )

    def _load_custom_filaments_from_database(self):
        """Load custom filament definitions from database."""
        try:
            custom_filaments_data = self.database.get_item(
                "HS3", "custom_filaments", {}
            ).result()
            
            if isinstance(custom_filaments_data, dict):
                self.custom_filaments = custom_filaments_data
                if self.custom_filaments:
                    logging.info(f"Loaded {len(self.custom_filaments)} custom filament(s) from database: "
                               f"{list(self.custom_filaments.keys())}")
            else:
                logging.warning(f"Invalid custom_filaments data in database, expected dict, got {type(custom_filaments_data)}")
                self.custom_filaments = {}
                
        except Exception as e:
            logging.warning(f"Failed to load custom filaments from database: {e}")
            self.custom_filaments = {}


    def _register_notifications(self):
        """Register WebSocket notifications."""
        self.server.register_notification("spool_tracker:usage_updated")
        self.server.register_notification("spool_tracker:filament_changed")

    def _register_endpoints(self):
        """Register HTTP API endpoints."""
        self.server.register_endpoint(
            "/server/spool_tracker/status",
            ["GET", "POST"],
            self._handle_status_request,
        )
        self.server.register_endpoint(
            "/server/spool_tracker/filament",
            ["GET", "POST"],
            self._handle_filament_request,
        )
        self.server.register_endpoint(
            "/server/spool_tracker/custom_filament",
            ["POST"],
            self._handle_register_custom_filament,
        )
        self.server.register_endpoint(
            "/server/spool_tracker/snapshots",
            ["GET"],
            self._handle_snapshots_list,
        )
        self.server.register_endpoint(
            "/server/spool_tracker/snapshot",
            ["POST"],
            self._handle_snapshot_now,
        )

    async def component_init(self) -> None:
        """Initialize component after server startup."""
        # Start tracking when Klipper is ready
        self.server.register_event_handler(
            "server:klippy_ready", self._handle_klippy_ready
        )
        self.snapshot_timer.start(delay=SNAPSHOT_FIRST_DELAY)
        logging.info("Spool Tracker component initialized")

    # ------------------------------------------------------------------
    # Daily service-tracker snapshots
    # ------------------------------------------------------------------
    def build_snapshot(self) -> Dict[str, Any]:
        """Current service-tracker data as a permanent snapshot record."""
        spc = self.shared_printer_config
        today = date.today().isoformat()
        return {
            "id": str(uuid.uuid4()),
            "day": today,
            "captured_at": time.time(),
            "odometer": {
                "x": self.odometer_x, "y": self.odometer_y,
                "z": self.odometer_z, "e": self.odometer_e,
            },
            "tripmeter": {
                "x": self.tripmeter_x, "y": self.tripmeter_y,
                "z": self.tripmeter_z, "e": self.tripmeter_e,
            },
            "nozzle_life": float(getattr(spc, "nozzle_life", 0.) or 0.),
            "remaining_nozzle_life": float(
                getattr(spc, "remaining_nozzle_life", 0.) or 0.),
            "nozzle_size": getattr(spc, "nozzle", None),
            "nozzle_type": getattr(spc, "nozzle_type", None),
            "filament_type": self.current_filament_type,
            "spool_qr_code": self.spool_qr_code,
            "weights": {
                "initial_weight": self.initial_weight,
                "used_weight": self.used_weight,
                "remaining_weight": self.get_remaining_weight(),
            },
            "lengths": {
                "used_length": self.used_length,
                "remaining_length": self.get_remaining_length(),
            },
        }

    async def _snapshot_check(self, eventtime: float) -> float:
        """Write today's snapshot once (first opportunity after local
        midnight or after startup)."""
        try:
            today = date.today().isoformat()
            existing = await self.snapshot_ns.get(today, None)
            if existing is None:
                await self.capture_snapshot()
        except Exception:
            logging.exception("Spool Tracker: daily snapshot failed")
        return eventtime + SNAPSHOT_CHECK_INTERVAL

    async def capture_snapshot(self, force: bool = False) -> Dict[str, Any]:
        snap = self.build_snapshot()
        existing = await self.snapshot_ns.get(snap["day"], None)
        if isinstance(existing, dict) and not force:
            return existing
        if isinstance(existing, dict) and existing.get("id"):
            # keep the id stable for the day so collectors upsert in place
            snap["id"] = existing["id"]
        await self.snapshot_ns.insert(snap["day"], snap)
        logging.info(
            f"Spool Tracker: daily snapshot recorded for {snap['day']} "
            f"(odometer x={self.odometer_x:.0f} y={self.odometer_y:.0f} "
            f"z={self.odometer_z:.0f} e={self.odometer_e:.0f} mm)"
        )
        return snap

    async def _handle_snapshots_list(
        self, web_request: WebRequest
    ) -> Dict[str, Any]:
        since = web_request.get_str("since", None)
        limit = web_request.get_int("limit", 400)
        if limit <= 0 or limit > SNAPSHOT_LIST_MAX:
            limit = SNAPSHOT_LIST_MAX
        if since is not None:
            try:
                date.fromisoformat(since)
            except ValueError:
                raise self.server.error("'since' must be YYYY-MM-DD", 400)
        keys: List[str] = await self.snapshot_ns.keys()
        keys = sorted(k for k in keys if since is None or k >= since)
        page = keys[:limit]
        records = await self.snapshot_ns.get_batch(page) if page else {}
        snapshots = [records[k] for k in page if isinstance(records.get(k), dict)]
        return {
            "snapshots": snapshots,
            "count": len(snapshots),
            "latest_day": keys[-1] if keys else None,
            "has_more": len(keys) > limit,
        }

    async def _handle_snapshot_now(
        self, web_request: WebRequest
    ) -> Dict[str, Any]:
        force = web_request.get_boolean("force", False)
        snap = await self.capture_snapshot(force=force)
        return {"snapshot": snap}

    async def _handle_klippy_ready(self) -> None:
        """Subscribe to Klipper status updates when ready."""
        try:
            result: Dict[str, Dict[str, Any]]
            result = await self.klippy_apis.subscribe_objects(
                {"toolhead": ["position", "extruder"]}, 
                self._handle_status_update, 
                {}
            )
            
            toolhead = result.get("toolhead", {})
            self._current_extruder = toolhead.get("extruder", "extruder")
            position = toolhead.get("position", [None, None, None, None])
            
            # Initialize X, Y, Z, E positions for odometer tracking
            self._last_x_pos = position[0]
            self._last_y_pos = position[1]
            self._last_z_pos = position[2]
            self._last_e_pos = position[3]
            initial_e_pos = position[3]
            
            logging.debug(f"Initial position: X={self._last_x_pos}, Y={self._last_y_pos}, "
                         f"Z={self._last_z_pos}, E={initial_e_pos}")
            if initial_e_pos is not None:
                self._highest_epos = initial_e_pos
                self.report_timer.start()
                logging.info("Spool Tracker: Started monitoring filament usage, odometer, and tripmeter")
            else:
                logging.error("Spool Tracker: Unable to subscribe to extruder position")
                raise self.server.error("Unable to subscribe to extruder position")
        except Exception as e:
            logging.error(f"Spool Tracker: Failed to initialize Klipper monitoring: {e}")
            raise

    def _handle_status_update(self, status: Dict[str, Any], _: float) -> None:
        """Handle Klipper status updates to track filament usage, odometer, and tripmeter."""
        toolhead: Optional[Dict[str, Any]] = status.get("toolhead")
        if toolhead is None:
            return
        
        position = toolhead.get("position", [None, None, None, None])
        
        # Track X, Y, Z, E movement for odometer (always track, independent of filament)
        x_pos, y_pos, z_pos, e_pos = position[0], position[1], position[2], position[3]
        
        if x_pos is not None and self._last_x_pos is not None:
            delta_x = abs(x_pos - self._last_x_pos)
            self.odometer_x += delta_x
            self.tripmeter_x += delta_x
        if y_pos is not None and self._last_y_pos is not None:
            delta_y = abs(y_pos - self._last_y_pos)
            self.odometer_y += delta_y
            self.tripmeter_y += delta_y
        if z_pos is not None and self._last_z_pos is not None:
            delta_z = abs(z_pos - self._last_z_pos)
            self.odometer_z += delta_z
            self.tripmeter_z += delta_z
        if e_pos is not None and self._last_e_pos is not None:
            delta_e = abs(e_pos - self._last_e_pos)
            self.odometer_e += delta_e
            self.tripmeter_e += delta_e
        
        # Update last positions
        if x_pos is not None:
            self._last_x_pos = x_pos
        if y_pos is not None:
            self._last_y_pos = y_pos
        if z_pos is not None:
            self._last_z_pos = z_pos
        if e_pos is not None:
            self._last_e_pos = e_pos
        
        # Track forward E extrusion and extruder changes (always active)
        epos: float = e_pos if e_pos is not None else self._highest_epos
        extr = toolhead.get("extruder", self._current_extruder)
        
        if extr != self._current_extruder:
            self._highest_epos = epos
            self._current_extruder = extr
            logging.debug(f"Switched to extruder: {extr}")
        elif epos > self._highest_epos:
            extrusion_length = epos - self._highest_epos
            # Always accumulate for nozzle life tracking
            self._pending_nozzle_e_mm += extrusion_length
            # Only accumulate for filament usage if tracking is active
            if self._can_track():
                self._add_extrusion(extrusion_length)
            self._highest_epos = epos

    def _add_extrusion(self, length_mm: float) -> None:
        """Add extrusion to pending usage accumulator."""
        self.pending_usage_mm += length_mm
        logging.debug(f"Added {length_mm:.3f}mm extrusion, "
                     f"total pending: {self.pending_usage_mm:.3f}mm")

    def _can_track(self) -> bool:
        if not self._filament_exists(self.current_filament_type):
            return False
        if self.current_filament_type == "N/A":
            return False

        specs = self._get_filament_specs(self.current_filament_type)
        if specs.get("density", 0) <= 0:
            return False
        if specs.get("diameter", 0) <= 0:
            return False
        if self.remaining_weight <= 0:
            return False

        return True

    def _filament_exists(self, filament_type: str) -> bool:
        """Check if filament type exists in predefined or custom types."""
        return filament_type in FILAMENT_TYPES or filament_type in self.custom_filaments
    
    def _get_filament_specs(self, filament_type: str) -> Dict[str, Any]:
        """Get filament specifications from predefined or custom types."""
        if filament_type in FILAMENT_TYPES:
            return FILAMENT_TYPES[filament_type]
        elif filament_type in self.custom_filaments:
            return self.custom_filaments[filament_type]
        else:
            raise ValueError(f"Unknown filament type: {filament_type}")
    
    def _add_custom_filament(self, filament_type: str, specs: Dict[str, Any]) -> None:
        """Add a custom filament type with specifications."""
        required_fields = ["density", "diameter"]
        for field in required_fields:
            if field not in specs:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate numerical values
        if specs["density"] <= 0:
            raise ValueError("Density must be positive")
        if specs["diameter"] <= 0:
            raise ValueError("Diameter must be positive")
        
        # Add the filament
        self.custom_filaments[filament_type] = {
            "density": specs["density"],
            "diameter": specs["diameter"],
            "name": specs.get("name", filament_type)
        }
    
    def _calculate_weight_from_length(self, length_mm: float) -> float:
        """Calculate filament weight from length using current filament specs."""
        if not self._filament_exists(self.current_filament_type):
            return 0.0
        
        specs = self._get_filament_specs(self.current_filament_type)
        density = specs["density"]  # g/cm³
        diameter = specs["diameter"]  # mm
        
        # Calculate volume in cm³ and convert length from mm to cm
        radius_cm = (diameter / 2.0) / 10.0
        length_cm = length_mm / 10.0
        volume = math.pi * (radius_cm ** 2) * length_cm
        
        return density * volume

    def _calculate_weight_for_nozzle(self, length_mm: float) -> Optional[float]:
        """Calculate filament weight from length for nozzle life tracking.
        
        Returns weight in grams, or None if filament specs are missing.
        """
        if not self._filament_exists(self.current_filament_type):
            logging.debug("Skipping nozzle life update: no valid filament type")
            return None
        
        if self.current_filament_type == "N/A":
            logging.debug("Skipping nozzle life update: filament type is N/A")
            return None
        
        specs = self._get_filament_specs(self.current_filament_type)
        density = specs.get("density", 0)
        diameter = specs.get("diameter", 0)
        
        if density <= 0:
            logging.debug("Skipping nozzle life update: filament density is missing or zero")
            return None
        if diameter <= 0:
            logging.debug("Skipping nozzle life update: filament diameter is missing or zero")
            return None
        
        radius_cm = (diameter / 2.0) / 10.0
        length_cm = length_mm / 10.0
        volume = math.pi * (radius_cm ** 2) * length_cm
        return density * volume

    async def _report_usage(self, eventtime: float) -> float:
        """Periodically report filament usage to logs and sync to database."""
        
        # Filament usage tracking (only when filament tracking is active)
        if self._can_track() and self.pending_usage_mm > 0:
            weight_used = self._calculate_weight_from_length(self.pending_usage_mm)
            self.used_weight += weight_used
            self.used_length += self.pending_usage_mm / 1000.0  # Convert mm to meters
            
            self.remaining_weight = max(0, self.remaining_weight - weight_used)
            
            # Update timestamps
            if self.first_used is None:
                self.first_used = datetime.now()
            self.last_used = datetime.now()
            
            logging.debug(f"Consumed {self.pending_usage_mm:.1f}mm ({weight_used:.2f}g), "
                         f"remaining: {self.remaining_weight:.1f}g")
            
            # Emit usage update via WebSocket
            self.server.send_event(
                "spool_tracker:usage_updated",
                {
                    "used_length_mm": self.pending_usage_mm,
                    "used_weight_g": weight_used,
                    "total_used_weight": 0,  # Not tracked anymore, keeping for compatibility
                    "remaining_weight": self.remaining_weight,
                    "filament_type": self.current_filament_type,
                    "can_track": self._can_track(),
                    "spool_qr_code": self.spool_qr_code,
                }
            )
            
            # Reset pending usage accumulator
            self.pending_usage_mm = 0.0
        
        # Nozzle life tracking (always active, independent of filament weight tracking)
        if self._pending_nozzle_e_mm > 0:
            nozzle_weight_g = self._calculate_weight_for_nozzle(
                self._pending_nozzle_e_mm
            )
            if nozzle_weight_g is not None:
                nozzle_weight_kg = nozzle_weight_g / 1000.0
                self.shared_printer_config.remaining_nozzle_life = (
                    self.shared_printer_config.remaining_nozzle_life - nozzle_weight_kg
                )
                logging.debug(
                    f"Nozzle life: consumed {self._pending_nozzle_e_mm:.1f}mm "
                    f"({nozzle_weight_g:.2f}g / {nozzle_weight_kg:.4f}kg), "
                    f"remaining: {self.shared_printer_config.remaining_nozzle_life:.3f}kg"
                )
            self._pending_nozzle_e_mm = 0.0
        
        # Sync to database (even if no usage was logged this cycle)
        try:
            self.database.insert_item("HS3", "remaining_filament_weight", 
                                    self.remaining_weight)
            self.database.insert_item("HS3", "initial_filament_weight", 
                                    self.initial_weight)
            self.database.insert_item("HS3", "used_filament_weight", 
                                    self.used_weight)
            self.database.insert_item("HS3", "used_filament_length",
                                    self.used_length)
            self.database.insert_item("HS3", "spool_qr_code",
                                    self.spool_qr_code)
        except Exception as e:
            logging.warning(f"Failed to sync filament usage to database: {e}")

        # Sync odometer to database
        try:
            self.database.insert_item("HS3", "odometer_x", self.odometer_x)
            self.database.insert_item("HS3", "odometer_y", self.odometer_y)
            self.database.insert_item("HS3", "odometer_z", self.odometer_z)
            self.database.insert_item("HS3", "odometer_e", self.odometer_e)
        except Exception as e:
            logging.warning(f"Failed to sync odometer to database: {e}")
        
        # Sync tripmeter to database
        try:
            self.database.insert_item("HS3", "tripmeter_x", self.tripmeter_x)
            self.database.insert_item("HS3", "tripmeter_y", self.tripmeter_y)
            self.database.insert_item("HS3", "tripmeter_z", self.tripmeter_z)
            self.database.insert_item("HS3", "tripmeter_e", self.tripmeter_e)
        except Exception as e:
            logging.warning(f"Failed to sync tripmeter to database: {e}")
        
        # Sync nozzle life to database
        try:
            self.database.insert_item(
                "HS3", "nozzle_life", self.shared_printer_config.nozzle_life
            )
            self.database.insert_item(
                "HS3", "remaining_nozzle_life",
                self.shared_printer_config.remaining_nozzle_life
            )
        except Exception as e:
            logging.warning(f"Failed to sync nozzle life to database: {e}")
        
        # Return next timer interval
        return eventtime + self.sync_rate_seconds

    def get_remaining_weight(self) -> float:
        """Get current remaining filament weight."""
        return max(0, self.remaining_weight)

    def get_remaining_length(self) -> float:
        """Calculate remaining filament length in meters from weight."""
        if not self._filament_exists(self.current_filament_type) or self.remaining_weight <= 0:
            return 0.0
        
        specs = self._get_filament_specs(self.current_filament_type)
        density = specs["density"]  # g/cm³
        diameter = specs["diameter"]  # mm
        
        # Calculate length from weight
        # weight = density * volume
        # volume = pi * r^2 * length
        # length = weight / (density * pi * r^2)
        
        radius_cm = (diameter / 2.0) / 10.0
        area_cm2 = math.pi * (radius_cm ** 2)
        
        length_cm = self.remaining_weight / (density * area_cm2)
        return length_cm / 100.0  # Convert cm to meters

    def get_usage_percentage(self) -> float:
        """Calculate percentage of filament used from initial weight."""
        if self.initial_weight <= 0:
            return 0.0
        
        return (self.used_weight / self.initial_weight) * 100.0

    async def _handle_status_request(self, web_request: WebRequest):
        """Handle status GET/POST requests."""
        
        # Return minimal valid response if component not fully initialized
        if not self._ready:
            return {
                "filament_type": "N/A",
                "filament_name": "Initializing...",
                "filament_specs": {"density": 0, "diameter": 0},
                "weights": {
                    "initial_weight": 0.0,
                    "used_weight": 0.0,
                    "remaining_weight": 0.0,
                },
                "lengths": {
                    "used_length": 0.0,
                    "remaining_length": 0.0,
                },
                "usage_percentage": 0.0,
                "pending_usage_mm": 0.0,
                "can_track": False,
                "timestamps": {"first_used": None, "last_used": None},
                "available_filaments": {"predefined": [], "custom": [], "all": []},
                "odometer": {"x": 0.0, "y": 0.0, "z": 0.0, "e": 0.0},
                "tripmeter": {"x": 0.0, "y": 0.0, "z": 0.0, "e": 0.0},
                "nozzle_life": 0.0,
                "remaining_nozzle_life": 0.0,
            }
        
        # Handle POST updates
        if web_request.get_action() == "POST":
            # Handle odometer updates (can directly set values)
            odometer_x = web_request.get_float("odometer_x", None)
            odometer_y = web_request.get_float("odometer_y", None)
            odometer_z = web_request.get_float("odometer_z", None)
            
            if odometer_x is not None:
                if odometer_x < 0:
                    raise self.server.error("odometer X value must be non-negative")
                self.odometer_x = odometer_x
                logging.info(f"Updated odometer_x to {odometer_x:.2f}mm")
                try:
                    self.database.insert_item("HS3", "odometer_x", odometer_x)
                except Exception as e:
                    logging.warning(f"Failed to update odometer_x in database: {e}")
            
            if odometer_y is not None:
                if odometer_y < 0:
                    raise self.server.error("odometer Y value must be non-negative")
                self.odometer_y = odometer_y
                logging.info(f"Updated odometer_y to {odometer_y:.2f}mm")
                try:
                    self.database.insert_item("HS3", "odometer_y", odometer_y)
                except Exception as e:
                    logging.warning(f"Failed to update odometer_y in database: {e}")
            
            if odometer_z is not None:
                if odometer_z < 0:
                    raise self.server.error("odometer Z value must be non-negative")
                self.odometer_z = odometer_z
                logging.info(f"Updated odometer_z to {odometer_z:.2f}mm")
                try:
                    self.database.insert_item("HS3", "odometer_z", odometer_z)
                except Exception as e:
                    logging.warning(f"Failed to update odometer_z in database: {e}")
            
            odometer_e = web_request.get_float("odometer_e", None)
            if odometer_e is not None:
                if odometer_e < 0:
                    raise self.server.error("odometer E value must be non-negative")
                self.odometer_e = odometer_e
                logging.info(f"Updated odometer_e to {odometer_e:.2f}mm")
                try:
                    self.database.insert_item("HS3", "odometer_e", odometer_e)
                except Exception as e:
                    logging.warning(f"Failed to update odometer_e in database: {e}")
            
            # Handle tripmeter reset (only accepts reset to 0)
            reset_tripmeter_x = web_request.get_boolean("reset_tripmeter_x", False)
            reset_tripmeter_y = web_request.get_boolean("reset_tripmeter_y", False)
            reset_tripmeter_z = web_request.get_boolean("reset_tripmeter_z", False)
            reset_tripmeter_e = web_request.get_boolean("reset_tripmeter_e", False)
            
            if reset_tripmeter_x:
                self.tripmeter_x = 0.0
                logging.info("Reset tripmeter_x to 0.0mm")
                try:
                    self.database.insert_item("HS3", "tripmeter_x", 0.0)
                except Exception as e:
                    logging.warning(f"Failed to reset tripmeter_x in database: {e}")
            
            if reset_tripmeter_y:
                self.tripmeter_y = 0.0
                logging.info("Reset tripmeter_y to 0.0mm")
                try:
                    self.database.insert_item("HS3", "tripmeter_y", 0.0)
                except Exception as e:
                    logging.warning(f"Failed to reset tripmeter_y in database: {e}")
            
            if reset_tripmeter_z:
                self.tripmeter_z = 0.0
                logging.info("Reset tripmeter_z to 0.0mm")
                try:
                    self.database.insert_item("HS3", "tripmeter_z", 0.0)
                except Exception as e:
                    logging.warning(f"Failed to reset tripmeter_z in database: {e}")
            
            if reset_tripmeter_e:
                self.tripmeter_e = 0.0
                logging.info("Reset tripmeter_e to 0.0mm")
                try:
                    self.database.insert_item("HS3", "tripmeter_e", 0.0)
                except Exception as e:
                    logging.warning(f"Failed to reset tripmeter_e in database: {e}")
            
            # Handle nozzle life setting (sets both nozzle_life and remaining_nozzle_life)
            new_nozzle_life = web_request.get_float("nozzle_life", None)
            if new_nozzle_life is not None:
                if new_nozzle_life < 0:
                    raise self.server.error("nozzle_life must be non-negative")
                old_nozzle_life = self.shared_printer_config.nozzle_life
                self.shared_printer_config.nozzle_life = new_nozzle_life
                self.shared_printer_config.remaining_nozzle_life = new_nozzle_life
                logging.info(f"Set nozzle_life to {new_nozzle_life:.3f}kg "
                           f"(was {old_nozzle_life:.3f}kg), "
                           f"remaining_nozzle_life reset to {new_nozzle_life:.3f}kg")
                try:
                    self.database.insert_item("HS3", "nozzle_life", new_nozzle_life)
                    self.database.insert_item("HS3", "remaining_nozzle_life",
                                            new_nozzle_life)
                except Exception as e:
                    logging.warning(f"Failed to update nozzle_life in database: {e}")

            # Handle remaining nozzle life reset (resets back to nozzle_life value)
            reset_remaining_nozzle_life = web_request.get_boolean(
                "reset_remaining_nozzle_life", False
            )
            if reset_remaining_nozzle_life:
                self.shared_printer_config.remaining_nozzle_life = (
                    self.shared_printer_config.nozzle_life
                )
                logging.info(
                    f"Reset remaining_nozzle_life to "
                    f"{self.shared_printer_config.nozzle_life:.3f}kg"
                )
                try:
                    self.database.insert_item(
                        "HS3", "remaining_nozzle_life",
                        self.shared_printer_config.nozzle_life
                    )
                except Exception as e:
                    logging.warning(
                        f"Failed to reset remaining_nozzle_life in database: {e}"
                    )

            # Activity log events (additive)
            if new_nozzle_life is not None or reset_remaining_nozzle_life:
                self.server.send_event(
                    "spool_tracker:nozzle_life_changed",
                    {
                        "nozzle_life": self.shared_printer_config.nozzle_life,
                        "remaining_nozzle_life":
                            self.shared_printer_config.remaining_nozzle_life,
                        "reset": bool(reset_remaining_nozzle_life),
                    }
                )
            odo_set = {
                axis: val for axis, val in (
                    ("x", odometer_x), ("y", odometer_y),
                    ("z", odometer_z), ("e", odometer_e),
                ) if val is not None
            }
            trip_reset = [
                axis for axis, flag in (
                    ("x", reset_tripmeter_x), ("y", reset_tripmeter_y),
                    ("z", reset_tripmeter_z), ("e", reset_tripmeter_e),
                ) if flag
            ]
            if odo_set or trip_reset:
                self.server.send_event(
                    "spool_tracker:meters_changed",
                    {"odometer": odo_set, "tripmeter_reset": trip_reset}
                )
        
        # Return current status (for both GET and POST)
        if not self._filament_exists(self.current_filament_type):
            filament_spec = {"density": 0, "diameter": 0, "name": "Unknown"}
        else:
            filament_spec = self._get_filament_specs(self.current_filament_type)
        
        all_types = list(FILAMENT_TYPES.keys()) + list(self.custom_filaments.keys())
        
        return {
            "filament_type": self.current_filament_type,
            "filament_name": filament_spec["name"],
            "filament_specs": {
                "density": filament_spec["density"],
                "diameter": filament_spec["diameter"],
            },
            "weights": {
                "initial_weight": self.initial_weight,
                "used_weight": self.used_weight,
                "remaining_weight": self.get_remaining_weight(),
            },
            "lengths": {
                "used_length": self.used_length,
                "remaining_length": self.get_remaining_length(),
            },
            "usage_percentage": self.get_usage_percentage(),
            "pending_usage_mm": self.pending_usage_mm,
            "can_track": self._can_track(),
            "spool_qr_code": self.spool_qr_code,
            "timestamps": {
                "first_used": self.first_used.isoformat() if self.first_used else None,
                "last_used": self.last_used.isoformat() if self.last_used else None,
            },
            "available_filaments": {
                "predefined": list(FILAMENT_TYPES.keys()),
                "custom": list(self.custom_filaments.keys()),
                "all": all_types,
            },
            "odometer": {
                "x": self.odometer_x,
                "y": self.odometer_y,
                "z": self.odometer_z,
                "e": self.odometer_e,
            },
            "tripmeter": {
                "x": self.tripmeter_x,
                "y": self.tripmeter_y,
                "z": self.tripmeter_z,
                "e": self.tripmeter_e,
            },
            "nozzle_life": self.shared_printer_config.nozzle_life,
            "remaining_nozzle_life": self.shared_printer_config.remaining_nozzle_life,
        }

    async def _handle_filament_request(self, web_request: WebRequest):
        """Handle filament type GET/POST requests."""
        
        # Return minimal valid response if component not fully initialized
        if not self._ready:
            return {
                "filament_type": "N/A",
                "filament_specs": {"density": 0, "diameter": 0, "name": "Initializing..."},
                "remaining_weight": 0.0,
                "can_track": False,
                "spool_qr_code": "",
                "available_types": {"predefined": [], "custom": [], "all": []},
            }
        
        if web_request.get_action() == "POST":
            # Extract both filament type and weight from request
            new_type = web_request.get_str("filament_type", None)
            new_weight = web_request.get_float("weight", None)
            new_qr_code = web_request.get_str("qr_code", None)
            
            # If only weight is provided (no filament type), sync filament type from shared config first
            if new_weight is not None and new_type is None:
                try:
                    # Get current filament type from shared config (which reflects database state)
                    shared_filament_type = getattr(self.shared_printer_config, 'filament', '') or "N/A"
                    
                    # If shared config filament type differs from our cached type, update it
                    if shared_filament_type != self.current_filament_type:
                        logging.info(f"Syncing filament type from shared config: {self.current_filament_type} -> {shared_filament_type}")
                        old_type = self.current_filament_type
                        self.current_filament_type = shared_filament_type
                        self._last_known_filament_type = shared_filament_type
                        
                        # Send filament change notification
                        if self._filament_exists(shared_filament_type):
                            self.server.send_event(
                                "spool_tracker:filament_changed",
                                {
                                    "old_type": old_type,
                                    "new_type": shared_filament_type,
                                    "new_specs": self._get_filament_specs(shared_filament_type),
                                    "spool_qr_code": self.spool_qr_code,
                                }
                            )
                except Exception as e:
                    logging.warning(f"Failed to sync filament type from shared config during weight update: {e}")
            
            # Handle filament type change (explicit update)
            if new_type is not None:
                # Validate filament type exists
                if not self._filament_exists(new_type):
                    raise self.server.error(f"Unknown filament type: {new_type}")
                
                # Update database with new filament type
                try:
                    self.database.insert_item("HS3", "filament_type", new_type)
                except Exception as e:
                    logging.warning(f"Failed to update filament type in database: {e}")
                
                # Update local state immediately to prevent reset-to-0 behavior
                old_type = self.current_filament_type
                self.current_filament_type = new_type
                self._last_known_filament_type = new_type
                
                logging.info(f"Updated filament type from {old_type} to {new_type}")
                
                # Send filament change notification
                self.server.send_event(
                    "spool_tracker:filament_changed",
                    {
                        "old_type": old_type,
                        "new_type": new_type,
                        "new_specs": self._get_filament_specs(new_type),
                        "spool_qr_code": self.spool_qr_code,
                    }
                )

            # Handle spool QR code
            if new_qr_code is not None:
                # Explicit QR code provided — set it
                self.spool_qr_code = new_qr_code
                logging.info(f"Spool QR code set to: {new_qr_code}")
            elif new_type is not None:
                # Filament type changed without QR code — clear it (manual workflow)
                self.spool_qr_code = ""
                logging.info("Spool QR code cleared (manual filament change)")
            # If only weight updated (no new_type, no new_qr_code) — leave qr_code unchanged

            # Persist QR code and sync to shared config
            if new_qr_code is not None or new_type is not None:
                try:
                    self.database.insert_item("HS3", "spool_qr_code", self.spool_qr_code)
                except Exception as e:
                    logging.warning(f"Failed to persist spool_qr_code: {e}")
                self.shared_printer_config.spool_qr_code = self.spool_qr_code

            # Handle weight change
            if new_weight is not None:
                if new_weight < 0:
                    raise self.server.error("Weight must be non-negative")
                
                old_weight = self.remaining_weight
                self.remaining_weight = new_weight
                
                # Set initial_weight and reset tracking counters when user sets a new weight
                self.initial_weight = new_weight
                self.used_weight = 0.0
                self.used_length = 0.0
                
                logging.info(f"Updated remaining weight from {old_weight:.1f}g to {new_weight:.1f}g")
                logging.info(f"Set initial_weight to {new_weight:.1f}g and reset usage tracking")
                
                # Sync to database immediately
                try:
                    self.database.insert_item("HS3", "remaining_filament_weight", 
                                            new_weight)
                    self.database.insert_item("HS3", "initial_filament_weight", 
                                            new_weight)
                    self.database.insert_item("HS3", "used_filament_weight", 
                                            0.0)
                    self.database.insert_item("HS3", "used_filament_length", 
                                            0.0)
                except Exception as e:
                    logging.warning(f"Failed to update database: {e}")
                
                # Send usage update notification
                self.server.send_event(
                    "spool_tracker:usage_updated",
                    {
                        "weight_changed": True,
                        "old_weight": old_weight,
                        "new_weight": new_weight,
                        "remaining_weight": self.remaining_weight,
                        "total_used_weight": 0,
                        "filament_type": self.current_filament_type,
                        "can_track": self._can_track(),
                        "spool_qr_code": self.spool_qr_code,
                    }
                )

            if new_weight is not None or new_qr_code is not None:
                # Activity log: a spool was physically loaded / registered.
                self.server.send_event(
                    "spool_tracker:spool_loaded",
                    {
                        "filament_type": self.current_filament_type,
                        "weight": new_weight,
                        "qr_code": self.spool_qr_code,
                        "type_changed": new_type is not None,
                    }
                )

            if new_type is not None or new_weight is not None or new_qr_code is not None:
                # Any spool change (type, weight, or QR) means the operator is
                # physically handling filament, so require a fresh prime
                # confirmation.  set_prime_state() no-ops when already 0.
                kconn = self.server.lookup_component("klippy_connection")
                kconn.set_prime_state(0, "spool_change")

        # Return current filament info (for both GET and POST)
        all_types = list(FILAMENT_TYPES.keys()) + list(self.custom_filaments.keys())
        
        if self._filament_exists(self.current_filament_type):
            filament_specs = self._get_filament_specs(self.current_filament_type)
        else:
            filament_specs = {"density": 0, "diameter": 0, "name": "Unknown"}
        
        return {
            "filament_type": self.current_filament_type,
            "filament_specs": filament_specs,
            "remaining_weight": self.remaining_weight,
            "can_track": self._can_track(),
            "spool_qr_code": self.spool_qr_code,
            "available_types": {
                "predefined": list(FILAMENT_TYPES.keys()),
                "custom": list(self.custom_filaments.keys()),
                "all": all_types,
            },
        }

    async def _handle_register_custom_filament(self, web_request: WebRequest):
        """Handle POST requests to register a custom filament with specs."""
        
        # Return error if component not fully initialized
        if not self._ready:
            raise self.server.error("Spool Tracker not initialized yet")
        
        # Extract required parameters
        filament_name = web_request.get_str("name")
        density = web_request.get_float("density")
        diameter = web_request.get_float("diameter")
        
        # Validate parameters
        if not filament_name:
            raise self.server.error("Missing required parameter: name")
        if density is None or density < 0:
            density = 0.0
        if diameter is None or diameter < 0:
            diameter = 0.0
        
        # Register the custom filament
        self.custom_filaments[filament_name] = {
            "density": density,
            "diameter": diameter,
            "name": filament_name
        }
        
        # Persist custom filaments to database
        try:
            self.database.insert_item("HS3", "custom_filaments", self.custom_filaments)
            logging.info(f"Registered and saved custom filament: {filament_name} "
                        f"(density={density}g/cm³, diameter={diameter}mm)")
        except Exception as e:
            logging.error(f"Failed to save custom filament to database: {e}")
            # Remove from memory if database save failed
            del self.custom_filaments[filament_name]
            raise self.server.error(f"Failed to save custom filament: {e}")
        
        # Return success response with the registered filament specs
        return {
            "filament_name": filament_name,
            "specs": self.custom_filaments[filament_name],
            "registered": True
        }

    async def close(self):
        """Clean shutdown of component."""
        logging.info("Shutting down Spool Tracker")
        self.report_timer.stop()
        
        # Final database sync
        try:
            self.database.insert_item("HS3", "remaining_filament_weight", 
                                    self.remaining_weight)
            self.database.insert_item("HS3", "initial_filament_weight", 
                                    self.initial_weight)
            self.database.insert_item("HS3", "used_filament_weight", 
                                    self.used_weight)
            self.database.insert_item("HS3", "used_filament_length", 
                                    self.used_length)
        except Exception as e:
            logging.warning(f"Failed final database sync: {e}")
        
        # Final odometer sync
        try:
            self.database.insert_item("HS3", "odometer_x", self.odometer_x)
            self.database.insert_item("HS3", "odometer_y", self.odometer_y)
            self.database.insert_item("HS3", "odometer_z", self.odometer_z)
            self.database.insert_item("HS3", "odometer_e", self.odometer_e)
        except Exception as e:
            logging.warning(f"Failed final odometer sync: {e}")
        
        # Final tripmeter sync
        try:
            self.database.insert_item("HS3", "tripmeter_x", self.tripmeter_x)
            self.database.insert_item("HS3", "tripmeter_y", self.tripmeter_y)
            self.database.insert_item("HS3", "tripmeter_z", self.tripmeter_z)
            self.database.insert_item("HS3", "tripmeter_e", self.tripmeter_e)
        except Exception as e:
            logging.warning(f"Failed final tripmeter sync: {e}")
        
        # Final nozzle life sync
        try:
            self.database.insert_item(
                "HS3", "nozzle_life", self.shared_printer_config.nozzle_life
            )
            self.database.insert_item(
                "HS3", "remaining_nozzle_life",
                self.shared_printer_config.remaining_nozzle_life
            )
        except Exception as e:
            logging.warning(f"Failed final nozzle life sync: {e}")
        
        # Final custom filaments sync
        try:
            if self.custom_filaments:
                self.database.insert_item("HS3", "custom_filaments", self.custom_filaments)
                logging.info(f"Saved {len(self.custom_filaments)} custom filament(s) to database")
        except Exception as e:
            logging.warning(f"Failed final custom filaments sync: {e}")
        
        # Log final stats
        if self.remaining_weight > 0:
            logging.info(f"Final remaining weight: {self.remaining_weight:.1f}g")
        if self.initial_weight > 0:
            logging.info(f"Final initial weight: {self.initial_weight:.1f}g, "
                        f"used weight: {self.used_weight:.1f}g, "
                        f"used length: {self.used_length:.2f}m")
        logging.info(f"Final odometer: X={self.odometer_x:.2f}mm, "
                    f"Y={self.odometer_y:.2f}mm, Z={self.odometer_z:.2f}mm, "
                    f"E={self.odometer_e:.2f}mm")
        logging.info(f"Final tripmeter: X={self.tripmeter_x:.2f}mm, "
                    f"Y={self.tripmeter_y:.2f}mm, Z={self.tripmeter_z:.2f}mm, "
                    f"E={self.tripmeter_e:.2f}mm")
        logging.info(
            f"Final nozzle life: total={self.shared_printer_config.nozzle_life:.3f}kg, "
            f"remaining={self.shared_printer_config.remaining_nozzle_life:.3f}kg"
        )


def load_component(config: ConfigHelper) -> SpoolTracker:
    return SpoolTracker(config)