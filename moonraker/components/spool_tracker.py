# Spool Tracker Component for Moonraker
#
# Copyright (C) 2024 <Your Name>
#
# This file may be distributed under the terms of the GNU GPLv3 license.

from __future__ import annotations
import asyncio
import logging
import math
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Dict,
    Any,
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


class SpoolTracker:
    def __init__(self, config: ConfigHelper):
        self.server = config.get_server()
        self.eventloop = self.server.get_event_loop()
        
        # Configuration
        self.sync_rate_seconds = config.getint("sync_rate", default=5, minval=1)
        
        # Custom filament types (runtime additions)
        self.custom_filaments: Dict[str, Dict[str, Any]] = {}
        
        # Database and shared config references
        self.database: MoonrakerDatabase = self.server.lookup_component("database")
        self.shared_printer_config = self.server.shared_printer_config
        
        # Initialize filament state from shared config and database
        self._initialize_filament_state()
        
        # Usage tracking
        self.pending_usage_mm = 0.0  # Accumulated extrusion in mm
        self._highest_epos: float = 0.0
        self._current_extruder: str = "extruder"
        self._last_known_filament_type: str = self.current_filament_type
        
        # Timer for periodic reporting and database sync
        self.report_timer = self.eventloop.register_timer(self._report_usage)
        
        # Component references
        self.klippy_apis: APIComp = self.server.lookup_component("klippy_apis")
        
        self._register_notifications()
        self._register_endpoints()
        
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
        
        # If filament type is invalid or N/A, stop tracking
        if not self._filament_exists(self.current_filament_type) or self.current_filament_type == "N/A":
            self.current_filament_type = "N/A"
            self.remaining_weight = 0.0
            logging.info("Spool Tracker: Invalid or missing filament type, tracking disabled")

    def _register_notifications(self):
        """Register WebSocket notifications."""
        self.server.register_notification("spool_tracker:usage_updated")
        self.server.register_notification("spool_tracker:filament_changed")

    def _register_endpoints(self):
        """Register HTTP API endpoints."""
        self.server.register_endpoint(
            "/server/spool_tracker/status",
            ["GET"],
            self._handle_status_request,
        )
        self.server.register_endpoint(
            "/server/spool_tracker/filament",
            ["GET", "POST"],
            self._handle_filament_request,
        )

    async def component_init(self) -> None:
        """Initialize component after server startup."""
        # Start tracking when Klipper is ready
        self.server.register_event_handler(
            "server:klippy_ready", self._handle_klippy_ready
        )
        logging.info("Spool Tracker component initialized")

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
            initial_e_pos = toolhead.get("position", [None]*4)[3]
            
            logging.debug(f"Initial E position: {initial_e_pos}")
            if initial_e_pos is not None:
                self._highest_epos = initial_e_pos
                self.report_timer.start()
                logging.info("Spool Tracker: Started monitoring filament usage")
            else:
                logging.error("Spool Tracker: Unable to subscribe to extruder position")
                raise self.server.error("Unable to subscribe to extruder position")
        except Exception as e:
            logging.error(f"Spool Tracker: Failed to initialize Klipper monitoring: {e}")
            raise

    def _handle_status_update(self, status: Dict[str, Any], _: float) -> None:
        """Handle Klipper status updates to track filament usage."""
        # Only track if we have valid filament type and weight
        if not self._can_track():
            return
            
        toolhead: Optional[Dict[str, Any]] = status.get("toolhead")
        if toolhead is None:
            return

        epos: float = toolhead.get("position", [0, 0, 0, self._highest_epos])[3]
        extr = toolhead.get("extruder", self._current_extruder)
        
        # Handle extruder changes
        if extr != self._current_extruder:
            self._highest_epos = epos
            self._current_extruder = extr
            logging.debug(f"Switched to extruder: {extr}")
        elif epos > self._highest_epos:
            # Calculate extrusion length
            extrusion_length = epos - self._highest_epos
            self._add_extrusion(extrusion_length)
            self._highest_epos = epos

    def _add_extrusion(self, length_mm: float) -> None:
        """Add extrusion to pending usage accumulator."""
        self.pending_usage_mm += length_mm
        logging.debug(f"Added {length_mm:.3f}mm extrusion, "
                     f"total pending: {self.pending_usage_mm:.3f}mm")

    def _can_track(self) -> bool:
        """Check if tracking is possible (valid filament type and weight > 0)."""
        return (self._filament_exists(self.current_filament_type) and 
                self.current_filament_type != "N/A" and 
                self.remaining_weight > 0)

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
                raise ValueError(f"Missing required field '{field}' in filament specs")
            if not isinstance(specs[field], (int, float)) or specs[field] <= 0:
                raise ValueError(f"Field '{field}' must be a positive number")
        
        # Add default name if not provided
        if "name" not in specs:
            specs["name"] = f"Custom {filament_type}"
        
        self.custom_filaments[filament_type] = specs
        logging.info(f"Added custom filament type '{filament_type}': {specs}")

    def _length_to_weight(self, length_mm: float) -> float:
        """Convert filament length to weight using current filament specs."""
        if not self._filament_exists(self.current_filament_type):
            return 0.0
            
        filament_spec = self._get_filament_specs(self.current_filament_type)
        diameter = filament_spec["diameter"]
        density = filament_spec["density"]
        
        # Volume calculation: π × (d/2)² × length
        volume_mm3 = length_mm * math.pi * (diameter / 2) ** 2
        volume_cm3 = volume_mm3 / 1000  # Convert mm³ to cm³
        weight_g = density * volume_cm3
        
        return weight_g

    def _weight_to_length(self, weight_g: float) -> float:
        """Convert filament weight to length using current filament specs."""
        if not self._filament_exists(self.current_filament_type):
            return 0.0
            
        filament_spec = self._get_filament_specs(self.current_filament_type)
        diameter = filament_spec["diameter"]
        density = filament_spec["density"]
        
        volume_cm3 = weight_g / density
        volume_mm3 = volume_cm3 * 1000
        length_mm = volume_mm3 / (math.pi * (diameter / 2) ** 2)
        
        return length_mm

    async def _report_usage(self, eventtime: float) -> float:
        """Periodic task to process accumulated usage, sync database, and check for filament changes."""
        
        # Check for filament type changes from shared config (database updates)
        try:
            shared_filament_type = getattr(self.shared_printer_config, 'filament', '') or "N/A"
            
            # If shared config filament type differs from our cached type, update it
            if shared_filament_type != self.current_filament_type:
                logging.info(f"Detected filament type change from shared config: {self.current_filament_type} -> {shared_filament_type}")
                old_type = self.current_filament_type
                self.current_filament_type = shared_filament_type
                self._last_known_filament_type = shared_filament_type
                
                # Send filament change notification if the new filament type is valid
                if self._filament_exists(shared_filament_type):
                    self.server.send_event(
                        "spool_tracker:filament_changed",
                        {
                            "old_type": old_type,
                            "new_type": shared_filament_type,
                            "new_specs": self._get_filament_specs(shared_filament_type),
                            "source": "database_sync"  # Indicate this came from database sync
                        }
                    )
                else:
                    # If new filament type is invalid, disable tracking
                    self.remaining_weight = 0.0
                    logging.info(f"Filament type '{shared_filament_type}' is invalid, disabling tracking")
        except Exception as e:
            logging.warning(f"Failed to sync filament type from shared config: {e}")
        
        # Convert accumulated length to weight
        weight_used = self._length_to_weight(self.pending_usage_mm)
        
        # Update remaining weight
        self.remaining_weight = max(self.remaining_weight - weight_used, 0.0)
        current_time = datetime.now()
        
        if self.first_used is None:
            self.first_used = current_time
        self.last_used = current_time
        
        logging.info(
            f"Filament usage: +{self.pending_usage_mm:.2f}mm (+{weight_used:.3f}g), "
            f"Remaining: {self.remaining_weight:.1f}g"
        )
        
        # Send WebSocket notification with current state to prevent desync
        self.server.send_event(
            "spool_tracker:usage_updated",
            {
                "used_length_mm": self.pending_usage_mm,
                "used_weight_g": weight_used,
                "total_used_weight": 0,  # Not tracked anymore, keeping for compatibility
                "remaining_weight": self.remaining_weight,
                "filament_type": self.current_filament_type,  # Add current filament type
                "can_track": self._can_track(),  # Add current tracking capability
            }
        )
        
        # Only process usage if we can track and have pending usage
        if not self._can_track() or self.pending_usage_mm <= 0:
            return eventtime + self.sync_rate_seconds

        # Sync to database (ignore failures)
        try:
            self.database.insert_item("HS3", "remaining_filament_weight", 
                                    self.remaining_weight)
        except Exception as e:
            logging.warning(f"Failed to sync remaining weight to database: {e}")
        
        # Reset accumulator
        self.pending_usage_mm = 0.0
        
        return eventtime + self.sync_rate_seconds

    def get_remaining_weight(self) -> float:
        """Get remaining filament weight."""
        return self.remaining_weight

    def get_remaining_length(self) -> float:
        """Calculate remaining filament length."""
        return self._weight_to_length(self.remaining_weight)

    def get_usage_percentage(self) -> float:
        """Calculate percentage of filament used (not available without initial weight)."""
        return 0.0  # Cannot calculate without initial weight

    async def _handle_status_request(self, web_request: WebRequest):
        """Handle GET /server/spool_tracker/status requests."""
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
                "initial_weight": 0,  # Not tracked
                "used_weight": 0,     # Not tracked
                "remaining_weight": self.get_remaining_weight(),
            },
            "lengths": {
                "used_length": 0,     # Not tracked
                "remaining_length": self.get_remaining_length(),
            },
            "usage_percentage": self.get_usage_percentage(),
            "pending_usage_mm": self.pending_usage_mm,
            "can_track": self._can_track(),
            "timestamps": {
                "first_used": self.first_used.isoformat() if self.first_used else None,
                "last_used": self.last_used.isoformat() if self.last_used else None,
            },
            "available_filaments": {
                "predefined": list(FILAMENT_TYPES.keys()),
                "custom": list(self.custom_filaments.keys()),
                "all": all_types,
            },
        }

    async def _handle_filament_request(self, web_request: WebRequest):
        """Handle filament type GET/POST requests."""
        if web_request.get_action() == "POST":
            # Extract both filament type and weight from request
            new_type = web_request.get_str("filament_type", None)
            new_weight = web_request.get_float("weight", None)
            
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
                    }
                )
            
            # Handle weight change
            if new_weight is not None:
                if new_weight < 0:
                    raise self.server.error("Weight must be non-negative")
                
                old_weight = self.remaining_weight
                self.remaining_weight = new_weight
                logging.info(f"Updated remaining weight from {old_weight:.1f}g to {new_weight:.1f}g")
                
                # Sync to database immediately
                try:
                    self.database.insert_item("HS3", "remaining_filament_weight", 
                                            new_weight)
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
                    }
                )
        
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
            "available_types": {
                "predefined": list(FILAMENT_TYPES.keys()),
                "custom": list(self.custom_filaments.keys()),
                "all": all_types,
            },
        }

    async def close(self):
        """Clean shutdown of component."""
        logging.info("Shutting down Spool Tracker")
        self.report_timer.stop()
        
        # Final database sync
        try:
            self.database.insert_item("HS3", "remaining_filament_weight", 
                                    self.remaining_weight)
        except Exception as e:
            logging.warning(f"Failed final database sync: {e}")
        
        # Log final stats
        if self.remaining_weight > 0:
            logging.info(f"Final remaining weight: {self.remaining_weight:.1f}g")


def load_component(config: ConfigHelper) -> SpoolTracker:
    return SpoolTracker(config)