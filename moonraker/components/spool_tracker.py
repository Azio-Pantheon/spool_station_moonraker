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
        
        # Filament state (in-memory for now)
        self.current_filament_type = config.get("default_filament", "PETG-CF")
        self.initial_weight = config.getfloat("initial_weight", 1000.0)  # grams
        self.used_weight = 0.0  # grams
        self.first_used: Optional[datetime] = None
        self.last_used: Optional[datetime] = None
        
        # Usage tracking
        self.pending_usage_mm = 0.0  # Accumulated extrusion in mm
        self._highest_epos: float = 0.0
        self._current_extruder: str = "extruder"
        
        # Timer for periodic reporting
        self.report_timer = self.eventloop.register_timer(self._report_usage)
        
        # Component references
        self.klippy_apis: APIComp = self.server.lookup_component("klippy_apis")
        
        # Validate initial filament type
        if not self._filament_exists(self.current_filament_type):
            logging.warning(
                f"Unknown filament type '{self.current_filament_type}', defaulting to PLA"
            )
            self.current_filament_type = "PLA"
        
        self._register_notifications()
        self._register_endpoints()
        
        logging.info(f"Spool Tracker initialized with {self.current_filament_type}, "
                    f"initial weight: {self.initial_weight}g")

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
        self.server.register_endpoint(
            "/server/spool_tracker/reset",
            ["POST"],
            self._handle_reset_request,
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
        filament_spec = self._get_filament_specs(self.current_filament_type)
        diameter = filament_spec["diameter"]
        density = filament_spec["density"]
        
        volume_cm3 = weight_g / density
        volume_mm3 = volume_cm3 * 1000
        length_mm = volume_mm3 / (math.pi * (diameter / 2) ** 2)
        
        return length_mm

    async def _report_usage(self, eventtime: float) -> float:
        """Periodic task to process accumulated usage."""
        if self.pending_usage_mm <= 0:
            return eventtime + self.sync_rate_seconds

        # Convert accumulated length to weight
        weight_used = self._length_to_weight(self.pending_usage_mm)
        
        # Update usage tracking
        self.used_weight += weight_used
        current_time = datetime.now()
        
        if self.first_used is None:
            self.first_used = current_time
        self.last_used = current_time
        
        logging.info(
            f"Filament usage: +{self.pending_usage_mm:.2f}mm (+{weight_used:.3f}g), "
            f"Total used: {self.used_weight:.1f}g, "
            f"Remaining: {self.get_remaining_weight():.1f}g"
        )
        
        # Send notification
        self.server.send_event(
            "spool_tracker:usage_updated",
            {
                "used_length_mm": self.pending_usage_mm,
                "used_weight_g": weight_used,
                "total_used_weight": self.used_weight,
                "remaining_weight": self.get_remaining_weight(),
            }
        )
        
        # Reset accumulator
        self.pending_usage_mm = 0.0
        
        return eventtime + self.sync_rate_seconds

    def get_remaining_weight(self) -> float:
        """Calculate remaining filament weight."""
        return max(self.initial_weight - self.used_weight, 0.0)

    def get_remaining_length(self) -> float:
        """Calculate remaining filament length."""
        remaining_weight = self.get_remaining_weight()
        return self._weight_to_length(remaining_weight)

    def get_usage_percentage(self) -> float:
        """Calculate percentage of filament used."""
        if self.initial_weight <= 0:
            return 0.0
        return min((self.used_weight / self.initial_weight) * 100, 100.0)

    async def _handle_status_request(self, web_request: WebRequest):
        """Handle GET /server/spool_tracker/status requests."""
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
                "used_length": self._weight_to_length(self.used_weight),
                "remaining_length": self.get_remaining_length(),
            },
            "usage_percentage": self.get_usage_percentage(),
            "pending_usage_mm": self.pending_usage_mm,
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
            # Change filament type and/or weight
            new_type = web_request.get_str("filament_type", self.current_filament_type)
            new_weight = web_request.get_float("weight", None)
            
            # Check if this is a new custom filament type
            if not self._filament_exists(new_type):
                filament_specs = web_request.get("filament_specs", None)
                if filament_specs is None:
                    raise self.server.error(
                        f"Unknown filament type '{new_type}'. "
                        "Please provide filament_specs with 'density' and 'diameter' fields."
                    )
                
                # Validate and add custom filament
                try:
                    self._add_custom_filament(new_type, filament_specs)
                except ValueError as e:
                    raise self.server.error(str(e))
            
            # Update filament type if changed
            old_type = self.current_filament_type
            if new_type != old_type:
                self.current_filament_type = new_type
                logging.info(f"Changed filament type from {old_type} to {new_type}")
                
                # Send filament change notification
                self.server.send_event(
                    "spool_tracker:filament_changed",
                    {
                        "old_type": old_type,
                        "new_type": new_type,
                        "new_specs": self._get_filament_specs(new_type),
                    }
                )
            
            # Update initial weight if provided
            if new_weight is not None:
                if new_weight <= 0:
                    raise self.server.error("Weight must be greater than 0")
                
                old_weight = self.initial_weight
                self.initial_weight = new_weight
                logging.info(f"Updated initial weight from {old_weight:.1f}g to {new_weight:.1f}g")
                
                # Send usage update notification
                self.server.send_event(
                    "spool_tracker:usage_updated",
                    {
                        "initial_weight_changed": True,
                        "old_weight": old_weight,
                        "new_weight": new_weight,
                        "remaining_weight": self.get_remaining_weight(),
                        "total_used_weight": self.used_weight,
                    }
                )
        
        # Return current filament info (for both GET and POST)
        all_types = list(FILAMENT_TYPES.keys()) + list(self.custom_filaments.keys())
        
        return {
            "filament_type": self.current_filament_type,
            "filament_specs": self._get_filament_specs(self.current_filament_type),
            "initial_weight": self.initial_weight,
            "available_types": {
                "predefined": list(FILAMENT_TYPES.keys()),
                "custom": list(self.custom_filaments.keys()),
                "all": all_types,
            },
        }

    async def _handle_reset_request(self, web_request: WebRequest):
        """Handle POST /server/spool_tracker/reset requests."""
        reset_type = web_request.get_str("type", "usage")
        
        if reset_type == "usage":
            # Reset usage counters
            old_used = self.used_weight
            self.used_weight = 0.0
            self.first_used = None
            self.last_used = None
            self.pending_usage_mm = 0.0
            logging.info(f"Reset usage counters (was {old_used:.1f}g used)")
            
        elif reset_type == "weight":
            # Reset initial weight
            new_weight = web_request.get_float("weight")
            old_weight = self.initial_weight
            self.initial_weight = new_weight
            logging.info(f"Reset initial weight from {old_weight:.1f}g to {new_weight:.1f}g")
            
        elif reset_type == "all":
            # Reset everything
            self.used_weight = 0.0
            self.first_used = None
            self.last_used = None
            self.pending_usage_mm = 0.0
            new_weight = web_request.get_float("weight", self.initial_weight)
            self.initial_weight = new_weight
            logging.info("Reset all tracking data")
            
        else:
            raise self.server.error(f"Invalid reset type: {reset_type}")
        
        # Send notification
        self.server.send_event(
            "spool_tracker:usage_updated",
            {
                "reset_type": reset_type,
                "remaining_weight": self.get_remaining_weight(),
                "total_used_weight": self.used_weight,
            }
        )
        
        return {"message": f"Reset {reset_type} successful"}

    async def close(self):
        """Clean shutdown of component."""
        logging.info("Shutting down Spool Tracker")
        self.report_timer.stop()
        
        # Log final stats
        if self.used_weight > 0:
            logging.info(
                f"Final usage: {self.used_weight:.1f}g "
                f"({self.get_usage_percentage():.1f}%), "
                f"Remaining: {self.get_remaining_weight():.1f}g"
            )


def load_component(config: ConfigHelper) -> SpoolTracker:
    return SpoolTracker(config)