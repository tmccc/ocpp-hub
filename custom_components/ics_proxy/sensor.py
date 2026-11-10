"""Measured power/current and bounded proxy session history."""

from __future__ import annotations

from typing import Any

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity, SensorStateClass
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import ProxyEntry
from .coordinator import ProxyCoordinator
from .entity import ProxyEntity

VALUES = (
    ("power_w", "Charging power", "W", SensorDeviceClass.POWER),
    ("current_a", "Charging current", "A", SensorDeviceClass.CURRENT),
    ("native_limit_a", "Charger safety limit", "A", SensorDeviceClass.CURRENT),
    ("station_ceiling_a", "Local output limit", "A", SensorDeviceClass.CURRENT),
    ("control_mode", "Control mode", None, None),
    ("last_session_energy", "Last session energy", "kWh", SensorDeviceClass.ENERGY),
)


# Add the same stable set after every reload, independent of whether a vehicle or active session is present.
async def async_setup_entry(hass: HomeAssistant, entry: ProxyEntry, add_entities: AddEntitiesCallback) -> None:
    add_entities([ProxySensor(entry.runtime_data, *description) for description in VALUES])


class ProxySensor(ProxyEntity, SensorEntity):
    """No last-session meter value is treated as current power, and a finished record is not a cumulative total."""

    # State classes are limited to current measurements to avoid false long-term energy statistics.
    def __init__(self, coordinator: ProxyCoordinator, key: str, name: str, unit: str | None, device: Any) -> None:
        entity_key = "power" if key == "power_w" else "current" if key == "current_a" else key
        super().__init__(coordinator, "sensor", entity_key, name)
        self.key = key
        self._attr_native_unit_of_measurement = unit
        self._attr_device_class = device
        if unit is not None:
            self._attr_suggested_display_precision = (
                3 if key == "last_session_energy" else 1 if key == "current_a" else 0
            )
        if key in ("power_w", "current_a"):
            self._attr_state_class = SensorStateClass.MEASUREMENT

    # The latest charger session needs a fresh source too; otherwise an older session could masquerade as the newest.
    @property
    def available(self) -> bool:
        return super().available and (self.key == "control_mode" or self.native_fresh)

    # The service supplies facts; this layer adds only concise human-readable mode labels.
    @property
    def native_value(self) -> Any:
        data = self.coordinator.data
        if self.key == "control_mode":
            if data.get("desired_control") == "paused":
                return "Paused locally" if data.get("local_hold_verified") and self.native_fresh else "Pause requested"
            if data.get("healthy") is True and data.get("owner_control_enabled") is True:
                return "App / schedule"
            return "Needs attention" if data.get("state") in ("attention", "degraded") else "Waiting for verification"
        if self.key == "last_session_energy":
            return next((r["energy_kwh"] for r in data.get("charger_sessions", []) if r.get("stopped_at")), None)
        return data.get(self.key)

    # Only redacted completed/live proxy session summaries are attached, with a bounded twenty-record history.
    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        return (
            {
                "charger_sessions": self.coordinator.data.get("charger_sessions", []),
                "proxy_sessions": self.coordinator.data.get("sessions", []),
            }
            if self.key == "last_session_energy"
            else None
        )
