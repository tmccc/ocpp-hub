"""Physical connection/charging and management health as distinct measured states."""

from __future__ import annotations

from homeassistant.components.binary_sensor import BinarySensorDeviceClass, BinarySensorEntity
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import ProxyEntry
from .coordinator import ProxyCoordinator
from .entity import ProxyEntity

VALUES = (
    ("vehicle_connected", "Vehicle connected", BinarySensorDeviceClass.PLUG),
    ("charging", "Charging", BinarySensorDeviceClass.BATTERY_CHARGING),
    ("cloud_connected", "App connection", BinarySensorDeviceClass.CONNECTIVITY),
    ("local_hold_verified", "Local pause active", None),
    ("problem", "Proxy problem", BinarySensorDeviceClass.PROBLEM),
)


# The problem indicator is independent from physical charging, so a lost reading is never interpreted as zero.
async def async_setup_entry(hass: HomeAssistant, entry: ProxyEntry, add_entities: AddEntitiesCallback) -> None:
    add_entities([ProxyBinary(entry.runtime_data, *description) for description in VALUES])


class ProxyBinary(ProxyEntity, BinarySensorEntity):
    """The charger's local reading remains the physical truth regardless of any stale manufacturer status."""

    # Keep status keys stable for dashboards and explicit user automations.
    def __init__(
        self, coordinator: ProxyCoordinator, key: str, name: str, device: BinarySensorDeviceClass | None
    ) -> None:
        super().__init__(coordinator, "binary_sensor", key, name)
        self.key, self._attr_device_class = key, device

    # Only physical readings require native freshness; health stays visible while the service reports a fault.
    @property
    def available(self) -> bool:
        if self.key == "problem":
            return True
        if self.key == "cloud_connected":
            return super().available and self.service_fresh
        return super().available and (
            self.key not in ("vehicle_connected", "charging", "local_hold_verified") or self.native_fresh
        )

    # Actual positive power defines charging, independently from an open but suspended transaction.
    @property
    def is_on(self) -> bool | None:
        data = self.coordinator.data
        if self.key == "problem":
            return not self.coordinator.last_update_success or not self.coordinator.fresh_healthy
        if self.key == "charging":
            power = data.get("power_w")
            return power > 10 if isinstance(power, (int, float)) else None
        return data.get(self.key)
