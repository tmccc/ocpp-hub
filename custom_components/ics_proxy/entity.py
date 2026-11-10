"""Shared stable device identity and freshness checks for the management client entities."""

from __future__ import annotations

from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .coordinator import DOMAIN, ProxyCoordinator


class ProxyEntity(CoordinatorEntity[ProxyCoordinator]):
    """Entities expose the service's measured facts and never infer power from a cloud transaction state."""

    _attr_has_entity_name = True

    # Explicit entity ids keep dashboard references stable regardless of the instance language or device name.
    def __init__(self, coordinator: ProxyCoordinator, kind: str, key: str, name: str) -> None:
        super().__init__(coordinator)
        self.entity_id = f"{kind}.ics_proxy_{key}"
        self._attr_unique_id = "ics_proxy_local_" + key
        self._attr_name = name
        self._attr_device_info = DeviceInfo(identifiers={(DOMAIN, "local_managed_proxy")}, name="iCS local proxy")

    # A stale native observation is unavailable, never silently displayed as current zero power or disconnected.
    @property
    def service_fresh(self) -> bool:
        age = self.coordinator.age("status_age_seconds")
        return self.coordinator.data.get("running") is True and isinstance(age, (int, float)) and 0 <= age <= 15

    # Live physical values additionally need a current independent native observation.
    @property
    def native_fresh(self) -> bool:
        age = self.coordinator.age("native_age_seconds")
        return self.service_fresh and isinstance(age, (int, float)) and 0 <= age <= 15
