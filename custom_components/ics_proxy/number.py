"""Requested resume current is separate from measured current and independently verified electrical limits."""

from __future__ import annotations

import math

from homeassistant.components.number import NumberDeviceClass, NumberEntity, NumberMode
from homeassistant.const import UnitOfElectricCurrent
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import ProxyEntry
from .api import ProxyError
from .coordinator import DOMAIN, ProxyCoordinator
from .entity import ProxyEntity


# Adding the selector subscribes to status only; a selected value changes only after an explicit user request.
async def async_setup_entry(hass: HomeAssistant, entry: ProxyEntry, add_entities: AddEntitiesCallback) -> None:
    add_entities([ProxyCurrent(entry.runtime_data)])


class ProxyCurrent(ProxyEntity, NumberEntity):
    """One idle-only integer request cannot initiate charging or optimistically claim an applied physical limit."""

    _attr_native_min_value = 6
    _attr_native_max_value = 32
    _attr_native_step = 1
    _attr_native_unit_of_measurement = UnitOfElectricCurrent.AMPERE
    _attr_device_class = NumberDeviceClass.CURRENT
    _attr_mode = NumberMode.BOX
    _attr_icon = "mdi:current-ac"
    _attr_translation_key = "charge_current_a"

    # Keep a stable entity key while letting the standard HA translation supply the selector's name.
    def __init__(self, coordinator: ProxyCoordinator) -> None:
        super().__init__(coordinator, "number", "charge_current_a", "Charge current limit")
        del self._attr_name

    # A legacy, missing or malformed setting is unknown; booleans and fractional limits are never ampere choices.
    @property
    def native_value(self) -> int | None:
        value = self.coordinator.data.get("requested_current_a")
        return value if type(value) is int and 6 <= value <= 32 else None

    # The service proves idle closed/all-phase zero independently from manufacturer cloud connectivity.
    @property
    def available(self) -> bool:
        return (
            super().available
            and self.native_fresh
            and self.coordinator.fresh_local_healthy
            and self.coordinator.data.get("can_set_current") is True
            and self.native_value is not None
        )

    # Send one bounded command, wait for its durable receipt, and read back status even after a lost response.
    async def async_set_native_value(self, value: float) -> None:
        if (
            type(value) not in (int, float)
            or not math.isfinite(value)
            or not float(value).is_integer()
            or not 6 <= value <= 32
        ):
            raise HomeAssistantError(translation_domain=DOMAIN, translation_key="invalid_current")
        if not self.available:
            raise HomeAssistantError(translation_domain=DOMAIN, translation_key="current_unavailable")
        try:
            await self.coordinator.client.command(
                "set_current", self.coordinator.data["control_revision"], current_a=int(value)
            )
        except ProxyError as error:
            raise HomeAssistantError(str(error)) from None
        finally:
            await self.coordinator.async_request_refresh()
