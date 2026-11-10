"""Explicit local controls and one bounded test charge; setup never initiates charging."""

from __future__ import annotations

from homeassistant.components.button import ButtonEntity
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import ProxyEntry
from .api import ProxyError
from .coordinator import ProxyCoordinator
from .entity import ProxyEntity


# Register user-invoked actions only; adding or reloading these entities performs no physical operation.
async def async_setup_entry(hass: HomeAssistant, entry: ProxyEntry, add_entities: AddEntitiesCallback) -> None:
    add_entities(
        [
            ProxyButton(entry.runtime_data, "pause", "Pause charger"),
            ProxyButton(entry.runtime_data, "app_control", "Return to app / schedule"),
            ProxyButton(entry.runtime_data, "start_test", "Test charge (2 minutes)"),
        ]
    )


class ProxyButton(ProxyEntity, ButtonEntity):
    """A press waits for durable physical confirmation and never optimistically marks a hold as released."""

    # Persist stable action keys separately from their user-facing labels.
    def __init__(self, coordinator: ProxyCoordinator, action: str, name: str) -> None:
        super().__init__(coordinator, "button", action, name)
        self.action = action
        self._attr_icon = {
            "pause": "mdi:pause-circle",
            "app_control": "mdi:calendar-clock",
            "start_test": "mdi:play-circle-outline",
        }[action]

    # Returning requires the latest confirmed hold/revision; Stop remains available for an ordinary cloud outage.
    @property
    def available(self) -> bool:
        key = {"pause": "can_pause", "app_control": "can_return", "start_test": "can_start_test"}[self.action]
        return (
            super().available
            and self.coordinator.data.get(key) is True
            and (self.action == "pause" or self.coordinator.fresh_healthy)
        )

    # A deliberately chosen action uses one request id; a timeout refreshes status without repeating the write.
    async def async_press(self) -> None:
        try:
            await self.coordinator.client.command(self.action, self.coordinator.data["control_revision"])
        except ProxyError as error:
            raise HomeAssistantError(str(error)) from None
        finally:
            await self.coordinator.async_request_refresh()
