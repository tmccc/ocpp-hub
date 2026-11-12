"""Thin Home Assistant client for the independent iCS proxy service."""

from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import MAJOR_VERSION, MINOR_VERSION, Platform
from homeassistant.core import HomeAssistant

from .coordinator import ProxyCoordinator
from .notification import NOTICES

PLATFORMS = [Platform.SENSOR, Platform.BINARY_SENSOR, Platform.BUTTON, Platform.NUMBER]
type ProxyEntry = ConfigEntry[ProxyCoordinator]


# Connect only to the authenticated management channel; the OCPP service remains independently supervised.
async def async_setup_entry(hass: HomeAssistant, entry: ProxyEntry) -> bool | None:
    coordinator = ProxyCoordinator(hass, entry)
    await coordinator.async_config_entry_first_refresh()
    entry.runtime_data = coordinator
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    if (MAJOR_VERSION, MINOR_VERSION) < (2026, 11):
        return True
    return None


# HA reload and shutdown retire only the client's subscriptions, preserving the user's durable requested mode.
async def async_unload_entry(hass: HomeAssistant, entry: ProxyEntry) -> bool:
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        entry.runtime_data.notice.suspend()
    return unloaded


# Removing an entry must retire its independent setup-outage timer and stable persistent notice.
async def async_remove_entry(hass: HomeAssistant, entry: ProxyEntry) -> None:
    notice = hass.data.get(NOTICES, {}).get(entry.entry_id)
    if notice is not None:
        notice.close(dismiss=True)
