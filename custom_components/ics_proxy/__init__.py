"""Thin Home Assistant client for the independent iCS proxy service."""

from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant

from .coordinator import ProxyCoordinator

PLATFORMS = [Platform.SENSOR, Platform.BINARY_SENSOR, Platform.BUTTON]
type ProxyEntry = ConfigEntry[ProxyCoordinator]


# Connect only to the authenticated management channel; the OCPP service remains independently supervised.
async def async_setup_entry(hass: HomeAssistant, entry: ProxyEntry) -> None:
    coordinator = ProxyCoordinator(hass, entry)
    await coordinator.async_config_entry_first_refresh()
    entry.runtime_data = coordinator
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)


# HA reload and shutdown retire only the client's subscriptions, preserving the user's durable requested mode.
async def async_unload_entry(hass: HomeAssistant, entry: ProxyEntry) -> bool:
    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
