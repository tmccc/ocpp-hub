"""Minimal actual-core fixture, deliberately independent of the older HA test plugin."""

from pathlib import Path
from unittest.mock import patch

import pytest
from aiohttp.resolver import ThreadedResolver
from homeassistant import loader
from homeassistant.config_entries import ConfigEntries
from homeassistant.core import HomeAssistant
from homeassistant.helpers import area_registry, device_registry, entity_registry, frame

from tests.conftest import unused_tcp_port as unused_tcp_port
from tests.conftest import unused_tcp_port_factory as unused_tcp_port_factory


class LoopbackResolver(ThreadedResolver):
    """Mirror HA's resolver lifecycle without network discovery in a network-none container."""

    # Keep HA's actual aiohttp resolver cleanup contract while disabling multicast construction in isolation.
    async def real_close(self):
        await super().close()

    # HA retains the shared resolver until its final lifecycle close.
    async def close(self):
        pass


# Supply actual HA registries/config entries without importing the incompatible older HA test plugin.
@pytest.fixture
async def actual_hass(tmp_path):
    (tmp_path / "configuration.yaml").write_text("{}\n")
    custom = tmp_path / "custom_components"
    custom.mkdir()
    (custom / "__init__.py").write_text("")
    (custom / "ics_proxy").symlink_to(
        Path(__file__).resolve().parents[1] / "custom_components/ics_proxy", target_is_directory=True
    )
    hass = HomeAssistant(str(tmp_path))
    frame.async_setup(hass)
    loader.async_setup(hass)
    hass.config_entries = ConfigEntries(hass, {})
    await hass.config_entries.async_initialize()
    device_registry.async_setup(hass)
    await area_registry.async_load(hass, load_empty=True)
    await device_registry.async_load(hass, load_empty=True)
    await entity_registry.async_load(hass, load_empty=True)
    try:
        # Loopback-only tests require no multicast interfaces or zeroconf discovery.
        # Keep real HA/aiohttp lifecycle and networking; replace only DNS construction.
        with patch("homeassistant.helpers.aiohttp_client._async_make_resolver", return_value=LoopbackResolver()):
            yield hass
    finally:
        await hass.async_stop(force=True)
