"""Tests of the Home Assistant layer. They need pytest-homeassistant-custom-component, so they run in .venv-ha:

    .venv-ha/bin/python -m pytest tests_ha

The engine tests in tests/ run without Home Assistant; these prove that the layer wires the engine to entities.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.ocpp_hub.const import CONF_PORT, DOMAIN
from tests.fake_charger import FakeCharger


# Intent: let Home Assistant find the integration in custom_components for every test.
# Why: without it the loader only knows the built-in integrations and the config entry cannot be set up.
@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations: None) -> None:
    return None


# Intent: allow real local sockets in every test.
# Why: the hub is a real websocket server and the fake charger a real client; the plugin blocks sockets by default.
@pytest.fixture(autouse=True)
def allow_local_sockets(socket_enabled: None) -> None:
    return None


# Intent: bind the actual integration listener to loopback in the isolated test environment.
# Why: these tests need real local peers while the offline audit must reject wildcard and external listeners.
@pytest.fixture(autouse=True)
def local_listener_address(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("custom_components.ocpp_hub.BIND_HOST", "127.0.0.1")


# Wait until a condition holds, letting Home Assistant finish its work between looks.
# Frames travel over real sockets, so an effect shows up a moment after the cause and a fixed sleep would be a guess.
async def wait_for(
    hass: HomeAssistant, check: Callable[[], object], message: str = "condition not met", timeout: float = 5.0
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not check():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(message)
        await asyncio.sleep(0.02)
        await hass.async_block_till_done()


# The state of an entity as a string, or None when it does not exist.
def state_of(hass: HomeAssistant, entity_id: str) -> str | None:
    state = hass.states.get(entity_id)
    return state.state if state is not None else None


# Intent: factory fixture that sets up an OCPP Hub entry on a free port and unloads it at the end of the test.
# Why: a test that leaves the server running would fail the plugin's check for lingering tasks.
@pytest.fixture
async def setup_hub(hass: HomeAssistant) -> AsyncIterator[Callable[..., Awaitable[MockConfigEntry]]]:
    entries: list[MockConfigEntry] = []

    # Intent: add and set up one entry with the given options.
    # Why: most tests differ only in the vendor options.
    async def _setup(options: dict[str, Any] | None = None) -> MockConfigEntry:
        entry = MockConfigEntry(domain=DOMAIN, data={CONF_PORT: 0}, options=options or {})
        entry.add_to_hass(hass)
        entries.append(entry)
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        return entry

    yield _setup
    for entry in entries:
        if entry.state is ConfigEntryState.LOADED:
            await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


# Intent: factory fixture that connects fake chargers to a running hub and closes them at the end.
# Why: every entity test starts from a charger that connected over a real websocket.
@pytest.fixture
async def connect_charger() -> AsyncIterator[Callable[..., Awaitable[FakeCharger]]]:
    chargers: list[FakeCharger] = []

    # Intent: connect one fake charger to the entry's server.
    # Why: the port is random in tests, so it is read from the running entry.
    async def _connect(entry: MockConfigEntry, cp_id: str = "TEST0001") -> FakeCharger:
        charger = FakeCharger(entry.runtime_data.central.port, cp_id)
        await charger.connect()
        chargers.append(charger)
        return charger

    yield _connect
    for charger in chargers:
        await charger.close()
