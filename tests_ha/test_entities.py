"""Setup, entities, unload and the mirror, with a fake charger over a real websocket."""

from __future__ import annotations

import logging
import socket
from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import patch

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.entity_component import async_update_entity
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.ocpp_hub.const import (
    CONF_PORT,
    CONF_VENDOR_PASSWORD,
    CONF_VENDOR_URL,
    CONF_VENDOR_USER,
    DOMAIN,
)
from tests.fake_charger import FakeCharger
from tests.fake_vendor import FakeVendor
from tests_ha.conftest import state_of, wait_for

STATUS = "sensor.ocpp_hub_test0001_status"
POWER = "sensor.ocpp_hub_test0001_power"
CONNECTED = "binary_sensor.ocpp_hub_test0001_connected"
MIRROR_STATUS = "sensor.ocpp_hub_test0001_mirror_status"
MIRROR_QUEUE = "sensor.ocpp_hub_test0001_mirror_queue"

METER_VALUES = {
    "connectorId": 1,
    "meterValue": [
        {
            "timestamp": "2026-10-20T18:00:00Z",
            "sampledValue": [
                {"measurand": "Energy.Active.Import.Register", "value": "12345", "unit": "Wh"},
                {"measurand": "Power.Active.Import", "value": "7400", "unit": "W"},
                {"measurand": "Current.Import", "phase": "L1", "value": "10.5", "unit": "A"},
                {"measurand": "Current.Import", "phase": "L2", "value": "10.4", "unit": "A"},
                {"measurand": "Current.Import", "phase": "L3", "value": "10.6", "unit": "A"},
                {"measurand": "Voltage", "phase": "L1-N", "value": "231", "unit": "V"},
                {"measurand": "Current.Offered", "value": "16", "unit": "A"},
            ],
        }
    ],
}


# Intent: entities appear once the charger has booted, show what it reports, and go unavailable when it leaves.
# Why: this is the whole promise of the integration to dashboards and the energy manager.
async def test_entities_follow_the_charger(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any
) -> None:
    entry = await setup_hub()
    assert state_of(hass, STATUS) is None

    charger: FakeCharger = await connect_charger(entry)
    await charger.boot()
    await wait_for(hass, lambda: state_of(hass, CONNECTED) == "on", "entities did not appear after the boot")
    await charger.call("StatusNotification", {"connectorId": 1, "status": "Charging", "errorCode": "NoError"})
    await charger.call("MeterValues", METER_VALUES)
    await wait_for(hass, lambda: state_of(hass, POWER) == "7400.0", "meter values did not arrive")

    assert state_of(hass, STATUS) == "charging"
    assert float(state_of(hass, "sensor.ocpp_hub_test0001_energy") or 0) == 12.345
    assert [state_of(hass, f"sensor.ocpp_hub_test0001_current_l{n}") for n in (1, 2, 3)] == ["10.5", "10.4", "10.6"]
    assert state_of(hass, "sensor.ocpp_hub_test0001_current_offered") == "16.0"
    assert state_of(hass, "sensor.ocpp_hub_test0001_model") == "NEO_22T_A"
    assert state_of(hass, "sensor.ocpp_hub_test0001_firmware") == "s8100000001131&2.0.5_134"
    device = dr.async_get(hass).async_get_device_by_identifier((DOMAIN, "TEST0001"), entry.entry_id)
    assert device is not None and device.manufacturer == "RAEDIAN" and device.model == "NEO_22T_A"

    await charger.close()
    await wait_for(hass, lambda: state_of(hass, CONNECTED) == "off", "disconnect did not show")
    assert state_of(hass, POWER) == "unavailable" and state_of(hass, STATUS) == "unavailable"


# Intent: voltage sensors are disabled by default and the entity ids come from the stable keys.
# Why: ids must not depend on the instance language, and most installations only need the currents.
async def test_entity_ids_are_stable_and_voltage_is_off_by_default(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any
) -> None:
    entry = await setup_hub()
    charger: FakeCharger = await connect_charger(entry)
    await charger.boot()
    await wait_for(hass, lambda: state_of(hass, CONNECTED) == "on", "entities did not appear")

    registry = er.async_get(hass)
    voltage = registry.async_get("sensor.ocpp_hub_test0001_voltage_l1")
    assert voltage is not None and voltage.disabled_by is er.RegistryEntryDisabler.INTEGRATION
    assert state_of(hass, "sensor.ocpp_hub_test0001_voltage_l1") is None
    assert registry.async_get(STATUS).unique_id == "TEST0001:status"
    assert state_of(hass, MIRROR_STATUS) is None


# Intent: unloading the entry stops the server, so the port is free again for the other OCPP integration.
# Why: the swap of integrations at the live switch depends on the port really being released.
async def test_unload_releases_the_port(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any
) -> None:
    entry = await setup_hub()
    port = entry.runtime_data.central.port
    charger: FakeCharger = await connect_charger(entry)
    await charger.boot()
    await wait_for(hass, lambda: state_of(hass, CONNECTED) == "on", "entities did not appear")

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()

    with socket.socket() as probe:
        assert probe.connect_ex(("127.0.0.1", port)) != 0
    assert entry.state is ConfigEntryState.NOT_LOADED


# Intent: a port that is taken makes setup retry instead of failing for good.
# Why: during the swap the old OCPP integration may still hold the port for a moment.
async def test_port_in_use_retries_setup(hass: HomeAssistant) -> None:
    with socket.socket() as blocker:
        blocker.bind(("127.0.0.1", 0))
        blocker.listen()
        entry = MockConfigEntry(domain=DOMAIN, data={CONF_PORT: blocker.getsockname()[1]})
        entry.add_to_hass(hass)

        assert not await hass.config_entries.async_setup(entry.entry_id)

        assert entry.state is ConfigEntryState.SETUP_RETRY


# Intent: setup logs the build and the port at INFO even when Home Assistant logs warnings only.
# Why: the log lines are how the owner sees which code runs and whether the charger came back.
async def test_setup_logs_the_build_and_port(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], caplog: pytest.LogCaptureFixture
) -> None:
    logging.getLogger("custom_components.ocpp_hub").setLevel(logging.WARNING)
    caplog.set_level(logging.INFO)

    entry = await setup_hub()

    assert f"OCPP Hub listening on port {entry.runtime_data.central.port}" in caplog.text
    assert "[build " in caplog.text


# Intent: with a vendor URL in the options, the charger's boot reaches the vendor and the mirror entities say so.
# Why: this is the second goal of the integration, and the sensors are how the owner sees that it works.
async def test_mirror_forwards_and_reports(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any
) -> None:
    vendor = FakeVendor()
    await vendor.start()
    try:
        entry = await setup_hub(
            {CONF_VENDOR_URL: vendor.url, CONF_VENDOR_USER: "mirror", CONF_VENDOR_PASSWORD: "s3cret"}
        )
        charger: FakeCharger = await connect_charger(entry)
        await charger.boot()
        await wait_for(hass, lambda: "BootNotification" in vendor.actions(), "the boot did not reach the vendor")

        # Refresh the mirror sensor and tell whether it shows a connected link with an empty queue.
        async def mirror_connected() -> bool:
            await async_update_entity(hass, MIRROR_STATUS)
            await async_update_entity(hass, MIRROR_QUEUE)
            return state_of(hass, MIRROR_STATUS) == "connected" and state_of(hass, MIRROR_QUEUE) == "0"

        for _ in range(100):
            if await mirror_connected():
                break
            await hass.async_block_till_done()
        assert await mirror_connected(), "the mirror sensors never showed a delivered link"
        assert vendor.authorizations[0] is not None and vendor.authorizations[0].startswith("Basic ")
        attributes = hass.states.get(MIRROR_STATUS).attributes
        assert attributes["last_success"] is not None and attributes["dropped_frames"] == 0

        await charger.close()
        await wait_for(hass, lambda: state_of(hass, CONNECTED) == "off", "disconnect did not show")
        await async_update_entity(hass, MIRROR_STATUS)
    finally:
        await vendor.stop()


# Intent: changing the options reloads the entry, so a new vendor URL needs no restart.
# Why: the owner fills in the URL in the options and expects the mirror to start.
async def test_options_change_starts_the_mirror(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]]
) -> None:
    entry = await setup_hub()
    assert entry.runtime_data.mirror is None

    hass.config_entries.async_update_entry(entry, options={CONF_VENDOR_URL: "ws://127.0.0.1:1/ocpp"})
    await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.LOADED and entry.runtime_data.mirror is not None


# Intent: options that cannot work leave the hub running without a mirror and say why in the log.
# Why: local control of the charger matters more than the courtesy copy to the vendor.
async def test_invalid_options_run_without_a_mirror(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], caplog: pytest.LogCaptureFixture
) -> None:
    entry = await setup_hub({CONF_VENDOR_URL: "http://vendor.example/ocpp"})

    assert entry.state is ConfigEntryState.LOADED and entry.runtime_data.mirror is None
    assert "invalid_url" in caplog.text


# Intent: a setup that fails after the server started closes the server and the mirror again.
# Why: Home Assistant never unloads an entry whose setup failed, so otherwise the port stays taken until a restart
# and every retry fails with "cannot listen".
async def test_failed_setup_releases_the_port_and_the_mirror(hass: HomeAssistant) -> None:
    entry = MockConfigEntry(domain=DOMAIN, data={CONF_PORT: 0}, options={CONF_VENDOR_URL: "ws://127.0.0.1:1/ocpp"})
    entry.add_to_hass(hass)
    seen: dict[str, Any] = {}

    # Remember the running server, then fail the way a broken platform would.
    async def broken_forward(config_entry: MockConfigEntry, platforms: object) -> None:
        seen["port"] = config_entry.runtime_data.central.port
        seen["mirror"] = config_entry.runtime_data.mirror
        raise RuntimeError("platform setup failed")

    with patch.object(hass.config_entries, "async_forward_entry_setups", broken_forward):
        assert not await hass.config_entries.async_setup(entry.entry_id)

    assert entry.state is ConfigEntryState.SETUP_ERROR and seen["mirror"] is not None
    with socket.socket() as probe:
        assert probe.connect_ex(("127.0.0.1", seen["port"])) != 0


# Intent: the connect notification names the charger, and what the charger supplied is shown as plain text.
# Why: vendor, model and firmware come from a device on the network, and Markdown from it must not render in the UI.
async def test_connect_notification_shows_charger_strings_as_plain_text(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]], connect_charger: Any
) -> None:
    from homeassistant.components import persistent_notification

    entry = await setup_hub()
    charger: FakeCharger = await connect_charger(entry)

    await charger.boot(chargePointVendor="![x](http://example.invalid/p.png)", chargePointModel="NEO_{0}`\nx\u202e")
    notifications = persistent_notification._async_get_or_create_notifications(hass)
    await wait_for(hass, lambda: notifications, "no notification")

    notification = next(iter(notifications.values()))
    assert notification["title"] == "Charger connected"
    assert "`![x](http://example.invalid/p.png)`" in notification["message"]
    assert "`NEO_{0} x`" in notification["message"] and "ocpp1.6" in notification["message"]


# Intent: entities that were dropped in a later version are removed from the entity registry when the entry sets up,
# while the entities that still exist are left alone.
# Why: Home Assistant keeps the registry entry of a vanished entity for ever as an "unavailable" entity that nothing
# updates, which is exactly the clutter the cleanup is meant to remove.
async def test_retired_entities_are_removed_from_the_registry(
    hass: HomeAssistant, setup_hub: Callable[..., Awaitable[MockConfigEntry]]
) -> None:
    entry = MockConfigEntry(domain=DOMAIN, data={CONF_PORT: 0})
    entry.add_to_hass(hass)
    registry = er.async_get(hass)
    for key in ("session_energy", "session_started", "session_duration"):
        registry.async_get_or_create("sensor", DOMAIN, f"TEST0001:{key}", config_entry=entry)
    kept = registry.async_get_or_create("sensor", DOMAIN, "TEST0001:power", config_entry=entry)

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    left = {e.unique_id for e in er.async_entries_for_config_entry(registry, entry.entry_id)}
    assert left == {kept.unique_id}
    await hass.config_entries.async_unload(entry.entry_id)


# Intent: the plugged-in, car-charging and problem sensors and the error code follow the status the charger reports, and
# the raw frames reach the log only when the frames logger is set to debug.
# Why: they are the triggers for automations; and a frame can hold an id tag, so it must never be logged by default.
async def test_socket_sensors_follow_the_status_and_frames_are_logged_only_on_debug(
    hass: HomeAssistant,
    setup_hub: Callable[..., Awaitable[MockConfigEntry]],
    connect_charger: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    entry = await setup_hub()
    charger = await connect_charger(entry)
    await charger.boot()
    await wait_for(hass, lambda: state_of(hass, STATUS) is not None, "no entities")
    plugged = "binary_sensor.ocpp_hub_test0001_plugged_in"
    car = "binary_sensor.ocpp_hub_test0001_car_charging"
    problem = "binary_sensor.ocpp_hub_test0001_problem"

    await charger.call("StatusNotification", {"connectorId": 1, "status": "Charging", "errorCode": "NoError"})
    await wait_for(hass, lambda: state_of(hass, car) == "on", "car charging not shown")
    quiet = [r.getMessage() for r in caplog.records if "ocpp_hub.frames" in r.name]
    caplog.set_level(logging.DEBUG, logger="custom_components.ocpp_hub.frames")
    await charger.call("StatusNotification", {"connectorId": 1, "status": "Faulted", "errorCode": "GroundFailure"})
    await wait_for(hass, lambda: state_of(hass, problem) == "on", "problem not shown")

    assert state_of(hass, plugged) == "unknown" and state_of(hass, car) == "off"
    assert state_of(hass, "sensor.ocpp_hub_test0001_error_code") == "ground_failure"
    assert quiet == [] and any("GroundFailure" in r.getMessage() for r in caplog.records if "frames" in r.name)
