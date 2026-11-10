"""Actual HA current selection and locally healthy operation through a loopback-only management peer."""

from __future__ import annotations

import asyncio
import copy

import pytest
from aiohttp import web
from homeassistant.exceptions import HomeAssistantError

from tests_ha_proxy.test_client import TOKEN, setup


# Keep readiness and all physical observations in the independent peer, rather than in the entity under test.
@pytest.fixture
async def current_peer():
    state = {
        "api_version": 1,
        "healthy": True,
        "local_healthy": True,
        "running": True,
        "state": "healthy",
        "status_age_seconds": 0,
        "native_age_seconds": 0,
        "power_w": 0,
        "current_a": 0,
        "native_limit_a": 6,
        "station_ceiling_a": 6,
        "requested_current_a": 6,
        "desired_control": "app",
        "owner_control_enabled": True,
        "local_hold_verified": False,
        "cloud_connected": True,
        "vehicle_connected": True,
        "can_pause": True,
        "can_return": False,
        "can_start": True,
        "can_start_test": True,
        "can_set_current": True,
        "control_revision": 0,
        "local_charge": None,
        "test_charge": None,
        "sessions": [],
        "charger_sessions": [],
    }
    control = {
        "state": state,
        "commands": [],
        "requests": [],
        "lose_reply": False,
        "pending_receipt": False,
        "reject_current": False,
    }
    receipts = {}

    # One strict request changes only the selected limit; the peer's idle/paused evidence determines actual limits.
    async def endpoint(request):
        if request.headers.get("Authorization") != "Bearer " + TOKEN:
            raise web.HTTPUnauthorized()
        socket = web.WebSocketResponse(protocols=["ics-proxy.v1"])
        await socket.prepare(request)
        value = await socket.receive_json()
        command = value["command"]
        control["commands"].append(command)
        control["requests"].append(copy.deepcopy(value))
        if command == "status":
            result = copy.deepcopy(state)
        elif command == "receipt":
            result = receipts[value["request_id"]]
        elif command in ("set_current", "start"):
            fields = {"id", "command", "revision"} | ({"current_a"} if command == "set_current" else set())
            assert set(value) == fields
            permitted = state["can_set_current"] if command == "set_current" else state["can_start"]
            if command == "set_current":
                assert type(value["current_a"]) is int and 6 <= value["current_a"] <= 32
                permitted = permitted and not control["reject_current"]
            if not permitted or value["revision"] != state["control_revision"]:
                await socket.send_json({"id": value["id"], "ok": False, "error": "not_ready"})
                await socket.close()
                return socket
            state["control_revision"] += 1
            if command == "set_current":
                state["requested_current_a"] = value["current_a"]
                state["can_start_test"] = value["current_a"] == 6
                if state["desired_control"] == "app":
                    state["native_limit_a"] = state["station_ceiling_a"] = value["current_a"]
            else:
                state.update(
                    can_start=False, can_set_current=False, local_charge={"state": "running", "charging": False}
                )
            receipts[value["id"]] = {"id": value["id"], "state": "completed"}
            result = {"id": value["id"], "state": "accepted"} if control["pending_receipt"] else receipts[value["id"]]
            if control["lose_reply"]:
                await socket.close()
                return socket
        else:
            raise AssertionError("unexpected current-client command")
        await socket.send_json({"id": value["id"], "ok": True, "result": result})
        await socket.close()
        return socket

    app = web.Application()
    app.router.add_get("/management", endpoint)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    control["port"] = runner.addresses[0][1]
    try:
        yield control
    finally:
        await runner.cleanup()


# Intent: adding/reloading/unloading a translated6..32A selector only reads status and leaves all limits untouched.
# Why: presenting a current choice must never implicitly select it, start charging or resume a pause.
async def test_selector_lifecycle_is_passive_and_has_integer_current_metadata(actual_hass, current_peer):
    hass = actual_hass
    entry = await setup(hass, current_peer)
    number = hass.states.get("number.ics_proxy_charge_current_a")
    assert number.state == "6"
    assert {key: number.attributes[key] for key in ("min", "max", "step", "unit_of_measurement", "device_class")} == {
        "min": 6,
        "max": 32,
        "step": 1,
        "unit_of_measurement": "A",
        "device_class": "current",
    }
    assert "Charge current limit" in number.attributes["friendly_name"]
    assert await hass.config_entries.async_reload(entry.entry_id)
    assert await hass.config_entries.async_unload(entry.entry_id)
    assert set(current_peer["commands"]) == {"status"}
    assert current_peer["state"]["requested_current_a"] == 6


# Intent: explicit selection sends exactly one integer request and keeps a paused station at actual zero.
# Why: choosing a future resume current is distinct from starting or returning authority to the app.
@pytest.mark.parametrize("paused", [False, True], ids=["app", "paused"])
async def test_explicit_current_selection_uses_receipt_and_does_not_start(actual_hass, current_peer, paused):
    if paused:
        current_peer["state"].update(
            desired_control="paused", local_hold_verified=True, owner_control_enabled=False, station_ceiling_a=0
        )
    current_peer["pending_receipt"] = True
    hass = actual_hass
    await setup(hass, current_peer)
    await hass.services.async_call(
        "number", "set_value", {"entity_id": "number.ics_proxy_charge_current_a", "value": 16}, blocking=True
    )
    assert hass.states.get("number.ics_proxy_charge_current_a").state == "16"
    request = next(v for v in current_peer["requests"] if v["command"] == "set_current")
    assert set(request) == {"id", "command", "revision", "current_a"}
    assert request["revision"] == 0 and type(request["current_a"]) is int and request["current_a"] == 16
    assert current_peer["commands"].count("set_current") == current_peer["commands"].count("receipt") == 1
    assert set(current_peer["commands"]) <= {"status", "set_current", "receipt"}
    assert hass.states.get("sensor.ics_proxy_power").state == "0"
    assert hass.states.get("button.ics_proxy_start").attributes["friendly_name"].endswith("Start charging (16 A)")
    if paused:
        assert hass.states.get("sensor.ics_proxy_station_ceiling_a").state == "0"
        assert hass.states.get("sensor.ics_proxy_native_limit_a").state == "6"
        assert hass.states.get("sensor.ics_proxy_control_mode").state == "Paused locally"


# Intent: valid boundary choices and ordinary whole values are transported as integers rather than floats.
# Why: the management boundary rejects booleans/fractions and supports no current outside the approved range.
@pytest.mark.parametrize("current", [6, 7, 32])
async def test_valid_current_bounds_are_strict_integer_requests(actual_hass, current_peer, current):
    await setup(actual_hass, current_peer)
    await actual_hass.services.async_call(
        "number", "set_value", {"entity_id": "number.ics_proxy_charge_current_a", "value": current}, blocking=True
    )
    sent = next(v for v in current_peer["requests"] if v["command"] == "set_current")
    assert type(sent["current_a"]) is int and sent["current_a"] == current


# Intent: invalid fractional/out-of-range choices produce no management mutation and retain the observed value.
# Why: a UI step is guidance, while the entity must also validate values submitted through HA services.
@pytest.mark.parametrize("current", [5, 33, 6.5])
async def test_invalid_current_does_not_reach_the_management_service(actual_hass, current_peer, current):
    await setup(actual_hass, current_peer)
    with pytest.raises(HomeAssistantError):
        await actual_hass.services.async_call(
            "number", "set_value", {"entity_id": "number.ics_proxy_charge_current_a", "value": current}, blocking=True
        )
    assert set(current_peer["commands"]) == {"status"}
    assert actual_hass.states.get("number.ics_proxy_charge_current_a").state == "6"


# Intent: a rejected current operation retains its old reading and a lost reply never causes another request.
# Why: neither an error nor a timeout proves the remote effect did not happen, and the entity is not optimistic.
@pytest.mark.parametrize("lost", [False, True], ids=["rejected", "lost-reply"])
async def test_current_errors_refresh_status_without_retry_or_optimistic_state(actual_hass, current_peer, lost):
    current_peer["lose_reply"] = lost
    current_peer["reject_current"] = not lost
    await setup(actual_hass, current_peer)
    with pytest.raises(HomeAssistantError):
        await actual_hass.services.async_call(
            "number", "set_value", {"entity_id": "number.ics_proxy_charge_current_a", "value": 16}, blocking=True
        )
    assert current_peer["commands"].count("set_current") == 1
    assert actual_hass.states.get("number.ics_proxy_charge_current_a").state == ("16" if lost else "6")
    assert set(current_peer["commands"]) <= {"status", "set_current"}


# Intent: readiness, local health, current-source freshness and a valid selected value are mandatory.
# Why: a cached ready flag or malformed/legacy response cannot enable the idle-only current selector.
@pytest.mark.parametrize(
    "change",
    [
        {"can_set_current": False},
        {"local_healthy": False},
        {"native_age_seconds": 90},
        {"status_age_seconds": 90},
        {"requested_current_a": True},
        {"requested_current_a": 32.5},
        {"requested_current_a": None},
        {"can_set_current": None},
    ],
)
async def test_selector_requires_fresh_valid_idle_readiness(actual_hass, current_peer, change):
    current_peer["state"].update(change)
    await setup(actual_hass, current_peer)
    assert actual_hass.states.get("number.ics_proxy_charge_current_a").state == "unavailable"
    await actual_hass.services.async_call(
        "number", "set_value", {"entity_id": "number.ics_proxy_charge_current_a", "value": 16}, blocking=True
    )
    assert set(current_peer["commands"]) == {"status"}


# Intent: an old response lacking the new selector fields remains usable for its established controls only.
# Why: adding a platform cannot infer a selected current or permission from a legacy physical limit.
async def test_legacy_status_has_no_current_selector_permission(actual_hass, current_peer):
    current_peer["state"].pop("requested_current_a")
    current_peer["state"].pop("can_set_current")
    current_peer["state"].pop("local_healthy")
    await setup(actual_hass, current_peer)
    assert actual_hass.states.get("number.ics_proxy_charge_current_a").state == "unavailable"
    assert actual_hass.states.get("button.ics_proxy_start").state != "unavailable"
    assert set(current_peer["commands"]) == {"status"}


# Intent: local health permits current selection and normal Start when only the manufacturer cloud is offline.
# Why: native proof and the service's explicit readiness are independent from cloud reachability.
async def test_offline_cloud_keeps_local_current_and_normal_start_available(actual_hass, current_peer):
    current_peer["state"].update(healthy=False, local_healthy=True, cloud_connected=False)
    hass = actual_hass
    entry = await setup(hass, current_peer)
    assert hass.states.get("number.ics_proxy_charge_current_a").state == "6"
    assert hass.states.get("button.ics_proxy_start").state != "unavailable"
    assert hass.states.get("button.ics_proxy_start_test").state == "unavailable"
    assert hass.states.get("binary_sensor.ics_proxy_cloud_connected").state == "off"
    await hass.services.async_call(
        "number", "set_value", {"entity_id": "number.ics_proxy_charge_current_a", "value": 16}, blocking=True
    )
    await hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_start"}, blocking=True)
    await entry.runtime_data.async_refresh()
    mode = hass.states.get("sensor.ics_proxy_control_mode")
    assert mode.state == "Local session" and mode.attributes["local_charge"] == {"state": "running", "charging": False}
    assert hass.states.get("binary_sensor.ics_proxy_charging").state == "off"
    assert current_peer["commands"].count("set_current") == current_peer["commands"].count("start") == 1


# Intent: a selected normal current above six cannot enable the separate fixed-six two-minute test.
# Why: the test action must not inherit a higher current from the new normal charging preference.
async def test_timed_test_remains_unavailable_at_higher_selected_current(actual_hass, current_peer):
    current_peer["state"].update(requested_current_a=32, can_start_test=True)
    await setup(actual_hass, current_peer)
    assert actual_hass.states.get("button.ics_proxy_start_test").state == "unavailable"
    assert (
        actual_hass.states.get("button.ics_proxy_start").attributes["friendly_name"].endswith("Start charging (32 A)")
    )
    assert set(current_peer["commands"]) == {"status"}


# Intent: snapshot expiry disables the selector and normal Start even during a cloud-only outage.
# Why: cloud-independent local authority cannot outlive its separate native observation deadline.
async def test_offline_local_controls_expire_without_polling(actual_hass, current_peer):
    current_peer["state"].update(healthy=False, local_healthy=True, cloud_connected=False, native_age_seconds=14)
    await setup(actual_hass, current_peer)
    assert actual_hass.states.get("number.ics_proxy_charge_current_a").state == "6"
    requests = len(current_peer["commands"])
    await asyncio.sleep(1.2)
    await actual_hass.async_block_till_done()
    assert actual_hass.states.get("number.ics_proxy_charge_current_a").state == "unavailable"
    assert actual_hass.states.get("button.ics_proxy_start").state == "unavailable"
    assert len(current_peer["commands"]) == requests
