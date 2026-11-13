"""Real HA services distinguish a pending physical Pause from a receipt, and lost writes from observed state."""

import asyncio
import copy

import pytest
from aiohttp import web
from homeassistant.exceptions import HomeAssistantError

from tests_ha_proxy.conftest import actual_hass as actual_hass
from tests_ha_proxy.test_client import TOKEN, setup
from tests_ha_proxy.test_periodic_progress import configured_timing, progressed


# This independent peer separates accepted intent, applied current and verified physical zero.
@pytest.fixture
async def control_peer():
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
        "native_limit_a": 32,
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
        "receipts": {},
        "pending_pause": False,
        "lose_current_before_effect": False,
        "pause_accepted": asyncio.Event(),
        "release_physical_zero": asyncio.Event(),
        "physical_zero_observed": asyncio.Event(),
        "receipt_read": asyncio.Event(),
        "current_lost": False,
        "hold_status_after_failure": False,
        "status_held": asyncio.Event(),
        "release_status": asyncio.Event(),
    }
    workers = []

    # Only the independent external zero event may complete a pending Pause and make Return physically admissible.
    async def apply_zero(identifier):
        await control["release_physical_zero"].wait()
        state.update(
            healthy=True,
            local_healthy=True,
            state="healthy",
            station_ceiling_a=0,
            current_a=0,
            power_w=0,
            local_hold_verified=True,
            can_return=True,
            can_start=True,
            can_set_current=True,
        )
        control["receipts"][identifier] = {"id": identifier, "state": "completed"}
        control["physical_zero_observed"].set()

    # Every command and reply crosses actual authenticated websocket framing with independent request/result IDs.
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
            if control["hold_status_after_failure"] and control["current_lost"]:
                control["status_held"].set()
                await control["release_status"].wait()
            result = copy.deepcopy(state)
        elif command == "receipt":
            control["receipt_read"].set()
            result = copy.deepcopy(control["receipts"][value["request_id"]])
        elif command == "set_current":
            assert set(value) == {"id", "command", "revision", "current_a"}
            assert type(value["current_a"]) is int and 6 <= value["current_a"] <= 32
            assert value["revision"] == state["control_revision"]
            if control["lose_current_before_effect"]:
                control["current_lost"] = True
                await socket.close()
                return socket
            state.update(requested_current_a=value["current_a"], control_revision=state["control_revision"] + 1)
            if state["desired_control"] == "app":
                state["station_ceiling_a"] = value["current_a"]
            state["can_start_test"] = value["current_a"] == 6
            result = {"id": value["id"], "state": "completed"}
            control["receipts"][value["id"]] = dict(result)
        elif command == "pause":
            state.update(
                desired_control="paused",
                control_revision=state["control_revision"] + 1,
                healthy=False,
                local_healthy=False,
                state="pausing",
                owner_control_enabled=False,
                local_hold_verified=False,
                can_return=False,
                can_start=False,
                can_start_test=False,
                can_set_current=False,
            )
            result = {"id": value["id"], "state": "accepted"}
            control["receipts"][value["id"]] = dict(result)
            control["pause_accepted"].set()
            workers.append(asyncio.create_task(apply_zero(value["id"])))
            if not control["pending_pause"]:
                control["release_physical_zero"].set()
        else:
            raise AssertionError("unexpected control-completion peer command")
        if control.get("result_filter") is not None:
            result = control["result_filter"](command, value, result)
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
        control["release_physical_zero"].set()
        control["release_status"].set()
        await asyncio.gather(*workers, return_exceptions=True)
        await runner.cleanup()


# Intent: accepted Pause remains physically unverified and Return unavailable until the independent zero event.
# Why: a pending receipt must not make HA announce a completed hold or finish its user's service call early.
async def test_pending_pause_waits_for_independent_zero(actual_hass, control_peer, monkeypatch):
    hass, peer = actual_hass, control_peer
    configured_timing(monkeypatch, 1)
    peer["pending_pause"] = True
    peer["state"].update(current_a=6, power_w=1440)
    entry = await setup(hass, peer)
    await hass.async_start()
    request = asyncio.create_task(
        hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_pause"}, blocking=True)
    )
    try:
        assert await progressed(peer["pause_accepted"].is_set, 3), "peer never received the explicit Pause"
        assert await progressed(peer["receipt_read"].is_set, 3), "HA did not poll the accepted receipt"
        assert await progressed(
            lambda: (
                entry.runtime_data.data.get("desired_control") == "paused"
                and hass.states.get("sensor.ics_proxy_control_mode").state == "Pause requested"
            ),
            4,
        ), "periodic status did not expose the independently pending hold"
        assert not request.done() and not peer["physical_zero_observed"].is_set()
        assert peer["state"]["current_a"] == 6 and peer["state"]["power_w"] == 1440
        assert hass.states.get("binary_sensor.ics_proxy_local_hold_verified").state == "off"
        assert hass.states.get("button.ics_proxy_app_control").state == "unavailable"
        peer["release_physical_zero"].set()
        await asyncio.wait_for(request, 4)
        assert peer["physical_zero_observed"].is_set()
        assert await progressed(
            lambda: (
                hass.states.get("sensor.ics_proxy_control_mode").state == "Paused locally"
                and hass.states.get("binary_sensor.ics_proxy_local_hold_verified").state == "on"
                and hass.states.get("button.ics_proxy_app_control").state != "unavailable"
            ),
            4,
        ), "HA did not present the independent verified hold"
        assert peer["commands"].count("pause") == 1
        assert set(peer["commands"]) <= {"status", "pause", "receipt"}
    finally:
        peer["release_physical_zero"].set()
        await asyncio.gather(request, return_exceptions=True)


# Intent: losing a current request before any effect retains the observed old value and never retries the write.
# Why: the applied-then-lost case alone cannot expose an entity which optimistically adopts an unconfirmed target.
@pytest.mark.parametrize("paused", [False, True])
async def test_lost_current_before_effect_preserves_observed_value(actual_hass, control_peer, monkeypatch, paused):
    hass, peer = actual_hass, control_peer
    configured_timing(monkeypatch, 1)
    if paused:
        peer["state"].update(
            desired_control="paused",
            local_hold_verified=True,
            owner_control_enabled=False,
            station_ceiling_a=0,
            can_return=True,
        )
    await setup(hass, peer)
    await hass.async_start()
    peer["lose_current_before_effect"] = True
    peer["hold_status_after_failure"] = True
    request = asyncio.create_task(
        hass.services.async_call(
            "number", "set_value", {"entity_id": "number.ics_proxy_charge_current_a", "value": 16}, blocking=True
        )
    )
    try:
        assert await progressed(peer["status_held"].is_set, 4), "peer never held the post-failure status response"
        assert not request.done()
        number = hass.data["entity_components"]["number"].get_entity("number.ics_proxy_charge_current_a")
        assert number.native_value == 6 and hass.states.get(number.entity_id).state == "6"
        assert peer["state"]["requested_current_a"] == 6 and peer["state"]["control_revision"] == 0
        peer["release_status"].set()
        with pytest.raises(HomeAssistantError):
            await request
    finally:
        peer["release_status"].set()
        await asyncio.gather(request, return_exceptions=True)
    assert hass.states.get("number.ics_proxy_charge_current_a").state == "6"
    assert peer["state"]["requested_current_a"] == 6 and peer["state"]["control_revision"] == 0
    assert peer["state"]["station_ceiling_a"] == (0 if paused else 6)
    assert peer["commands"].count("set_current") == 1 and "receipt" not in peer["commands"]
    first = next(request["id"] for request in peer["requests"] if request["command"] == "set_current")
    peer["lose_current_before_effect"] = False
    await hass.services.async_call(
        "number", "set_value", {"entity_id": "number.ics_proxy_charge_current_a", "value": 16}, blocking=True
    )
    assert await progressed(lambda: hass.states.get("number.ics_proxy_charge_current_a").state == "16", 4), (
        "status readback did not publish the later explicitly applied current"
    )
    commands = [request for request in peer["requests"] if request["command"] == "set_current"]
    assert len(commands) == 2 and commands[-1]["id"] != first
    assert set(peer["commands"]) <= {"status", "set_current"}
