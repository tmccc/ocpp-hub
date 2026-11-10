"""Actual Home Assistant 2026.11 entries, entities and actions against a real local management protocol peer."""

from __future__ import annotations

import asyncio
import copy
import time

import pytest
from aiohttp import web
from homeassistant.config_entries import ConfigEntryState
from homeassistant.exceptions import HomeAssistantError
from homeassistant.setup import async_setup_component

TOKEN = "a" * 64  # Synthetic loopback-only test credential.


# Exercise real websocket authentication/framing without importing the OCPP engine into the HA client.
@pytest.fixture
async def peer():
    state = {
        "api_version": 1,
        "healthy": True,
        "running": True,
        "state": "healthy",
        "status_age_seconds": 0,
        "native_age_seconds": 0,
        "power_w": 0,
        "current_a": 0,
        "native_limit_a": 6,
        "station_ceiling_a": 6,
        "desired_control": "app",
        "owner_control_enabled": True,
        "local_hold_verified": False,
        "cloud_connected": True,
        "vehicle_connected": True,
        "can_pause": True,
        "can_return": False,
        "control_revision": 0,
        "sessions": [
            {
                "started_at": "2026-11-10T00:00:00Z",
                "stopped_at": "2026-11-10T06:00:02Z",
                "energy_kwh": 5.614,
                "cloud_acknowledged": True,
            }
        ],
    }
    state["charger_sessions"] = copy.deepcopy(state["sessions"])
    control = {"state": state, "commands": [], "lose_reply": False}

    # Replies model only the documented client contract; physical authority is covered by independent core tests.
    async def endpoint(request):
        if request.headers.get("Authorization") != "Bearer " + TOKEN:
            raise web.HTTPUnauthorized()
        socket = web.WebSocketResponse(protocols=["ics-proxy.v1"])
        await socket.prepare(request)
        value = await socket.receive_json()
        command = value["command"]
        control["commands"].append(command)
        if command == "status":
            result = copy.deepcopy(state)
        elif command in ("pause", "app_control"):
            if command == "app_control" and value["revision"] != state["control_revision"]:
                await socket.send_json({"ok": False, "error": "not_ready"})
                await socket.close()
                return socket
            paused = command == "pause"
            state.update(
                desired_control="paused" if paused else "app",
                control_revision=state["control_revision"] + 1,
                local_hold_verified=paused,
                owner_control_enabled=not paused,
                station_ceiling_a=0 if paused else 6,
                can_return=paused,
            )
            result = {"id": value["id"], "state": "completed"}
            if control["lose_reply"]:
                await socket.close()
                return socket
        else:
            raise AssertionError("unexpected client command")
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


# Configure the actual custom integration through its real import flow without touching live HA configuration.
async def setup(hass, peer):
    assert await async_setup_component(hass, "persistent_notification", {})
    result = await hass.config_entries.flow.async_init(
        "ics_proxy", context={"source": "import"}, data={"port": peer["port"], "token": TOKEN}
    )
    await hass.async_block_till_done()
    assert result["type"] == "create_entry", result
    entry = result["result"]
    assert entry.state is ConfigEntryState.LOADED
    return entry


# Intent: HA startup, reload and unload perform status reads only and never instantiate the legacy OCPP backend.
# Why: managing HA must not change a deliberate charger hold or take ownership away from the standalone service.
async def test_actual_setup_reload_and_unload_are_passive(actual_hass, peer):
    hass = actual_hass
    entry = await setup(hass, peer)
    assert hass.states.get("sensor.ics_proxy_power").state == "0"
    assert hass.states.get("sensor.ics_proxy_control_mode").state == "App / schedule"
    assert hass.states.get("binary_sensor.ics_proxy_vehicle_connected").state == "on"
    assert hass.states.get("sensor.ics_proxy_last_session_energy").state == "5.614"
    assert hass.states.get("button.ics_proxy_pause").state != "unavailable"
    assert hass.states.get("button.ics_proxy_app_control").state == "unavailable"
    assert all(e.domain != "ocpp_hub" for e in hass.config_entries.async_entries())
    assert await hass.config_entries.async_reload(entry.entry_id)
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    assert set(peer["commands"]) == {"status"}


# Intent: deliberate button presses use only Pause/Return commands, update actual mode, and never send Start.
# Why: returning app/schedule authority is different from initiating a new charge.
async def test_buttons_pause_and_return_with_real_ha_services(actual_hass, peer):
    hass = actual_hass
    entry = await setup(hass, peer)
    await hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_pause"}, blocking=True)
    await entry.runtime_data.async_refresh()
    assert hass.states.get("sensor.ics_proxy_control_mode").state == "Paused locally"
    assert hass.states.get("binary_sensor.ics_proxy_local_hold_verified").state == "on"
    assert hass.states.get("binary_sensor.ics_proxy_problem").state == "off"
    assert hass.states.get("sensor.ics_proxy_station_ceiling_a").state == "0"
    await hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_app_control"}, blocking=True)
    await entry.runtime_data.async_refresh()
    assert hass.states.get("sensor.ics_proxy_control_mode").state == "App / schedule"
    assert peer["commands"].count("pause") == 1 and peer["commands"].count("app_control") == 1
    assert set(peer["commands"]) <= {"status", "pause", "app_control"}


# Intent: stale native data and the claimed latest session become unavailable while health shows a problem.
# Why: yesterday's or a disconnected charger's values must not masquerade as fresh zero power or a current plug state.
async def test_stale_readings_are_unavailable_and_alerts_are_debounced(actual_hass, peer):
    hass = actual_hass
    entry = await setup(hass, peer)
    coordinator = entry.runtime_data
    peer["state"].update(native_age_seconds=90, healthy=False, state="degraded", can_return=False)
    await coordinator.async_refresh()
    assert hass.states.get("sensor.ics_proxy_power").state == "unavailable"
    assert hass.states.get("binary_sensor.ics_proxy_vehicle_connected").state == "unavailable"
    assert hass.states.get("binary_sensor.ics_proxy_problem").state == "on"
    assert hass.states.get("sensor.ics_proxy_last_session_energy").state == "unavailable"
    assert not coordinator.notified
    coordinator.problem_since = time.monotonic() - 61
    await coordinator.async_refresh()
    assert coordinator.notified
    peer["state"].update(native_age_seconds=0, healthy=True, state="healthy")
    await coordinator.async_refresh()
    assert not coordinator.notified and coordinator.problem_since is None


# Intent: a lost mutating reply is surfaced to HA and followed only by status refresh, never an automatic retry.
# Why: duplicate user-visible operations cannot be inferred safe from a transport timeout.
async def test_lost_pause_reply_does_not_retry_control(actual_hass, peer):
    hass = actual_hass
    entry = await setup(hass, peer)
    peer["lose_reply"] = True
    with pytest.raises(HomeAssistantError):
        await hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_pause"}, blocking=True)
    await entry.runtime_data.async_refresh()
    assert peer["commands"].count("pause") == 1
    assert hass.states.get("sensor.ics_proxy_control_mode").state == "Paused locally"


# Intent: the config flow rejects invalid credentials without sending controls or printing their content.
# Why: connection setup may authenticate and read only; it is not an implicit user command.
async def test_bad_token_cannot_configure_or_issue_controls(actual_hass, peer):
    result = await actual_hass.config_entries.flow.async_init(
        "ics_proxy", context={"source": "user"}, data={"port": peer["port"], "token": "b" * 64}
    )
    assert result["type"] == "form" and result["errors"]["base"] == "cannot_connect"
    assert peer["commands"] == []


# Intent: a nearly expired reading becomes unavailable on its deadline without waiting for another successful poll.
# Why: static age values must not keep old power, plug or last-session information visible during a slow request.
async def test_snapshot_expires_between_polls(actual_hass, peer):
    hass = actual_hass
    peer["state"]["native_age_seconds"] = 14
    entry = await setup(hass, peer)
    assert hass.states.get("sensor.ics_proxy_power").state == "0"
    requests = len(peer["commands"])
    await asyncio.sleep(1.2)
    await hass.async_block_till_done()
    assert len(peer["commands"]) == requests
    assert hass.states.get("sensor.ics_proxy_power").state == "unavailable"
    assert hass.states.get("binary_sensor.ics_proxy_vehicle_connected").state == "unavailable"
    assert hass.states.get("sensor.ics_proxy_last_session_energy").state == "unavailable"
    assert hass.states.get("binary_sensor.ics_proxy_problem").state == "on"
    peer["state"]["native_age_seconds"] = 0
    await entry.runtime_data.async_refresh()
    assert hass.states.get("sensor.ics_proxy_power").state == "0"
