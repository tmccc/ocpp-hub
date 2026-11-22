"""Actual Home Assistant 2026.11 entries, entities and actions against a real local management protocol peer."""

from __future__ import annotations

import asyncio
import copy
import json
import time

import pytest
from aiohttp import ClientConnectionError, ClientWebSocketResponse, WSMsgType, web
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
        "can_start": True,
        "local_charge": None,
        "can_start_test": True,
        "test_charge": None,
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
    control = {
        "state": state,
        "commands": [],
        "requests": [],
        "lose_reply": False,
        "pending_receipt": False,
        "receipt_delay": 0,
        "status_delay": 0,
        "status_failure": False,
    }
    receipts = {}

    # Replies model only the documented client contract; physical authority is covered by independent core tests.
    async def endpoint(request):
        if request.headers.get("Authorization") != "Bearer " + TOKEN:
            raise web.HTTPUnauthorized()
        socket = web.WebSocketResponse(protocols=["ics-proxy.v1"])
        await socket.prepare(request)
        message = await socket.receive()
        if message.type != WSMsgType.TEXT:
            return socket
        value = message.json()
        command = value["command"]
        control["commands"].append(command)
        control["requests"].append(copy.deepcopy(value))
        if command == "pause" and control.get("pre_admission_close") is not None:
            await socket.close(code=1013, message=control["pre_admission_close"].encode())
            return socket
        if command == "status":
            result = copy.deepcopy(state)
            if control.get("response_gate") is not None:
                await control["response_gate"](command, value, result)
            await asyncio.sleep(control["status_delay"])
            if control["status_failure"]:
                await socket.close()
                return socket
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
                can_start=True,
                can_start_test=not paused,
            )
            receipts[value["id"]] = {"id": value["id"], "action": command, "state": "completed"}
            result = {"id": value["id"], "state": "accepted"} if control["pending_receipt"] else receipts[value["id"]]
            if control["lose_reply"]:
                await socket.close()
                return socket
        elif command == "start_test":
            if not state["can_start_test"] or value["revision"] != state["control_revision"]:
                await socket.send_json({"ok": False, "error": "not_ready"})
                await socket.close()
                return socket
            state.update(
                can_start_test=False,
                test_charge={"source": "local", "state": "armed", "duration_seconds": 120, "seconds_remaining": 120},
            )
            receipts[value["id"]] = {"id": value["id"], "action": command, "state": "completed"}
            result = {"id": value["id"], "state": "accepted"} if control["pending_receipt"] else receipts[value["id"]]
            if control["lose_reply"]:
                await socket.close()
                return socket
        elif command == "start":
            if not state["can_start"] or value["revision"] != state["control_revision"]:
                await socket.send_json({"ok": False, "error": "not_ready"})
                await socket.close()
                return socket
            state.update(
                desired_control="app",
                control_revision=state["control_revision"] + 1,
                can_start=False,
                can_start_test=False,
                can_return=False,
                local_hold_verified=False,
                station_ceiling_a=6,
                owner_control_enabled=True,
                local_charge={"state": "running", "charging": False},
            )
            receipts[value["id"]] = {"id": value["id"], "action": command, "state": "completed"}
            result = {"id": value["id"], "state": "accepted"} if control["pending_receipt"] else receipts[value["id"]]
            if control["lose_reply"]:
                await socket.close()
                return socket
        elif command == "receipt":
            if control.get("close_first_receipt") and control["commands"].count("receipt") == 1:
                await socket.close(code=1013, message=b"Busy")
                return socket
            await asyncio.sleep(control["receipt_delay"])
            request_id = value["request_id"]
            result = receipts.get(request_id, {"id": request_id, "state": "not_found"})
        else:
            raise AssertionError("unexpected client command")
        if control.get("result_filter") is not None:
            result = control["result_filter"](command, value, result)
        try:
            raw = control.get("raw_response")
            response = raw(command, value, result) if raw is not None else None
            if response is None:
                await socket.send_json({"id": value["id"], "ok": True, "result": result})
            else:
                await socket.send_str(response)
        except ConnectionResetError:
            pass  # The independent client may already have timed out after durable command acceptance.
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


# Intent: a recorded Pause reports unknown confirmation on a slow receipt while a fast receipt completes normally.
# Why: a failed read must not erase known acceptance or cause a second physical command.
@pytest.mark.parametrize("delay", [0, 11])
async def test_deferred_pause_receipt_keeps_acceptance_classification(actual_hass, peer, delay):
    await setup(actual_hass, peer)
    peer["pending_receipt"], peer["receipt_delay"] = True, delay
    if delay:
        with pytest.raises(HomeAssistantError, match="request was recorded; physical confirmation is still pending"):
            await actual_hass.services.async_call(
                "button", "press", {"entity_id": "button.ics_proxy_pause"}, blocking=True
            )
    else:
        await actual_hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_pause"}, blocking=True)
    assert peer["commands"].count("pause") == 1
    count = peer["commands"].count("receipt")
    assert (2 <= count <= 3) if delay else (count == 1)


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
    assert hass.states.get("button.ics_proxy_start_test").state != "unavailable"
    assert hass.states.get("button.ics_proxy_start").state != "unavailable"
    assert all(e.domain != "ocpp_hub" for e in hass.config_entries.async_entries())
    assert await hass.config_entries.async_reload(entry.entry_id)
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    assert set(peer["commands"]) == {"status"}


# Intent: a loaded HA entry can still Pause when only the proxy's status reply fails.
# Why: a reporting failure must not hide the restrictive control while the command socket can still receive it.
async def test_pause_still_works_during_status_only_failure(actual_hass, peer):
    hass = actual_hass
    entry = await setup(hass, peer)
    coordinator = entry.runtime_data
    pause = "button.ics_proxy_pause"
    assert hass.states.get(pause).state != "unavailable"
    cached_revision = coordinator.data["control_revision"]
    peer["state"]["control_revision"] = cached_revision + 1
    peer["status_failure"] = True
    await coordinator.async_refresh()

    assert not coordinator.last_update_success
    assert hass.states.get("button.ics_proxy_start").state == "unavailable"
    assert hass.states.get("button.ics_proxy_start_test").state == "unavailable"
    assert hass.states.get(pause).state != "unavailable"
    await hass.services.async_call("button", "press", {"entity_id": pause}, blocking=True)

    requests = [request for request in peer["requests"] if request["command"] != "status"]
    assert len(requests) == 1 and requests[0]["command"] == "pause"
    assert requests[0]["revision"] == cached_revision
    assert peer["state"]["local_hold_verified"] is True
    assert not coordinator.last_update_success

    peer["status_failure"] = False
    peer["state"]["can_pause"] = False
    await coordinator.async_refresh()
    assert coordinator.last_update_success
    assert hass.states.get("binary_sensor.ics_proxy_local_hold_verified").state == "on"
    assert hass.states.get(pause).state == "unavailable"


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
    assert hass.states.get("button.ics_proxy_start_test").state == "unavailable"
    assert not coordinator.notified
    coordinator.problem_since = time.monotonic() - 61
    await coordinator.async_refresh()
    assert coordinator.notified
    peer["state"].update(native_age_seconds=0, healthy=True, state="healthy")
    await coordinator.async_refresh()
    assert not coordinator.notified and coordinator.problem_since is None


# Intent: a dropped reply or explicit unknown result is resolved by reading the original Pause receipt.
# Why: the saved result can confirm the hold without issuing a second Pause.
@pytest.mark.parametrize("drop_reply", [True, False], ids=["dropped-reply", "unknown-outcome"])
async def test_lost_pause_reply_does_not_retry_control(actual_hass, peer, drop_reply):
    hass = actual_hass
    entry = await setup(hass, peer)
    peer["lose_reply"] = drop_reply

    # Model the proxy's explicit response when acceptance commits but its first receipt read fails.
    def unknown_reply(command, request, result):
        if command == "pause":
            return json.dumps({"id": request["id"], "ok": False, "error": "outcome_unknown"})
        return None

    if not drop_reply:
        peer["raw_response"] = unknown_reply
    await hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_pause"}, blocking=True)
    await entry.runtime_data.async_refresh()
    pause, lookup = (request for request in peer["requests"] if request["command"] != "status")
    assert pause["command"] == "pause"
    assert lookup["command"] == "receipt" and lookup["request_id"] == pause["id"] and lookup["id"] != pause["id"]
    assert hass.states.get("sensor.ics_proxy_control_mode").state == "Paused locally"


# Intent: a temporary receipt refusal can recover whether the original Pause reply arrived or was lost.
# Why: the safe follow-up is another read of the same request, never a second charger command.
@pytest.mark.parametrize("drop_reply", [False, True])
@pytest.mark.parametrize("failure", ["not-ready", "socket-close"])
async def test_temporary_pause_receipt_failure_retries_only_the_read(actual_hass, peer, drop_reply, failure):
    await setup(actual_hass, peer)
    peer["lose_reply"] = drop_reply
    peer["pending_receipt"] = True

    # The original Pause still settles. Refuse just the first read of its saved result.
    def first_read_unavailable(command, _request, _result):
        if command == "receipt" and peer["commands"].count("receipt") == 1:
            return json.dumps({"ok": False, "error": "not_ready"})
        return None

    if failure == "socket-close":
        peer["close_first_receipt"] = True
    else:
        peer["raw_response"] = first_read_unavailable
    await actual_hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_pause"}, blocking=True)
    pause, first, final = (request for request in peer["requests"] if request["command"] != "status")
    assert pause["command"] == "pause" and first["command"] == final["command"] == "receipt"
    assert first["request_id"] == final["request_id"] == pause["id"]


# Intent: a possibly sent command needs an exact, readable receipt to confirm success.
# Why: even a freshly paused charger does not identify which request caused the hold.
@pytest.mark.parametrize(
    "lookup_failure", ["wrong-id", "wrong-action", "wrong-action-after-acceptance", "unreadable", "uncertain", "failed"]
)
async def test_lost_pause_reply_requires_a_matching_result(actual_hass, peer, lookup_failure):
    entry = await setup(actual_hass, peer)
    peer["lose_reply"] = True

    # Preserve real response framing while changing only the saved result of the follow-up read.
    def raw_receipt(command, request, result):
        if command != "receipt":
            return None
        if lookup_failure == "unreadable":
            return "{"
        if lookup_failure == "wrong-id":
            result = {**result, "id": "f" * 32}
        elif lookup_failure == "wrong-action-after-acceptance" and peer["commands"].count("receipt") == 1:
            result = {**result, "state": "accepted", "action": "pause"}
        elif lookup_failure.startswith("wrong-action"):
            result = {**result, "action": "start"}
        else:
            result = {**result, "state": lookup_failure}
        return json.dumps({"id": request["id"], "ok": True, "result": result})

    peer["raw_response"] = raw_receipt
    messages = {
        "failed": "could not confirm the requested control state",
        "uncertain": "The proxy recorded the request. The charger outcome is uncertain",
        "wrong-action-after-acceptance": "returned another command's receipt",
    }
    message = messages.get(lookup_failure, "outcome is unknown")
    with pytest.raises(HomeAssistantError, match=message):
        await actual_hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_pause"}, blocking=True)
    await entry.runtime_data.async_refresh()
    pause, *lookups = (request for request in peer["requests"] if request["command"] != "status")
    assert pause["command"] == "pause"
    assert len(lookups) == (2 if lookup_failure == "wrong-action-after-acceptance" else 1)
    assert all(
        lookup["command"] == "receipt" and lookup["request_id"] == pause["id"] and lookup["id"] != pause["id"]
        for lookup in lookups
    )
    assert actual_hass.states.get("sensor.ics_proxy_control_mode").state == "Paused locally"


# Intent: a replaced Pause defers to the current status, while a Return stopped before send explains what happened.
# Why: the client must read the exact outcome without repeating a command or claiming physical confirmation.
@pytest.mark.parametrize("action", ["pause", "app_control"])
async def test_command_outcome_uses_saved_reason(actual_hass, peer, action):
    from custom_components.ics_proxy.api import ProxyError

    entry = await setup(actual_hass, peer)
    peer["lose_reply"] = True
    newer_id = "d" * 32
    newer_read, confirmed = asyncio.Event(), asyncio.Event()

    # The independent peer retains Pause as requested while publishing the old command's separate outcome.
    def saved_receipt(command, request, result):
        if command != "receipt":
            return result
        if request.get("request_id") == newer_id:
            newer_read.set()
            completed = confirmed.is_set()
            if completed:
                peer["state"].update(local_hold_verified=True, pause_pending=False)
            return {"id": newer_id, "action": "pause", "state": "completed" if completed else "accepted"}
        peer["state"].update(
            desired_control="paused", local_hold_verified=False, owner_control_enabled=False, pause_pending=True
        )
        reason = (
            {"classification": "superseded_by_pause", "superseded_by": newer_id}
            if action == "pause"
            else {"classification": "return_not_sent"}
        )
        return {**result, "state": "failed", "result": reason}

    peer["result_filter"] = saved_receipt
    if action == "pause":
        press = asyncio.create_task(
            actual_hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_pause"}, blocking=True)
        )
        try:
            await asyncio.wait_for(newer_read.wait(), 3)
            assert not press.done() and not peer["state"]["local_hold_verified"]
        finally:
            confirmed.set()
            await asyncio.wait_for(press, 3)
        assert entry.runtime_data.data["local_hold_verified"]
    else:
        with pytest.raises(ProxyError, match="stopped before sending the release command"):
            await entry.runtime_data.client.command(action, 0)
    sent, lookup, *later = [request for request in peer["requests"] if request["command"] != "status"]
    assert sent["command"] == action and lookup["command"] == "receipt" and lookup["request_id"] == sent["id"]
    assert len(later) == (2 if action == "pause" else 0)
    assert all(request["command"] == "receipt" and request["request_id"] == newer_id for request in later)


# Intent: a transport failure during sending can only be resolved by the matching saved receipt.
# Why: send_json can fail before sending any bytes or after the complete request reached the real local peer.
@pytest.mark.parametrize("after_send", [False, True])
@pytest.mark.parametrize("command", ["pause", "start", "start_test"])
async def test_send_failure_uses_uncertain_delivery_wording(actual_hass, peer, monkeypatch, after_send, command):
    entry = await setup(actual_hass, peer)
    client = entry.runtime_data.client
    from custom_components.ics_proxy.api import ProxyError

    original, attempts = ClientWebSocketResponse.send_json, []

    # Fail only the actual client transport boundary; the real HA client and server still own their protocol state.
    async def failed_send(connection, value, *args, **kwargs):
        if value.get("command") != command:
            return await original(connection, value, *args, **kwargs)
        attempts.append(value)
        if after_send:
            await original(connection, value, *args, **kwargs)
        raise ClientConnectionError("synthetic send interruption")

    monkeypatch.setattr(ClientWebSocketResponse, "send_json", failed_send)
    if after_send:
        await client.command(command, 0)
    else:
        with pytest.raises(ProxyError, match="may have been sent; its outcome is unknown"):
            await client.command(command, 0)
    assert len(attempts) == 1
    await actual_hass.async_block_till_done()
    if after_send:
        async with asyncio.timeout(2):
            while peer["commands"].count(command) == 0:
                await asyncio.sleep(0.01)
    assert peer["commands"].count(command) == int(after_send)
    (lookup,) = (request for request in peer["requests"] if request["command"] == "receipt")
    assert lookup["request_id"] == attempts[0]["id"]


# Intent: pending Start and current work retain their actual operation label under both app and paused intent.
# Why: a persisted paused intent does not mean a newly pending Start is a new Pause or a completed physical hold.
@pytest.mark.parametrize("mode", ["app", "paused"])
@pytest.mark.parametrize(
    "fault,label",
    [
        ("start_operation_pending", "Start pending"),
        ("current_update_pending", "Updating current"),
    ],
)
async def test_pending_operation_label_does_not_invent_pause(actual_hass, peer, mode, fault, label):
    peer["state"].update(
        healthy=False,
        local_healthy=False,
        state="reconciling",
        fault=fault,
        desired_control=mode,
        owner_control_enabled=False,
        local_hold_verified=False,
        can_start=False,
        can_start_test=False,
        current_a=None,
        native_age_seconds=None,
        power_w=None,
        station_ceiling_a=None,
    )
    entry = await setup(actual_hass, peer)
    assert actual_hass.states.get("sensor.ics_proxy_control_mode").state == label
    assert actual_hass.states.get("button.ics_proxy_start").state == "unavailable"
    assert actual_hass.states.get("sensor.ics_proxy_current").state == "unavailable"
    peer["state"]["control_mode"] = "waiting"
    await entry.runtime_data.async_refresh()
    assert actual_hass.states.get("sensor.ics_proxy_control_mode").state == "Waiting for verification"


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
    assert hass.states.get("sensor.ics_proxy_control_mode").state == "App / schedule"
    requests = len(peer["commands"])
    await asyncio.sleep(1.2)
    await hass.async_block_till_done()
    assert len(peer["commands"]) == requests
    assert hass.states.get("sensor.ics_proxy_power").state == "unavailable"
    assert hass.states.get("binary_sensor.ics_proxy_vehicle_connected").state == "unavailable"
    assert hass.states.get("sensor.ics_proxy_last_session_energy").state == "unavailable"
    assert hass.states.get("binary_sensor.ics_proxy_problem").state == "on"
    assert hass.states.get("sensor.ics_proxy_control_mode").state == "Waiting for verification"
    assert hass.states.get("button.ics_proxy_start_test").state == "unavailable"
    assert hass.states.get("button.ics_proxy_start").state == "unavailable"
    peer["state"]["native_age_seconds"] = 0
    await entry.runtime_data.async_refresh()
    assert hass.states.get("sensor.ics_proxy_power").state == "0"
    assert hass.states.get("sensor.ics_proxy_control_mode").state == "App / schedule"
    assert hass.states.get("button.ics_proxy_start_test").state != "unavailable"
    assert hass.states.get("button.ics_proxy_start").state != "unavailable"


# Intent: verified local app control names a cloud outage and returns to its ordinary label after cloud recovery.
# Why: local control stays available during an outage; Waiting for verification wrongly implies unfinished work.
async def test_app_schedule_mode_reports_local_only_and_cloud_recovery(actual_hass, peer):
    peer["state"].update(healthy=False, local_healthy=True, cloud_connected=False, state="local_only")
    entry = await setup(actual_hass, peer)
    assert actual_hass.states.get("sensor.ics_proxy_control_mode").state == "App / schedule (cloud offline)"
    assert actual_hass.states.get("button.ics_proxy_start").state != "unavailable"
    peer["state"].update(healthy=True, cloud_connected=True, state="healthy")
    await entry.runtime_data.async_refresh()
    assert actual_hass.states.get("sensor.ics_proxy_control_mode").state == "App / schedule"
    assert set(peer["commands"]) == {"status"}


# Intent: locally verified app control expires on either reading deadline even while the cloud remains offline.
# Why: the outage label cannot preserve stale local permission between management polls.
@pytest.mark.parametrize("age_key", ["native_age_seconds", "status_age_seconds"])
async def test_local_only_app_mode_expires_between_polls(actual_hass, peer, age_key):
    peer["state"].update(healthy=False, local_healthy=True, cloud_connected=False, state="local_only")
    peer["state"][age_key] = 14
    await setup(actual_hass, peer)
    assert actual_hass.states.get("sensor.ics_proxy_control_mode").state == "App / schedule (cloud offline)"
    requests = len(peer["commands"])
    await asyncio.sleep(1.2)
    await actual_hass.async_block_till_done()
    assert actual_hass.states.get("sensor.ics_proxy_control_mode").state == "Waiting for verification"
    assert len(peer["commands"]) == requests


# Intent: cloud outage presentation cannot stand in for owner permission, service readiness or local health.
# Why: only fresh verified app control deserves a stable app/schedule label.
@pytest.mark.parametrize("missing_proof", ["owner_control_enabled", "running", "local_healthy"])
async def test_local_only_app_mode_keeps_verification_guards(actual_hass, peer, missing_proof):
    peer["state"].update(healthy=False, local_healthy=True, cloud_connected=False, state="local_only")
    peer["state"][missing_proof] = False
    await setup(actual_hass, peer)
    assert actual_hass.states.get("sensor.ics_proxy_control_mode").state == "Waiting for verification"
    assert set(peer["commands"]) == {"status"}


# Intent: absent or unknown cloud connection facts cannot be displayed as a confirmed cloud outage.
# Why: a locally healthy status can establish app control without establishing why cloud health is unavailable.
@pytest.mark.parametrize("missing", [False, True], ids=["unknown", "absent"])
async def test_app_schedule_mode_does_not_invent_cloud_outage(actual_hass, peer, missing):
    peer["state"].update(healthy=False, local_healthy=True, cloud_connected=None, state="local_only")
    if missing:
        del peer["state"]["cloud_connected"]
    await setup(actual_hass, peer)
    assert actual_hass.states.get("sensor.ics_proxy_control_mode").state == "App / schedule"
    assert set(peer["commands"]) == {"status"}


# Intent: a normal explicit Start works from ready app or paused control and a known zero-watt session stays idle.
# Why: starting a session can resume a deliberate pause but cannot manufacture actual power or a test deadline.
@pytest.mark.parametrize("paused", [False, True], ids=["app", "paused"])
async def test_normal_start_from_ready_control_reports_zero_watt_session_honestly(actual_hass, peer, paused):
    if paused:
        peer["state"].update(
            desired_control="paused",
            owner_control_enabled=False,
            local_hold_verified=True,
            station_ceiling_a=0,
            can_return=True,
            can_start_test=False,
        )
    peer["pending_receipt"] = True
    hass = actual_hass
    entry = await setup(hass, peer)
    assert hass.states.get("button.ics_proxy_start").state != "unavailable"
    await hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_start"}, blocking=True)
    mode = hass.states.get("sensor.ics_proxy_control_mode")
    assert mode.state == "Local session" and mode.attributes["local_charge"] == {"state": "running", "charging": False}
    assert mode.attributes["test_charge"] is None
    assert hass.states.get("sensor.ics_proxy_power").state == "0"
    assert hass.states.get("binary_sensor.ics_proxy_charging").state == "off"
    assert hass.states.get("button.ics_proxy_start").state == "unavailable"
    request = next(value for value in peer["requests"] if value["command"] == "start")
    assert set(request) == {"id", "command", "revision"} and request["revision"] == 0
    assert peer["commands"].count("start") == 1 and peer["commands"].count("receipt") == 1
    await hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_start"}, blocking=True)
    assert peer["commands"].count("start") == 1
    peer["state"].update(power_w=1440, current_a=6, local_charge={"state": "running", "charging": True})
    await entry.runtime_data.async_refresh()
    assert hass.states.get("sensor.ics_proxy_control_mode").state == "Local charging"
    assert hass.states.get("binary_sensor.ics_proxy_charging").state == "on"
    assert "start_test" not in peer["commands"]


# Intent: a normal Start with a lost response is recovered from its original receipt.
# Why: acknowledgement uncertainty cannot authorize a second session or silently convert it into a timed test.
async def test_lost_normal_start_reply_does_not_retry(actual_hass, peer):
    hass = actual_hass
    entry = await setup(hass, peer)
    peer["lose_reply"] = True
    await hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_start"}, blocking=True)
    await entry.runtime_data.async_refresh()
    start, lookup = (request for request in peer["requests"] if request["command"] != "status")
    assert start["command"] == "start"
    assert lookup["command"] == "receipt" and lookup["request_id"] == start["id"]
    assert hass.states.get("sensor.ics_proxy_control_mode").state == "Local session"
    assert hass.states.get("button.ics_proxy_start").state == "unavailable"


# Intent: the normal Start uses both the explicit readiness flag and independently fresh health evidence.
# Why: a stale ready flag or refused start condition cannot surface as an actionable charging control.
@pytest.mark.parametrize(
    "change", [{"can_start": False}, {"healthy": False}, {"native_age_seconds": 90}, {"status_age_seconds": 90}]
)
async def test_normal_start_requires_fresh_readiness(actual_hass, peer, change):
    peer["state"].update(change)
    await setup(actual_hass, peer)
    assert actual_hass.states.get("button.ics_proxy_start").state == "unavailable"
    await actual_hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_start"}, blocking=True)
    assert set(peer["commands"]) == {"status"}


# Intent: local lifecycle labels are distinct from measured charging and disclose only the two public fields.
# Why: a starting or stopping session does not prove present power, and private protocol identifiers stay private.
@pytest.mark.parametrize(
    "state,charging,label",
    [
        ("starting", False, "Starting locally"),
        ("awaiting_session", False, "Starting locally"),
        ("running", False, "Local session"),
        ("running", True, "Local charging"),
        ("stopping", False, "Stopping locally"),
    ],
)
async def test_local_charge_labels_and_progress_are_redacted(actual_hass, peer, state, charging, label):
    local = {"state": state, "charging": charging}
    peer["state"].update(
        can_start=False,
        local_charge=local | {"transaction_id": "private", "id_tag": "secret"},
    )
    await setup(actual_hass, peer)
    mode = actual_hass.states.get("sensor.ics_proxy_control_mode")
    assert mode.state == label and mode.attributes["local_charge"] == local


# Intent: expired local-session facts yield to verification status and remove their extra attributes.
# Why: an old running session or charging flag must not survive its independent observation deadline.
async def test_local_charge_progress_expires_between_polls(actual_hass, peer):
    peer["state"].update(native_age_seconds=14, can_start=False, local_charge={"state": "running", "charging": True})
    await setup(actual_hass, peer)
    assert actual_hass.states.get("sensor.ics_proxy_control_mode").state == "Local charging"
    requests = len(peer["commands"])
    await asyncio.sleep(1.2)
    await actual_hass.async_block_till_done()
    mode = actual_hass.states.get("sensor.ics_proxy_control_mode")
    assert mode.state == "Waiting for verification" and mode.attributes["local_charge"] is None
    assert actual_hass.states.get("button.ics_proxy_start").state == "unavailable"
    assert len(peer["commands"]) == requests


# Intent: completed/interrupted/failed normal sessions yield to the authoritative app or retained pause mode.
# Why: a terminal record describes the previous local session and cannot imply that charging continues.
@pytest.mark.parametrize("state", ["completed", "interrupted", "failed"])
@pytest.mark.parametrize("paused", [False, True])
async def test_terminal_local_charge_returns_to_current_control_mode(actual_hass, peer, state, paused):
    peer["state"].update(
        local_charge={"state": state, "charging": False},
        desired_control="paused" if paused else "app",
        local_hold_verified=paused,
        owner_control_enabled=not paused,
        station_ceiling_a=0 if paused else 6,
    )
    await setup(actual_hass, peer)
    mode = actual_hass.states.get("sensor.ics_proxy_control_mode")
    assert mode.state == ("Paused locally" if paused else "App / schedule")
    assert mode.attributes["local_charge"] == {"state": state, "charging": False}


# Intent: one explicit press requests the fixed test once and polls its receipt without another Start command.
# Why: waiting for acceptance or pressing again during the active test cannot silently initiate a second charge.
async def test_explicit_test_button_sends_one_fixed_request_and_disables_duplicate_press(actual_hass, peer):
    hass = actual_hass
    peer["pending_receipt"] = True
    await setup(hass, peer)
    await hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_start_test"}, blocking=True)
    assert hass.states.get("sensor.ics_proxy_control_mode").state == "Local test"
    assert hass.states.get("button.ics_proxy_start_test").state == "unavailable"
    assert peer["commands"].count("start_test") == 1 and peer["commands"].count("receipt") == 1
    request = next(value for value in peer["requests"] if value["command"] == "start_test")
    assert set(request) == {"id", "command", "revision"} and request["revision"] == 0
    await hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_start_test"}, blocking=True)
    assert peer["commands"].count("start_test") == 1


# Intent: a lost test-start response is recovered without repeating the mutation.
# Why: an accepted test and its automatic Stop remain service-owned even when HA misses the response.
async def test_lost_test_start_reply_never_retries_or_rearms(actual_hass, peer):
    hass = actual_hass
    entry = await setup(hass, peer)
    peer["lose_reply"] = True
    await hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_start_test"}, blocking=True)
    await entry.runtime_data.async_refresh()
    start, lookup = (request for request in peer["requests"] if request["command"] != "status")
    assert start["command"] == "start_test"
    assert lookup["command"] == "receipt" and lookup["request_id"] == start["id"]
    assert hass.states.get("button.ics_proxy_start_test").state == "unavailable"
    assert hass.states.get("sensor.ics_proxy_control_mode").state == "Local test"


# Intent: both the service readiness flag and current independent health are required for the test button.
# Why: an old ready flag cannot enable charging during a fault, an expired read or a refused readiness check.
@pytest.mark.parametrize(
    "change",
    [{"can_start_test": False}, {"healthy": False}, {"native_age_seconds": 90}, {"status_age_seconds": 90}],
)
async def test_test_charge_button_requires_fresh_confirmed_readiness(actual_hass, peer, change):
    peer["state"].update(change)
    hass = actual_hass
    await setup(hass, peer)
    assert hass.states.get("button.ics_proxy_start_test").state == "unavailable"
    await hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_start_test"}, blocking=True)
    assert set(peer["commands"]) == {"status"}


# Intent: active local and app test states have distinct labels and expose only their redacted fixed-test progress.
# Why: protocol identifiers and arbitrary peer fields must not reach a household dashboard's state attributes.
@pytest.mark.parametrize("source,label", [("local", "Local test"), ("cloud", "App test")])
@pytest.mark.parametrize("state", ["armed", "starting", "awaiting_power", "charging", "stopping"])
async def test_active_test_mode_labels_and_attributes_are_redacted(actual_hass, peer, source, label, state):
    test = {"source": source, "state": state, "duration_seconds": 120, "seconds_remaining": 72}
    peer["state"].update(can_start_test=False, test_charge=test | {"transaction_id": "private", "token": "secret"})
    await setup(actual_hass, peer)
    mode = actual_hass.states.get("sensor.ics_proxy_control_mode")
    assert mode.state == label and mode.attributes["test_charge"] == test
    assert actual_hass.states.get("button.ics_proxy_start_test").state == "unavailable"


# Intent: expired test information yields to verification status, hides progress and cannot keep a Start ready.
# Why: a live-looking countdown or source label must not survive the observation that established it.
async def test_active_test_mode_and_progress_expire_without_a_network_poll(actual_hass, peer):
    peer["state"].update(
        native_age_seconds=14,
        can_start_test=False,
        test_charge={"source": "local", "state": "charging", "duration_seconds": 120, "seconds_remaining": 72},
    )
    await setup(actual_hass, peer)
    assert actual_hass.states.get("sensor.ics_proxy_control_mode").state == "Local test"
    requests = len(peer["commands"])
    await asyncio.sleep(1.2)
    await actual_hass.async_block_till_done()
    mode = actual_hass.states.get("sensor.ics_proxy_control_mode")
    assert mode.state == "Waiting for verification" and mode.attributes["test_charge"] is None
    assert actual_hass.states.get("button.ics_proxy_start_test").state == "unavailable"
    assert len(peer["commands"]) == requests


# Intent: terminal test records remain progress history and no longer override the ordinary control-mode label.
# Why: a completed or interrupted test is not evidence that a test charge is still running.
@pytest.mark.parametrize("state", ["completed", "interrupted"])
async def test_terminal_test_status_returns_to_ordinary_mode(actual_hass, peer, state):
    peer["state"]["test_charge"] = {"source": "local", "state": state, "duration_seconds": 120, "seconds_remaining": 0}
    await setup(actual_hass, peer)
    mode = actual_hass.states.get("sensor.ics_proxy_control_mode")
    assert mode.state == "App / schedule" and mode.attributes["test_charge"]["state"] == state


# Intent: a measured-output fault raises the HA alarm even with fresh metering.
# Why: the notification persists through retries and clears once recovery is published.
async def test_measured_output_alarm(actual_hass, peer):
    entry = await setup(actual_hass, peer)
    c = entry.runtime_data
    peer["state"].update(native_age_seconds=0, healthy=False, state="degraded", fault="measured_output_limit_exceeded")
    await c.async_refresh()
    assert actual_hass.states.get("binary_sensor.ics_proxy_problem").state == "on" and not c.notified
    c.problem_since = time.monotonic() - 61
    await c.async_refresh()
    assert c.notified
    peer["state"].update(healthy=True, state="healthy", fault=None)
    await c.async_refresh()
    assert not c.notified and actual_hass.states.get("binary_sensor.ics_proxy_problem").state == "off"
