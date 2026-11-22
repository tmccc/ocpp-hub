"""Actual HA and raw loopback replies keep decoder/numeric failures inside the ordinary outage/recovery path."""

import json
import sys
import time
from types import SimpleNamespace

import pytest
from homeassistant.exceptions import HomeAssistantError

from tests_ha_proxy.test_client import peer as peer
from tests_ha_proxy.test_client import setup
from tests_ha_proxy.test_setup_outage_notice import _async_get_or_create_notifications


# Intent: a busy or displaced management socket tells the client that Pause was never admitted.
# Why: a definite pre-admission close should never be mistaken for a lost reply to an accepted command.
@pytest.mark.parametrize("reason", ["Idle connection replaced", "Busy"])
async def test_pre_admission_close_has_a_definite_outcome(actual_hass, peer, reason):
    from custom_components.ics_proxy.api import ProxyError

    entry = await setup(actual_hass, peer)
    peer["pre_admission_close"] = reason
    with pytest.raises(ProxyError, match="too busy to accept this request"):
        await entry.runtime_data.client.request("pause", revision=0)
    assert peer["commands"][-1] == "pause"


# Intent: recursive JSON and out-of-range integer ages start the real outage deadline and later recover normally.
# Why: decoder/arithmetic exceptions must not bypass ProxyError/freshness handling or permanently suppress notices.
@pytest.mark.parametrize("shape", ["recursive-json", "huge-native-age", "huge-status-age"])
async def test_adversarial_status_retains_outage_and_recovery(actual_hass, peer, monkeypatch, shape):
    entry = await setup(actual_hass, peer)
    c = entry.runtime_data
    module = sys.modules["custom_components.ics_proxy.coordinator"]
    clock, original_clock = SimpleNamespace(value=0.0), time.monotonic
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: original_clock() + clock.value))

    # Raw text reaches the real JSON decoder; the independent peer never tries to encode a recursive Python object.
    def raw(command, request, result):
        if command == "status" and shape == "recursive-json":
            assert sys.getrecursionlimit() < 25000
            return '{"id":' + json.dumps(request["id"]) + ',"ok":true,"result":' + "[" * 25000 + "0" + "]" * 25000 + "}"
        return None

    peer["raw_response"] = raw
    if shape.startswith("huge-"):
        peer["state"]["native_age_seconds" if shape == "huge-native-age" else "status_age_seconds"] = 10**400
    await c.async_refresh()
    assert c.problem_since is not None
    assert not c.last_update_success or not c.fresh_healthy
    assert actual_hass.states.get("button.ics_proxy_start").state == "unavailable"
    clock.value += 61
    await c.async_refresh()
    assert c.notification_id in _async_get_or_create_notifications(actual_hass)
    peer["raw_response"] = None
    peer["state"].update(native_age_seconds=0, status_age_seconds=0)
    await c.async_refresh()
    assert c.fresh_healthy and c.problem_since is None
    assert c.notification_id not in _async_get_or_create_notifications(actual_hass)
    assert set(peer["commands"]) == {"status"}


# Intent: malformed native history leaves last energy unknown and preserves valid, redacted older rows.
# Why: entity publication cannot trust nested shape or substitute an older row for an invalid latest record.
async def test_invalid_nested_history_preserves_siblings(actual_hass, peer):
    valid = peer["state"]["charger_sessions"][0].copy()
    peer["state"]["charger_sessions"] = [
        None,
        {**valid, "energy_kwh": 10**400},
        {**valid, "energy_kwh": -1},
        {**valid, "stopped_at": "2025-01-01T00:00:00Z"},
        {**valid, "private_detail": "not public"},
    ]
    entry = await setup(actual_hass, peer)
    sensor_id = "sensor.ics_proxy_last_session_energy"
    state = actual_hass.states.get(sensor_id)
    assert state.state == "unknown"
    assert state.attributes["charger_sessions"] == [
        {key: valid[key] for key in ("started_at", "stopped_at", "energy_kwh")}
    ]
    peer["state"]["charger_sessions"] = [valid]
    await entry.runtime_data.async_refresh()
    assert actual_hass.states.get(sensor_id).state == str(valid["energy_kwh"])
    peer["state"]["last_session_energy_kwh"] = None
    await entry.runtime_data.async_refresh()
    assert actual_hass.states.get(sensor_id).state == "unknown"
    peer["state"]["last_session_energy_kwh"] = True
    await entry.runtime_data.async_refresh()
    assert actual_hass.states.get(sensor_id).state == "unknown"
    peer["state"]["last_session_energy_kwh"] = 0.0
    peer["state"]["charger_sessions"] = [None, valid]
    await entry.runtime_data.async_refresh()
    assert actual_hass.states.get(sensor_id).state == "0.0"


# Intent: even a direct enormous-integer current request gets a safe validation error before any management command.
# Why: HA's normal service coercion is not the numeric boundary's only callable path.
async def test_huge_integer_current_is_safely_rejected(actual_hass, peer):
    entry = await setup(actual_hass, peer)
    from custom_components.ics_proxy.number import ProxyCurrent

    control = ProxyCurrent(entry.runtime_data)
    before = list(peer["commands"])
    with pytest.raises(HomeAssistantError):
        await control.async_set_native_value(10**400)
    assert peer["commands"] == before


# Intent: every malformed countdown stays unknown while a later valid observation restores ordinary test progress.
# Why: an enormous JSON integer must not break HA entity publication or leave the previous countdown displayed.
@pytest.mark.parametrize(
    "remaining",
    [10**400, -(10**400), True, None, "72", -1, 121],
    ids=["huge-positive", "huge-negative", "bool", "null", "string", "negative", "over-120"],
)
async def test_invalid_countdown_does_not_break_entity_updates(actual_hass, peer, remaining):
    peer["state"]["test_charge"] = {
        "source": "local",
        "state": "charging",
        "duration_seconds": 120,
        "seconds_remaining": 72,
    }
    entry = await setup(actual_hass, peer)
    entity = "sensor.ics_proxy_control_mode"
    assert actual_hass.states.get(entity).attributes["test_charge"]["seconds_remaining"] == 72
    peer["state"]["test_charge"]["seconds_remaining"] = remaining
    await entry.runtime_data.async_refresh()
    assert actual_hass.states.get(entity).state == "Waiting for verification"
    assert actual_hass.states.get(entity).attributes["test_charge"] is None
    peer["state"]["test_charge"]["seconds_remaining"] = 71
    await entry.runtime_data.async_refresh()
    assert actual_hass.states.get(entity).attributes["test_charge"]["seconds_remaining"] == 71
    assert set(peer["commands"]) == {"status"}


# Produce malformed bytes independently of the production JSON/receipt decoder and its validation rules.
def malformed_reply(shape, identifier, result):
    if shape == "recursive":
        return "[" * 25000 + "0" + "]" * 25000
    if shape == "oversized":
        return json.dumps({"id": identifier, "ok": True, "result": result, "padding": "x" * 65536})
    literals = {"empty": "", "truncated": "{", "array": "[]", "null": "null", "scalar": "1"}
    if shape in literals:
        return literals[shape]
    envelope = {"id": identifier, "ok": True, "result": {}}
    if shape == "wrong-id":
        envelope["id"] = "0" * 32 if identifier != "0" * 32 else "1" * 32
    elif shape == "missing-id":
        del envelope["id"]
    elif shape == "string-ok":
        envelope["ok"] = "true"
    else:
        envelope["result"] = []
    return json.dumps(envelope)


# Intent: eleven raw response failures are classified at status, mutation acknowledgement and receipt boundaries.
# Why: malformed replies at each position must retain honest acceptance, never retry writes, and recover on fresh reads.
@pytest.mark.parametrize(
    "shape",
    [
        "empty",
        "truncated",
        "array",
        "null",
        "scalar",
        "recursive",
        "oversized",
        "wrong-id",
        "missing-id",
        "string-ok",
        "array-result",
    ],
)
@pytest.mark.parametrize("position", ["status", "pause", "receipt"])
async def test_raw_reply_failures_never_retry_and_later_recover(actual_hass, peer, shape, position):
    entry = await setup(actual_hass, peer)
    revision = peer["state"]["control_revision"]
    peer["pending_receipt"] = position == "receipt"

    # Change only one class of actual peer response; every other management exchange uses its ordinary contract.
    def raw(command, request, result):
        return malformed_reply(shape, request["id"], result) if command == position else None

    peer["raw_response"] = raw
    looked_up = position == "receipt"
    if position == "status":
        await entry.runtime_data.async_refresh()
        assert not entry.runtime_data.last_update_success
        assert entry.runtime_data.problem_since is not None
        assert set(peer["commands"]) == {"status"}
    else:
        lost_first_reply = position == "pause" and shape in ("empty", "truncated", "oversized")
        if lost_first_reply:
            await actual_hass.services.async_call(
                "button", "press", {"entity_id": "button.ics_proxy_pause"}, blocking=True
            )
            looked_up = True
        elif position == "pause" and shape == "recursive":
            # A decoder exception permits a receipt read; a parsed invalid envelope is still refused.
            try:
                await actual_hass.services.async_call(
                    "button", "press", {"entity_id": "button.ics_proxy_pause"}, blocking=True
                )
            except HomeAssistantError as error:
                assert str(error) == "The local charger proxy could not confirm this request"
            else:
                looked_up = True
        else:
            message = (
                "The request was recorded; physical confirmation is still pending"
                if position == "receipt"
                else "The local charger proxy could not confirm this request"
            )
            with pytest.raises(HomeAssistantError, match=message):
                await actual_hass.services.async_call(
                    "button", "press", {"entity_id": "button.ics_proxy_pause"}, blocking=True
                )
        assert peer["commands"].count("pause") == 1
        assert peer["commands"].count("receipt") == int(looked_up)
        assert peer["state"]["desired_control"] == "paused"
        assert set(peer["commands"]) <= {"status", "pause", "receipt"}
    peer["raw_response"] = None
    await entry.runtime_data.async_refresh()
    assert entry.runtime_data.last_update_success and entry.runtime_data.fresh_healthy
    assert entry.runtime_data.problem_since is None
    if position != "status":
        assert peer["commands"].count("pause") == 1
        assert peer["commands"].count("receipt") == int(looked_up)
        assert peer["state"]["desired_control"] == "paused"
        assert peer["state"]["control_revision"] == revision + 1
        assert peer["state"]["station_ceiling_a"] == 0
        assert peer["state"]["local_hold_verified"] is True
        assert peer["state"]["owner_control_enabled"] is False


# Intent: a valid padded reply below the actual transport bound remains usable at all three exchange positions.
# Why: the oversized negative must depend on the message bound, rather than merely an invalid schema.
@pytest.mark.parametrize("position", ["status", "pause", "receipt"])
async def test_bounded_valid_padded_reply_is_accepted(actual_hass, peer, position):
    entry = await setup(actual_hass, peer)
    peer["pending_receipt"] = position == "receipt"

    # Padding changes message size only; correlation, actual status and receipt content remain correct.
    def raw(command, request, result):
        if command == position:
            return json.dumps({"id": request["id"], "ok": True, "result": result, "padding": "x" * 32000})
        return None

    peer["raw_response"] = raw
    if position == "status":
        await entry.runtime_data.async_refresh()
        assert entry.runtime_data.last_update_success
    else:
        await actual_hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_pause"}, blocking=True)
        assert peer["commands"].count("pause") == 1
        assert peer["commands"].count("receipt") == int(position == "receipt")
