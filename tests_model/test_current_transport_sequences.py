"""Every literal current pair reaches the authenticated API, exact wire, durable receipt and idle physical view."""

import asyncio
import copy
import uuid

import pytest
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings
from websockets.asyncio.client import connect

from tests import test_native_bounded_session as native_bench
from tests.test_managed_api import TOKEN, call
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_proxy import healthy, starts
from tests.test_managed_start_refusal_native32 import commissioned
from tests_model.catalog import input_digest
from tests_model.current_obligations import require_completed_current, require_transport_summary
from tests_model.deep_cases import current_transport_candidates
from tests_model.test_schedule_sequences import listener_port, tls_for_case
from tests_model.test_schedule_sequences import model_tls as model_tls

EVIDENCE = (
    "reset_verified",
    "wire_value_verified",
    "exact_receipt_verified",
    "replay_no_effect",
    "future_value_verified",
    "physical_verified",
)


# Extract the actual reserved profile's literal wire value without calling the engine's encoder or validator.
def wire_limits(frames):
    return [
        frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
        for frame in frames
        if frame[2] == "SetChargingProfile" and frame[3]["csChargingProfiles"]["chargingProfileId"] == 7001
    ]


# Snapshot only durable control/request state; background cloud heartbeat counters are not receipt side effects.
def durable_control(j):
    return {
        "intent": j.get("managed_control_intent"),
        "control": j.get("control"),
        "current": j.get("managed_current_change"),
        "requests": [tuple(row) for row in j.db.execute("SELECT * FROM managed_requests ORDER BY id")],
        "arguments": [tuple(row) for row in j.db.execute("SELECT * FROM managed_current_requests ORDER BY id")],
    }


# A real API command completes only when its own exact durable receipt says the independently expected literal.
async def choose(socket, b, value):
    identifier, revision = uuid.uuid4().hex, b.controller.requests.intent().revision
    before = len(b.rig.bench.device_calls)
    reply = await call(socket, "set_current", id=identifier, revision=revision, current_a=value)
    assert reply["ok"] and reply["result"]["id"] == identifier
    await b.controller.current.settle()
    await healthy(b.controller)
    receipt = (await call(socket, "receipt", request_id=identifier))["result"]
    assert receipt["id"] == identifier and receipt["state"] == "completed"
    assert receipt["result"] == {"current_a": value, "charging_started": False}
    return identifier, revision, receipt, copy.deepcopy(b.rig.bench.device_calls[before:])


# Intent: no pair is sampled away; each intent resets its exact old selection before its target and UUID replay.
# Why: a reduced recovery suffix cannot stand in for an untested literal frame, pair metadata or same-value receipt.
@pytest.mark.parametrize("case", current_transport_candidates(), ids=lambda case: case.identifier)
async def test_current_transport_sequences(tmp_path, model_tls, monkeypatch, record_property, case):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    events, complete = [], False
    evidence = dict.fromkeys(EVIDENCE, False)
    try:
        async with commissioned(
            tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch, case.initial
        ) as b:
            c, j, firmware = b.controller, b.session.journal, b.rig.bench.firmware
            plans, configuration = copy.deepcopy(firmware.plans), copy.deepcopy(b.rig.peer.configuration)
            published = {**await healthy(c), "running": True, "certificate_remaining_seconds": 86400}
            api = ManagementServer(
                ManagementSettings(listener_port(), tmp_path / "unused", TOKEN), c, lambda: published
            )
            await api.start()
            try:
                async with connect(
                    f"ws://127.0.0.1:{api.settings.port}/management",
                    additional_headers={"Authorization": "Bearer " + TOKEN},
                    subprotocols=["ics-proxy.v1"],
                ) as socket:
                    if case.intent == "paused":
                        assert (await call(socket, "pause", revision=c.requests.intent().revision))["ok"]
                        if c.pause_task is not None:
                            await c.pause_task
                        assert (await healthy(c))["local_hold_verified"] and firmware.effective_limit() == 0
                    _, _, _, reset_frames = await choose(socket, b, case.initial)
                    assert c.current.selected() == case.initial
                    assert wire_limits(reset_frames) == ([] if case.intent == "paused" else [case.initial])
                    assert firmware.effective_limit() == (0 if case.intent == "paused" else case.initial)
                    reset_selected, reset_profile = c.current.selected(), firmware.effective_limit()
                    evidence["reset_verified"] = True
                    events.append("reset")
                    identifier, revision, receipt, frames = await choose(socket, b, case.target)
                    require_completed_current(b, identifier, revision, case.target, case.intent)
                    events.append("choose")
                    evidence["exact_receipt_verified"] = True
                    events.append("receipt")
                    assert wire_limits(frames) == ([] if case.intent == "paused" else [case.target])
                    evidence["wire_value_verified"] = True
                    before, durable = copy.deepcopy(b.rig.bench.device_calls), durable_control(j)
                    duplicate = await call(
                        socket, "set_current", id=identifier, revision=revision, current_a=case.target
                    )
                    await c.current.settle()
                    assert duplicate["ok"] and duplicate["result"] == receipt
                    assert (await call(socket, "receipt", request_id=identifier))["result"] == receipt
                    assert b.rig.bench.device_calls == before and durable_control(j) == durable
                    evidence["replay_no_effect"] = True
                    events.append("replay")
                    native = await c.native.read(closed=True)
                    assert c.current.selected() == case.target
                    assert firmware.effective_limit() == (0 if case.intent == "paused" else case.target)
                    assert firmware.current == 0 and not native.physical.transaction_open
                    assert all(value == 0 for value in native.currents_a + native.powers_w)
                    assert firmware.plans == plans and b.rig.peer.configuration == configuration
                    assert not starts(b.rig) and j.next_delivery() is None
                    require_transport_summary(
                        case.initial,
                        case.target,
                        case.intent,
                        {
                            "reset_selected": reset_selected,
                            "reset_profile": reset_profile,
                            "receipt_state": receipt["state"],
                            "receipt_current": receipt["result"]["current_a"],
                            "selected": c.current.selected(),
                            "profile": firmware.effective_limit(),
                            "new_profile_limits": wire_limits(frames),
                            "replayed_receipt_equal": duplicate["result"] == receipt,
                            "replay_new_wires": len(b.rig.bench.device_calls) - len(before),
                            "replay_durable_equal": durable_control(j) == durable,
                        },
                    )
                    if case.intent == "paused":
                        returning, revision = uuid.uuid4().hex, c.requests.intent().revision
                        before_return = len(b.rig.bench.device_calls)
                        assert (await call(socket, "app_control", id=returning, revision=revision))["ok"]
                        async with asyncio.timeout(5):
                            while True:
                                status = await healthy(c)
                                terminal = (await call(socket, "receipt", request_id=returning))["result"]
                                if terminal["state"] == "completed":
                                    assert terminal["id"] == returning
                                    assert status["desired_control"] == "app" and status["normal_start_ready"]
                                    break
                                assert terminal["state"] in ("accepted", "running")
                                await asyncio.sleep(0.05)
                        assert c.current.selected() == firmware.effective_limit() == case.target
                        assert wire_limits(b.rig.bench.device_calls[before_return:]) == [case.target]
                        events.append("return")
                    evidence["future_value_verified"] = True
                    evidence["physical_verified"] = True
                    events.append("physical")
                    complete = True
            finally:
                await api.close()
    finally:
        record_property("model_case", case.identifier)
        record_property("model_family", "current_transport")
        record_property("model_input_sha256", input_digest("current_transport", case))
        record_property("completed_events", events)
        record_property("suffix_complete", complete)
        for name, value in evidence.items():
            record_property(name, value)
