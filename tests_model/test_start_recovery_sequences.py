"""Cross failure, outage restoration, Pause, receipt replay and abrupt replacement against real peers."""

import asyncio
import copy
import os
import uuid
from contextlib import AsyncExitStack

import pytest
from ocpp_hub_core.frames import build_call_error
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.managed import ManagedController

from tests import conftest as _engine  # noqa: F401 - initialize the HA-free package before importing it.
from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import crash
from tests.recovery_barriers import returned_to_app, witnessed_native_loss
from tests.test_managed_proxy import healthy, managed_session, starts
from tests.test_managed_start_cap_failure import BeforeCapFailurePeer
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_test_charge import ChargeRig, positive_flow
from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import until
from tests_model.catalog import input_digest
from tests_model.recovery_cases import start_cases
from tests_model.test_schedule_sequences import listener_port, tls_for_case
from tests_model.test_schedule_sequences import model_tls as model_tls


# Select one ordering per physical/fault seed solely for explicit development checks.
def selected_cases():
    cases = start_cases()
    if os.environ.get("MODEL_SMOKE") == "1":
        return cases[::30]
    if os.environ.get("MODEL_SMOKE") == "orders":
        return tuple({case.identifier: case for case in (*cases[:30], *cases[::30])}.values())
    return cases


# A few fair turns observe genuine state transitions; no predicate or engine method supplies the expected answer.
async def observe(c, count=4):
    for _ in range(count):
        await c.tick()
        await asyncio.sleep(0)
    if c.pause_task is not None:
        await c.pause_task


# Close both real lifecycle channels in the requested order without sending a guessed RemoteStop.
async def close_daily(b, local, first, *, timestamp=None, meter_stop=1100):
    peer, firmware = b.rig.peer, b.rig.bench.firmware
    peer.permission_enabled = False
    firmware.requested_current = 0
    b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"

    # A genuine immutable report has the original transaction identity and its actual final meter value.
    async def report():
        reply = await b.rig.bench.call(
            b.charger,
            uuid.uuid4().hex,
            "StopTransaction",
            {"transactionId": local, "timestamp": timestamp or START["timestamp"], "meterStop": meter_stop},
        )
        assert reply[0] == 3
        await until(lambda: b.session.journal.next_delivery() is None)

    if first == "native":
        firmware.active = None
        await observe(b.controller)
        await report()
    else:
        await report()
        firmware.active = local
        await observe(b.controller)
        firmware.active = None
    peer.plan_session = None
    await observe(b.controller)


# Intent: all causally admitted five-event orders must preserve zero authority and recover the next real daily charge.
# Why: a refused command must not leave metadata that disables a later session's outage or reconnect protection.
@pytest.mark.parametrize("case", selected_cases(), ids=lambda case: case.identifier)
async def test_start_failure_recovery_sequences(tmp_path, model_tls, monkeypatch, record_property, case):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", BeforeCapFailurePeer)
    completed, suffix_complete = [], False
    async with commissioned(
        tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch, case.selected
    ) as b:
        c, s, sock, rig = b.controller, b.session, b.charger, b.rig
        peer, firmware = rig.peer, rig.bench.firmware
        original_plan, original_config = copy.deepcopy(firmware.plans), copy.deepcopy(peer.configuration)
        actual, send, faults = firmware.answer, sock.send, []
        baseline_calls = len(rig.bench.device_calls)
        peer.start_journal, peer.hold_after_failure = s.journal, True
        if case.fault.startswith("positive-") and case.fault.split("positive-", 1)[1] in ("503", "eof", "bad-json"):
            peer.start_failure = case.fault.split("positive-", 1)[1]
        elif case.fault == "remote-503":
            peer.start_failure, peer.start_boundary = "503", "remote"
        elif case.fault == "preflight":
            rig.bench.cp, rig.bench.connection = "A", "Unplug"

        # Refuse only the declared actual command at this independent charger, then retain HTTP loss until restore.
        def answer(action, payload):
            target = False
            if not faults and action == "SetChargingProfile":
                limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                target = case.fault == "zero-rejected" and limit == 0 or case.fault == "positive-rejected" and limit > 0
            elif not faults and action == "RemoteStartTransaction" and case.fault == "start-rejected":
                target = True
            if target:
                faults.append(action)
                peer.unavailable = True
                return {"status": "Rejected"}
            return actual(action, payload)

        # The firmware may have applied a positive profile even when its correlated response is a CALLERROR.
        async def reply(raw):
            frame = decode(raw)
            call = next((x for x in rig.bench.device_calls if x[1] == frame[1]), None)
            if (
                case.fault == "positive-call-error"
                and not faults
                and frame[0] == 3
                and call
                and call[2] == "SetChargingProfile"
            ):
                limit = call[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit > 0:
                    faults.append(call[2])
                    peer.unavailable = True
                    raw = build_call_error(frame[1], "InternalError", "synthetic uncertain positive ceiling")
            await send(raw)

        monkeypatch.setattr(firmware, "answer", answer)
        monkeypatch.setattr(sock, "send", reply)
        c.command(uuid.uuid4().hex, case.action, c.requests.intent().revision)
        await c.tests.settle()
        if c.pause_task is not None:
            await c.pause_task
        if case.fault == "preflight":
            assert len(rig.bench.device_calls) == baseline_calls
            peer.unavailable = True
            rig.bench.cp, rig.bench.connection = "B", "Plug"
        else:
            assert faults or peer.failures
        expected_starts = int(case.fault == "start-rejected")
        assert len(starts(rig)) == expected_starts
        pause_id, pause_revision = None, None
        async with AsyncExitStack() as stack:
            try:
                for event in case.events:
                    if event == "restore":
                        peer.unavailable = False
                    elif event == "pause":
                        pause_id, pause_revision = uuid.uuid4().hex, c.requests.intent().revision
                        c.command(pause_id, "pause", pause_revision)
                        if c.pause_task is not None:
                            await c.pause_task
                    elif event == "replay":
                        c.command(pause_id, "pause", pause_revision)
                    elif event == "observe":
                        await observe(c)
                    elif event == "restart-ready":
                        # Workers failed; cleanup may remain unresolved. Do not drain debt before replacement.
                        await c.tests.close()
                        if c.pause_task is not None:
                            await c.pause_task
                        await crash(s)
                        s = managed_session(rig)
                        await s.start()
                        c = ManagedController(s)
                        peer.start_journal = s.journal
                        sock = await stack.enter_async_context(rig.charger(s))
                        assert (await rig.bench.call(sock, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
                    completed.append(event)
                    assert c.current.selected() == case.selected
                    assert firmware.plans == original_plan and peer.configuration == original_config
                    assert len(starts(rig)) == expected_starts and firmware.current == 0
                    assert 0 <= firmware.effective_limit() <= case.selected
                    if pause_id is not None:
                        assert c.requests.intent().mode == "paused"
                assert completed == list(case.events)
                await until(lambda: s.gateway.controls.ready)
                c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
                assert (await healthy(c))["local_hold_verified"]
                daily = ChargeRig(rig, s, c, sock)
                status = await returned_to_app(daily, case.selected)
                assert status["normal_start_ready"] and firmware.effective_limit() == case.selected
                local = await positive_flow(daily, "daily")
                if not case.demand:
                    peer.permission_enabled = False
                    firmware.requested_current = 0
                await healthy(c)
                assert await witnessed_native_loss(daily, case.selected, local) >= 3
                await sock.close()
                await until(lambda: not s.runtime.status.charger_connected)
                sock = await stack.enter_async_context(rig.charger(s))
                assert (await rig.bench.call(sock, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
                await healthy(c)
                assert firmware.effective_limit() == case.selected
                await close_daily(ChargeRig(rig, s, c, sock), local, case.first_close)
                assert len(starts(rig)) == expected_starts
                assert not any(frame[2] == "RemoteStopTransaction" for frame in rig.bench.device_calls)
                suffix_complete = True
            finally:
                peer.unavailable = False
                await c.tests.close()
                if c.pause_task is not None:
                    await c.pause_task
                await s.close()
                record_property("model_case", case.identifier)
                record_property("model_family", "start_recovery")
                record_property("model_input_sha256", input_digest("start_recovery", case))
                record_property("completed_events", completed)
                record_property("suffix_complete", suffix_complete)
