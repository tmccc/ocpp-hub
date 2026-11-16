"""Standalone Peer contract: completed validation and a genuinely queued zero have different outcomes.

The validator Future is an explicit unit collaborator. This is not a native HTTP, SQLite,
websocket or complete managed-engine reproduction; separate real-peer tests cover those paths.
"""

import asyncio

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.frames import build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.native_faults import NativeContentError
from ocpp_hub_core.prototype.rpc import Peer, UnavailableError

pytestmark = pytest.mark.asyncio


# Inspect the actual suspended call without replacing or advancing the Peer implementation.
def pending_call_locals(task, code):
    item = task.get_coro()
    for _ in range(40):
        if item is None:
            break
        actual_code = getattr(item, "cr_code", None) or getattr(item, "gi_code", None)
        if actual_code is code:
            frame = getattr(item, "cr_frame", None) or getattr(item, "gi_frame", None)
            return dict(frame.f_locals) if frame is not None else {}
        item = getattr(item, "cr_await", None) or getattr(item, "gi_yieldfrom", None)
    return {}


# Intent: a completed strict verdict survives simultaneous queued zero, while successful validation still yields.
# Why: preemption must retire an unsent positive request without hiding an already completed validator refusal.
@pytest.mark.parametrize("outcome", ["strict", "success"])
async def test_completed_validation_and_queued_zero_preserve_their_distinct_outcomes(outcome):
    peer = Peer("charger", 2)
    loop = asyncio.get_running_loop()
    validation = loop.create_future()
    entered, witnessed = asyncio.Event(), asyncio.Event()
    strict = NativeContentError("configuration", "unit collaborator completed a strict native verdict")
    sent, witnesses = [], []
    positive = None
    urgent = None

    # This plain observer runs after the restrictive task completes and before the waiting Peer resumes.
    def record_witness():
        phase = pending_call_locals(positive, Peer._validate.__code__)
        restrictive = phase.get("restrictive")
        value = validation.exception() if validation.done() and not validation.cancelled() else None
        witnesses.append(
            {
                "validation_done": validation.done(),
                "validation_cancelled": validation.cancelled(),
                "validation_error_is_strict": value is strict,
                "validation_error_is_none": value is None,
                "same_validation": phase.get("validation") is validation,
                "restrictive_waiting": peer.gate.restrictive_waiting.is_set(),
                "restrictive_task_done": isinstance(restrictive, asyncio.Task) and restrictive.done(),
                "before_wait_decision": "done" not in phase,
                "positive_still_pending": not positive.done(),
                "urgent_still_pending": urgent is not None and not urgent.done(),
                "wire_dispatches": len(sent),
            }
        )
        witnessed.set()

    # Register before Peer registers its wait callback, then observe after zero's genuinely queued signal task.
    def schedule_witness(future):
        loop.call_soon(record_witness)

    validation.add_done_callback(schedule_witness)

    # Supply an explicitly controlled unit collaborator; zero and all other requests validate immediately.
    def validate(action, payload):
        if action == "SetChargingProfile" and payload["csChargingProfiles"]["chargingProfileId"] == 1:
            entered.set()
            return validation
        ready = loop.create_future()
        ready.set_result(None)
        return ready

    # Record only frames actually dispatched by the real Peer and return their real correlated unit replies.
    async def sender(raw):
        frame = decode(raw)
        sent.append(frame)
        peer.receive(decode(build_call_result(frame[1], {"status": "Accepted"})))

    peer.validate_send, peer.sender = validate, sender
    positive = asyncio.create_task(
        peer.call(
            "SetChargingProfile",
            commands.set_charging_profile(0, commands.PURPOSE_CHARGE_POINT_MAX, 6, 1),
            priority=1,
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), 1)
        phase = pending_call_locals(positive, Peer._validate.__code__)
        assert phase.get("validation") is validation and "done" not in phase
        assert not validation.done() and not peer.gate.restrictive_waiting.is_set() and not sent

        # Schedule both real inputs in one turn; no task, guard, gate flag or Peer method is replaced.
        urgent = asyncio.create_task(
            peer.call(
                "SetChargingProfile",
                commands.set_charging_profile(0, commands.PURPOSE_CHARGE_POINT_MAX, 0, 2),
                priority=0,
            )
        )
        if outcome == "strict":
            validation.set_exception(strict)
        else:
            validation.set_result(None)
        await asyncio.wait_for(witnessed.wait(), 1)
        assert len(witnesses) == 1
        witness = witnesses[0]
        assert witness["validation_done"] and not witness["validation_cancelled"]
        assert witness["same_validation"] and witness["restrictive_waiting"]
        assert witness["restrictive_task_done"] and witness["before_wait_decision"]
        assert witness["positive_still_pending"] and witness["urgent_still_pending"]
        assert witness["wire_dispatches"] == 0
        assert witness["validation_error_is_strict"] is (outcome == "strict")
        assert witness["validation_error_is_none"] is (outcome == "success")

        observed = None
        try:
            await positive
        except (NativeContentError, UnavailableError) as error:
            observed = error
        if outcome == "strict":
            assert observed is strict, {"observed": repr(observed), "witness": witness}
        else:
            assert isinstance(observed, UnavailableError), {"observed": repr(observed), "witness": witness}
        assert (await asyncio.wait_for(urgent, 1))[2]["status"] == "Accepted"
        assert len(sent) == 1 and sent[0][2] == "SetChargingProfile"
        assert sent[0][3]["csChargingProfiles"]["chargingProfileId"] == 2
        assert sent[0][3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
        assert not peer._pending and not peer.gate.busy and not peer.gate.restrictive_waiting.is_set()
    finally:
        if not validation.done():
            validation.cancel()
        for task in (positive, urgent):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(positive, *([] if urgent is None else [urgent]), return_exceptions=True)
