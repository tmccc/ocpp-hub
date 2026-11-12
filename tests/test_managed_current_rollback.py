"""Failed app current selections preserve the previous resume setting and require real zero before Return."""

from __future__ import annotations

import asyncio
import uuid

import pytest
from ocpp_hub_core.prototype.managed import ManagedController

from tests.handover_helpers import crash
from tests.test_managed_current import adjustable, select
from tests.test_managed_current_overlap import interrupted_increase
from tests.test_managed_proxy import healthy, introduced, managed_session, starts
from tests.test_managed_schedule_cutoff import timer_bench
from tests.test_prototype_tls import tls as tls


# Intent: both a rejected target and a physically applied but unverified target restore the earlier selection.
# Why: an uncertain request must not become future resume authority merely because zero was later verified.
@pytest.mark.parametrize("outcome", ["rejected", "unverified"])
async def test_failed_app_current_returns_only_to_previous_selection(
    tmp_path, tls, unused_tcp_port, monkeypatch, outcome
):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        actual = test.rig.bench.firmware.answer
        failed = False
        applied_at_failure = None

        # Refuse a genuine profile or its actual post-apply composite proof, without replacing engine behavior.
        def refuse_target(action, payload):
            nonlocal failed, applied_at_failure
            if action == "SetChargingProfile":
                limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if outcome == "rejected" and limit == 16 and not failed:
                    failed = True
                    applied_at_failure = test.rig.bench.firmware.effective_limit()
                    return {"status": "Rejected"}
            if (
                outcome == "unverified"
                and action == "GetCompositeSchedule"
                and test.rig.bench.firmware.effective_limit() == 16
                and not failed
            ):
                failed = True
                applied_at_failure = test.rig.bench.firmware.effective_limit()
                return {"status": "Rejected"}
            return actual(action, payload)

        monkeypatch.setattr(test.rig.bench.firmware, "answer", refuse_target)
        identifier = await select(test, 16)
        status = await healthy(test.controller)
        journal = test.session.journal
        receipt = test.controller.requests.receipt(identifier)
        assert failed and applied_at_failure == (6 if outcome == "rejected" else 16)
        assert receipt["state"] == "uncertain"
        assert test.controller.current.selected() == 6 and journal.get("control")["resume_limit"] == 6
        assert receipt["result"] == {
            "classification": "current_unconfirmed",
            "requested_current_a": 16,
            "previous_current_a": 6,
        }
        change = journal.get("managed_current_change")
        assert change["current_a"] == 16 and change["previous_a"] == 6 and change["mode"] == "app"
        assert change["state"] == "uncertain" and test.controller.current.selected() == 6
        assert journal.get("control")["resume_limit"] == 6 and journal.get("control")["inhibit"] is True
        assert status["local_hold_verified"] and test.rig.bench.firmware.effective_limit() == 0
        assert not journal.get("managed_control_fault")
        before = list(test.rig.bench.device_calls)
        assert test.controller.command(identifier, "set_current", 0, current_a=16) == receipt
        assert test.rig.bench.device_calls == before
        returning = uuid.uuid4().hex
        test.controller.command(returning, "app_control", test.controller.requests.intent().revision)
        status = await healthy(test.controller)
        assert test.controller.requests.receipt(returning)["state"] == "completed"
        assert status["owner_control_enabled"] and test.rig.bench.firmware.effective_limit() == 6
        assert test.controller.current.selected() == 6 and not starts(test.rig)


# Intent: a newer Pause cancels an unsent app increase and preserves the prior future selection.
# Why: failure cleanup may neither promote its old target nor replace the owner's newer restrictive revision.
async def test_pause_during_unsent_app_increase_keeps_previous_selection(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        entered, proceed = asyncio.Event(), asyncio.Event()
        actual = test.session.gateway.charger.validate_send
        assert actual is not None

        # Pause at the real validation boundary and retain its complete native verification after release.
        async def delayed(action, payload):
            if (
                action == "SetChargingProfile"
                and payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 16
            ):
                entered.set()
                await proceed.wait()
            await actual(action, payload)

        monkeypatch.setattr(test.session.gateway.charger, "validate_send", delayed)
        identifier = uuid.uuid4().hex
        test.controller.command(identifier, "set_current", 0, current_a=16)
        try:
            await asyncio.wait_for(entered.wait(), 3)
            pause = uuid.uuid4().hex
            test.controller.command(pause, "pause", test.controller.requests.intent().revision)
            intent = test.controller.requests.intent()
            count = len(test.rig.bench.received_limits)
            proceed.set()
            await test.controller.current.settle()
            status = await healthy(test.controller)
            assert status["local_hold_verified"] and test.controller.requests.intent() == intent
            assert test.controller.requests.receipt(pause)["state"] == "completed"
            assert test.controller.requests.receipt(identifier)["state"] == "failed"
            assert test.controller.current.selected() == 6 and test.rig.bench.firmware.effective_limit() == 0
            assert all(limit == 0 for limit in test.rig.bench.received_limits[count:])
            assert not starts(test.rig)
        finally:
            proceed.set()


# Intent: restart retains the old selection whether process loss preceded or followed the actual target profile.
# Why: interrupted idle recovery and an already newer Pause must not turn a pending target into resume authority.
@pytest.mark.parametrize("paused", [False, True])
@pytest.mark.parametrize("applied", [False, True])
async def test_interrupted_idle_app_increase_keeps_previous_selection(
    tmp_path, tls, unused_tcp_port, monkeypatch, paused, applied
):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as bench:
        async with interrupted_increase(bench, applied) as (request, release):
            if paused:
                bench.c.command(uuid.uuid4().hex, "pause", bench.c.requests.intent().revision)
                await bench.c.pause_task
            prior_intent = bench.c.requests.intent()
            bench.c.current.worker.cancel()
            await asyncio.gather(bench.c.current.worker, return_exceptions=True)
            release.set()
        await crash(bench.session)
        bench.session = managed_session(bench.rig)
        await bench.session.start()
        bench.c = ManagedController(bench.session)
        if paused:
            assert bench.c.current.selected() == 6
            assert bench.c.requests.intent() == prior_intent
        async with bench.rig.charger(bench.session) as bench.sock:
            await introduced(bench.rig, bench.session, bench.sock, "rollback-pending-selection")
            status = await healthy(bench.c)
            change = bench.session.journal.get("managed_current_change")
            assert change["id"] == request and change["current_a"] == 32
            assert change["previous_a"] == 6 and change["mode"] == "app"
            assert status["local_hold_verified"] and bench.c.requests.intent().mode == "paused"
            assert bench.c.current.selected() == 6 and bench.device.effective_limit() == 0
            returning = uuid.uuid4().hex
            bench.c.command(returning, "app_control", bench.c.requests.intent().revision)
            assert (await healthy(bench.c))["owner_control_enabled"]
            assert bench.c.requests.receipt(returning)["state"] == "completed"
            assert bench.device.effective_limit() == 6 and not bench.stops() and not starts(bench.rig)


# Intent: an unverified applied target cannot Return until an actual station zero has been confirmed.
# Why: restoring the earlier resume metadata must not turn an unknown positive physical outcome into verified safety.
async def test_failed_applied_current_requires_actual_zero_before_return(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        actual = test.rig.bench.firmware.answer
        failed_proof = False
        allow_zero = False
        accepted_limits = []

        # Apply the target, reject its proof and subsequent zeros, then permit real zero and Return normally.
        def withhold_zero(action, payload):
            nonlocal failed_proof
            if (
                action == "GetCompositeSchedule"
                and test.rig.bench.firmware.effective_limit() == 16
                and not failed_proof
            ):
                failed_proof = True
                return {"status": "Rejected"}
            if action == "SetChargingProfile":
                limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit == 0 and not allow_zero:
                    return {"status": "Rejected"}
                answer = actual(action, payload)
                if answer == {"status": "Accepted"}:
                    accepted_limits.append(limit)
                return answer
            return actual(action, payload)

        monkeypatch.setattr(test.rig.bench.firmware, "answer", withhold_zero)
        identifier = await select(test, 16)
        if test.controller.pause_task is not None:
            await test.controller.pause_task
        assert failed_proof and test.controller.requests.receipt(identifier)["state"] == "uncertain"
        assert test.controller.current.selected() == 6 and test.session.gateway.controls.desired() == 0
        assert test.session.journal.get("control")["inhibit"] is True
        assert not test.session.gateway.controls.ready and test.rig.bench.firmware.effective_limit() == 16
        returning = uuid.uuid4().hex
        test.controller.command(returning, "app_control", test.controller.requests.intent().revision)
        for _ in range(2):
            status = await test.controller.tick()
            assert not status.get("owner_control_enabled")
            assert test.controller.requests.receipt(returning)["state"] != "completed"
            assert test.session.gateway.controls.desired() == 0 and accepted_limits == [16]
        allow_zero = True
        test.controller.command(uuid.uuid4().hex, "pause", test.controller.requests.intent().revision)
        assert (await healthy(test.controller))["local_hold_verified"]
        returning = uuid.uuid4().hex
        test.controller.command(returning, "app_control", test.controller.requests.intent().revision)
        assert (await healthy(test.controller))["owner_control_enabled"]
        assert test.controller.requests.receipt(returning)["state"] == "completed"
        assert accepted_limits == [16, 0, 6] and test.rig.bench.firmware.effective_limit() == 6
        assert not starts(test.rig)
