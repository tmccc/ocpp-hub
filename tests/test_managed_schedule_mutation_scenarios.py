"""N08 retries preserve physical authority after refusal and post-application reply loss."""

from __future__ import annotations

import asyncio
import copy

import pytest
from ocpp_hub_core.prototype.release_attempt import ReleaseOutcome
from ocpp_hub_core.prototype.rpc import UnavailableError

from tests.managed_scenario_helpers import (
    END,
    close_in_order,
    next_daily,
    originals,
    replace_controller,
    require_originals,
    select_current,
    settle,
    unknown_report,
)
from tests.peer_mutation_helpers import fail_one_operation as fail_one_operation
from tests.peer_mutation_helpers import matches_operation as matches_operation
from tests.peer_mutation_helpers import observe_device_replies
from tests.release_outcome_helpers import explicit_release_recovery
from tests.test_managed_proxy import healthy, introduced
from tests.test_managed_schedule_cutoff import timer_bench
from tests.test_managed_schedule_scenarios import early_cleanup_selected
from tests.test_prototype_tls import tls as tls


# Intent: restrictive work retries, while a refused or late positive release needs fresh owner recovery.
# Why: failures cannot expand Stop scope or erase positive dispatch; real refusal and application remain distinct.
@pytest.mark.parametrize("operation", ["zero", "zero_before_cutoff", "stop", "clear", "restore"])
@pytest.mark.parametrize("lost_ack", [False, True])
@pytest.mark.parametrize("restart", [False, True])
async def test_n08_physical_mutation_recovery(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, operation, lost_ack, restart
):
    async with timer_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        await select_current(b, 32)
        saved = originals(b)
        await b.arm()
        if operation == "clear":
            # A clear requires a physically retained plan; inject before its removal, after real transaction closure.
            b.clock.now = b.start
            local = await b.start_plan(b.timer)
            await healthy(b.c)
            b.clock.now = END
            await settle(b)
            assert b.stops() == [local]
            await b.close_transaction(local)
        else:
            await early_cleanup_selected(b, 32)
            b.clock.now = END - 60
            local = await unknown_report(b, b.start, None, True)
            zero = operation in ("zero", "zero_before_cutoff")
            if not zero:
                await settle(b)
            # Fault the first required zero, rather than waiting until the unknown-source hold has already succeeded.
            if operation != "zero_before_cutoff":
                b.clock.now = END
            if not zero:
                await settle(b)
            if operation == "stop":
                b.rig.peer.plan_session = (local, b.timer)
            elif operation == "restore":
                b.rig.peer.plan_session = (local, 9)
        witness = {}
        initial_profiles = copy.deepcopy(b.device.profiles)
        wire = len(b.rig.bench.device_calls)
        replies = observe_device_replies(b, monkeypatch) if operation == "restore" else []
        unmatched = b.session.gateway.charger.diagnostics.snapshot()["unmatched_replies"]
        async with fail_one_operation(b, operation, lost_ack, witness) as entered:
            work = asyncio.create_task(b.c.tick())
            try:
                await asyncio.wait_for(entered.wait(), 5)
                if operation == "restore" and lost_ack:
                    assert b.device.effective_limit() == 32
                if operation == "clear":
                    assert b.device.effective_limit() == 0
                status = await asyncio.wait_for(work, 12)
                if operation in ("zero", "zero_before_cutoff") and not lost_ack:
                    assert b.device.effective_limit() == 32 and status["state"] != "healthy"
                elif operation == "clear" and lost_ack and b.device.effective_limit() == 32:
                    native = await b.c.native.read(closed=True)
                    assert native.saved_schedule_digest == b.rig.spec.saved_schedule_digest
                    assert not native.physical.transaction_open and b.timer not in b.device.plans
                elif operation != "restore" or not lost_ack:
                    assert b.device.effective_limit() == 0
                assert set(b.stops()).issubset({local}) if operation in ("stop", "clear") else not b.stops()
            finally:
                if not work.done():
                    work.cancel()
                    await asyncio.gather(work, return_exceptions=True)
        if operation == "restore":
            attempt = witness["attempt"]
            assert attempt.finished and not attempt.cancelled and attempt.positive_may_have_dispatched
            assert not attempt.positive_confirmed and attempt.zero_may_have_dispatched and attempt.zero_confirmed
            assert type(attempt.positive_failure) is UnavailableError and attempt.failure is attempt.positive_failure
            assert attempt.outcome() is ReleaseOutcome.POSSIBLE_POSITIVE
            profiles = [frame for frame in b.rig.bench.device_calls[wire:] if frame[2] == "SetChargingProfile"]
            limits = [
                frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                for frame in profiles
            ]
            assert len(limits) >= 2 and limits[0] == 32 and all(limit == 0 for limit in limits[1:])
            if lost_ack:
                assert witness["physical_limit"] == 32
                profile = profiles[0][3]["csChargingProfiles"]
                assert witness["device_profiles"][profile["chargingProfileId"]] == profile
                assert isinstance(attempt.positive_failure.__cause__, TimeoutError)
                assert [3, profiles[0][1], {"status": "Accepted"}] in replies
                assert b.session.gateway.charger.diagnostics.snapshot()["unmatched_replies"] > unmatched
            else:
                assert witness["physical_limit"] == 0 and witness["device_profiles"] == initial_profiles
                assert attempt.positive_failure.__cause__ is None
                assert [3, profiles[0][1], {"status": "Rejected"}] in replies
            witness["fault"] = copy.deepcopy(b.session.journal.get("managed_control_fault"))
            assert witness["fault"]["classification"] == "owner_release_unconfirmed"
            assert b.session.journal.get("control")["uncertain"] is True and b.device.effective_limit() == 0
        b.clock.now += 301
        if restart:
            await replace_controller(b)
        from contextlib import nullcontext

        async with b.rig.charger(b.session) if restart else nullcontext(b.sock) as b.sock:
            if restart:
                await introduced(b.rig, b.session, b.sock, "mutation-recovery")
            if operation == "restore":
                await settle(b, 6)
                assert b.session.journal.get("managed_control_fault") == witness["fault"]
                assert b.c.requests.intent().mode == "app" and b.device.effective_limit() == 0 and not b.stops()
                await explicit_release_recovery(b, local, 32, verify_next_daily=False)
                stops = tuple(b.stops())
                assert set(stops) == {local}  # The new owner's Pause supplies this known Stop authority.
            elif operation == "clear":
                await settle(b, 6)
                await healthy(b.c)
                stops = tuple(b.stops())
                assert set(stops) == {local}
            else:
                if operation in ("zero", "zero_before_cutoff"):
                    await settle(b)
                    assert b.device.effective_limit() == 0 and not b.stops()
                    b.rig.peer.plan_session = (local, 9)
                await settle(b, 6)
                stops = tuple(b.stops())
                assert set(stops) == ({local} if operation == "stop" else set())
                if operation != "stop":
                    assert b.device.effective_limit() == 32
                await close_in_order(b, local, "native", stops)
                await healthy(b.c)
            require_originals(b, saved)
            await next_daily(b, 32, saved, stops)
