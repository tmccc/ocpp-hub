"""Restrictive retry pacing follows elapsed time independently from the calendar."""

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_schedule import KEY

from tests.handover_helpers import crash
from tests.managed_scenario_helpers import END, select_current
from tests.test_managed_proxy import introduced, managed_session
from tests.test_managed_schedule_cutoff import timer_bench
from tests.test_prototype_tls import tls as tls


# Intent: retry a rejected physical zero after a backward wall-clock correction.
# Why: the station's already-revoked current must not stay positive for the entire clock correction.
@pytest.mark.parametrize("rewind", [0, 600], ids=["control-no-correction", "clock-back-ten-minutes"])
async def test_cutoff_retry_after_backward_clock(tmp_path, tls, unused_tcp_port, monkeypatch, rewind):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await select_current(b, 32)
        await b.arm()
        b.clock.now = b.start
        local = await b.start_plan(9)
        b.rig.peer.plan_session = (local, 987654)
        await b.c.native.read()
        answer = b.device.answer
        rejections = []

        # The physical device rejects the first restrictive profile and preserves its old actual current.
        def reject_zero_once(action, payload):
            if action == "SetChargingProfile" and not rejections:
                limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit == 0:
                    rejections.append(limit)
                    return {"status": "Rejected"}
            return answer(action, payload)

        b.device.answer = reject_zero_once
        b.clock.now = END
        await b.c.schedule.protect_deadline(b.session.journal.get(KEY))
        assert rejections == [0] and b.device.effective_limit() == 32
        assert b.session.journal.get(KEY)["idle_cutoff_hold"]
        b.device.answer = answer
        b.clock.now = END - rewind
        for _ in range(4):
            b.clock.now += 16
            await b.c.tick()
        assert b.device.effective_limit() == 0


# Intent: a forward correction cannot spend the retry cooldown; a new controller cannot inherit a stale cooldown.
# Why: wall-clock changes neither authorize rapid repeats nor defer a still-required physical zero after restart.
@pytest.mark.parametrize("restart", [False, True], ids=["wall-forward", "service-restart"])
async def test_retry_clock_correction_and_restart(tmp_path, tls, unused_tcp_port, monkeypatch, restart):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await select_current(b, 32)
        await b.arm()
        b.clock.now = b.start
        local = await b.start_plan(9)
        b.rig.peer.plan_session = (local, 987654)
        await b.c.native.read()
        original = b.device.answer
        rejected = []

        # Refuse only the first actual zero and leave the physical positive profile in place.
        def answer(action, payload):
            if action == "SetChargingProfile" and not rejected:
                if payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0:
                    rejected.append(True)
                    return {"status": "Rejected"}
            return original(action, payload)

        b.device.answer = answer
        b.clock.now = END
        await b.c.schedule.protect_deadline(b.session.journal.get(KEY))
        assert rejected and b.device.effective_limit() == 32
        b.device.answer = original
        if restart:
            b.clock.step_wall(END - 600)
            await crash(b.session)
            assert b.device.effective_limit() == 32
            replacement = managed_session(b.rig)
            await replacement.start()
            try:
                controller = ManagedController(replacement)
                async with b.rig.charger(replacement) as charger:
                    await introduced(b.rig, replacement, charger, "retry-clock-restart")
                    await controller.schedule.protect_station(replacement.journal.get(KEY))
                    assert b.device.effective_limit() == 0
            finally:
                await replacement.close()
        else:
            b.clock.step_wall(END + 86400)
            await b.c.schedule.protect_station(b.session.journal.get(KEY))
            assert b.device.effective_limit() == 32
            b.clock.elapsed += 16
            await b.c.schedule.protect_station(b.session.journal.get(KEY))
            assert b.device.effective_limit() == 0
