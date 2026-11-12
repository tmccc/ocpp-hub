"""Restrictive physical plan cleanup follows elapsed cooldowns despite backward/forward host clock changes."""

import copy
from contextlib import nullcontext

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_schedule import KEY

from tests.handover_helpers import crash
from tests.test_managed_proxy import introduced, managed_session, starts
from tests.test_managed_schedule_cutoff import timer_bench
from tests.test_prototype_tls import tls as tls


# Intent: wall rollback never postpones a safe elapsed cleanup retry and wall jumps never create a request flood.
# Why: fresh native presence and exact retry identity permit one bounded attempt after replacement.
@pytest.mark.parametrize("restart", [False, True])
async def test_cleanup_uses_elapsed_gate_across_clock_step_and_replacement(
    tmp_path, tls, unused_tcp_port, monkeypatch, restart
):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        original = copy.deepcopy(b.device.plans[9])
        answer = b.device.answer
        clears = []

        # Only the independent device refuses removal; real requests/replies still cross the TLS connection.
        def reject_clear(action, payload):
            if action == "DataTransfer" and payload["messageId"] == "clearPlans":
                clears.append(copy.deepcopy(payload))
                return {"status": "Rejected", "data": ""}
            return answer(action, payload)

        monkeypatch.setattr(b.device, "answer", reject_clear)
        b.clock.now = b.start + 601
        await b.c.tick()
        assert len(clears) == 1 and b.timer in b.device.plans
        wall_deadline = b.session.journal.get(KEY)["next_clear"]
        b.clock.step_wall(b.clock.now - 3600)
        b.clock.elapsed += 301
        await b.c.tick()
        assert len(clears) == 2
        assert b.clock.now < wall_deadline
        b.clock.step_wall(b.clock.now + 7200)
        for _ in range(3):
            await b.c.tick()
        assert len(clears) == 2  # No elapsed time passed despite crossing the saved wall timestamp.
        if restart:
            await crash(b.session)
            session = managed_session(b.rig)
            await session.start()
            c = ManagedController(session)
        else:
            session, c = b.session, b.c
            b.clock.elapsed += 301
        async with b.rig.charger(session) if restart else nullcontext(b.sock) as sock:
            if restart:
                await introduced(b.rig, session, sock, "cleanup-monotonic-replacement")
            await c.tick()
            assert len(clears) == 3 and session.journal.get(KEY)["cleanup_attempts"] == 3
            for _ in range(2):
                await c.tick()
            assert len(clears) == 3
            assert b.device.plans[9] == original and b.timer in b.device.plans
            assert not starts(b.rig) and not b.stops()
