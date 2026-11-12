"""Shared restrictive retry pacing and durable occurrence cutoff duties."""

import asyncio

import pytest
from ocpp_hub_core.prototype import managed_schedule
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.rpc import UnavailableError

from tests.handover_helpers import crash
from tests.managed_scenario_helpers import END
from tests.test_managed_proxy import healthy, introduced, managed_session
from tests.test_managed_schedule_cutoff import timer_bench
from tests.test_prototype_tls import tls as tls


# Intent: stale callers and concurrent deadline paths share one elapsed Stop retry gate.
# Why: calendar corrections cannot spend or prolong a restrictive retry cooldown.
@pytest.mark.parametrize("wall_step", [-600, 86400])
async def test_stop_gate_uses_current_journal_and_shared_elapsed_clock(
    tmp_path, tls, unused_tcp_port, monkeypatch, wall_step
):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        local = await b.advance_idle_to(b.start)
        await healthy(b.c)
        b.clock.now = END
        g, j = b.session.gateway, b.session.journal
        stale = j.get(managed_schedule.KEY)
        original = b.device.answer

        # Refuse only the fake device's Stop while retaining all other ordinary protocol responses.
        def reject_stop(action, payload):
            return {"status": "Rejected"} if action == "RemoteStopTransaction" else original(action, payload)

        b.device.answer = reject_stop
        with pytest.raises(UnavailableError):
            await managed_schedule.request_timer_stop(g, stale, b.c.schedule.hold_authorized)
        assert b.stops() == [local] and b.device.effective_limit() == 0
        assert j.get(managed_schedule.KEY)["next_stop"]
        b.clock.step_wall(END + wall_step)
        await asyncio.gather(
            *[managed_schedule.request_timer_stop(g, stale, b.c.schedule.hold_authorized) for _ in range(3)]
        )
        assert b.stops() == [local]
        b.clock.elapsed += 16
        with pytest.raises(UnavailableError):
            await managed_schedule.request_timer_stop(g, j.get(managed_schedule.KEY), b.c.schedule.hold_authorized)
        assert b.stops() == [local, local]


# Intent: a genuinely restarted gateway retries a still-required Stop under the current Boot.
# Why: persisted wall timestamps do not prove an elapsed cooldown in the new process.
async def test_restart_does_not_inherit_stop_wall_cooldown(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        local = await b.advance_idle_to(b.start)
        await healthy(b.c)
        b.clock.now = END
        original = b.device.answer

        # Refuse only the fake device's Stop while retaining all other ordinary protocol responses.
        def reject_stop(action, payload):
            return {"status": "Rejected"} if action == "RemoteStopTransaction" else original(action, payload)

        b.device.answer = reject_stop
        with pytest.raises(UnavailableError):
            await b.c.schedule.protect_deadline(b.session.journal.get(managed_schedule.KEY))
        assert b.stops() == [local]
        b.clock.step_wall(END - 600)
        await crash(b.session)
        replacement = managed_session(b.rig)
        await replacement.start()
        b.c, b.session = ManagedController(replacement), replacement
        async with b.rig.charger(replacement) as charger:
            await introduced(b.rig, replacement, charger, "stop-retry-restart")
            assert b.stops() == [local, local]
            before = len(b.stops())
            await b.c.schedule.protect_deadline(replacement.journal.get(managed_schedule.KEY))
            assert len(b.stops()) == before


# Intent: an independently verified zero or new durable hold supersedes a previous retry gate.
# Why: an obsolete failed attempt cannot suppress a new restrictive obligation.
async def test_zero_confirmation_and_live_new_hold_reset_cap_cache(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        j, g = b.session.journal, b.session.gateway
        lease = j.get(managed_schedule.KEY)
        await b.c.schedule.protect_station(lease)
        assert g.controls.ready and g.controls.desired() == 0
        assert (lease["id"], "next_cap") not in g.timer_retry_after
        # A newly recorded hold explicitly clears the diagnostic attempt, superseding that old gate.
        g.timer_retry_after[(lease["id"], "next_cap")] = b.clock.elapsed + 1000
        current = j.get(managed_schedule.KEY)
        j.put(managed_schedule.KEY, {**current, "next_cap": 0})
        assert managed_schedule.restrictive_retry_allowed(g, lease, "next_cap")


# Intent: an elapsed cutoff retains its exact Stop duty across a backward wall-clock correction.
# Why: a failed Stop cannot reopen the expired temporary window or postpone retries until the clock catches up.
@pytest.mark.parametrize("watchdog", [False, True], ids=["foreground", "watchdog"])
async def test_latched_deadline_stop_retries_after_wall_rewind(tmp_path, tls, unused_tcp_port, monkeypatch, watchdog):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        local = await b.advance_idle_to(b.start)
        await healthy(b.c)
        b.clock.now = END
        original = b.device.answer

        # Refuse only the fake device's Stop while retaining all other ordinary protocol responses.
        def reject_stop(action, payload):
            return {"status": "Rejected"} if action == "RemoteStopTransaction" else original(action, payload)

        b.device.answer = reject_stop
        with pytest.raises(UnavailableError):
            await b.c.schedule.protect_deadline(b.session.journal.get(managed_schedule.KEY))
        assert b.stops() == [local] and b.device.effective_limit() == 0
        b.device.answer = original
        b.clock.step_wall(END - 600)
        b.clock.elapsed += 16
        if watchdog:
            watcher = asyncio.create_task(b.c.schedule.watch_deadline())
            try:
                async with asyncio.timeout(2):
                    while len(b.stops()) < 2:
                        b.c.schedule.changed.set()
                        await asyncio.sleep(0.01)
            finally:
                watcher.cancel()
                await asyncio.gather(watcher, return_exceptions=True)
        else:
            for _ in range(2):
                await b.c.tick()
        assert b.stops() == [local, local]
        assert b.session.journal.get(managed_schedule.KEY)["cutoff_reached"]


# Intent: an elapsed timer cutoff does not claim an independently identified original daily transaction.
# Why: retaining cutoff history through a rewind must not turn a valid daily continuation into a guessed Stop.
async def test_daily_continues_across_latched_cutoff_and_rewind(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        b.clock.now = b.start - 60
        local = await b.start_plan(9)
        await healthy(b.c)
        b.clock.now = END
        await b.c.native.read()
        await b.c.schedule.protect_deadline(b.session.journal.get(managed_schedule.KEY))
        assert b.session.journal.get(managed_schedule.KEY)["cutoff_reached"]
        b.clock.step_wall(END - 600)
        b.clock.elapsed += 16
        await healthy(b.c)
        assert b.session.journal.active() == local and b.device.effective_limit() > 0
        assert not b.stops()
