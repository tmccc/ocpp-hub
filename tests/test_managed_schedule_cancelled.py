"""Preserve an earlier installed occurrence's cutoff when a later request never reaches installation."""

import asyncio
import uuid

import pytest
from ocpp_hub_core.prototype.managed_occurrences import occurrences
from ocpp_hub_core.prototype.managed_schedule import KEY, idle_cutoff_pending

from tests.managed_scenario_helpers import (
    END,
    close_in_order,
    next_daily,
    originals,
    select_current,
    settle,
    unknown_report,
)
from tests.test_managed_proxy import healthy
from tests.test_managed_schedule_cutoff import timer_bench
from tests.test_prototype_tls import tls as tls


# Intent: follow the reproduced cutoff gap through supported cancellation, source recovery, closure and next daily.
# Why: a release finding needs a real reachable consequence, with its later recovery and scope established.
@pytest.mark.parametrize("prior_count", [1, 3])
@pytest.mark.parametrize("cancel", ["disconnect", "restart"])
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("first", ["native", "ledger"])
@pytest.mark.parametrize("resolution", ["daily", "temporary"])
async def test_cancelled_rebooking_retains_cutoff(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, prior_count, cancel, demand, first, resolution
):
    from contextlib import nullcontext

    from tests.managed_scenario_helpers import (
        deadline_task,
        next_daily,
        originals,
        replace_controller,
        require_originals,
    )
    from tests.test_managed_proxy import introduced

    async with timer_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        await select_current(b, 32)
        saved = originals(b)
        await b.arm()
        old_id = b.session.journal.get(KEY)["id"]
        old_timer = b.timer
        b.clock.now = b.start + 601
        await settle(b, 6)
        for _ in range(prior_count - 1):
            await b.wait_for_admission()
            b.start = (int(b.clock.now) // 60 + 4) * 60
            await b.arm()
            b.clock.now = b.start + 601
            await settle(b, 6)
        assert idle_cutoff_pending(b.session.journal.get(KEY))
        await b.wait_for_admission()
        b.start = b.clock.now + 179
        b.request()
        new_id = b.session.journal.get(KEY)["id"]
        if cancel == "restart":
            await replace_controller(b)
        else:
            b.rig.bench.cp, b.rig.bench.connection = "A", "Unplug"
            await settle(b)
            b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
        async with b.rig.charger(b.session) if cancel == "restart" else nullcontext(b.sock) as b.sock:
            if cancel == "restart":
                await introduced(b.rig, b.session, b.sock, "cancel-before-install-boot")
            await settle(b, 6)
            assert occurrences(b.session.journal)[new_id]["result"] == "cancelled_before_install"
            assert idle_cutoff_pending(occurrences(b.session.journal)[old_id])
            async with deadline_task(b):
                b.clock.now = END - 60
                local = await unknown_report(b, b.clock.now, None, demand)
                await settle(b)
                b.clock.now = END
                await settle(b, 6)
                assert b.device.effective_limit() == b.device.current == 0
                assert not b.stops() and b.session.journal.active() == local
                assert not b.session.journal.get("managed_control_fault")
                b.rig.peer.plan_session = (local, 9 if resolution == "daily" else old_timer)
                await settle(b)
                stops = (local,) if resolution == "temporary" else ()
                assert b.stops() == list(stops)
                assert b.device.effective_limit() == (0 if stops else 32)
                if first == "native":
                    b.device.active = None
                    b.rig.peer.permission_enabled = False
                    b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
                    b.device.requested_current = 0
                    await settle(b)
                    await b.close_transaction(local)
                else:
                    await b.close_transaction(local)
                    b.device.active = local
                    await settle(b)
                    b.device.active = None
                await settle(b, 6)
                require_originals(b, saved)
                await next_daily(b, 32, saved, stops)


# Intent: actual completion of a later authorized timer cannot leave an earlier idle duty to revive on cancellation.
# Why: the reciprocal case must preserve a newer pending window when an older delayed temporary session closes.
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("first", ["native", "ledger"])
async def test_newer_temporary_closure_discharges_older_idle_duty(
    tmp_path, tls, unused_tcp_port, monkeypatch, demand, first
):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await select_current(b, 32)
        saved = originals(b)
        await b.arm()
        older = b.session.journal.get(KEY)["id"]
        b.clock.now = b.start + 601
        await settle(b, 6)
        await b.wait_for_admission()
        b.start = (int(b.clock.now) // 60 + 4) * 60
        await b.arm()
        b.clock.now = b.start
        local = await b.start_plan(b.timer, demand=demand)
        await healthy(b.c)
        b.clock.now += 60
        await close_in_order(b, local, first, ())
        await healthy(b.c)
        assert not idle_cutoff_pending(occurrences(b.session.journal)[older])
        await b.wait_for_admission()
        b.start = (int(b.clock.now) // 60 + 4) * 60
        b.request()
        cancelled = b.session.journal.get(KEY)["id"]
        b.rig.bench.cp, b.rig.bench.connection = "A", "Unplug"
        await settle(b)
        b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
        await settle(b)
        assert occurrences(b.session.journal)[cancelled]["result"] == "cancelled_before_install"
        b.clock.now = END
        await settle(b)
        assert b.device.effective_limit() == 32 and not b.stops()
        await next_daily(b, 32, saved)


# Intent: an explicit successful Return clears old retired duties while ordinary cleanup cannot clear them.
# Why: replaying the final cancellation must not resurrect a duty the owner deliberately overrode.
@pytest.mark.parametrize("selected", [6, 32])
async def test_return_discharges_archived_idle_duties(tmp_path, tls, unused_tcp_port, monkeypatch, selected):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await select_current(b, selected)
        saved = originals(b)
        for index in range(3):
            if index:
                await b.wait_for_admission()
                b.start = (int(b.clock.now) // 60 + 4) * 60
            await b.arm()
            b.clock.now = b.start + 601
            await settle(b, 6)
        b.c.command(uuid.uuid4().hex, "pause", b.c.requests.intent().revision)
        await b.c.pause_task
        await healthy(b.c)
        b.c.command(uuid.uuid4().hex, "app_control", b.c.requests.intent().revision)
        await healthy(b.c)
        assert all(not idle_cutoff_pending(lease) for lease in occurrences(b.session.journal).values())
        b.clock.now = END
        await settle(b)
        assert b.device.effective_limit() == selected and not b.stops()
        await next_daily(b, selected, saved)


# Intent: an unsent replacement cannot hide the old deadline while its installation read is stalled.
# Why: cancellation and station zero must remain independent of the foreground native HTTP request.
@pytest.mark.parametrize("demand", [False, True])
async def test_unsent_rebooking_stalled_across_deadline(tmp_path, tls, unused_tcp_port, monkeypatch, demand):
    from tests.managed_scenario_helpers import deadline_task, demand_from_vehicle

    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await select_current(b, 32)
        saved = originals(b)
        await b.arm()
        reported = b.start
        b.clock.now = b.start + 601
        await settle(b, 6)
        await b.wait_for_admission()
        b.start = (int(b.clock.now) // 60 + 4) * 60
        b.request()
        requested = b.session.journal.get(KEY)["id"]
        entered, release = asyncio.Event(), asyncio.Event()

        # Keep the real HTTP response open while the independent OCPP transport remains writable.
        async def stalled(reader, writer):
            await reader.readuntil(b"\r\n\r\n")
            entered.set()
            await release.wait()
            writer.close()
            await writer.wait_closed()

        observer = b.session.prepared.observer
        original_port = observer.port
        async with await asyncio.start_server(stalled, "127.0.0.1", 0) as server, deadline_task(b):
            observer.port = server.sockets[0].getsockname()[1]
            poll = asyncio.create_task(b.c.tick())
            try:
                await asyncio.wait_for(entered.wait(), 2)
                b.device.active = 987654
                b.rig.peer.plan_session = (987654, None)
                demand_from_vehicle(b, demand)
                b.clock.now = END
                b.c.schedule.changed.set()
                async with asyncio.timeout(2):
                    while b.device.effective_limit() != 0:
                        await asyncio.sleep(0.01)
                assert b.device.current == 0 and not b.stops() and not poll.done()
                assert occurrences(b.session.journal)[requested]["result"] == "cancelled_before_install"
            finally:
                observer.port = original_port
                release.set()
                await asyncio.gather(poll, return_exceptions=True)
        local = await unknown_report(b, reported, None, demand)
        await settle(b)
        assert b.device.effective_limit() == 0 and not b.stops()
        b.rig.peer.plan_session = (local, 9)
        await settle(b)
        await close_in_order(b, local, "native", ())
        await healthy(b.c)
        await next_daily(b, 32, saved)
