"""Longer independent-peer scenarios N01 through N05 from the explicit verification inventory."""

from __future__ import annotations

import asyncio
import uuid
from contextlib import nullcontext
from datetime import UTC, datetime

import pytest

from tests.managed_scenario_helpers import (
    END,
    close_in_order,
    deadline_task,
    demand_from_vehicle,
    next_daily,
    originals,
    replace_controller,
    require_originals,
    select_current,
    settle,
    unknown_report,
)
from tests.test_managed_proxy import healthy, introduced, starts
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_managed_start_overlap import delayed_owner, owner_tag
from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Intent: unused departures retain the original cutoff through a new future booking, actual charge and next daily.
# Why: neither timer removal nor a later receipt can grant current before an eligible identified session.
@pytest.mark.parametrize("wire_departure", [False, True])
@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("selected", [6, 32])
async def test_n01_unused_departure_future_booking(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, wire_departure, restart, selected
):
    async with timer_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        await select_current(b, selected)
        saved = originals(b)
        await b.arm()
        b.rig.bench.cp, b.rig.bench.connection = "A", "Unplug"
        if wire_departure:
            await b.rig.bench.call(
                b.sock,
                "departed",
                "StatusNotification",
                {"connectorId": 1, "status": "Available", "errorCode": "NoError"},
            )
        else:
            await b.c.native.read()
        b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
        await settle(b, 5)
        assert b.c.schedule.summary()["result"] == "vehicle_disconnected"
        require_originals(b, saved)
        if restart:
            await replace_controller(b)
        async with b.rig.charger(b.session) if restart else nullcontext(b.sock) as b.sock:
            if restart:
                await introduced(b.rig, b.session, b.sock, "unused-restart")
            async with deadline_task(b):
                b.clock.now = END
                await settle(b)
                assert (await healthy(b.c))["station_ceiling_a"] == b.device.effective_limit() == 0
                assert not b.stops()
                # The intervening daily trigger meets a full car without a new lifecycle.
                b.clock.now = int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp())
                b.device.fire_plan(9)
                demand_from_vehicle(b, False)
                await settle(b)
                assert b.session.journal.active() is None and b.device.effective_limit() == 0
                b.clock.now += 3 * 3600
                await settle(b)
                b.start = b.clock.now + 180
                await b.wait_for_admission()
                await b.arm()
                assert b.device.effective_limit() == 0 and not b.stops()
                b.clock.now = b.start
                local = await b.start_plan(b.timer)
                assert (await healthy(b.c))["station_ceiling_a"] == b.device.current == selected
                b.clock.now = int(datetime.fromtimestamp(b.start, ZONE).replace(hour=6).timestamp())
                await settle(b)
                assert b.device.effective_limit() == 0 and b.stops() == [local]
                await close_in_order(b, local, "native", (local,))
                await healthy(b.c)
                require_originals(b, saved)
                await next_daily(b, selected, saved, (local,))


# Intent: all N02 source, current, demand, resolution and closure combinations recover through a complete next daily.
# Why: early inventory completion cannot exempt an unidentified session arriving before the original deadline.
@pytest.mark.parametrize("source", [None, 987655])
@pytest.mark.parametrize("selected", [6, 32])
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("resolution", ["temporary", "daily"])
@pytest.mark.parametrize("first", ["native", "ledger"])
async def test_n02_pre_cutoff_unknown_after_cleanup(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, source, selected, demand, resolution, first
):
    async with timer_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        await select_current(b, selected)
        saved = originals(b)
        await b.arm()
        await early_cleanup_selected(b, selected)
        async with deadline_task(b):
            b.clock.now = END - 60
            local = await unknown_report(b, b.start, source, demand)
            await settle(b)
            b.clock.now = END
            await settle(b)
            assert b.device.effective_limit() == b.device.current == 0 and not b.stops()
            b.rig.peer.plan_session = (local, b.timer if resolution == "temporary" else 9)
            await settle(b)
            stops = (local,) if resolution == "temporary" else ()
            assert b.stops() == list(stops)
            assert b.device.effective_limit() == (0 if stops else selected)
            assert b.device.current == (selected if demand and not stops else 0)
            await close_in_order(b, local, first, stops)
            await healthy(b.c)
            require_originals(b, saved)
            await next_daily(b, selected, saved, stops)


# Remove only the unused one-off and retain the requested selected current for its pre-cutoff idle period.
async def early_cleanup_selected(b, selected):
    b.clock.now = b.start + 601
    await settle(b, 5)
    await healthy(b.c)
    assert b.c.schedule.summary()["state"] == "complete"
    assert set(b.device.plans) == {9} and b.device.effective_limit() == selected


# Intent: a genuine delayed or current-time report before cutoff retains its original occurrence and exact Stop.
# Why: changing the timestamp or arriving immediately before the deadline cannot create a new permission window.
@pytest.mark.parametrize("reported_at_start", [False, True])
@pytest.mark.parametrize("at_boundary", [False, True])
@pytest.mark.parametrize("demand", [False, True])
async def test_n03_reporting_time_before_cutoff(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, reported_at_start, at_boundary, demand
):
    async with timer_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        await select_current(b, 32)
        saved = originals(b)
        await b.arm()
        await early_cleanup_selected(b, 32)
        async with deadline_task(b):
            b.clock.now = END - 1 if at_boundary else b.start + 660
            local = await unknown_report(b, b.start if reported_at_start else b.clock.now, None, demand)
            await settle(b)
            b.clock.now = END
            await settle(b)
            assert b.device.effective_limit() == 0 and not b.stops()
            b.rig.peer.plan_session = (local, b.timer)
            await settle(b)
            assert b.device.effective_limit() == 0 and b.stops() == [local]
            await close_in_order(b, local, "ledger", (local,))
            await healthy(b.c)
            require_originals(b, saved)
            await next_daily(b, 32, saved, (local,))


# Intent: completed inventory remains supervised through every connection boundary and native interface outage.
# Why: fresh source verification owns positive restoration and an unavailable native interface owns no guessed Stop.
@pytest.mark.parametrize("boundary", ["reconnect", "restart", "boot"])
@pytest.mark.parametrize("outage", [False, True])
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("resolution", ["temporary", "daily"])
async def test_n04_completed_obligation_connection_boundary(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, boundary, outage, demand, resolution
):
    async with timer_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        await select_current(b, 32)
        saved = originals(b)
        await b.arm()
        await early_cleanup_selected(b, 32)
        b.clock.now = END - 60
        local = await unknown_report(b, b.start, None, demand)
        await settle(b)
        b.clock.now = END
        await settle(b)
        if boundary == "restart":
            await replace_controller(b)
        async with b.rig.charger(b.session) if boundary != "boot" else nullcontext(b.sock) as b.sock:
            observer = b.session.prepared.observer
            port = observer.port
            if outage:
                observer.port = unused_tcp_port_factory()
            try:
                assert (await b.rig.bench.call(b.sock, "one-boot", "BootNotification", BOOT))[0] == 3
                await until(
                    lambda: (
                        b.session.gateway.controls.ready
                        or b.session.runtime.status.last_failure == "BootOrCeilingUnavailable"
                    )
                )
                async with deadline_task(b):
                    await settle(b)
                    assert b.device.effective_limit() == 0 and not b.stops()
            finally:
                observer.port = port
            # Recover on this connection, without replacing the ambiguous session or replaying its Start.
            await settle(b, 5)
            await until(lambda: b.session.gateway.controls.ready)
            b.rig.peer.plan_session = (local, b.timer if resolution == "temporary" else 9)
            async with deadline_task(b):
                await settle(b, 5)
                stops = (local,) if resolution == "temporary" else ()
                # A real peer RPC or the independent supervisor may still own the channel after five polls.
                async with asyncio.timeout(5):
                    while b.stops() != list(stops) or b.device.effective_limit() != (0 if stops else 32):
                        assert set(b.stops()) <= set(stops)
                        assert not b.session.journal.get("managed_control_fault")
                        if stops:
                            assert b.device.effective_limit() == 0
                        await settle(b)
                        await asyncio.sleep(0.01)
                assert b.stops() == list(stops)
                assert b.device.effective_limit() == (0 if stops else 32)
                await close_in_order(b, local, "ledger" if outage else "native", stops)
                await healthy(b.c)
                require_originals(b, saved)
                await next_daily(b, 32, saved, stops)


# Intent: every generic recovery boundary preserves a confirmed daily after the actual temporary lifecycle ended.
# Why: a previous completed one-off supplies no authority to Stop the independent daily transaction.
@pytest.mark.parametrize("boundary", ["reconnect", "restart", "boot"])
@pytest.mark.parametrize("selected", [6, 32])
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("first", ["native", "ledger"])
async def test_n05_daily_after_actual_temporary_completion(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, boundary, selected, demand, first
):
    async with timer_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        await select_current(b, selected)
        saved = originals(b)
        await b.arm()
        b.clock.now = b.start
        temporary = await b.start_plan(b.timer)
        await healthy(b.c)
        b.clock.now = END
        await settle(b)
        assert b.stops() == [temporary]
        await close_in_order(b, temporary, "native", (temporary,))
        await healthy(b.c)
        require_originals(b, saved)
        b.clock.now = int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp())
        daily = await b.start_plan(9, demand=demand)
        await healthy(b.c)
        await until(lambda: b.session.journal.next_delivery() is None and not b.session.gateway.vendor._pending)
        if boundary == "restart":
            await replace_controller(b)
        async with b.rig.charger(b.session) if boundary != "boot" else nullcontext(b.sock) as b.sock:
            await introduced(b.rig, b.session, b.sock, "independent-daily-boot")
            if boundary == "boot":
                # A repeated message on the existing socket refreshes source proof without a transport introduction.
                await b.c.native.read()
                assert (
                    b.session.journal.get("managed_temporary_schedule")["source_boot"]
                    == b.session.gateway.boot_observation
                )
            else:
                assert b.device.effective_limit() == 0
            async with deadline_task(b):
                status = await healthy(b.c)
                assert status["station_ceiling_a"] == b.device.effective_limit() == selected
                assert b.device.current == (selected if demand else 0) and b.stops() == [temporary]
                await close_in_order(b, daily, first, (temporary,))
                await healthy(b.c)
                require_originals(b, saved)
                await next_daily(b, selected, saved, (temporary,))


# Intent: a restart retires an unexpired unbound owner Start without stopping its independent daily session.
# Why: command acknowledgement order and source identity remain distinct from explicit owner Pause authority.
@pytest.mark.parametrize("before_ack", [False, True])
@pytest.mark.parametrize("selected", [6, 32])
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("first", ["native", "ledger"])
async def test_n06_unexpired_start_daily_restart(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, before_ack, selected, demand, first
):
    async with timer_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        await select_current(b, selected)
        saved = originals(b)
        owner_tag(b)
        daily_job = None

        # A real independent Start can arrive while the device withholds the earlier command acknowledgement.
        async def before_reply(action, payload):
            nonlocal daily_job
            if before_ack and action == "RemoteStartTransaction":
                daily_job = asyncio.create_task(b.start_plan(9, demand=demand))
                await until(lambda: b.session.journal.active() is not None)

        b.rig.bench.before_reply = before_reply
        try:
            b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
            await b.c.tests.settle()
            daily = await daily_job if daily_job is not None else await b.start_plan(9, demand=demand)
        finally:
            b.rig.bench.before_reply = None
        await healthy(b.c)
        assert len(starts(b.rig)) == 1 and b.session.journal.get("control")["start_pending"]
        await replace_controller(b)
        async with b.rig.charger(b.session) as b.sock:
            await introduced(b.rig, b.session, b.sock, "pending-owner-restart")
            await healthy(b.c)
            assert b.c.requests.intent().mode == "app"
            assert b.session.journal.get("local_start_receipt")["local"] is None
            assert b.device.effective_limit() == selected and not b.stops()
            await close_in_order(b, daily, "native", ())
            assert b.session.journal.get("control")["start_pending"] and not b.stops()
            owner = await delayed_owner(b)
            await settle(b)
            assert set(b.stops()) == {owner} and b.device.effective_limit() == 0
            stops = tuple(b.stops())
            await close_in_order(b, owner, first, stops)
            await healthy(b.c)
            assert not b.session.journal.get("control").get("start_pending")
            require_originals(b, saved, 1)
            await next_daily(b, selected, saved, stops, 1)


# Submit a distinct, acknowledged owner command and its actual lifecycle through both production protocol peers.
async def confirmed_cloud_owner(b, demand):
    accepted = await b.rig.bench.cloud_call(
        "independent-owner", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}
    )
    assert accepted[0] == 3 and accepted[2]["status"] == "Accepted"
    reported = await b.rig.bench.call(
        b.sock,
        "owner-lifecycle",
        "StartTransaction",
        START | {"idTag": "CLOUD_OWNER", "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()},
    )
    assert reported[0] == 3 and reported[2]["idTagInfo"]["status"] == "Accepted"
    local = reported[2]["transactionId"]
    b.rig.peer.plan_session = (local, 987655)
    demand_from_vehicle(b, demand)
    return local


# Intent: closure lag remains recoverable after unused cleanup or a real completed temporary lifecycle.
# Why: a daily or independently acknowledged owner session needs both closures without an unrelated permanent fault.
@pytest.mark.parametrize("completed_temporary", [False, True])
@pytest.mark.parametrize("cloud_owner", [False, True])
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("first", ["native", "ledger"])
async def test_n07_post_completion_closure(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, completed_temporary, cloud_owner, demand, first
):
    async with timer_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        await select_current(b, 32)
        saved = originals(b)
        await b.arm()
        stops = ()
        if completed_temporary:
            b.clock.now = b.start
            temporary = await b.start_plan(b.timer)
            await healthy(b.c)
            b.clock.now = END
            await settle(b)
            stops = (temporary,)
            await close_in_order(b, temporary, "native", stops)
            await healthy(b.c)
            b.clock.now = int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp())
        else:
            await early_cleanup_selected(b, 32)
            b.clock.now = END - 60
        local = await confirmed_cloud_owner(b, demand) if cloud_owner else await b.start_plan(9, demand=demand)
        assert (await healthy(b.c))["station_ceiling_a"] == 32
        assert b.device.current == (32 if demand else 0)
        await close_in_order(b, local, first, stops)
        await healthy(b.c)
        starts_count = int(cloud_owner)
        require_originals(b, saved, starts_count)
        await next_daily(b, 32, saved, stops, starts_count)
