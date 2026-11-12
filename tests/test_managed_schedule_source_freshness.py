"""At cutoff, a positive exception describes the freshly identified physical session on the live socket."""

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from ocpp_hub_core.prototype import managed_schedule
from ocpp_hub_core.prototype.native_http import NativeTransportUnavailableError
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.security import SecurityError

from tests.test_managed_proxy import healthy, starts
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_managed_schedule_source_boot import arm_at_32
from tests.test_prototype import START
from tests.test_prototype_tls import tls as tls


# Intent: an open socket cannot prove that the physical source is still the previous independently identified daily.
# Why: native HTTP can fail or stall across 06:00 while a different physical transaction's OCPP Start is pending.
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("stalled", [False, True])
async def test_unreported_turnover_on_uninterrupted_socket(
    tmp_path, tls, unused_tcp_port, monkeypatch, demand, stalled
):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await arm_at_32(b)
        b.clock.now = b.start - 60
        old = await b.start_plan(9, demand=demand)
        await healthy(b.c)
        b.device.fire_plan(b.timer)

        # Deliver the old Stop and new Start later, while the peer already presents the different native transaction.
        def show_new_source():
            b.device.active = 987654
            b.rig.peer.plan_session = (987654, b.timer)
            b.rig.peer.paused = False
            b.rig.peer.permission_enabled = demand
            b.rig.bench.cp, b.rig.bench.connection = ("C", "Charging") if demand else ("B", "Plug")
            b.device.requested_current = 32 if demand else 0

        show_new_source()
        observer = b.session.prepared.observer
        port = observer.port
        entered, release = asyncio.Event(), asyncio.Event()

        # Either close a real HTTP connection immediately or keep its response outstanding past the deadline.
        async def interrupted(reader, writer):
            await reader.readuntil(b"\r\n\r\n")
            entered.set()
            if stalled:
                await release.wait()
            writer.close()
            await writer.wait_closed()

        async with await asyncio.start_server(interrupted, "127.0.0.1", 0) as server:
            end = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
            loop = asyncio.get_running_loop()
            boundary = loop.time() + 0.6
            monkeypatch.setattr(managed_schedule, "time", SimpleNamespace(time=lambda: end + loop.time() - boundary))
            observer.port = server.sockets[0].getsockname()[1]
            poll = asyncio.create_task(b.c.tick())
            watcher = asyncio.create_task(b.c.schedule.watch_deadline())
            try:
                await asyncio.wait_for(entered.wait(), 1)
                async with asyncio.timeout(2):
                    while b.device.effective_limit() != 0:
                        await asyncio.sleep(0.01)
                assert loop.time() < boundary + 0.5 and b.device.current == 0
                if stalled:
                    assert not poll.done()
                assert b.session.journal.active() == old and b.device.active == 987654 and not b.stops()
            finally:
                observer.port = port
                release.set()
                await asyncio.gather(poll, return_exceptions=True)
                watcher.cancel()
                await asyncio.gather(watcher, return_exceptions=True)
        b.clock.now = end + 16
        monkeypatch.setattr(managed_schedule, "time", SimpleNamespace(time=lambda: b.clock.now))
        for _ in range(3):
            await b.c.tick()
        assert b.device.effective_limit() == 0 and not b.stops()
        stamp = datetime.fromtimestamp(b.start, UTC).isoformat()
        closed = await b.rig.bench.call(
            b.sock,
            "overlap-old-stop",
            "StopTransaction",
            {"transactionId": old, "meterStop": START["meterStart"], "timestamp": stamp},
        )
        assert closed[0] == 3
        show_new_source()
        await b.c.tick()
        reply = await b.rig.bench.call(
            b.sock,
            "overlap-new-start",
            "StartTransaction",
            START | {"idTag": b.device.plans[b.timer]["idTag"], "timestamp": stamp},
        )
        assert reply[0] == 3
        local = reply[2]["transactionId"]
        b.rig.peer.plan_session = (local, b.timer)
        for _ in range(3):
            b.clock.now += 16
            await b.c.tick()
        assert local not in (old, 987654) and set(b.stops()) == {local} and b.device.current == 0
        await b.close_transaction(local)
        assert (await healthy(b.c))["station_ceiling_a"] == 32 and set(b.device.plans) == {9}
        following = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
        assert following not in (old, local) and (await healthy(b.c))["current_a"] == 32
        assert set(b.stops()) == {local} and not starts(b.rig)


# Intent: a continuing daily session retains selected current across cutoff and subsequent native-evidence expiry.
# Why: a weekly timer remains independent when fresh HTTP reads identify its exact transaction despite 5 s polling.
async def test_fresh_daily_keeps_current_through_cutoff(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await arm_at_32(b)
        b.clock.now = b.start
        local = await b.start_plan(9)
        assert (await healthy(b.c))["current_a"] == 32
        loop = asyncio.get_running_loop()
        end = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
        boundary = loop.time() + 0.3
        monkeypatch.setattr(managed_schedule, "time", SimpleNamespace(time=lambda: end + loop.time() - boundary))
        writes = len(b.rig.bench.received_limits)
        watcher = asyncio.create_task(b.c.schedule.watch_deadline())
        try:
            await asyncio.sleep(5.1)
            assert (await healthy(b.c))["current_a"] == 32
            await asyncio.sleep(0.3)
            assert b.session.journal.active() == local and b.device.current == 32
            assert not b.stops() and 0 not in b.rig.bench.received_limits[writes:]
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)


# Intent: each outgoing continuation must honor a failed source audit, but a fresh open audit still identifies the car.
# Why: both results can arrive after the positive profile was applied and before its confirmation has returned.
@pytest.mark.parametrize("failure", ["missing", "open"])
async def test_source_freshness_during_positive_confirmation(tmp_path, tls, unused_tcp_port, monkeypatch, failure):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await arm_at_32(b)
        b.clock.now = b.start
        local = await b.start_plan(9)
        b.rig.peer.plan_session = (local, 987654)
        b.clock.now = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
        await b.c.tick()
        assert b.device.effective_limit() == 0 and not b.stops()
        b.rig.peer.plan_session = (local, 9)
        observer = b.session.prepared.observer
        port = observer.port
        observed = asyncio.Event()

        # The independent native endpoint loses its real response while the positive profile ACK remains outstanding.
        async def unavailable(reader, writer):
            try:
                await reader.readuntil(b"\r\n\r\n")
            finally:
                writer.close()
                await writer.wait_closed()

        async with await asyncio.start_server(unavailable, "127.0.0.1", 0) as server:
            # A valid open audit retains its source; lost HTTP evidence retains only the existing timer's zero hold.
            async def during_ack(action, payload):
                if (
                    action == "SetChargingProfile"
                    and not observed.is_set()
                    and payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] > 0
                ):
                    if failure == "open":
                        with pytest.raises(SecurityError):
                            await b.c.native.read(closed=True)
                    else:
                        observer.port = server.sockets[0].getsockname()[1]
                        with pytest.raises(UnavailableError) as unavailable_audit:
                            await b.c.native.read()
                        assert isinstance(unavailable_audit.value.__cause__, NativeTransportUnavailableError)
                    observed.set()

            b.rig.bench.before_reply = during_ack
            try:
                await b.c.tick()
                assert observed.is_set() and b.device.effective_limit() == (0 if failure == "missing" else 32)
                assert b.session.journal.active() == local and not b.stops()
            finally:
                observer.port = port
                b.rig.bench.before_reply = None
        assert (await healthy(b.c))["current_a"] == 32 and not b.stops()
        await b.close_transaction(local)
        await healthy(b.c)
        assert set(b.device.plans) == {9} and not starts(b.rig)
