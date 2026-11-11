"""The same ledger ID and acknowledged Boot payload can persist while its observed native source changes."""

import asyncio
import uuid
from contextlib import nullcontext
from datetime import UTC, datetime

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.native_bounded_handover import AuditSupersededError

from tests.handover_helpers import crash
from tests.test_managed_proxy import healthy, managed_session, starts
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_prototype import BOOT, START
from tests.test_prototype_tls import tls as tls


# Set a real 32 A preference, then arm the one-off using the ordinary request and readback paths.
async def arm_at_32(b):
    b.c.command(uuid.uuid4().hex, "set_current", b.c.requests.intent().revision, current_a=32)
    await b.c.current.settle()
    await healthy(b.c)
    await b.arm()


# Intent: stale daily or temporary origin cannot grant current or issue a Stop across a connection or Boot boundary.
# Why: the same transaction number and tag can remain open while the new native observation reports another source.
@pytest.mark.parametrize("boundary", ["restart", "reconnect", "repeat_boot"])
@pytest.mark.parametrize("prior", ["daily", "temporary"])
@pytest.mark.parametrize("demand", [False, True])
async def test_cutoff_source_needs_current_boot(tmp_path, tls, unused_tcp_port, monkeypatch, demand, prior, boundary):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await arm_at_32(b)
        b.clock.now = b.start
        local = await b.start_plan(9 if prior == "daily" else b.timer, demand=demand)
        await healthy(b.c)
        async with asyncio.timeout(3):
            while b.session.journal.next_delivery() is not None:
                await asyncio.sleep(0.01)
        if boundary == "restart":
            await crash(b.session)
            b.session = managed_session(b.rig)
            await b.session.start()
            b.c = ManagedController(b.session)
        native = b.c.native
        observe = native.observe
        captured, release = asyncio.Event(), asyncio.Event()
        pending = None

        # Delay an in-flight complete read from the old Boot; subsequent reads fail before seeing any new origin.
        async def interrupted():
            if asyncio.current_task() is pending:
                snapshot = await observe()
                captured.set()
                await release.wait()
                return snapshot
            raise TimeoutError("no native observations available on this connection")

        native.observe = interrupted
        if boundary == "repeat_boot":
            pending = asyncio.create_task(native.read())
            await asyncio.wait_for(captured.wait(), 2)
        b.rig.peer.plan_session = (local, 987655)
        end = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
        b.clock.now = end - 8
        try:
            async with b.rig.charger(b.session) if boundary != "repeat_boot" else nullcontext(b.sock) as b.sock:
                reply = await b.rig.bench.call(b.sock, "new-introduction", "BootNotification", BOOT)
                assert reply[0] == 3
                if pending:
                    release.set()
                    with pytest.raises(AuditSupersededError):
                        await pending
                # A missing fresh source means zero even before the deadline and while demand persists at 06:00.
                for now in (end - 8, end):
                    b.clock.now = now
                    await b.c.tick()
                    assert b.device.effective_limit() == 0 and b.device.current == 0
                assert b.session.journal.active() == local and not b.stops()
                native.observe = observe
                for _ in range(3):
                    await b.c.tick()
                assert b.device.effective_limit() == 0 and not b.stops()
                b.rig.peer.plan_session = (local, 9)
                assert (await healthy(b.c))["station_ceiling_a"] == 32
                assert b.device.current == (32 if demand else 0) and not b.stops()
                await b.close_transaction(local)
                await healthy(b.c)
                assert set(b.device.plans) == {9}
                following = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
                assert following != local and (await healthy(b.c))["current_a"] == 32
                assert not b.stops() and not starts(b.rig)
        finally:
            release.set()
            native.observe = observe
            if pending:
                await asyncio.gather(pending, return_exceptions=True)


# Intent: a different physical transaction can open while the old daily transaction's genuine OCPP Stop is delayed.
# Why: an HTTP outage after reconnect must cap the new physical session even if no read observed the turnover.
@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("observed_before", [False, True])
@pytest.mark.parametrize("demand", [False, True])
async def test_physical_turnover_before_reconnect(
    tmp_path, tls, unused_tcp_port, monkeypatch, restart, observed_before, demand
):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await arm_at_32(b)
        b.clock.now = b.start - 60
        old = await b.start_plan(9, demand=demand)
        await healthy(b.c)
        async with asyncio.timeout(3):
            while b.session.journal.next_delivery() is not None:
                await asyncio.sleep(0.01)

        # The daily session physically closes and then the one-off fires, with both lifecycle messages still in transit.
        def pending_temporary():
            b.device.active = 987654
            b.rig.peer.plan_session = (987654, b.timer)
            b.device.requested_current = 32 if demand else 0
            b.rig.peer.permission_enabled = demand
            b.rig.bench.cp, b.rig.bench.connection = ("C", "Charging") if demand else ("B", "Plug")

        b.clock.now = b.start
        b.device.fire_plan(b.timer)
        pending_temporary()
        if observed_before:
            await b.c.native.read()
        if restart:
            await crash(b.session)
            b.session = managed_session(b.rig)
        observer = b.session.prepared.observer
        port = observer.port

        # Break actual native HTTP connections; the laptop's authenticated charger transport keeps answering OCPP.
        async def interrupted(reader, writer):
            await reader.readuntil(b"\r\n\r\n")
            writer.close()
            await writer.wait_closed()

        async with await asyncio.start_server(interrupted, "127.0.0.1", 0) as server:
            observer.port = server.sockets[0].getsockname()[1]
            if restart:
                await b.session.start()
                b.c = ManagedController(b.session)
            end = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
            b.clock.now = end - 4
            async with b.rig.charger(b.session) as b.sock:
                watcher = asyncio.create_task(b.c.schedule.watch_deadline())
                try:
                    await b.rig.bench.call(b.sock, "no-turnover-evidence", "BootNotification", BOOT)
                    async with asyncio.timeout(3):
                        while b.device.effective_limit() != 0:
                            await asyncio.sleep(0.01)
                    b.clock.now = end
                    for _ in range(3):
                        await b.c.tick()
                    assert b.device.current == 0 and b.device.effective_limit() == 0
                    assert b.session.journal.active() == old and b.device.active == 987654 and not b.stops()

                    observer.port = port
                    b.clock.now += 16
                    async with asyncio.timeout(5):
                        while b.c.native.introduced_epoch != b.session.gateway._charger_epoch:
                            await b.c.tick()
                            await asyncio.sleep(0.02)
                    assert b.device.effective_limit() == 0 and not b.stops()
                    stamp = datetime.fromtimestamp(b.start, UTC).isoformat()
                    closed = await b.rig.bench.call(
                        b.sock,
                        "old-daily-stop",
                        "StopTransaction",
                        {"transactionId": old, "meterStop": START["meterStart"], "timestamp": stamp},
                    )
                    assert closed[0] == 3
                    pending_temporary()
                    await b.c.tick()
                    body = START | {"idTag": b.device.plans[b.timer]["idTag"], "timestamp": stamp}
                    opened = await b.rig.bench.call(b.sock, "new-temporary-start", "StartTransaction", body)
                    assert opened[0] == 3
                    local = opened[2]["transactionId"]
                    assert local not in (old, 987654)
                    b.rig.peer.plan_session = (local, b.timer)
                    for _ in range(3):
                        b.clock.now += 16
                        await b.c.tick()
                    assert set(b.stops()) == {local} and b.device.effective_limit() == 0
                    await b.close_transaction(local)
                    assert (await healthy(b.c))["station_ceiling_a"] == 32 and set(b.device.plans) == {9}
                    following = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
                    assert following not in (old, local) and (await healthy(b.c))["current_a"] == 32
                    assert set(b.stops()) == {local} and not starts(b.rig)
                finally:
                    observer.port = port
                    watcher.cancel()
                    await asyncio.gather(watcher, return_exceptions=True)
