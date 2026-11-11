"""Gate independent real loopback peers across temporary recovery zero confirmation."""

import asyncio
import json
from datetime import UTC, datetime

import pytest
from ocpp_hub_core.prototype.managed import ManagedController

from tests.handover_helpers import crash
from tests.test_managed_proxy import healthy, managed_session, starts
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_managed_schedule_source_boot import arm_at_32
from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests.test_trial_session import Bench


# Intent: changed Boot metadata revokes the accepted cloud introduction, while an identical retry retains it.
# Why: temporary zero verification may retain a session only while its actual admitted Boot binding agrees.
@pytest.mark.parametrize("changed", [False, True])
async def test_cloud_registration_remains_bound_to_boot(tmp_path, tls, unused_tcp_port, monkeypatch, changed):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        gateway, runtime = b.session.gateway, b.session.runtime
        epoch = gateway._vendor_epoch
        cloud_boots = [frame for frame in b.rig.bench.calls if frame[0] == 2 and frame[2] == "BootNotification"]
        assert runtime.status.vendor_connected and gateway.vendor_registration == "Accepted"
        payload = BOOT | {"chargePointModel": "Unapproved replacement"} if changed else BOOT
        result = await b.rig.bench.call(b.sock, "later-real-boot", "BootNotification", payload)
        assert result[0] == 3
        await b.c.tick()
        if changed:
            await until(lambda: not runtime.status.vendor_connected, timeout=1)
            assert gateway._vendor_epoch != epoch and gateway.vendor_registration != "Accepted"
            assert b.session.journal.get("trial_phase") == "recovering" and not gateway.controls.ready
        else:
            await asyncio.sleep(10 * runtime.settings.pump_interval)
            assert runtime.status.vendor_connected and gateway._vendor_epoch == epoch
            assert gateway.vendor_registration == "Accepted"
        assert [f for f in b.rig.bench.calls if f[0] == 2 and f[2] == "BootNotification"] == cloud_boots


# Intent: a cloud Start acknowledgement commits while a local zero re-confirmation is independently pending.
# Why: readiness is allowed to suspend new cloud openings, but must not cancel an admitted lifecycle RPC.
@pytest.mark.parametrize("demand", [False, True])
async def test_start_ack_survives_zero_reconfirmation(tmp_path, tls, unused_tcp_port, monkeypatch, demand):
    second_start_received, zero_inflight = asyncio.Event(), asyncio.Event()
    allow_cloud_ack, release_zero = asyncio.Event(), asyncio.Event()
    target = [None]
    starts_seen = [0]
    vendor = Bench.vendor

    class GateConnection:
        # Keep independent peer state outside the engine and journal.
        def __init__(self, connection):
            self.connection = connection

        # Recognize the real delayed temporary Start on the cloud socket.
        def __aiter__(self):
            # Yield every wire frame intact while identifying the second genuine Start.
            async def frames():
                async for raw in self.connection:
                    frame = json.loads(raw)
                    if frame[0] == 2 and frame[2] == "StartTransaction":
                        starts_seen[0] += 1
                        if starts_seen[0] == 2:
                            target[0] = frame[1]
                            second_start_received.set()
                    yield raw

            return frames()

        # Gate only the genuine cloud acknowledgement; all other protocol traffic remains independent.
        async def send(self, raw):
            frame = json.loads(raw)
            if frame[0] == 3 and frame[1] == target[0]:
                await allow_cloud_ack.wait()
            await self.connection.send(raw)

    # Run the existing fake cloud against a gated real connection.
    async def gated_vendor(bench, connection):
        await vendor(bench, GateConnection(connection))

    monkeypatch.setattr(Bench, "vendor", gated_vendor)
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
            await b.session.start()
            b.c = ManagedController(b.session)
            end = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
            b.clock.now = end - 4
            async with b.rig.charger(b.session) as b.sock:
                watcher = asyncio.create_task(b.c.schedule.watch_deadline())
                acknowledgement = None
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

                    # Hold only the physical ceiling result triggered after the delayed Start is admitted.
                    async def gate_zero(action, payload):
                        if (
                            action == "SetChargingProfile"
                            and payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                            == 0
                            and second_start_received.is_set()
                            and not zero_inflight.is_set()
                        ):
                            zero_inflight.set()
                            await release_zero.wait()

                    b.rig.bench.before_reply = gate_zero
                    opened = await b.rig.bench.call(b.sock, "new-temporary-start", "StartTransaction", body)
                    assert opened[0] == 3
                    local = opened[2]["transactionId"]
                    assert local not in (old, 987654)
                    b.rig.peer.plan_session = (local, b.timer)
                    await asyncio.wait_for(second_start_received.wait(), 2)

                    # Both peers are independently gated. Give the actual presence poll time to observe lost readiness.
                    # The established session must process its ACK before a separate zero confirmation completes.
                    async def acknowledge_during_zero():
                        try:
                            await asyncio.wait_for(zero_inflight.wait(), 2)
                            assert not b.session.gateway.controls.ready
                            await asyncio.sleep(3 * b.session.runtime.settings.pump_interval)
                            assert not b.session.gateway.controls.ready
                            assert b.session.runtime.status.vendor_connected
                            allow_cloud_ack.set()
                            await until(lambda: b.session.journal.vendor_id(local) is not None, timeout=2)
                            assert not b.session.gateway.controls.ready
                            assert starts_seen[0] == 2
                        finally:
                            release_zero.set()
                            allow_cloud_ack.set()

                    acknowledgement = asyncio.create_task(acknowledge_during_zero())
                    for _ in range(3):
                        b.clock.now += 16
                        await b.c.tick()
                    await acknowledgement
                    assert set(b.stops()) == {local} and b.device.effective_limit() == 0
                    await b.close_transaction(local)
                    assert (await healthy(b.c))["station_ceiling_a"] == 32 and set(b.device.plans) == {9}
                    following = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
                    assert following not in (old, local) and (await healthy(b.c))["current_a"] == 32
                    assert set(b.stops()) == {local} and not starts(b.rig)
                finally:
                    release_zero.set()
                    allow_cloud_ack.set()
                    if acknowledgement is not None:
                        await asyncio.gather(acknowledgement, return_exceptions=True)
                    observer.port = port
                    watcher.cancel()
                    await asyncio.gather(watcher, return_exceptions=True)
