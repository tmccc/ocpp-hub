"""A real OCPP report can precede the independent physical source after idle retirement."""

from datetime import UTC, datetime, timedelta

import pytest

from tests.test_managed_proxy import healthy, starts
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_managed_schedule_source_boot import arm_at_32
from tests.test_prototype import BOOT, START
from tests.test_prototype_tls import tls as tls


# Intent: retain the report at zero through missing evidence or a new Boot, then act on its verified source.
# Why: an apparently idle charger can first disclose the same tag and transaction before its independent plan field.
@pytest.mark.parametrize("source", ["temporary", "daily", "unknown"])
@pytest.mark.parametrize("boundary", ["ordinary", "outage", "boot"])
@pytest.mark.parametrize("demand", [False, True])
async def test_idle_cutoff_protocol_first(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, source, boundary, demand
):
    async with timer_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        await arm_at_32(b)
        b.clock.now = b.start + 601
        for _ in range(5):
            await b.c.tick()
        assert set(b.device.plans) == {9}
        end = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
        b.clock.now = end
        for _ in range(4):
            await b.c.tick()
        assert b.device.effective_limit() == 0 and not b.stops()
        b.clock.now = int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()) if source == "daily" else end + 60
        stamp = b.clock.now if source == "daily" else b.start
        payload = START | {
            "idTag": b.device.plans[9]["idTag"],
            "timestamp": datetime.fromtimestamp(stamp, UTC).isoformat(),
        }
        observer = b.session.prepared.observer
        old_port = observer.port
        try:
            if boundary == "outage":
                observer.port = unused_tcp_port_factory()
            result = await b.rig.bench.call(b.sock, "actual-ocpp-before-source", "StartTransaction", payload)
            assert result[0] == 3 and result[2]["idTagInfo"]["status"] == "Accepted"
            local = result[2]["transactionId"]
            assert b.session.journal.active() == local and b.device.effective_limit() == 0 and not b.stops()
            if boundary == "boot":
                reply = await b.rig.bench.call(b.sock, "boot-before-source", "BootNotification", BOOT)
                assert reply[0] == 3
            b.rig.peer.plan_session = (local, 987655)
            b.rig.peer.permission_enabled = demand
            b.rig.peer.paused = False
            b.device.requested_current = 32 if demand else 0
            b.rig.bench.cp, b.rig.bench.connection = ("C", "Charging") if demand else ("B", "Plug")
            for _ in range(3):
                await b.c.tick()
            assert b.device.effective_limit() == b.device.current == 0 and not b.stops()
        finally:
            observer.port = old_port
        b.rig.peer.plan_session = (local, {"temporary": b.timer, "daily": 9, "unknown": 987655}[source])
        for _ in range(4):
            await b.c.tick()
        if source == "daily":
            assert b.device.effective_limit() == 32 and b.device.current == (32 if demand else 0) and not b.stops()
        elif source == "temporary":
            assert b.device.effective_limit() == b.device.current == 0 and b.stops() == [local]
        else:
            assert b.device.effective_limit() == b.device.current == 0 and not b.stops()
            b.rig.peer.plan_session = (local, b.timer)
            for _ in range(4):
                await b.c.tick()
            assert b.stops() == [local] and b.device.effective_limit() == 0
        repeated = await b.rig.bench.call(b.sock, "duplicate-real-report", "StartTransaction", payload)
        assert repeated[2] == result[2]
        b.clock.now += 60
        await b.close_transaction(local)
        for _ in range(5):
            await b.c.tick()
        tomorrow = datetime.fromtimestamp(b.clock.now, ZONE).date() + timedelta(days=1)
        midnight = datetime.combine(tomorrow, datetime.min.time(), tzinfo=ZONE)
        daily = await b.advance_idle_to(int(midnight.timestamp()))
        assert daily not in (None, local) and (await healthy(b.c))["current_a"] == 32
        assert daily not in b.stops() and set(b.device.plans) == {9} and not starts(b.rig)
