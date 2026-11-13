"""Cached cloud publication does not erase a new connection's conservative local cable-departure evidence."""

from datetime import UTC, datetime

import pytest
from ocpp_hub_core.prototype import managed_schedule

from tests.test_managed_proxy import healthy, starts
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_prototype import BOOT
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Intent: a pre-Boot replacement's exact Available retry publishes once and still retires the earlier one-off.
# Why: receipt replay must not bypass the local status observer or let a new cable inherit the old timer.
@pytest.mark.parametrize("timed", [False, True])
async def test_cached_available_retires_managed_timer_on_replacement(
    tmp_path, tls, unused_tcp_port, monkeypatch, timed
):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        event = {"connectorId": 1, "status": "Available", "errorCode": "NoError"}
        if timed:
            event["timestamp"] = datetime.fromtimestamp(b.clock.now - 60, UTC).isoformat()
        assert (await b.rig.bench.call(b.sock, "same-departure", "StatusNotification", event))[0] == 3
        await b.wait_for_admission()
        await b.arm()
        await until(lambda: b.session.journal.next_delivery() is None)
        before = len([frame for frame in b.rig.bench.calls if frame[0] == 2 and frame[2] == "StatusNotification"])
        assert before == 1
        await b.sock.close()
        await until(lambda: not b.session.runtime.status.charger_connected)
        async with b.rig.charger(b.session) as sock:
            b.sock = sock
            assert (await b.rig.bench.call(sock, "same-departure", "StatusNotification", event))[0] == 3
            assert b.session.journal.get(managed_schedule.KEY).get("retire_reason") == "vehicle_disconnected"
            assert (await b.rig.bench.call(sock, "replacement-boot", "BootNotification", BOOT))[0] == 3
            await healthy(b.c)
            assert b.c.schedule.summary()["result"] == "vehicle_disconnected" and set(b.device.plans) == {9}
            assert (
                len([frame for frame in b.rig.bench.calls if frame[0] == 2 and frame[2] == "StatusNotification"])
                == before
            )
            assert not starts(b.rig)
            local = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
            assert local is not None and (await healthy(b.c))["power_w"] > 0
            await b.close_transaction(local)
            assert (await healthy(b.c))["normal_start_ready"]
