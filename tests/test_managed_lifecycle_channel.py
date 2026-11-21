"""Gate independent real loopback peers across temporary recovery zero confirmation."""

import asyncio

import pytest

from tests.managed_rig import starts
from tests.native_schedule_support import timer_bench
from tests.prototype_rig import BOOT
from tests.runtime_rig import until
from tests.tls_support import tls as tls


# Intent: an intervening Boot change revokes the old registration even if the original body returns before polling.
# Why: comparing only the current body can erase a real A-to-B-to-A introduction revocation.
async def test_intervening_boot_change_revokes_cloud(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port) as b:
        gateway, runtime = b.session.gateway, b.session.runtime
        epoch = gateway._vendor_epoch
        assert runtime.status.vendor_connected and gateway.vendor_registration == "Accepted"
        (presence,) = (
            task for task in runtime._session_tasks if task.get_coro().__qualname__.endswith(".charger_presence")
        )
        parked, resume = asyncio.Event(), asyncio.Event()
        sleep = asyncio.sleep

        # Hold just one real presence interval while both charger reports cross the independent receiver.
        async def hold_presence_interval(delay, result=None):
            if asyncio.current_task() is presence and not parked.is_set():
                parked.set()
                await resume.wait()
                return result
            return await sleep(delay, result)

        with monkeypatch.context() as timing:
            timing.setattr(asyncio, "sleep", hold_presence_interval)
            try:
                await asyncio.wait_for(parked.wait(), 3)
                intervening = BOOT | {"chargePointModel": "Unapproved replacement"}
                changed = await b.rig.bench.call(b.sock, "intervening-boot", "BootNotification", intervening)
                restored = await b.rig.bench.call(b.sock, "restored-boot", "BootNotification", BOOT)
                assert changed[0] == restored[0] == 3 and b.session.journal.get("boot") == BOOT
                assert not presence.done() and gateway._vendor_epoch == epoch
                before = starts(b.rig)
                denied = await b.rig.bench.cloud_call(
                    "start-on-old-boot", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}
                )
                assert denied[0] == 4 and starts(b.rig) == before
            finally:
                resume.set()
        await until(lambda: not runtime.status.vendor_connected, timeout=1)
        assert gateway._vendor_epoch != epoch and gateway.vendor_registration != "Accepted"
        assert b.session.journal.get("trial_phase") == "recovering" and not gateway.controls.ready


# Intent: changed Boot metadata revokes the accepted cloud introduction, while an identical retry retains it.
# Why: temporary zero verification may retain a session only while its actual admitted Boot binding agrees.
@pytest.mark.parametrize("changed", [False, True])
async def test_cloud_registration_remains_bound_to_boot(tmp_path, tls, unused_tcp_port, monkeypatch, changed):
    async with timer_bench(tmp_path, tls, unused_tcp_port) as b:
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
