"""Gate independent real loopback peers across temporary recovery zero confirmation."""

import asyncio
from dataclasses import replace

import pytest

from tests.native_schedule_support import timer_bench
from tests.test_prototype import BOOT
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Intent: an intervening Boot change revokes the old registration even if the original body returns before polling.
# Why: comparing only the current body can erase a real A-to-B-to-A introduction revocation.
async def test_intervening_boot_change_revokes_cloud(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        gateway, runtime = b.session.gateway, b.session.runtime
        epoch = gateway._vendor_epoch
        settings, permitted = runtime.settings, runtime.vendor_session_enabled
        runtime.settings = replace(settings, pump_interval=0.25)
        observed = asyncio.Event()
        polls = 0

        # Observe the real policy check without changing its decision, then use its configured sleeping interval.
        def observe_presence():
            nonlocal polls
            polls += 1
            observed.set()
            return permitted()

        runtime.vendor_session_enabled = observe_presence
        try:
            await asyncio.wait_for(observed.wait(), 1)
            before = polls
            changed = await b.rig.bench.call(
                b.sock, "intervening-boot", "BootNotification", BOOT | {"chargePointModel": "Unapproved replacement"}
            )
            restored = await b.rig.bench.call(b.sock, "restored-boot", "BootNotification", BOOT)
            assert polls == before, "the two genuine reports must fall between presence polls"
            assert changed[0] == restored[0] == 3 and b.session.journal.get("boot") == BOOT
            await until(lambda: not runtime.status.vendor_connected, timeout=1)
            assert gateway._vendor_epoch != epoch and gateway.vendor_registration != "Accepted"
            assert b.session.journal.get("trial_phase") == "recovering" and not gateway.controls.ready
        finally:
            runtime.settings, runtime.vendor_session_enabled = settings, permitted


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
