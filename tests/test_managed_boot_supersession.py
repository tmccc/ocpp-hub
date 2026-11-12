"""Expected-behaviour regression: ordinary introduction survives a repeated identical Boot."""

import asyncio

import pytest
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.managed import ManagedController

from tests.test_managed_proxy import healthy, managed_bench, managed_session
from tests.test_prototype import BOOT
from tests.test_prototype_tls import tls as tls

pytestmark = pytest.mark.asyncio


# Intent: a newer identical Boot restarts the full restrictive proof without losing normal introduction.
# Why: zero/composite acknowledgements cannot strand the listener or be reused across Boot observations.
@pytest.mark.parametrize("boundary", ["SetChargingProfile", "GetCompositeSchedule"])
@pytest.mark.parametrize("duplicate", [False, True], ids=["normal-control", "identical-boot"])
async def test_introduction_eventually_healthy(tmp_path, tls, unused_tcp_port, monkeypatch, duplicate, boundary):
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            peer_send = charger.send
            captured = []
            zero_dispatched = asyncio.Event()

            # Delay only an actual independent peer reply, leaving all production validation intact.
            async def delay_first_zero_reply(raw):
                answer = decode(raw)
                call = next((frame for frame in rig.bench.device_calls if frame[1] == answer[1]), None)
                if answer[0] == 3 and call is not None and call[2] == boundary and not captured:
                    captured.append(raw)
                    zero_dispatched.set()
                    return
                await peer_send(raw)

            # Change only the independent peer's response timing; keep the production engine intact.
            monkeypatch.setattr(charger, "send", delay_first_zero_reply)
            assert (await rig.bench.call(charger, "first-boot", "BootNotification", BOOT))[0] == 3
            await asyncio.wait_for(zero_dispatched.wait(), 2)
            if duplicate:
                for number in range(2):
                    assert (await rig.bench.call(charger, f"retry-boot-{number}", "BootNotification", BOOT))[0] == 3
            after_boot = len(rig.bench.device_calls)
            await peer_send(captured[0])

            # The repeated boot changes no identity, configuration, physical state or requested current.
            # Both cases should complete the safe zero proof and subsequently release normal 6 A control.
            status = await healthy(controller)
            assert status["state"] == "healthy"
            assert session.gateway.controls.ready
            assert rig.bench.received_limits[0] == 0
            assert rig.bench.received_limits[-1] == 6
            if duplicate:
                fresh = rig.bench.device_calls[after_boot:]
                assert fresh[0][2] == "SetChargingProfile"
                assert fresh[0][3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
                assert fresh[1][2] == "GetCompositeSchedule"
            assert not any(frame[2] == "RemoteStartTransaction" for frame in rig.bench.device_calls)
