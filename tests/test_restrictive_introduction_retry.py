"""Whole restrictive introduction sequences recover on real TLS without replaying introduction or Stop."""

import asyncio
import uuid

import pytest
from ocpp_hub_core.prototype.gateway import decode

from tests.test_managed_introduction_retry import selected_ready
from tests.test_managed_proxy import starts
from tests.test_prototype import BOOT
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Intent: timeout during capability, zero ACK or composite is retried under the same admitted current Boot.
# Why: one lost response cannot strand startup, and retrying a zero must not repeat an introduction's Stop logic.
@pytest.mark.parametrize("current", [6, 32])
@pytest.mark.parametrize("stage", ["capability", "zero", "composite"])
async def test_restrictive_introduction_retries_whole_zero_sequence(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, stage
):
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        rig, session, c = b.rig, b.session, b.controller
        await b.charger.close()
        await until(lambda: not session.runtime.status.charger_connected)
        first_profile = len(rig.bench.received_limits)
        reads = c.native.read_sequence
        async with rig.charger(session) as charger:
            ordinary_send = charger.send
            dropped = []

            # Suppress only one actual independent fake-peer reply after its ordinary physical application.
            async def send(raw):
                answer = decode(raw)
                call = next((f for f in rig.bench.device_calls if f[1] == answer[1]), None)
                target = call is not None and (
                    stage == "capability" and call[2] == "GetConfiguration"
                    or stage == "zero" and call[2] == "SetChargingProfile"
                    or stage == "composite" and call[2] == "GetCompositeSchedule"
                )
                if answer[0] == 3 and target and not dropped:
                    dropped.append(call[2])
                    return
                await ordinary_send(raw)

            monkeypatch.setattr(charger, "send", send)
            assert (await rig.bench.call(charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
            await until(
                lambda: session.gateway.controls.ready and session.gateway.vendor_registration == "Accepted", 12
            )
            assert dropped
            assert rig.bench.firmware.effective_limit() == 0
            assert c.native.read_sequence == reads + 1
            assert not session.journal.get("managed_control_fault")
            assert not any(frame[2] == "RemoteStopTransaction" for frame in rig.bench.device_calls)
            assert not starts(rig)
            async with asyncio.timeout(5):
                while (await c.tick())["state"] != "healthy":
                    await asyncio.sleep(0.01)
            assert rig.bench.firmware.effective_limit() == current
            assert rig.bench.received_limits[first_profile:][0] == 0
            assert set(rig.bench.received_limits[first_profile:]) <= {0, current}
