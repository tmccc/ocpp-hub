"""Durable denied-Start mapping and completed physical zero are distinct real transport boundaries."""

import asyncio
import uuid

import pytest
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench, two_later_sessions
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_stop_reply_recovery import pause
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests_model.test_schedule_sequences import listener_port, tls_for_case
from tests_model.test_schedule_sequences import model_tls as model_tls


class HeldZeroInput:
    """Only the independent charger's application of one actually received zero command is withheld."""

    # Ordinary local reply handling and every later device command retain the same actual TLS socket.
    def __init__(self, socket, bench):
        self.socket, self.bench = socket, bench

    # Gate before firmware.answer applies the incoming profile, so saved mapping alone still observes prior output.
    async def __aiter__(self):
        async for raw in self.socket:
            frame = decode(raw)
            if self.bench.hold_zero and frame[0] == 2 and frame[2] == "SetChargingProfile":
                profile = frame[3]["csChargingProfiles"]
                if profile["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0:
                    self.bench.hold_zero = False
                    self.bench.zero_received.set()
                    await self.bench.release_zero.wait()
            yield raw

    # Send genuine replies unchanged on the original transport.
    async def send(self, raw):
        await self.socket.send(raw)


class DenialBoundaryBench(StartReplyBench):
    """Cloud and physical charger have independently controlled actual reply/application gates."""

    # Gates remain disarmed throughout ordinary commissioning and initial healthy positive control.
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hold_zero = False
        self.zero_received, self.release_zero = asyncio.Event(), asyncio.Event()

    # The usual independent charger implements every wire command after this input-boundary wrapper.
    async def read_charger(self, charger):
        await super().read_charger(HeldZeroInput(charger, self))


# Intent: denied mapping may precede real station zero, but autonomous zero completion needs no controller tick.
# Why: waiting only for queue drain either races the physical hold or mistakes an unfinished command for proof.
@pytest.mark.parametrize("selected", [6, 16, 32])
@pytest.mark.parametrize("denial", ["Blocked", "Expired", "Invalid", "ConcurrentTx"])
async def test_denied_mapping_precedes_independently_confirmed_zero(tmp_path, model_tls, monkeypatch, selected, denial):
    monkeypatch.setattr(mp, "OwnerBench", DenialBoundaryBench)
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with commissioned(tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch, selected) as b:
        rig, s, c, sock = b.rig, b.session, b.controller, b.charger
        rig.bench.start_reply_gate = asyncio.Event()
        rig.bench.start_replies = [denial]
        reply = await rig.bench.call(sock, uuid.uuid4().hex, "StartTransaction", START | {"idTag": "NATIVE_TIMER"})
        assert reply[0] == 3
        local = reply[2]["transactionId"]
        rig.peer.plan_session, rig.peer.permission_enabled = (local, 9), True
        rig.bench.cp, rig.bench.connection, rig.bench.firmware.requested_current = "C", "Charging", selected
        rig.bench.hold_zero = True
        rig.bench.start_reply_gate.set()
        try:
            await asyncio.wait_for(rig.bench.zero_received.wait(), 3)
            assert s.journal.vendor_id(local) == rig.bench.backend_starts[0]["answer"]["transactionId"]
            assert s.journal.next_delivery() is None
            fault = s.journal.get("managed_control_fault")
            assert fault and s.journal.get("control")["inhibit"] and s.gateway.controls.desired() == 0
            assert rig.bench.firmware.effective_limit() == selected and rig.bench.firmware.current == selected
            assert not s.gateway.controls.ready
            with pytest.raises(RecoveryRequiredError):
                c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
        finally:
            rig.bench.release_zero.set()
        await until(
            lambda: (
                rig.bench.firmware.effective_limit() == 0
                and s.gateway.controls.ready
                and s.gateway.controls.desired() == 0
            )
        )
        assert s.journal.get("managed_control_fault") == fault and not mp.starts(rig)
        rig.peer.permission_enabled, rig.bench.firmware.requested_current = False, 0
        assert (
            await rig.bench.call(
                sock,
                uuid.uuid4().hex,
                "StopTransaction",
                {
                    "transactionId": local,
                    "meterStop": 1100,
                    "timestamp": "2026-01-01T00:01:00Z",
                },
            )
        )[0] == 3
        await until(lambda: s.journal.next_delivery() is None)
        await pause(c)
        assert (await mp.healthy(c))["local_hold_verified"] and s.journal.get("managed_control_fault") is None
        await two_later_sessions(b, selected=selected, original_starts=0)
