"""A Pause accepted before first Boot retains safe recovery across the first owned zero RPC."""

import asyncio
import copy
import uuid
from contextlib import AsyncExitStack

import pytest
from ocpp_hub_core.frames import build_call_error
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.managed import ManagedController

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench, two_later_sessions
from tests.recovery_barriers import completed_control, returned_to_app
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_test_charge import ChargeRig
from tests.test_paused_introduction_recovery import retire
from tests.test_prototype import BOOT
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Intent: first-Boot zero failure retries from fresh native evidence, then its exact Pause/Return and later use work.
# Why: the first owned write happens before introduction markers; a lost or refused ACK cannot strand it forever.
@pytest.mark.parametrize(
    ("fault", "early_return"),
    [(fault, early) for fault in ("timeout", "call-error", "rejected-unapplied") for early in (False, True)]
    + [("superseding-pause", False)],
)
async def test_first_pause_recovers_across_pre_marker_zero_failure(
    tmp_path, tls, unused_tcp_port, monkeypatch, fault, early_return
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    async with mp.managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        assert isinstance(rig.bench, StartReplyBench)
        session = mp.managed_session(rig)
        await session.start()
        c, g, j = ManagedController(session), session.gateway, session.journal
        original_config, plans = copy.deepcopy(rig.peer.configuration), copy.deepcopy(rig.bench.firmware.plans)
        pause = uuid.uuid4().hex
        accepted = c.command(pause, "pause", c.requests.intent().revision)
        assert accepted["state"] == "accepted"
        await c.pause_task
        assert j.get("trial_binding") is None and j.get("trial_starting") is True
        assert j.get("control")["uncertain"] is True and c.requests.intent().mode == "paused"
        pause_revision = 1
        return_id = uuid.uuid4().hex if early_return else None
        if return_id is not None:
            assert c.command(return_id, "app_control", c.requests.intent().revision)["state"] == "accepted"
        rig.route_to_trial()
        original_config.update(rig.spec.trial_route)
        failed = []
        releases = [asyncio.Event(), asyncio.Event()]
        entered = asyncio.Queue()
        firmware_answer = rig.bench.firmware.answer
        refused = []

        # A genuine refusal leaves the device unchanged; timeout/CALLERROR variants retain actual applied zero.
        def answer(action, payload):
            zero = action == "SetChargingProfile" and (
                payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
            )
            if fault == "rejected-unapplied" and zero and len(refused) < 2:
                refused.append(copy.deepcopy(payload))
                return {"status": "Rejected"}
            return firmware_answer(action, payload)

        monkeypatch.setattr(rig.bench.firmware, "answer", answer)
        async with AsyncExitStack() as stack:
            charger = await stack.enter_async_context(rig.charger(session))
            stack.push_async_callback(retire, session, c)
            normal_send = charger.send

            # Hold each real TLS answer so the test observes both boundaries before allowing its actual failure.
            async def send(raw):
                frame = decode(raw)
                call = next((row for row in rig.bench.device_calls if row[1] == frame[1]), None)
                zero = (
                    call is not None
                    and call[2] == "SetChargingProfile"
                    and (call[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0)
                )
                if frame[0] == 3 and zero and len(failed) < 2:
                    attempt = len(failed)
                    failed.append(copy.deepcopy(call))
                    entered.put_nowait(attempt)
                    await releases[attempt].wait()
                    if fault == "timeout":
                        return
                    if fault == "call-error":
                        await normal_send(build_call_error(frame[1], "InternalError", "actual initial zero refusal"))
                        return
                await normal_send(raw)

            monkeypatch.setattr(charger, "send", send)
            assert (await rig.bench.call(charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
            epoch, boot = g._charger_epoch, g.boot_observation
            try:
                assert await asyncio.wait_for(entered.get(), 3) == 0
                assert c.native.introduced_epoch != epoch and j.get("trial_starting") is True
                assert not g.controls.ready and c.requests.receipt(pause)["state"] == "accepted"
                assert j.get("trial_binding") is not None and j.get("managed_control_fault") is None
                assert len(failed) == 1 and not mp.starts(rig)
                if fault == "superseding-pause":
                    previous = pause
                    pause = uuid.uuid4().hex
                    assert c.command(pause, "pause", c.requests.intent().revision)["state"] == "accepted"
                    assert c.requests.receipt(previous)["state"] == "failed"
                    assert c.requests.receipt(previous)["result"]["superseded_by"] == pause
                    pause_revision = 2
                releases[0].set()
                assert await asyncio.wait_for(entered.get(), 8) == 1
                # Initial retries retain trial_starting; only a fully confirmed zero can leave this stage.
                assert c.native.introduced_epoch != epoch and j.get("trial_starting") is True
                assert not g.controls.ready and c.requests.receipt(pause)["state"] == "accepted"
                assert j.get("managed_control_fault") is None and not mp.starts(rig)
                if fault == "rejected-unapplied":
                    assert rig.bench.firmware.profiles == {} and len(refused) == 2
                else:
                    assert rig.bench.firmware.effective_limit() == 0
                releases[1].set()
                await until(lambda: g.controls.ready and not g.charger._pending, timeout=8)
                b = ChargeRig(rig, session, c, charger)
                # Complete the exact accepted Return; paused health alone cannot satisfy this contract.
                if return_id is not None:
                    status = await completed_control(b, return_id, mode="app", revision=2, ceiling=6)
                else:
                    status = await completed_control(b, pause, mode="paused", revision=pause_revision, ceiling=0)
                    assert status["local_hold_verified"] and rig.bench.firmware.effective_limit() == 0
                    await returned_to_app(b, 6)
                assert c.requests.receipt(pause)["state"] == "completed"
                assert g._charger_epoch == epoch and g.boot_observation == boot
                assert j.get("managed_control_fault") is None and len(failed) == 2 and not mp.starts(rig)
                assert len([row for row in rig.bench.device_calls if row[2] == "RemoteStopTransaction"]) == 0
                await two_later_sessions(b, selected=6, original_starts=0, first_minute=20)
                assert rig.peer.configuration == original_config and rig.bench.firmware.plans == plans
                assert j.active() is None and not mp.starts(rig)
            finally:
                for release in releases:
                    release.set()
