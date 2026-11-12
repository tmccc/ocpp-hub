"""An initial zero-profile refusal must not manufacture an unresolved Start lifecycle."""

import json
import uuid

import pytest
from ocpp_hub_core.prototype.managed import ManagedController

from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import crash
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_proxy import healthy, introduced, managed_session, starts
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_test_charge import ChargeRig, actual_stop, positive_flow, ready
from tests.test_ordinary_recovery import close_session
from tests.test_prototype_tls import tls as tls

pytestmark = pytest.mark.asyncio


# Intent: preserve a valid lifecycle across this independently injected failure and its control.
# Why: a refused or unsent Start must not strand later supervision, Pause or owner readiness.
@pytest.mark.parametrize("action,refuse", [("start", False), ("start_test", True), ("start", True)])
async def test_unissued_start_has_no_lifecycle_debt(tmp_path, tls, unused_tcp_port, monkeypatch, action, refuse):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        actual = b.rig.bench.firmware.answer
        failed = False

        # Keep this synthetic peer or lifecycle helper independent of the engine under test.
        def answer(name, payload):
            nonlocal failed
            if refuse and not failed and name == "SetChargingProfile":
                limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit == 0:
                    failed = True
                    return {"status": "Rejected"}
            return actual(name, payload)

        monkeypatch.setattr(b.rig.bench.firmware, "answer", answer)
        identifier = uuid.uuid4().hex
        b.controller.command(identifier, action, b.controller.requests.intent().revision)
        await b.controller.tests.settle()
        if not refuse:
            local = await positive_flow(b)
            await healthy(b.controller)
            b.controller.command(uuid.uuid4().hex, "pause", b.controller.requests.intent().revision)
            await actual_stop(b, local)
        else:
            assert failed and not starts(b.rig)
            if b.controller.pause_task is not None:
                await b.controller.pause_task
            status = await healthy(b.controller)
            assert status["local_hold_verified"]
        # A genuinely new owner Pause and Return fully reconcile physical zero, then app authority.
        b.controller.command(uuid.uuid4().hex, "pause", b.controller.requests.intent().revision)
        await healthy(b.controller)
        returning = uuid.uuid4().hex
        b.controller.command(returning, "app_control", b.controller.requests.intent().revision)
        status = await healthy(b.controller)
        assert b.controller.requests.receipt(returning)["state"] == "completed"
        assert status["owner_control_enabled"] and b.rig.bench.firmware.effective_limit() == 6
        print(
            json.dumps(
                {
                    "action": action,
                    "initial_zero_rejected": refuse,
                    "state": b.controller.tests.state()["state"],
                    "local_start_receipt": b.session.journal.get("local_start_receipt"),
                    "normal_start_ready": status["normal_start_ready"],
                    "physical_starts": len(starts(b.rig)),
                }
            )
        )
        assert status["normal_start_ready"], (
            "An unissued Start has no lifecycle report to await after verified Pause/Return"
        )


# Intent: a crash during zero-only cleanup cannot suppress supervision of a later daily session.
# Why: replacement rewrites transient lifecycle state, but explicit proof that Start was never attempted survives.
async def test_zero_refusal_cleanup_survives_replacement(tmp_path, tls, unused_tcp_port, monkeypatch):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, 32) as b:
        actual, refused = b.rig.bench.firmware.answer, False

        # Reject the first physical zero command at the independent charger, before any Start RPC.
        def answer(name, payload):
            nonlocal refused
            if not refused and name == "SetChargingProfile":
                limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit == 0:
                    refused = True
                    b.rig.peer.unavailable = True
                    return {"status": "Rejected"}
            return actual(name, payload)

        monkeypatch.setattr(b.rig.bench.firmware, "answer", answer)
        b.controller.command(uuid.uuid4().hex, "start", b.controller.requests.intent().revision)
        await b.controller.tests.settle()
        if b.controller.pause_task is not None:
            await b.controller.pause_task
        assert refused and not starts(b.rig)
        assert b.controller.tests.state()["state"] == "stopping"
        assert b.controller.tests.state()["start_attempted"] is False
        await b.controller.tests.close()
        await crash(b.session)
        b.rig.peer.unavailable = False
        s = managed_session(b.rig)
        await s.start()
        c = ManagedController(s)
        async with b.rig.charger(s) as charger:
            try:
                await introduced(b.rig, s, charger, "unissued-cleanup-replacement")
                await healthy(c)
                assert c.tests.state()["state"] == "completed"
                c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
                await healthy(c)
                c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
                await healthy(c)
                fresh = ChargeRig(b.rig, s, c, charger)
                local = await positive_flow(fresh, "daily")
                await healthy(c)
                b.rig.peer.unavailable = True
                try:
                    for _ in range(5):
                        await c.tick()
                    assert b.rig.bench.firmware.effective_limit() == 0
                finally:
                    b.rig.peer.unavailable = False
                    await close_session(b.rig, s, charger, local)
            finally:
                await c.tests.close()
                await s.close()
