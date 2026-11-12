"""Offline regression: a harmless preflight refusal must leave ordinary recovery available."""

import json
import uuid

import pytest

from tests import test_native_bounded_session as native_bench
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_proxy import healthy, introduced, starts
from tests.test_managed_test_charge import positive_flow, ready
from tests.test_ordinary_recovery import close_session
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls

pytestmark = pytest.mark.asyncio


# Keep this synthetic peer or lifecycle helper independent of the engine under test.
async def refuse_unsent_start(b, action):
    if action is None:
        return
    # Native peer changes after last health view and before the real Start worker.
    b.rig.bench.cp, b.rig.bench.connection = "A", "Unplug"
    before = list(b.rig.bench.device_calls)
    identifier = uuid.uuid4().hex
    b.controller.command(identifier, action, b.controller.requests.intent().revision)
    await b.controller.tests.settle()
    assert b.controller.requests.receipt(identifier)["state"] == "failed"
    assert b.rig.bench.device_calls == before and not starts(b.rig)
    assert b.controller.requests.intent().mode == "app"
    b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
    await healthy(b.controller)


# Intent: preserve a valid lifecycle across this independently injected failure and its control.
# Why: a refused or unsent Start must not strand later supervision, Pause or owner readiness.
@pytest.mark.parametrize("action", [None, "start", "start_test"])
async def test_outage_after_unsent_refusal(tmp_path, tls, unused_tcp_port, monkeypatch, action):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await refuse_unsent_start(b, action)
        local = await positive_flow(b, "daily")
        await healthy(b.controller)
        b.rig.peer.unavailable = True
        try:
            statuses = [await b.controller.tick() for _ in range(5)]
            print(
                json.dumps(
                    {
                        "case": action or "normal_control",
                        "ordinary": b.controller.ordinary.ordinary(),
                        "test_state": b.controller.tests.state().get("state"),
                        "status": statuses[-1]["state"],
                        "native_unavailable": b.controller.native_unavailable,
                        "effective_limit": b.rig.bench.firmware.effective_limit(),
                    }
                )
            )
            assert b.rig.bench.firmware.effective_limit() == 0, "Native outage must retain its bounded zero policy"
        finally:
            b.rig.peer.unavailable = False
            await close_session(b.rig, b.session, b.charger, local)


# Intent: preserve a valid lifecycle across this independently injected failure and its control.
# Why: a refused or unsent Start must not strand later supervision, Pause or owner readiness.
@pytest.mark.parametrize("action", [None, "start"])
async def test_reconnect_after_unsent_refusal(tmp_path, tls, unused_tcp_port, monkeypatch, action):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await refuse_unsent_start(b, action)
        local = await positive_flow(b, "daily")
        await healthy(b.controller)
        await b.charger.close()
        await until(lambda: not b.session.runtime.status.charger_connected)
        async with b.rig.charger(b.session) as charger:
            await introduced(b.rig, b.session, charger, "post-refusal-boot")
            statuses = [await b.controller.tick() for _ in range(5)]
            try:
                print(
                    json.dumps(
                        {
                            "case": action or "normal_control",
                            "ordinary": b.controller.ordinary.ordinary(),
                            "test_state": b.controller.tests.state().get("state"),
                            "status": statuses[-1]["state"],
                            "effective_limit": b.rig.bench.firmware.effective_limit(),
                        }
                    )
                )
                assert b.rig.bench.firmware.effective_limit() == 6, (
                    "Known ordinary daily must recover its selected current"
                )
            finally:
                await close_session(b.rig, b.session, charger, local)
