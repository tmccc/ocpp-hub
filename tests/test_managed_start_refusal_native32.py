"""Commission actual managed capability before exercising the same terminal-state exclusion."""

import json

import pytest
from ocpp_hub_core.prototype.managed import ManagedController

from tests.charge_rig import ChargeRig, positive_flow
from tests.current_rig import commissioned
from tests.handover_helpers import crash
from tests.managed_rig import healthy, introduced, managed_session
from tests.native_peer_failures import InterruptedNativePeer
from tests.test_managed_start_refusal import refuse_unsent_start
from tests.test_ordinary_recovery import close_session
from tests.tls_support import tls as tls

pytestmark = pytest.mark.asyncio


# Intent: preserve a valid lifecycle across this independently injected failure and its control.
# Why: a refused or unsent Start must not strand later supervision, Pause or owner readiness.
@pytest.mark.parametrize("current,action", [(16, None), (16, "start"), (32, None), (32, "start")])
async def test_prepared_native32_outage(tmp_path, tls, unused_tcp_port, monkeypatch, current, action):
    async with commissioned(
        tmp_path, tls, unused_tcp_port, monkeypatch, current, native_peer=InterruptedNativePeer
    ) as b:
        await refuse_unsent_start(b, action)
        local = await positive_flow(b, "daily")
        await healthy(b.controller)
        b.rig.peer.unavailable = True
        try:
            for _ in range(5):
                await b.controller.tick()
            print(
                json.dumps(
                    {
                        "prepared": True,
                        "selected_a": current,
                        "case": action or "normal_control",
                        "effective_limit": b.rig.bench.firmware.effective_limit(),
                        "test_state": b.controller.charging.state().get("state"),
                    }
                )
            )
            assert b.rig.bench.firmware.effective_limit() == 0
        finally:
            b.rig.peer.unavailable = False
            await close_session(b.rig, b.session, b.charger, local)


# Intent: preserve a valid lifecycle across this independently injected failure and its control.
# Why: a refused or unsent Start must not strand later supervision, Pause or owner readiness.
@pytest.mark.parametrize("action", [None, "start"])
async def test_preflight_failure_survives_replacement(tmp_path, tls, unused_tcp_port, monkeypatch, action):
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, 32, native_peer=InterruptedNativePeer) as b:
        await refuse_unsent_start(b, action)
        if action == "start":
            # Reproduce the old persisted failed lifecycle while retaining its actual no-effects management receipt.
            legacy = {key: value for key, value in b.controller.charging.state().items() if key != "start_attempted"}
            b.session.journal.put("managed_test_charge", {**legacy, "state": "failed"})
        await b.controller.charging.close()
        await crash(b.session)
        s = managed_session(b.rig)
        await s.start()
        c = ManagedController(s)
        async with b.rig.charger(s) as sock:
            await introduced(b.rig, s, sock, "retained-failed-preflight")
            await healthy(c)
            fresh = ChargeRig(b.rig, s, c, sock)
            local = await positive_flow(fresh, "daily")
            await healthy(c)
            b.rig.peer.unavailable = True
            try:
                for _ in range(5):
                    await c.tick()
                print(
                    json.dumps(
                        {
                            "replaced": True,
                            "case": action or "normal_control",
                            "effective_limit": b.rig.bench.firmware.effective_limit(),
                            "test_state": c.charging.state().get("state"),
                        }
                    )
                )
                assert b.rig.bench.firmware.effective_limit() == 0
            finally:
                b.rig.peer.unavailable = False
                await close_session(b.rig, s, sock, local)
                await c.charging.close()
                await s.close()
