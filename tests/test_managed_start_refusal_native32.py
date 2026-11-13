"""Commission actual managed capability before exercising the same terminal-state exclusion."""

import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_current_prepare import prepare_current_capability

from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import crash
from tests.test_managed_current import select
from tests.test_managed_current_prepare import native_writer, paused_ready
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_proxy import healthy, introduced, managed_session
from tests.test_managed_start_refusal import refuse_unsent_start
from tests.test_managed_test_charge import ChargeRig, positive_flow
from tests.test_ordinary_recovery import close_session
from tests.test_prototype_tls import tls as tls

pytestmark = pytest.mark.asyncio


# Keep this synthetic peer or lifecycle helper independent of the engine under test.
@asynccontextmanager
async def commissioned(tmp_path, tls, port, monkeypatch, current, *, owner_tag=None):
    async with paused_ready(tmp_path, tls, port, monkeypatch) as prior:
        writes = []
        await prepare_current_capability(
            prior.session.journal,
            prior.session.book,
            prior.rig.spec,
            prior.rig.prepared.observer.observe_handover,
            native_writer(prior, writes),
        )
        assert writes == [{"AmpLmt": "32"}]
        await prior.controller.tests.close()
        await crash(prior.session)
        session = managed_session(prior.rig)
        if owner_tag is not None:
            session.gateway.policy = replace(
                session.gateway.policy, allowed_tags=frozenset({owner_tag, *session.gateway.policy.allowed_tags})
            )
            session.gateway.controls.policy = session.gateway.policy
        await session.start()
        c = ManagedController(session)
        async with prior.rig.charger(session) as charger:
            await introduced(prior.rig, session, charger, "commissioned32")
            await healthy(c)
            assert session.gateway.policy.max_current_a == 32
            c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
            await healthy(c)
            b = ChargeRig(prior.rig, session, c, charger)
            await select(b, current)
            assert (await healthy(c))["station_ceiling_a"] == current
            try:
                yield b
            finally:
                await c.tests.close()
                if c.pause_task is not None:
                    await asyncio.gather(c.pause_task, return_exceptions=True)
                await session.close()


# Intent: preserve a valid lifecycle across this independently injected failure and its control.
# Why: a refused or unsent Start must not strand later supervision, Pause or owner readiness.
@pytest.mark.parametrize("current,action", [(16, None), (16, "start"), (32, None), (32, "start")])
async def test_prepared_native32_outage(tmp_path, tls, unused_tcp_port, monkeypatch, current, action):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
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
                        "test_state": b.controller.tests.state().get("state"),
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
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, 32) as b:
        await refuse_unsent_start(b, action)
        if action == "start":
            # Reproduce the old persisted failed lifecycle while retaining its actual no-effects management receipt.
            legacy = {key: value for key, value in b.controller.tests.state().items() if key != "start_attempted"}
            b.session.journal.put("managed_test_charge", {**legacy, "state": "failed"})
        await b.controller.tests.close()
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
                            "test_state": c.tests.state().get("state"),
                        }
                    )
                )
                assert b.rig.bench.firmware.effective_limit() == 0
            finally:
                b.rig.peer.unavailable = False
                await close_session(b.rig, s, sock, local)
                await c.tests.close()
                await s.close()
