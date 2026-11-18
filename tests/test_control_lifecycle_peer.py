"""Actual peer and owned-task witnesses for control lifecycle entry and legacy status settlement."""

import asyncio
import uuid

import pytest
from ocpp_hub_core.prototype.recovery_contracts import Prerequisite

from tests.charge_rig import ready
from tests.managed_rig import healthy, starts
from tests.test_managed_current_tick_race import OneNativeResponseGate
from tests.tls_support import tls as tls


# Intent: the actual healthy-status path completes only the old accepted app-mode Return, without physical work.
# Why: current atomic handoffs do not exercise this retained legacy bookkeeping branch or its negative controls.
@pytest.mark.parametrize("state", ["accepted", "running", "uncertain", "completed", "failed"])
async def test_legacy_app_return_uses_actual_healthy_status(tmp_path, tls, unused_tcp_port, monkeypatch, state):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        expected, identifier = c.requests.intent(), uuid.uuid4().hex
        c.requests.accept(identifier, "app_control", expected.revision)
        c.requests.finish(identifier, state, {"legacy": True})
        before = c.requests.receipt(identifier)
        limits, start_calls = list(b.rig.bench.received_limits), starts(b.rig)
        status = await c.tick()
        assert status["state"] == "healthy" and status["owner_control_enabled"]
        assert c.requests.intent() == expected
        if state == "accepted":
            assert c.requests.receipt(identifier) == {
                "id": identifier,
                "action": "app_control",
                "state": "completed",
                "result": {"app_control_enabled": True, "charging_started": False},
            }
        else:
            assert c.requests.receipt(identifier) == before
        assert b.rig.bench.received_limits == limits and starts(b.rig) == start_calls
        assert j.active() is None


# Intent: stopping waits without beginning Return, while a full real completion pool retains its old uncertainty.
# Why: moving the running claim must preserve both prelaunch exits and the queue-refusal boundary.
@pytest.mark.parametrize("boundary", ["stopping", "queue_full"])
async def test_return_prelaunch_disposition_uses_actual_owner(tmp_path, tls, unused_tcp_port, monkeypatch, boundary):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, controls = b.controller, b.session.gateway.controls
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        expected, identifier = c.requests.intent(), uuid.uuid4().hex
        c.command(identifier, "app_control", expected.revision)
        before = c.requests.receipt(identifier)
        limits, release = list(b.rig.bench.received_limits), asyncio.Event()
        tasks = []

        # Hold real registered completion work; do not replace the engine's launch or result handling.
        async def occupied():
            await release.wait()

        try:
            if boundary == "queue_full":
                tasks = [controls.launch_completion(occupied()) for _ in range(32)]
            else:
                c.request_stop()
            await c.return_requested(identifier, expected.revision)
            observed = c.requests.receipt(identifier)
            if boundary == "queue_full":
                assert observed["state"] == "uncertain"
                assert observed["result"] == {"classification": "return_not_confirmed"}
            else:
                assert observed == before
            assert c.requests.intent() == expected
            assert b.rig.bench.received_limits == limits and not starts(b.rig)
            assert b.rig.bench.firmware.effective_limit() == 0
        finally:
            release.set()
            await asyncio.gather(*tasks)


# Intent: a Return behind an actual suspended automatic restore stays accepted without launching another attempt.
# Why: busy completion is a scheduling prerequisite, distinct from an attempted positive release's uncertainty.
async def test_return_waits_for_actual_existing_restore(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch, native_peer=OneNativeResponseGate) as b:
        c, peer = b.controller, b.rig.peer
        peer.arm = True
        automatic = asyncio.create_task(c.recovery.restore())
        try:
            await asyncio.wait_for(peer.entered.wait(), 3)
            assert c.recovery.pending()
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            expected, identifier = c.requests.intent(), uuid.uuid4().hex
            c.command(identifier, "app_control", expected.revision)
            before, limits = c.requests.receipt(identifier), list(b.rig.bench.received_limits)
            progress = await c.recovery.restore(expected, return_request=identifier)
            assert not progress.shared and progress.pending.reason is Prerequisite.OWNED_OPERATION
            assert c.requests.receipt(identifier) == before and before["state"] == "accepted"
            assert c.requests.intent() == expected and b.rig.bench.received_limits == limits
            assert not starts(b.rig)
        finally:
            peer.release.set()
            await asyncio.wait_for(asyncio.gather(automatic, return_exceptions=True), 5)
        assert c.requests.intent().mode == "paused"
