"""Actual peer and owned-task witnesses for control lifecycle entry and legacy status settlement."""

import asyncio
import uuid

import pytest
from ocpp_hub_core.prototype.recovery_contracts import Prerequisite

from tests.charge_rig import actual_stop, positive_flow, ready
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
        calls = list(b.rig.bench.device_calls)
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
        assert b.rig.bench.device_calls == calls
        assert j.active() is None


# Intent: an old Return receipt can retry its cosmetic completion independently of a verified app charge.
# Why: briefly losing a history write cannot shut down an already working charger connection.
async def test_old_app_return_receipt_write_cannot_interrupt_charging(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        local = await positive_flow(b, "daily")
        await healthy(c)
        intent, identifier = c.requests.intent(), uuid.uuid4().hex
        c.requests.accept(identifier, "app_control", intent.revision)
        before = list(b.rig.bench.device_calls)
        j.db.execute(
            "CREATE TEMP TRIGGER refuse_legacy_return BEFORE UPDATE ON managed_requests "
            "WHEN NEW.action='app_control' AND NEW.state='completed' "
            "BEGIN SELECT RAISE(ABORT,'legacy receipt unavailable'); END"
        )
        try:
            status = await c.tick()
            assert status["state"] == "healthy" and status["power_w"] == 1440
            assert c.requests.receipt(identifier)["state"] == "accepted" and j.active() == local
        finally:
            j.db.execute("DROP TRIGGER refuse_legacy_return")
        assert (await healthy(c))["power_w"] == 1440
        assert c.requests.receipt(identifier)["state"] == "completed"
        assert b.rig.bench.device_calls == before
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        await actual_stop(b, local)


# Intent: the regular poll rejects a stale saved Return and leaves the current Pause in force.
# Why: an older accepted receipt cannot give the proxy permission to send any charger command.
async def test_stale_saved_return_keeps_verified_pause(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        intent, identifier = c.requests.intent(), uuid.uuid4().hex
        c.command(identifier, "app_control", intent.revision)
        # Model a saved Return from before the current Pause revision.
        j.db.execute("UPDATE managed_requests SET revision=? WHERE id=?", (intent.revision - 1, identifier))
        calls = list(b.rig.bench.device_calls)

        assert (await c.tick())["local_hold_verified"]
        assert c.requests.intent() == intent
        assert c.requests.receipt(identifier) == {
            "id": identifier,
            "action": "app_control",
            "state": "failed",
            "result": {"classification": "stale_revision"},
        }
        assert b.rig.bench.firmware.effective_limit() == 0 and b.rig.bench.device_calls == calls


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
                assert observed["state"] == "failed"
                assert observed["result"] == {"classification": "return_not_sent"}
            else:
                assert observed == before
            assert c.requests.intent() == expected
            assert b.rig.bench.received_limits == limits and not starts(b.rig)
            assert b.rig.bench.firmware.effective_limit() == 0
        finally:
            release.set()
            await asyncio.gather(*tasks)


# Intent: a failed first Return write leaves its exact accepted request available on the next poll.
# Why: no physical operation has started, so a temporary SQLite refusal must not stop supervision.
async def test_return_claim_retries_after_transient_storage(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        identifier = uuid.uuid4().hex
        c.command(identifier, "app_control", c.requests.intent().revision)
        j.db.execute(
            "CREATE TEMP TRIGGER refuse_return_claim BEFORE UPDATE ON managed_requests "
            "WHEN NEW.state='running' BEGIN SELECT RAISE(ABORT,'Return claim unavailable'); END"
        )
        before = list(b.rig.bench.device_calls)
        try:
            await c.tick()
            assert c.requests.receipt(identifier)["state"] == "accepted"
            assert b.rig.bench.firmware.effective_limit() == 0
        finally:
            j.db.execute("DROP TRIGGER refuse_return_claim")
        assert not any(frame[2] == "SetChargingProfile" for frame in b.rig.bench.device_calls[len(before) :])
        assert (await healthy(c))["owner_control_enabled"]
        assert c.requests.receipt(identifier)["state"] == "completed" and not starts(b.rig)


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


# Intent: a failed physical Return claim ends that request before sending and retains the verified Pause.
# Why: an unavailable journal write before dispatch does not create an unconfirmed charger operation.
async def test_return_hold_claim_refusal_keeps_verified_pause(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        expected, identifier = c.requests.intent(), uuid.uuid4().hex
        c.command(identifier, "app_control", expected.revision)
        before = list(b.rig.bench.device_calls)
        ticket = j.get("control")["ticket"]
        j.db.execute(
            "CREATE TEMP TRIGGER refuse_release_ticket BEFORE INSERT ON meta "
            "WHEN NEW.key='control' AND json_extract(NEW.value, '$.uncertain')=1 "
            "BEGIN SELECT RAISE(ABORT,'Return hold claim unavailable'); END"
        )
        try:
            result = await c.recovery.restore(expected, return_request=identifier, claim_return=True)
            assert not result.shared
            receipt = c.requests.receipt(identifier)
            assert receipt["state"] == "failed" and receipt["result"]["classification"] == "return_not_sent"
            assert not j.fault.latched() and j.get("control")["ticket"] == ticket
            assert (await healthy(c))["local_hold_verified"]
        finally:
            j.db.execute("DROP TRIGGER refuse_release_ticket")
        assert not any(frame[2] == "SetChargingProfile" for frame in b.rig.bench.device_calls[len(before) :])
        retry = uuid.uuid4().hex
        c.command(retry, "app_control", c.requests.intent().revision)
        assert (await healthy(c))["owner_control_enabled"]
        assert c.requests.receipt(retry)["state"] == "completed"
