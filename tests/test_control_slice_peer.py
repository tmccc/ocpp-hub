"""An independent real-peer witness feeds boundary facts into the effect-free candidate core."""

import asyncio
import copy
import json
import uuid

import pytest
from ocpp_hub_core.prototype.connection_scope import current_scope
from ocpp_hub_core.prototype.recovery_contracts import AuthorityStamp

from tests.recovery_barriers import completed_control
from tests.test_managed_normal_start import owner_rig
from tests.test_managed_proxy import healthy, starts
from tests.test_managed_test_charge import actual_stop, positive_flow
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests_model.control_slice import step
from tests_model.control_slice_state import (
    AuthorityChanged,
    CloudSettled,
    LifecycleBound,
    LifecycleClosed,
    OutcomeCommitted,
    PortRetired,
    SliceState,
    StartAnswered,
    StartDebt,
    StartDispatched,
    StartIdentity,
    StartOutcome,
    StartPrepared,
)


# Observe exact live scope and ownership; the shadow never supplies these facts back to the engine.
def observed_authority(c):
    g, j = c.session.gateway, c.session.journal
    scope = current_scope(g)
    assert scope is not None
    return AuthorityStamp(c.requests.intent(), scope, g.controls._revision, j.get("control")["ticket"])


# Intent: an actual dispatched Start's genuine Accepted arrives after a new Pause has committed.
# Why: the new owner must retain zero and exact authority while old acceptance/lifecycle/cloud debts survive.
@pytest.mark.asyncio
async def test_real_late_accepted_after_new_pause_keeps_authority_and_debt(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch, startup=20) as b:
        c, g, j, rig = b.controller, b.session.gateway, b.session.journal, b.rig
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        initial = c.requests.intent()
        plans, configuration = copy.deepcopy(rig.bench.firmware.plans), copy.deepcopy(rig.peer.configuration)
        entered, release = asyncio.Event(), asyncio.Event()
        old_hook, old_timeout, original_send = rig.bench.before_reply, g.charger.timeout, b.charger.send
        replies = []
        g.charger.timeout = 10

        # Hold the independent peer's real reply after its actual firmware has received the request.
        async def hold_reply(action, payload):
            if action == "RemoteStartTransaction":
                entered.set()
                async with asyncio.timeout(8):
                    await release.wait()
            if old_hook is not None:
                await old_hook(action, payload)

        # Forward the original reply bytes unchanged, then retain their independent socket witness.
        async def observe_reply(raw):
            await original_send(raw)
            frame = json.loads(raw)
            if frame[0] in (3, 4):
                replies.append(frame)

        rig.bench.before_reply = hold_reply
        monkeypatch.setattr(b.charger, "send", observe_reply)
        identifier, pause = uuid.uuid4().hex, uuid.uuid4().hex
        try:
            assert c.command(identifier, "start", initial.revision)["state"] == "accepted"
            await asyncio.wait_for(entered.wait(), 4)
            remote = starts(rig)
            assert len(remote) == 1 and rig.bench.firmware.effective_limit() == 6
            wire_id = remote[0][1]
            assert not any(frame[1] == wire_id for frame in replies)
            issued = copy.deepcopy(j.get("local_start_receipt"))
            assert issued["request_id"] == identifier and issued["stage"] == "issued"
            before = observed_authority(c)
            shadow = SliceState(before)
            identity = StartIdentity(wire_id, identifier, issued["digest"], before)
            shadow = step(shadow, StartPrepared(identity)).state
            shadow = step(shadow, StartDispatched(wire_id)).state

            # This ordinary owner command returns only after intent, hold claim and request COMMIT together.
            receipt = c.command(pause, "pause", initial.revision)
            assert receipt["state"] == "accepted" and not j.db.in_transaction
            newer = observed_authority(c)
            assert newer.intent.mode == "paused" and newer.intent.revision == initial.revision + 1
            assert newer.ticket != issued["ticket"] and j.get("managed_owner_pause")["ticket"] == newer.ticket
            assert not any(frame[1] == wire_id for frame in replies)
            shadow = step(shadow, AuthorityChanged(before, newer)).state

            release.set()
            await asyncio.wait_for(c.tests.settle(), 5)
            if c.pause_task is not None:
                await asyncio.wait_for(asyncio.shield(c.pause_task), 4)
            assert [3, wire_id, {"status": "Accepted"}] in replies
            actual = j.get("local_start_receipt")
            assert actual["request_id"] == identifier and actual["stage"] == "accepted"
            assert actual["ticket"] == issued["ticket"] and actual["local"] is None
            assert observed_authority(c) == newer and j.get("control")["start_pending"]
            assert c.requests.receipt(identifier)["state"] == "uncertain"
            assert rig.bench.firmware.effective_limit() == 0 and j.active() is None
            assert len(starts(rig)) == 1
            assert not any(frame[2] == "RemoteStopTransaction" for frame in rig.bench.device_calls)
            shadow = step(shadow, StartAnswered(wire_id, StartOutcome.ACCEPTED)).state
            shadow = step(shadow, OutcomeCommitted(wire_id, StartOutcome.ACCEPTED)).state
            assert not g.controls._operations and not g.charger._pending
            shadow = step(shadow, PortRetired(wire_id)).state
            assert shadow.authority == newer and shadow.start(wire_id).publication is None
            assert shadow.start(wire_id).debts == frozenset(StartDebt) and shadow.ports_retired

            # A genuine later lifecycle binds the old request; only that exact local session may receive Stop.
            local = await positive_flow(b, call_id=uuid.uuid4().hex)
            assert j.get("local_start_receipt")["local"] == local
            shadow = step(shadow, LifecycleBound(wire_id, local)).state
            await c.tick()
            await until(lambda: any(frame[2] == "RemoteStopTransaction" for frame in rig.bench.device_calls))
            stops = [frame for frame in rig.bench.device_calls if frame[2] == "RemoteStopTransaction"]
            assert stops and all(frame[3]["transactionId"] == local for frame in stops)
            status = await actual_stop(b, local)
            assert status["local_hold_verified"] and c.requests.receipt(pause)["state"] == "completed"
            recovered = c.requests.receipt(identifier)
            assert recovered["state"] == "completed"
            assert recovered["result"] == {"local_session_verified": True, "charging": False, "timed": False}
            assert j.active() is None and j.next_delivery() is None
            assert j.db.execute("SELECT stop FROM transactions WHERE local=?", (local,)).fetchone()[0] is not None
            shadow = step(shadow, LifecycleClosed(wire_id, local, True, True)).state
            assert shadow.start(wire_id).debts == {StartDebt.CLOUD_START, StartDebt.CLOUD_STOP}
            shadow = step(shadow, CloudSettled(wire_id, local, "StartTransaction")).state
            assert shadow.start(wire_id).debts == {StartDebt.CLOUD_STOP}
            shadow = step(shadow, CloudSettled(wire_id, local, "StopTransaction")).state
            assert not shadow.start(wire_id).debts and shadow.authority == newer

            returning, revision = uuid.uuid4().hex, c.requests.intent().revision
            c.command(returning, "app_control", revision)
            await completed_control(b, returning, mode="app", revision=revision + 1, ceiling=6)
            assert len(starts(rig)) == 1
            assert rig.bench.firmware.plans == plans and rig.peer.configuration == configuration
        finally:
            release.set()
            rig.bench.before_reply, g.charger.timeout = old_hook, old_timeout
            if c.tests.worker is not None:
                await asyncio.gather(c.tests.worker, return_exceptions=True)
            if c.pause_task is not None:
                await asyncio.gather(c.pause_task, return_exceptions=True)
