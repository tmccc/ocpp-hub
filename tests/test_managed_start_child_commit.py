"""Real Start replies and SQLite failures probe the child-owned authority handoff."""

import asyncio
import copy
import json
import sqlite3
import sys
import uuid

import pytest
from ocpp_hub_core.prototype.local_control import LocalControl

from tests.charge_rig import actual_stop, owner_rig, positive_flow
from tests.managed_rig import healthy, starts
from tests.recovery_barriers import completed_control
from tests.runtime_rig import until
from tests.tls_support import tls as tls

pytestmark = pytest.mark.asyncio


# Intent: one owned COMMIT publishes Start authority, while statement/COMMIT refusal preserves reply debt and zero.
# Why: actual Accepted evidence cannot disappear or grant app authority when the separate handoff fails storage.
@pytest.mark.parametrize("boundary", ["success", "statement", "commit", "accepted_commit"])
async def test_start_child_publishes_all_authority_or_retains_accepted_debt(
    tmp_path, tls, unused_tcp_port, monkeypatch, boundary
):
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch, startup=10) as b:
        c, g, j = b.controller, b.session.gateway, b.session.journal
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        intent, released = c.requests.intent(), c.released_epoch
        plans = copy.deepcopy(b.rig.bench.firmware.plans)
        configuration = copy.deepcopy(b.rig.peer.configuration)
        identifier, observations, wire_replies = uuid.uuid4().hex, [], []
        original_send = b.charger.send

        # Observe unchanged real wire bytes only after the independent socket has forwarded them.
        async def sent(raw):
            await original_send(raw)
            frame = json.loads(raw)
            if frame[0] in (3, 4):
                wire_replies.append(copy.deepcopy(frame))

        monkeypatch.setattr(b.charger, "send", sent)

        # Inspect only actual child/SQL execution; this callback performs no engine work or journal mutation.
        def observed():
            frame = sys._getframe(1)
            owned = None
            try:
                while frame is not None:
                    if frame.f_code is LocalControl._start_owned.__code__:
                        owned = frame.f_locals.get("attempt")
                    frame = frame.f_back
            finally:
                del frame
            observations.append(
                {
                    "attempt": owned,
                    "lease": c.charging.lease,
                    "transaction": j.db.in_transaction,
                    "intent": c.requests.intent(),
                    "receipt": copy.deepcopy(j.get("local_start_receipt")),
                    "limit": b.rig.bench.firmware.effective_limit(),
                }
            )
            return 0

        j.db.create_function("start_publication_observed", 0, observed)
        if boundary in ("commit", "accepted_commit"):
            j.db.execute("CREATE TABLE start_publication_parent(id INTEGER PRIMARY KEY)")
            j.db.execute(
                "CREATE TABLE start_publication_child("
                "id REFERENCES start_publication_parent(id) DEFERRABLE INITIALLY DEFERRED)"
            )
        selected = (
            "NEW.key='local_start_receipt' AND json_extract(NEW.value,'$.stage')='accepted'"
            if boundary == "accepted_commit"
            else "NEW.key='managed_test_charge' AND json_extract(NEW.value,'$.state')='awaiting_power'"
        )
        effect = (
            "SELECT RAISE(ABORT,'independent Start publication refused');"
            if boundary == "statement"
            else "INSERT INTO start_publication_child VALUES(123);"
            if boundary in ("commit", "accepted_commit")
            else ""
        )
        j.db.execute(
            "CREATE TEMP TRIGGER start_publication_cut BEFORE INSERT ON meta WHEN "
            + selected
            + " BEGIN SELECT start_publication_observed(); "
            + effect
            + " END"
        )
        try:
            c.command(identifier, "start", intent.revision)
            await asyncio.wait_for(c.charging.settle(), 5)
            assert len(observations) == 1 and observations[0]["transaction"]
            witness = observations[0]
            attempt, lease = witness["attempt"], witness["lease"]
            assert attempt is not None and lease is not None and attempt.finished
            result = attempt.result()
            assert result.remote_start_dispatched and result.answer[2] == {"status": "Accepted"}
            remote = starts(b.rig)
            assert len(remote) == 1 and [3, remote[0][1], {"status": "Accepted"}] in wire_replies
            assert witness["limit"] == 6 and not j.db.in_transaction
            receipt, control = j.get("local_start_receipt"), j.get("control")
            assert receipt["request_id"] == identifier and receipt["ticket"] == control["ticket"]
            assert receipt["local"] is None and j.active() is None and control["start_pending"]
            if boundary == "success":
                assert result.failure is None and result.publication is not None and not result.zero_verified
                assert c.requests.intent().mode == lease.mode == "app"
                assert c.requests.intent().revision == lease.revision == intent.revision + 1
                assert control["owner"] == "shared" and not control["inhibit"] and not control["uncertain"]
                assert c.charging.state()["state"] == "awaiting_power" and c.charging.state()["start_rpc_accepted"]
                assert c.released_epoch == g._charger_epoch
                assert c.requests.receipt(identifier)["state"] == "accepted"
            else:
                assert isinstance(result.failure, sqlite3.IntegrityError)
                assert result.publication is None and result.zero_verified
                assert c.requests.intent() == intent and lease.mode == intent.mode and lease.revision == intent.revision
                assert c.released_epoch == released
                assert control["owner"] == "local" and control["inhibit"] and control["uncertain"]
                assert c.charging.state()["state"] == "stopping" and not c.charging.state().get("start_rpc_accepted")
                assert c.requests.receipt(identifier)["state"] == "uncertain"
                assert receipt["stage"] == ("issued" if boundary == "accepted_commit" else "accepted")
                assert b.rig.bench.firmware.effective_limit() == 0
                if boundary in ("commit", "accepted_commit"):
                    assert result.failure.sqlite_errorcode == sqlite3.SQLITE_CONSTRAINT_FOREIGNKEY
                    assert j.db.execute("SELECT COUNT(*) FROM start_publication_child").fetchone()[0] == 0
            assert not any(frame[2] == "RemoteStopTransaction" for frame in b.rig.bench.device_calls)
        finally:
            j.db.execute("DROP TRIGGER start_publication_cut")
            j.db.create_function("start_publication_observed", 0, None)

        if boundary == "accepted_commit":
            await c.tick()
            assert j.get("local_start_receipt")["stage"] == "accepted"
            assert c.requests.intent() == intent and len(starts(b.rig)) == 1

        # A new explicit Pause owns zero; the original actual Start later supplies its own genuine lifecycle.
        pause = uuid.uuid4().hex
        c.command(pause, "pause", c.requests.intent().revision)
        if c.pause_task is not None:
            await c.pause_task
        assert b.rig.bench.firmware.effective_limit() == 0
        local = await positive_flow(b, call_id=uuid.uuid4().hex)
        assert j.get("local_start_receipt")["local"] == local and len(starts(b.rig)) == 1
        await c.tick()
        await until(lambda: any(frame[2] == "RemoteStopTransaction" for frame in b.rig.bench.device_calls))
        await actual_stop(b, local)
        assert c.requests.receipt(pause)["state"] == "completed"
        returning, revision = uuid.uuid4().hex, c.requests.intent().revision
        c.command(returning, "app_control", revision)
        await completed_control(b, returning, mode="app", revision=revision + 1, ceiling=6)
        assert len(starts(b.rig)) == 1
        assert b.rig.bench.firmware.plans == plans and b.rig.peer.configuration == configuration
