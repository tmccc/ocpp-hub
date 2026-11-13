"""A live exact local lease settles atomically while separate unknown cloud delivery remains retained."""

import copy
import sqlite3
import uuid

import pytest
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests import test_managed_proxy as mp
from tests.cloud_start_helpers import StartReplyBench
from tests.test_managed_stop_reply_recovery import running
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests_model.test_schedule_sequences import listener_port, tls_for_case
from tests_model.test_schedule_sequences import model_tls as model_tls


# Intent: each real SQLite refusal retains the exact local lease/request and all cloud evidence until one commit.
# Why: releasing in-memory authority before failed durable settlement can make a closed owner permanently unrecoverable.
@pytest.mark.parametrize("boundary", ["request", "metadata", "commit"])
async def test_live_bound_owner_retirement_keeps_cloud_debt_atomic(tmp_path, model_tls, monkeypatch, boundary):
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    async with mp.managed_bench(tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch) as rig:
        async with running(rig, "atomic-unmapped-owner", owner_tag="APP_OWNER") as (s, c, sock):
            await mp.healthy(c)
            identifier = uuid.uuid4().hex
            c.command(identifier, "start", c.requests.intent().revision)
            await c.tests.settle()
            lease = c.tests.lease
            assert lease is not None and lease.identifier == identifier and lease.normal
            rig.bench.start_replies = ["silent"]
            tag = mp.starts(rig)[-1][3]["idTag"]
            response = await rig.bench.call(sock, "actual-late-start", "StartTransaction", START | {"idTag": tag})
            assert response[0] == 3
            local = response[2]["transactionId"]
            rig.peer.plan_session, rig.peer.permission_enabled = (local, 987654), False
            await until(lambda: s.journal.next_delivery().state == "ambiguous", timeout=12)
            assert s.journal.vendor_id(local) is None
            assert (
                await rig.bench.call(
                    sock,
                    "actual-stop",
                    "StopTransaction",
                    {
                        "transactionId": local,
                        "timestamp": START["timestamp"],
                        "meterStop": 1100,
                    },
                )
            )[0] == 3
            j = s.journal
            if boundary == "commit":
                j.db.execute("CREATE TABLE owner_parent(id INTEGER PRIMARY KEY)")
                j.db.execute("CREATE TABLE owner_child(id REFERENCES owner_parent(id) DEFERRABLE INITIALLY DEFERRED)")
                trigger = f"AFTER UPDATE ON managed_requests WHEN NEW.id='{identifier}' AND NEW.state='completed'"
                effect = "INSERT INTO owner_child VALUES(1)"
            else:
                trigger = (
                    f"BEFORE UPDATE ON managed_requests WHEN NEW.id='{identifier}' AND NEW.state='completed'"
                    if boundary == "request"
                    else "BEFORE INSERT ON meta WHEN NEW.key='managed_test_charge' "
                    "AND json_extract(NEW.value,'$.state')='completed'"
                )
                effect = "SELECT RAISE(ABORT,'synthetic owner retirement refusal')"
            j.db.execute(f"CREATE TRIGGER fail_owner_retire {trigger} BEGIN {effect}; END")
            state, receipt = copy.deepcopy(c.tests.state()), c.requests.receipt(identifier)
            retained, fault = (
                [tuple(row) for row in j.db.execute("SELECT * FROM outbox")],
                j.get("managed_control_fault"),
            )
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            assert c.pause_task is not None
            await c.pause_task
            assert c.tests.lease is lease and c.tests.state() == state and c.requests.receipt(identifier) == receipt
            assert not j.db.in_transaction and rig.bench.firmware.effective_limit() == 0
            native = await c.native.read()
            c.require_paused(native)
            with pytest.raises(sqlite3.IntegrityError):
                await c.tick()
            assert c.tests.lease is lease and c.tests.state() == state and c.requests.receipt(identifier) == receipt
            assert [tuple(row) for row in j.db.execute("SELECT * FROM outbox")] == retained
            assert j.get("managed_control_fault") == fault and j.vendor_id(local) is None
            j.db.execute("DROP TRIGGER fail_owner_retire")
            await c.tick()
            assert (
                c.tests.lease is None
                and c.tests.state()["state"] == c.requests.receipt(identifier)["state"] == "completed"
            )
            assert c.tests.state()["start_rpc_accepted"] is state["start_rpc_accepted"] is True
            assert j.get("local_start_receipt")["stage"] == "accepted"
            assert [tuple(row) for row in j.db.execute("SELECT * FROM outbox")] == retained
            assert j.get("managed_control_fault") == fault and j.vendor_id(local) is None
            with pytest.raises(RecoveryRequiredError, match="latched fault"):
                c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
