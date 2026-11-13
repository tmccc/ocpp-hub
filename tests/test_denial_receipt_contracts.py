"""Journal contracts complement actual TLS recovery without claiming independent physical-zero proof."""

import copy
import json
import sqlite3
import uuid
from types import SimpleNamespace

import pytest
from ocpp_hub_core.prototype.managed_intent import ManagedRequests
from ocpp_hub_core.prototype.managed_start_delivery import (
    DENIALS,
    complete_denied_start,
    denied_start_evidence,
    record_start_denial,
)
from ocpp_hub_core.prototype.managed_stop_delivery import WIRE_ACK, record_stop_ack
from ocpp_hub_core.prototype.store import Journal

from tests import conftest as _core_setup  # noqa: F401 - load the HA-free source package.
from tests.test_prototype import START


# Use genuine immutable Journal Start/Stop allocation and acknowledgement callbacks, never invented receipt bodies.
@pytest.fixture
def denial_journal(tmp_path):
    j = Journal(tmp_path / "denial-contract.db", "DENIAL_CONTRACT")
    ManagedRequests(j)
    try:
        yield j
    finally:
        j.close()


# Queue distinct immutable closed pairs before any cloud denial can inhibit later management admission.
def queued_pairs(j, count):
    ids = []
    for number in range(count):
        opened = j.start(
            uuid.uuid4().hex,
            START | {"meterStart": 1000 + number * 100, "timestamp": f"2026-01-01T00:{number * 2:02d}:00Z"},
        )
        local = opened["transactionId"]
        ids.append(local)
        j.stop(
            uuid.uuid4().hex,
            {
                "transactionId": local,
                "meterStop": 1100 + number * 100,
                "timestamp": f"2026-01-01T00:{number * 2 + 1:02d}:00Z",
            },
        )
    return ids


# Deliver each real allocated body through Journal's actual denial/Stop ACK commit boundary.
def acknowledge_pairs(j, count):
    for number in range(count):
        item = j.next_delivery()
        assert item.action == "StartTransaction"
        j.mark_sent(item)
        reply = {"transactionId": 9001 + number, "idTagInfo": {"status": "Blocked"}}
        j.acknowledge(item, reply, lambda: record_start_denial(j, item, reply), allow_denied=True)
        stopped = j.next_delivery()
        assert stopped.action == "StopTransaction"
        j.mark_sent(stopped)
        j.acknowledge(stopped, {}, lambda: record_stop_ack(j, stopped, {}))
    assert j.next_delivery() is None


# This container passes only the exact real Journal expected by the narrow correlation reader.
def evidence(j, fault=None):
    return denied_start_evidence(
        SimpleNamespace(session=SimpleNamespace(journal=j)), j.get("managed_control_fault") if fault is None else fault
    )


# Intent: an exact legacy singleton remains sufficient for its own single denied and closed mapped session.
# Why: deploying per-denial receipts must not strand the already supported one-session historical layout.
def test_legacy_singleton_recovers_only_its_exact_session(denial_journal):
    j = denial_journal
    ids = queued_pairs(j, 1)
    acknowledge_pairs(j, 1)
    records = j.get(DENIALS)
    records[str(ids[0])].pop("wire_ack")
    j.put(DENIALS, records)
    proof = evidence(j)
    assert proof is not None and proof["wire"] == j.get(WIRE_ACK)
    with j.atomic():
        complete_denied_start(j, proof, "legacy-pause")
    assert j.get(DENIALS)[str(ids[0])]["completed"]["pause"] == "legacy-pause"


# Intent: two distinct legacy same-fault denials cannot borrow one global Stop acknowledgement.
# Why: a closed transaction and empty queue alone are not evidence that its own Stop was acknowledged.
def test_two_denials_cannot_share_legacy_singleton(denial_journal):
    j = denial_journal
    queued_pairs(j, 2)
    acknowledge_pairs(j, 2)
    records = j.get(DENIALS)
    for denial in records.values():
        denial.pop("wire_ack")
    j.put(DENIALS, records)
    before = copy.deepcopy(records), j.get("managed_control_fault")
    assert evidence(j) is None
    assert (j.get(DENIALS), j.get("managed_control_fault")) == before


# Intent: an unrelated null-fault historical denial remains byte-identical when a valid current denial completes.
# Why: legacy incomplete bookkeeping must neither acquire today's fault authority nor be falsely acknowledged.
def test_historical_null_fault_denial_is_preserved(denial_journal):
    j = denial_journal
    ids = queued_pairs(j, 2)
    acknowledge_pairs(j, 2)
    records = j.get(DENIALS)
    records[str(ids[1])]["fault"] = None
    records[str(ids[1])].pop("wire_ack")
    historical = copy.deepcopy(records[str(ids[1])])
    j.put(DENIALS, records)
    proof = evidence(j)
    assert proof is not None and proof["denial"]["local"] == ids[0]
    with j.atomic():
        complete_denied_start(j, proof, "current-pause")
    assert j.get(DENIALS)[str(ids[1])] == historical
    assert j.get(DENIALS)[str(ids[0])]["completed"]["pause"] == "current-pause"


# Intent: a real denial observed under an unrelated preservation fault cannot manufacture authority to clear it.
# Why: a new cloud lifecycle's closure does not explain why another commissioned-resource invariant failed.
def test_new_denial_preserves_foreign_fault(denial_journal):
    j = denial_journal
    ids = queued_pairs(j, 1)
    foreign = {"classification": "native_authority_or_preservation_failed", "at_epoch": 1000}
    j.put("managed_control_fault", foreign)
    acknowledge_pairs(j, 1)
    denial = j.get(DENIALS)[str(ids[0])]
    assert denial["fault"] is None and denial["wire_ack"]["fault"] is None
    assert evidence(j, foreign) is None and j.get("managed_control_fault") == foreign
    assert denial["completed"] is None


# Intent: a group's own receipt must match its local/cloud/start/Stop identity and exact current fault.
# Why: even another genuinely acknowledged session cannot substitute for the member whose closure is being consumed.
@pytest.mark.parametrize(
    "field", ["local_transaction_id", "vendor_transaction_id", "start_digest", "stop_digest", "fault"]
)
def test_denial_group_rejects_wrong_own_wire_identity(denial_journal, field):
    j = denial_journal
    ids = queued_pairs(j, 2)
    acknowledge_pairs(j, 2)
    records = j.get(DENIALS)
    wire = records[str(ids[1])]["wire_ack"]
    wire[field] = (
        999999 if field.endswith("_id") else {"classification": "another-fault"} if field == "fault" else "0" * 64
    )
    j.put(DENIALS, records)
    assert evidence(j) is None
    assert all(row["completed"] is None for row in j.get(DENIALS).values())


# Intent: per-denial wire storage, actual dequeue and the enclosing COMMIT succeed or roll back together.
# Why: a response passed to the real ACK writer cannot become durable evidence when the correlated event remains.
@pytest.mark.parametrize("boundary", ["receipt-update", "dequeue", "commit"])
def test_denial_stop_receipt_and_dequeue_are_atomic(denial_journal, boundary):
    j = denial_journal
    ids = queued_pairs(j, 1)
    item = j.next_delivery()
    j.mark_sent(item)
    reply = {"transactionId": 9001, "idTagInfo": {"status": "Blocked"}}
    j.acknowledge(item, reply, lambda: record_start_denial(j, item, reply), allow_denied=True)
    stopped = j.next_delivery()
    j.mark_sent(stopped)
    before = (
        copy.deepcopy(j.get(DENIALS)),
        [tuple(row) for row in j.db.execute("SELECT * FROM outbox")],
        j.get(WIRE_ACK),
    )
    owns_wire = f"NEW.key='{DENIALS}' AND json_extract(NEW.value,'$.\"{ids[0]}\".wire_ack') IS NOT NULL"
    if boundary == "dequeue":
        trigger = "BEFORE DELETE ON outbox WHEN OLD.action='StopTransaction'"
        effect = "SELECT RAISE(ABORT,'synthetic dequeue refusal')"
    elif boundary == "receipt-update":
        trigger, effect = f"BEFORE INSERT ON meta WHEN {owns_wire}", "SELECT RAISE(ABORT,'synthetic receipt refusal')"
    else:
        j.db.execute("CREATE TABLE ack_parent(id INTEGER PRIMARY KEY)")
        j.db.execute("CREATE TABLE ack_child(id REFERENCES ack_parent(id) DEFERRABLE INITIALLY DEFERRED)")
        trigger, effect = f"AFTER INSERT ON meta WHEN {owns_wire}", "INSERT INTO ack_child VALUES(1)"
    j.db.execute(f"CREATE TRIGGER fail_denial_ack {trigger} BEGIN {effect}; END")
    try:
        with pytest.raises(sqlite3.IntegrityError):
            j.acknowledge(stopped, {}, lambda: record_stop_ack(j, stopped, {}))
        assert not j.db.in_transaction
        assert (j.get(DENIALS), [tuple(row) for row in j.db.execute("SELECT * FROM outbox")], j.get(WIRE_ACK)) == before
    finally:
        j.db.execute("DROP TRIGGER fail_denial_ack")
    j.acknowledge(stopped, {}, lambda: record_stop_ack(j, stopped, {}))
    proof = evidence(j)
    assert proof is not None and proof["wire"]["delivery_token"] == stopped.token
    assert proof["denial"]["local"] == ids[0] and j.next_delivery() is None
    assert (
        json.loads(j.db.execute("SELECT stop FROM transactions WHERE local=?", (ids[0],)).fetchone()[0])
        == stopped.payload
    )
