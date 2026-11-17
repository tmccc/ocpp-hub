"""Fast stored-state contracts for the Start owner; physical/native proof remains in the peer tests."""

import sqlite3
import uuid

import pytest
from ocpp_hub_core.prototype.managed_intent import ManagedRequests
from ocpp_hub_core.prototype.managed_test_charge import TestLease as StartLease
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError, fingerprint

from tests.test_prototype import START


# Supply the real process-local identity without restoring it from persisted metadata.
def lease(identifier, revision=4):
    return StartLease(
        identifier=identifier,
        source="local",
        revision=revision,
        epoch=1,
        control_revision=1,
        identity=fingerprint("LIFECYCLE_CONTRACT"),
        spec="a" * 64,
        stop_epoch=0,
        deadline=100,
        hard_deadline=100,
        first_mutation=0,
        normal=True,
    )


# Build an actual closed local ledger while leaving its unmapped cloud Start and Stop queued.
@pytest.fixture
def closed_owner(tmp_path):
    j = Journal(tmp_path / "lifecycle.db", "LIFECYCLE_CONTRACT")
    ManagedRequests(j)
    identifier = uuid.uuid4().hex
    owner = lease(identifier)
    local = j.start("actual-start", START | {"idTag": "APP_OWNER"})["transactionId"]
    j.stop("actual-stop", {"transactionId": local, "meterStop": 1100, "timestamp": "2026-01-01T00:01:00Z"})
    digest = fingerprint("app_owner")
    j.db.execute("INSERT INTO managed_requests VALUES(?,'start',3,'accepted',?)", (identifier, '{"original": true}'))
    j.put(
        "managed_test_charge",
        {
            "id": identifier,
            "source": "local",
            "kind": "normal",
            "revision": 4,
            "state": "awaiting_power",
            "start_attempted": True,
            "start_rpc_accepted": True,
            "tag_digest": digest,
            "local_transaction": local,
        },
    )
    j.put(
        "local_start_receipt",
        {"request_id": identifier, "ticket": uuid.uuid4().hex, "digest": digest, "stage": "accepted", "local": local},
    )
    j.put("control", {"ticket": uuid.uuid4().hex, "inhibit": True, "start_pending": False})
    j.starts.set_lease(owner)
    try:
        yield j, owner, local
    finally:
        j.close()


# Intent: the canonical receipt may fill a missing projection, but contradictory stored copies revoke correlation.
# Why: selecting canonical fields must not silently wash out a corrupted tag, local ID or attempted marker.
@pytest.mark.parametrize(
    "field,value,stage,published",
    [
        ("tag_digest", None, "accepted", True),
        ("tag_digest", fingerprint("other"), "accepted", True),
        ("local_transaction", 9000, "accepted", True),
        ("start_attempted", False, "issued", False),
        ("start_attempted", None, "issued", False),
        ("start_attempted", False, "accepted", False),
        ("start_attempted", None, "accepted", False),
        ("start_rpc_accepted", True, "issued", True),
    ],
)
def test_normal_identity_refuses_conflicting_projections(closed_owner, field, value, stage, published):
    j, owner, local = closed_owner
    state = j.get("managed_test_charge")
    state.update({"revision": 3 + int(published), "start_rpc_accepted": published, field: value})
    j.put("managed_test_charge", state)
    j.put("local_start_receipt", {**j.get("local_start_receipt"), "stage": stage})
    record = j.starts.normal()
    assert record is not None and record.receipt.local == local
    assert j.starts.identity(local, strict_request=True, strict_unpublished=True) is None
    assert j.get("managed_test_charge") == state and j.starts.lease is owner


# Intent: current issued/accepted effects remain attributable before managed publication and despite projection lag.
# Why: failed publication or a newer Pause must not turn a genuinely bound and closed lifecycle into unowned debt.
@pytest.mark.parametrize("stage", ["issued", "accepted"])
def test_unpublished_closed_owner_keeps_truthful_identity(closed_owner, stage):
    j, _, local = closed_owner
    j.put(
        "managed_test_charge",
        {**j.get("managed_test_charge"), "revision": 3, "start_rpc_accepted": False, "local_transaction": None},
    )
    j.put("local_start_receipt", {**j.get("local_start_receipt"), "stage": stage})
    record = j.starts.closed(strict_request=True, strict_unpublished=True)
    assert record is not None and record.local == local and record.reported_local is None
    assert record.published is False and record.receipt.stage == stage
    assert j.starts.identity(local, strict_request=True, strict_unpublished=True)["request_revision"] == 3


# Intent: explicit published legacy identity remains readable, while an unresolved legacy request needs stricter proof.
# Why: upgrades must accept supported historical journals without assigning a receipt to an unrelated new request.
@pytest.mark.parametrize("receipt_present", [False, True])
@pytest.mark.parametrize("request_state", ["uncertain", "completed"])
def test_published_legacy_binding_is_explicit(closed_owner, receipt_present, request_state):
    j, _, local = closed_owner
    receipt = j.get("local_start_receipt")
    receipt.pop("request_id")
    if receipt_present:
        j.put("local_start_receipt", receipt)
    else:
        j.db.execute("DELETE FROM meta WHERE key='local_start_receipt'")
    j.db.execute("UPDATE managed_requests SET state=?", (request_state,))
    record = j.starts.closed()
    assert record is not None and record.local == local and record.published is True
    assert (j.starts.identity(local, strict_request=True) is not None) is (request_state == "completed")


# Intent: admitting a new request retains the old settled receipt without adopting its local session.
# Why: cancellation before preparation must preserve the earlier receipt and must not complete the new request from it.
def test_admission_keeps_prior_receipt_historical(closed_owner):
    j, _, local = closed_owner
    old_receipt = j.get("local_start_receipt")
    newcomer = lease(uuid.uuid4().hex, revision=7)
    j.starts.admit(newcomer, "start", 60, 120)
    record = j.starts.normal()
    assert j.get("local_start_receipt") == old_receipt
    assert record is not None and record.identifier == newcomer.identifier
    assert record.effect is None and record.local is None and record.session is None
    assert j.starts.identity(local) is None and j.starts.closed() is None
    assert j.starts.lease is newcomer


# Intent: a real recorded Accepted remains separate from publication under a newer restrictive claim.
# Why: the pending bit is stored evidence, and an old reply must not restore the previous Start authority.
def test_late_accepted_preserves_new_pause_and_unpublished_state(closed_owner):
    j, _, _ = closed_owner
    newcomer = lease(uuid.uuid4().hex, revision=7)
    j.starts.admit(newcomer, "start", 60, 120)
    start_ticket, pause_ticket = uuid.uuid4().hex, uuid.uuid4().hex
    j.put("control", {"ticket": start_ticket, "inhibit": True, "start_pending": False})
    with j.atomic():
        j.starts.prepare(start_ticket, fingerprint("app_owner"), newcomer.identifier)
        j.starts.prepared_metadata(newcomer, fingerprint("app_owner"))
    j.starts.armed(start_ticket)
    j.starts.record(start_ticket, "issued")
    paused = {**j.get("control"), "ticket": pause_ticket, "inhibit": True, "owner": "local"}
    j.put("control", paused)
    j.starts.record(start_ticket, "accepted")
    record = j.starts.normal()
    assert record is not None and record.receipt.stage == "accepted" and record.published is False
    assert record.pending and j.get("control") == paused
    assert newcomer.revision == 7 and j.starts.lease is newcomer


# Intent: local closure may finish one request while retaining its exact independent cloud delivery obligations.
# Why: an unmapped cloud Start and its Stop must not disappear merely because the local operation completed.
def test_local_completion_preserves_cloud_outbox(closed_owner):
    j, owner, local = closed_owner
    outbox = [tuple(row) for row in j.db.execute("SELECT * FROM outbox ORDER BY seq")]
    receipt = j.get("local_start_receipt")
    assert j.starts.finish_closed(owner)
    request = j.db.execute("SELECT state FROM managed_requests WHERE id=?", (owner.identifier,)).fetchone()
    assert request[0] == j.get("managed_test_charge")["state"] == "completed"
    assert j.starts.lease is None and j.vendor_id(local) is None
    assert [tuple(row) for row in j.db.execute("SELECT * FROM outbox ORDER BY seq")] == outbox
    assert j.get("local_start_receipt") == receipt


# Intent: no frozen aggregate can settle after its metadata, request, receipt, lifecycle or pending bit changes.
# Why: a stored candidate is evidence of a preimage, not continuing authority over a replacement record.
@pytest.mark.parametrize("changed", ["metadata", "request", "receipt", "transaction", "pending"])
def test_closed_candidate_requires_current_preimage(closed_owner, changed):
    j, owner, _ = closed_owner
    record = j.starts.closed()
    assert record is not None
    if changed == "metadata":
        j.put("managed_test_charge", {**j.get("managed_test_charge"), "charging": True})
    elif changed == "request":
        j.db.execute("UPDATE managed_requests SET result=' {\"changed\": true} '")
    elif changed == "receipt":
        j.put("local_start_receipt", {**j.get("local_start_receipt"), "verified_pause_revision": 10})
    elif changed == "transaction":
        j.db.execute("UPDATE transactions SET vendor=4001")
    else:
        j.put("control", {**j.get("control"), "start_pending": True})
    before = j.get("managed_test_charge")
    with pytest.raises(RecoveryRequiredError, match="changed"):
        with j.atomic():
            j.starts.complete_closed_zero(record)
    assert j.get("managed_test_charge") == before and j.starts.lease is owner


# Intent: request, metadata and deferred COMMIT refusals leave the lease and all persisted evidence intact.
# Why: memory release before a successful durable completion would make an exact closed owner unrecoverable.
@pytest.mark.parametrize("boundary", ["request", "metadata", "commit"])
def test_completion_refusal_retains_lease_and_exact_rows(closed_owner, boundary):
    j, owner, _ = closed_owner
    if boundary == "commit":
        j.db.execute("CREATE TABLE lifecycle_parent(id INTEGER PRIMARY KEY)")
        j.db.execute("CREATE TABLE lifecycle_child(id REFERENCES lifecycle_parent(id) DEFERRABLE INITIALLY DEFERRED)")
        trigger = "AFTER UPDATE ON managed_requests WHEN NEW.state='completed'"
        effect = "INSERT INTO lifecycle_child VALUES(1)"
    else:
        trigger = (
            "BEFORE UPDATE ON managed_requests WHEN NEW.state='completed'"
            if boundary == "request"
            else "BEFORE INSERT ON meta WHEN NEW.key='managed_test_charge' "
            "AND json_extract(NEW.value,'$.state')='completed'"
        )
        effect = "SELECT RAISE(ABORT,'synthetic lifecycle completion refusal')"
    j.db.execute(f"CREATE TRIGGER refuse_completion {trigger} BEGIN {effect}; END")
    metadata = j.db.execute("SELECT value FROM meta WHERE key='managed_test_charge'").fetchone()[0]
    request = tuple(j.db.execute("SELECT * FROM managed_requests").fetchone())
    receipt = j.get("local_start_receipt")
    outbox = [tuple(row) for row in j.db.execute("SELECT * FROM outbox ORDER BY seq")]
    with pytest.raises(sqlite3.IntegrityError):
        j.starts.finish_closed(owner)
    assert not j.db.in_transaction and j.starts.lease is owner
    assert j.db.execute("SELECT value FROM meta WHERE key='managed_test_charge'").fetchone()[0] == metadata
    assert tuple(j.db.execute("SELECT * FROM managed_requests").fetchone()) == request
    assert j.get("local_start_receipt") == receipt
    assert [tuple(row) for row in j.db.execute("SELECT * FROM outbox ORDER BY seq")] == outbox
    j.db.execute("DROP TRIGGER refuse_completion")
    assert j.starts.finish_closed(owner) and j.starts.lease is None


# Intent: rejected projection conflicts cannot return through a weaker normal observation path.
# Why: a later same-tag receipt is not identity when legacy metadata never named that local session.
@pytest.mark.parametrize("conflict", ["local", "tag", "legacy"])
def test_observation_cannot_bypass_refused_identity(closed_owner, conflict):
    j, owner, local = closed_owner
    state = j.get("managed_test_charge") | {"state": "running"}
    state["local_transaction"] = None if conflict == "legacy" else local + 1 if conflict == "local" else local
    if conflict == "tag":
        state["tag_digest"] = fingerprint("another")
    if conflict == "legacy":
        receipt = j.get("local_start_receipt")
        receipt.pop("request_id")
        j.put("local_start_receipt", receipt)
    j.put("managed_test_charge", state)
    before = tuple(j.db.execute("SELECT * FROM managed_requests").fetchone())
    assert j.starts.closed() is None
    # No native fields may be consulted after stored identity has already been refused.
    assert not j.starts.observe_normal(object(), allow_open=True)
    assert j.get("managed_test_charge") == state and j.starts.lease is owner
    assert tuple(j.db.execute("SELECT * FROM managed_requests").fetchone()) == before


# Intent: strict denial identity needs an explicit unpublished marker, while ordinary closed recovery keeps its lag.
# Why: missing legacy publication cannot be upgraded into the stronger denial proof by an attempted marker alone.
def test_missing_publication_does_not_satisfy_strict_denial(closed_owner):
    j, _, local = closed_owner
    state = j.get("managed_test_charge")
    state.pop("start_rpc_accepted")
    state["revision"] = 3
    j.put("managed_test_charge", state)
    assert j.starts.closed() is not None
    assert j.starts.identity(local, strict_unpublished=True) is None


# Intent: a genuine Rejected reply under a newer ticket retains the existing stored pending obligation.
# Why: consolidating the pending writer must not silently derive it from wire stage and erase historical debt.
def test_rejected_after_new_claim_preserves_pending(closed_owner):
    j, _, _ = closed_owner
    start_ticket, pause_ticket = uuid.uuid4().hex, uuid.uuid4().hex
    with j.atomic():
        j.starts.prepare(start_ticket, fingerprint("app_owner"), uuid.uuid4().hex)
    paused = {"ticket": pause_ticket, "inhibit": True, "start_pending": True}
    j.put("control", paused)
    j.starts.record(start_ticket, "rejected")
    assert j.get("control") == paused and j.get("local_start_receipt")["stage"] == "rejected"


# Intent: memory-releasing entry points cannot borrow an outer transaction that may later refuse COMMIT.
# Why: returning success with the lease already gone would orphan durable accepted work after a rollback.
@pytest.mark.parametrize("entry", ["finish", "observe"])
def test_memory_release_requires_owned_commit(closed_owner, entry):
    j, owner, _ = closed_owner
    before = j.get("managed_test_charge")
    with j.atomic():
        with pytest.raises(RecoveryRequiredError, match="own its COMMIT"):
            if entry == "finish":
                j.starts.finish_closed(owner)
            else:
                j.starts.observe_normal(object())
        assert j.starts.lease is owner and j.get("managed_test_charge") == before
    assert j.starts.finish_closed(owner) and j.starts.lease is None
