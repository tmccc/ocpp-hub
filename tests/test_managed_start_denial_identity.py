"""Stored correlation controls complement real held-reply/closure tests without claiming physical-zero proof."""

import uuid
from types import SimpleNamespace

import pytest
from ocpp_hub_core.prototype.managed_intent import ManagedRequests
from ocpp_hub_core.prototype.managed_start_delivery import (
    complete_denied_start,
    denied_normal_identity,
    denied_start_evidence,
    record_start_denial,
)
from ocpp_hub_core.prototype.managed_stop_delivery import record_stop_ack
from ocpp_hub_core.prototype.store import Journal, fingerprint

from tests.test_prototype import START


# Build a real immutable closed mapped ledger pair and one independently specified owner-request identity.
@pytest.fixture
def denied_identity(tmp_path):
    j = Journal(tmp_path / "identity.db", "DENIED_IDENTITY")
    ManagedRequests(j)
    identifier = uuid.uuid4().hex
    payload = START | {"idTag": "APP_OWNER"}
    local = j.start("start", payload)["transactionId"]
    item = j.next_delivery()
    j.mark_sent(item)
    response = {"transactionId": 9001, "idTagInfo": {"status": "Blocked"}}
    j.acknowledge(item, response, lambda: record_start_denial(j, item, response), allow_denied=True)
    j.stop("stop", {"transactionId": local, "meterStop": 1100, "timestamp": "2026-01-01T00:01:00Z"})
    stopped = j.next_delivery()
    j.mark_sent(stopped)
    j.acknowledge(stopped, {}, lambda: record_stop_ack(j, stopped, {}))
    j.db.execute("INSERT INTO managed_requests VALUES(?,?,1,'uncertain','{}')", (identifier, "start"))
    digest = fingerprint("app_owner")
    j.put(
        "managed_test_charge",
        {
            "id": identifier,
            "kind": "normal",
            "source": "local",
            "revision": 1,
            "start_rpc_accepted": False,
            "start_attempted": True,
            "tag_digest": digest,
            "local_transaction": None,
            "state": "interrupted",
        },
    )
    j.put(
        "local_start_receipt",
        {"request_id": identifier, "stage": "issued", "ticket": uuid.uuid4().hex, "digest": digest, "local": local},
    )
    try:
        yield j, identifier, local
    finally:
        j.close()


# Intent: exactly bound denied closure settles pending requests and preserves all preexisting terminal bytes.
# Why: cloud acknowledgement and fresh caller-verified Pause must not rewrite a failed/completed request's history.
@pytest.mark.parametrize("stage,published", [("issued", False), ("accepted", False), ("accepted", True)])
@pytest.mark.parametrize("result", ["accepted", "running", "uncertain", "completed", "failed"])
def test_denied_closure_request_identity_and_terminal_preservation(denied_identity, stage, published, result):
    j, identifier, local = denied_identity
    j.put("local_start_receipt", {**j.get("local_start_receipt"), "stage": stage})
    j.put(
        "managed_test_charge",
        {**j.get("managed_test_charge"), "start_rpc_accepted": published, "revision": 1 + int(published)},
    )
    j.db.execute("UPDATE managed_requests SET state=?,result=? WHERE id=?", (result, '{"original":true}', identifier))
    before = tuple(j.db.execute("SELECT * FROM managed_requests WHERE id=?", (identifier,)).fetchone())
    proof = denied_start_evidence(SimpleNamespace(session=SimpleNamespace(journal=j)), j.get("managed_control_fault"))
    assert proof is not None and proof["normal"]["id"] == identifier and proof["normal"]["request_revision"] == 1
    with j.atomic():
        complete_denied_start(j, proof, uuid.uuid4().hex)
    after = tuple(j.db.execute("SELECT * FROM managed_requests WHERE id=?", (identifier,)).fetchone())
    if result in ("completed", "failed"):
        assert after == before
    else:
        assert after[3] == "completed"
    assert j.get("local_start_receipt")["stage"] == stage
    assert j.get("managed_test_charge")["start_rpc_accepted"] is published
    assert j.get("managed_test_charge")["local_transaction"] == local


# Intent: unrelated request identity, wrong source tag or an unbound receipt cannot acquire closure authority.
# Why: the paused fault reconciliation excludes only its independently correlated owner request from pending work.
@pytest.mark.parametrize(
    "changed",
    [
        "missing-id",
        "wrong-id",
        "wrong-tag",
        "wrong-revision",
        "unbound",
        "unattempted",
        "wrong-stage",
        "issued-published",
        "wrong-action",
    ],
)
def test_denied_closure_rejects_uncorrelated_owner(denied_identity, changed):
    j, identifier, local = denied_identity
    owner, state = j.get("local_start_receipt"), j.get("managed_test_charge")
    if changed == "missing-id":
        owner.pop("request_id")
    elif changed == "wrong-id":
        owner["request_id"] = uuid.uuid4().hex
    elif changed == "wrong-tag":
        owner["digest"] = state["tag_digest"] = fingerprint("another-tag")
    elif changed == "wrong-revision":
        state["revision"] = 2
    elif changed == "unbound":
        owner["local"] = None
    elif changed == "unattempted":
        state["start_attempted"] = False
    elif changed == "wrong-stage":
        owner["stage"] = "prepared"
    elif changed == "issued-published":
        state["start_rpc_accepted"], state["revision"] = True, 2
    else:
        j.db.execute("UPDATE managed_requests SET action='start_test' WHERE id=?", (identifier,))
    j.put("local_start_receipt", owner)
    j.put("managed_test_charge", state)
    assert denied_normal_identity(j, local) is None
