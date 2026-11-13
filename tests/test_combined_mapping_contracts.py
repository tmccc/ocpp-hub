"""Narrow command association and transaction ownership contracts supplement actual-tool TLS histories."""

import copy
from types import SimpleNamespace

import pytest
from ocpp_hub_core.prototype.managed_combined_start_recovery import KIND, combined_candidate
from ocpp_hub_core.prototype.managed_debt_recovery import debt_snapshot
from ocpp_hub_core.prototype.managed_intent import ManagedRequests
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError, fingerprint

from tests import conftest as _core_setup  # noqa: F401 - load the HA-free source package.
from tests.test_prototype import START


# Create real journal command/Start/Stop preimages; this fixture claims neither native zero nor operator authority.
@pytest.fixture
def combined_journal(tmp_path):
    j = Journal(tmp_path / "combined-contract.db", "COMBINED_CONTRACT")
    ManagedRequests(j)
    command = {"idTag": "CLOUD_OWNER", "connectorId": 1}
    token = j.mutation_intent("vendor", "RemoteStartTransaction", command)
    j.mutation_finished(token, False)
    j.put(
        "uncertain_vendor_command",
        {"mutation": token, "action": "RemoteStartTransaction", "digest": fingerprint(command)},
    )
    local = j.start("actual-start", START | {"idTag": "CLOUD_OWNER"})["transactionId"]
    j.stop("actual-stop", {"transactionId": local, "meterStop": 1100, "timestamp": "2026-01-01T00:01:00Z"})
    j.db.execute("UPDATE outbox SET state='ambiguous' WHERE action='StartTransaction'")
    j.put("managed_control_fault", {"classification": "owner_control_or_delivery_uncertain", "at_epoch": 1000})
    try:
        yield j, token, local
    finally:
        j.close()


# Capture the actual journal projection for the pure association validator, without bypassing the full tool's gates.
def snapshot(j):
    return debt_snapshot(j, SimpleNamespace(digest="a" * 64), KIND)


# Intent: the inner ACK method cannot commit mapping/dequeue outside an already owned compound transaction.
# Why: factoring the existing validation body must not introduce an unprotected alternative acknowledgement API.
def test_acknowledge_inner_requires_real_transaction(combined_journal):
    j, _, local = combined_journal
    before = j.vendor_id(local), [tuple(row) for row in j.db.execute("SELECT * FROM outbox")]
    with pytest.raises(RecoveryRequiredError, match="existing journal transaction"):
        j.acknowledge_in_transaction(j.next_delivery(), {"transactionId": 9001, "idTagInfo": {"status": "Accepted"}})
    assert (j.vendor_id(local), [tuple(row) for row in j.db.execute("SELECT * FROM outbox")]) == before


# Intent: a later enclosing failure rolls back an otherwise valid inner mapping/dequeue and command retirement.
# Why: the composite operation needs one durable outcome, without nesting or weakening Journal.atomic.
def test_acknowledge_inner_obeys_enclosing_rollback(combined_journal):
    j, token, local = combined_journal
    before = snapshot(j)
    with pytest.raises(RuntimeError, match="later compound failure"):
        with j.atomic():
            j.reconcile_mutation(token)
            j.acknowledge_in_transaction(
                j.next_delivery(), {"transactionId": 9001, "idTagInfo": {"status": "Accepted"}}
            )
            assert j.vendor_id(local) == 9001
            raise RuntimeError("later compound failure")
    assert snapshot(j) == before and not j.db.in_transaction


# Intent: invalid cloud numbers roll back the whole compound transaction through the original Start validator.
# Why: the operator dataclass's wider general ID bound must never bypass the engine's actual cloud ID range.
@pytest.mark.parametrize("vendor", [True, -1, 2**31, 2**53])
def test_acknowledge_inner_preserves_cloud_id_validation(combined_journal, vendor):
    j, token, _ = combined_journal
    before = snapshot(j)
    with pytest.raises(RecoveryRequiredError):
        with j.atomic():
            j.reconcile_mutation(token)
            j.acknowledge_in_transaction(
                j.next_delivery(), {"transactionId": vendor, "idTagInfo": {"status": "Accepted"}}
            )
    assert snapshot(j) == before and not j.db.in_transaction


# Intent: optional profile content, another tag/connector/action or another mutation cannot borrow this association.
# Why: a coincident closed session is insufficient; only the exact admitted canonical command body is supported.
@pytest.mark.parametrize("changed", ["profile", "tag", "connector", "action", "extra-mutation", "marker"])
def test_combined_candidate_preserves_unrelated_command_boundaries(combined_journal, changed):
    j, token, _ = combined_journal
    command = {"idTag": "CLOUD_OWNER", "connectorId": 1}
    if changed == "profile":
        command["chargingProfile"] = {"chargingProfileId": 7007}
    elif changed == "tag":
        command["idTag"] = "UNRELATED"
    elif changed == "connector":
        command["connectorId"] = 2
    if changed in ("profile", "tag", "connector"):
        digest = fingerprint(command)
        j.db.execute("UPDATE mutations SET digest=? WHERE token=?", (digest, token))
        j.put("uncertain_vendor_command", {"mutation": token, "action": "RemoteStartTransaction", "digest": digest})
    elif changed == "action":
        j.db.execute("UPDATE mutations SET action='Reset' WHERE token=?", (token,))
    elif changed == "extra-mutation":
        j.mutation_intent("vendor", "ChangeConfiguration", {"key": "unrelated", "value": "unchanged"})
    else:
        j.put("uncertain_vendor_command", {"action": "RemoteStartTransaction", "digest": "0" * 64})
    before = copy.deepcopy(snapshot(j))
    with pytest.raises(RecoveryRequiredError):
        combined_candidate(j, snapshot(j))
    assert snapshot(j) == before


# Intent: canonical RemoteStart with its optional connector omitted still binds the same exact reported connector.
# Why: supporting the two actual standard body shapes must not infer unrecorded optional charging profiles.
def test_combined_candidate_allows_exact_omitted_connector(combined_journal):
    j, token, local = combined_journal
    digest = fingerprint({"idTag": "CLOUD_OWNER"})
    j.db.execute("UPDATE mutations SET digest=? WHERE token=?", (digest, token))
    j.put("uncertain_vendor_command", {"mutation": token, "action": "RemoteStartTransaction", "digest": digest})
    result = combined_candidate(j, snapshot(j))
    assert result["mutation_token"] == token and result["local_transaction_id"] == local
    assert result["payload_digest"] == digest
