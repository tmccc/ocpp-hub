"""Exact operator proofs retain the generic fault shared by still-unfinished denied lifecycles."""

from __future__ import annotations

import json
from typing import Any

from .managed_debt_evidence import ATTESTATIONS
from .store import Delivery, Journal, fingerprint

PROOFS = "managed_operator_debt_reconciliations"
DENIALS = "managed_start_denials"
DELIVERY_MAPPINGS = "managed_delivery_reconciliations"
GENERIC = "owner_control_or_delivery_uncertain"
OPERATOR = "operator_debt_reconciled_at_zero"
COMBINED = "vendor-start-mapping"


# Keep the identity of each outstanding mapping stable while its Stop acknowledgement is recorded.
def pending_mapping_headers(j: Journal) -> dict[str, str]:
    return {
        key: fingerprint({name: value for name, value in entry["proof"].items() if name != "stop_wire_ack"})
        for key, entry in j.get(DELIVERY_MAPPINGS, {}).items()
        if entry.get("completed") is None
    }


# Stored prior data belongs to this exact independent attestation, including older supported snapshot layouts.
def proof_headers_match(proof: dict[str, Any], identifier: str) -> bool:
    kind, statement, prior = proof.get("kind"), proof.get("attestation"), proof.get("prior")
    return (
        isinstance(kind, str)
        and kind in ATTESTATIONS
        and isinstance(statement, dict)
        and set(statement) == set(ATTESTATIONS[kind].__dataclass_fields__)
        and isinstance(prior, dict)
        and statement.get("candidate_digest") == fingerprint(prior)
        and fingerprint(statement) == identifier
    )


# This checks only immutable headers; readiness checks can call it without a cycle through denial evidence.
def current_operator_proof(j: Journal, fault: dict[str, Any]) -> dict[str, Any] | None:
    if fault.get("classification") != OPERATOR:
        return None
    identifier = fault.get("proof")
    entry = j.get(PROOFS, {}).get(identifier)
    if not entry or entry.get("completed") is not None or entry.get("superseded"):
        return None
    proof: dict[str, Any] = entry["proof"]
    if (
        not isinstance(identifier, str)
        or not proof_headers_match(proof, identifier)
        or proof.get("installed_fault") != fault
        or proof.get("kind") == COMBINED
    ):
        return None
    return proof


# Actual new denials must originate from a complete immutable Start archived when the operator accepted this debt.
def archived_delivery_matches(
    proof: dict[str, Any], action: str, token: str, local: int | None, digest: str, *, queued_only: bool = False
) -> bool:
    if type(local) is not int or local < 0:
        return False
    try:
        matches = [
            row
            for row in proof["prior"].get("outbox", [])
            if row.get("token") == token
            and row.get("action") == action
            and row.get("local") == local
            and (not queued_only or row.get("state") == "queued")
            and fingerprint(json.loads(row["payload"])) == digest
        ]
        return len(matches) == 1
    except ValueError, TypeError, KeyError:
        return False


# Denial and mapping proofs archive only Starts through this narrower helper.
def archived_start_matches(
    proof: dict[str, Any], token: str, local: int | None, digest: str, *, queued_only: bool = False
) -> bool:
    return archived_delivery_matches(proof, "StartTransaction", token, local, digest, queued_only=queued_only)


# The removed observation has its own durable rejection even while the earlier queue drains.
def observation_resolution(j: Journal, token: str) -> dict[str, Any]:
    row = j.db.execute("SELECT * FROM rejected_observations WHERE token=?", (token,)).fetchone()
    return {
        "rejection": dict(row) if row is not None else None,
        "retained": j.db.execute("SELECT 1 FROM outbox WHERE token=?", (token,)).fetchone() is not None,
    }


# The existing operator recovery owns its own wire replies before a later Start can be mapped alongside it.
def operator_wire_settled(proof: dict[str, Any]) -> bool:
    kind = proof["kind"]
    if kind == "start-delivery":
        return proof.get("start_wire_ack") is not None and proof.get("stop_wire_ack") is not None
    if kind in ("stop-delivery", "stop-fault", COMBINED):
        return proof.get("wire_ack") is not None
    return True


# Match mappings archived by the operator, plus later Starts that were still queued at that inspection.
def operator_mapping_members_match(j: Journal, operator: dict[str, Any]) -> bool:
    prior = operator["prior"]
    archived = prior.get("delivery_mappings", {})
    pending = pending_mapping_headers(j)
    if any(pending.get(key) != digest for key, digest in archived.items()):
        return False
    identifier = fingerprint(operator["attestation"])
    for key, entry in j.get(DELIVERY_MAPPINGS, {}).items():
        if key not in pending or key in archived:
            continue
        proof = entry["proof"]
        e = proof["attestation"]
        if proof.get("operator_parent") == identifier and archived_start_matches(
            operator, e["start_token"], e["local"], e["start_digest"], queued_only=True
        ):
            continue
        # Older operator snapshots retained the mapped transaction, before snapshots recorded mapping identities.
        if (
            "delivery_mappings" not in prior
            and proof.get("version") == 1
            and any(
                row.get("local") == e["local"]
                and row.get("vendor") == e["vendor"]
                and row.get("stop") is not None
                and fingerprint(json.loads(row["start"])) == e["start_digest"]
                and fingerprint(json.loads(row["stop"])) == e["stop_digest"]
                for row in prior.get("transactions", [])
            )
        ):
            continue
        return False
    return True


# New bridge kinds cannot borrow an older same-fault member which was absent from their immutable recovery input.
def proof_denial_member(proof: dict[str, Any], denial: dict[str, Any]) -> bool:
    if proof["kind"] in ("stop-delivery", "stop-fault"):
        # Preserve the existing legacy Stop bridge; every member still needs its own exact Stop wire receipt.
        return True
    prior = proof["prior"].get("denials", {}) or {}
    original = prior.get(str(denial["local"]))
    if original is not None:
        immutable = {key: value for key, value in original.items() if key not in ("wire_ack", "completed")}
        return original.get("completed") is None and immutable == {
            key: value for key, value in denial.items() if key not in ("wire_ack", "completed")
        }
    return archived_start_matches(proof, denial["start_token"], denial["local"], denial["start_digest"])


# A later queued lifecycle belongs to recovery only after the predecessor's own outcome is settled.
def operator_for_queued_lifecycle(j: Journal, fault: dict[str, Any], head: Delivery) -> dict[str, Any] | None:
    proof = current_operator_proof(j, fault)
    kinds: tuple[str, ...] = ("observation",)
    if head.action == "StartTransaction":
        kinds += ("start-delivery", "stop-delivery")
    if (
        proof is None
        or proof["kind"] not in kinds
        or not operator_wire_settled(proof)
        or not operator_mapping_members_match(j, proof)
        or not archived_delivery_matches(
            proof, head.action, head.token, head.transaction, fingerprint(head.payload), queued_only=True
        )
    ):
        return None
    if proof["kind"] == "observation" and proof.get("resolved") != observation_resolution(
        j, proof["attestation"]["observation_token"]
    ):
        return None
    source = proof["prior"].get("fault") or {}
    if source.get("classification") != GENERIC or any(
        denial.get("completed") is None and denial.get("fault") == source and not proof_denial_member(proof, denial)
        for denial in j.get(DENIALS, {}).values()
    ):
        return None
    return proof


# A new explicit proof may explain today's generic latch, but never an older null or unrelated native fault.
def denial_source_fault(j: Journal, fault: dict[str, Any]) -> dict[str, Any] | None:
    if fault.get("classification") == GENERIC:
        return fault
    proof = current_operator_proof(j, fault)
    source = proof["prior"].get("fault") if proof is not None else None
    return source if isinstance(source, dict) and source.get("classification") == GENERIC else None


# Repeated failure retains the original fault when it still owns a denial or an accepted Start mapping.
def retained_denial_fault(j: Journal, proof: dict[str, Any]) -> dict[str, Any] | None:
    current = j.fault.read({})
    entry = j.get(PROOFS, {}).get(fingerprint(proof.get("attestation")))
    if (
        not entry
        or entry.get("proof") != proof
        or entry.get("completed") is not None
        or entry.get("superseded")
        or proof.get("installed_fault") != current
        or not proof_headers_match(proof, fingerprint(proof.get("attestation")))
    ):
        return None
    source: dict[str, Any] | None
    if (
        proof["kind"] == COMBINED
        and current.get("classification") == GENERIC
        and proof["prior"].get("fault") == current
    ):
        source = current
    elif current_operator_proof(j, current) == proof:
        source = denial_source_fault(j, current)
    else:
        return None
    if source is None:
        return None
    mappings = pending_mapping_headers(j)
    if mappings and operator_mapping_members_match(j, proof):
        return source
    return (
        source
        if any(
            denial.get("completed") is None and denial.get("fault") == source and proof_denial_member(proof, denial)
            for denial in j.get(DENIALS, {}).values()
        )
        else None
    )


# Only a real queued Start archived by this exact proof may add a new denied member to its original generic fault.
def start_denial_fault(j: Journal, item: Delivery, fault: dict[str, Any]) -> dict[str, Any] | None:
    if fault.get("classification") == GENERIC:
        return fault
    proof = current_operator_proof(j, fault)
    source = denial_source_fault(j, fault)
    if proof is None or source is None:
        return None
    return source if archived_start_matches(proof, item.token, item.transaction, fingerprint(item.payload)) else None
