"""Exact operator proofs retain the generic fault shared by still-unfinished denied lifecycles."""

from __future__ import annotations

import json
from typing import Any

from .managed_debt_evidence import ATTESTATIONS
from .store import Delivery, Journal, fingerprint

PROOFS = "managed_operator_debt_reconciliations"
DENIALS = "managed_start_denials"
GENERIC = "owner_control_or_delivery_uncertain"
OPERATOR = "operator_debt_reconciled_at_zero"
COMBINED = "vendor-start-mapping"


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
def archived_start_matches(proof: dict[str, Any], token: str, local: int | None, digest: str) -> bool:
    if type(local) is not int or local < 0:
        return False
    try:
        matches = [
            row
            for row in proof["prior"].get("outbox", [])
            if row.get("token") == token
            and row.get("action") == "StartTransaction"
            and row.get("local") == local
            and fingerprint(json.loads(row["payload"])) == digest
        ]
        return len(matches) == 1
    except ValueError, TypeError, KeyError:
        return False


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


# A new explicit proof may explain today's generic latch, but never an older null or unrelated native fault.
def denial_source_fault(j: Journal, fault: dict[str, Any]) -> dict[str, Any] | None:
    if fault.get("classification") == GENERIC:
        return fault
    proof = current_operator_proof(j, fault)
    source = proof["prior"].get("fault") if proof is not None else None
    return source if isinstance(source, dict) and source.get("classification") == GENERIC else None


# Repeated failure supersedes permission to retry while retaining the original association of unresolved denials.
def retained_denial_fault(j: Journal, proof: dict[str, Any]) -> dict[str, Any] | None:
    current = j.get("managed_control_fault", {})
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
