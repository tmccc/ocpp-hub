"""Read settled vendor recovery records while the rollback checker owns a stopped snapshot."""

from __future__ import annotations

import json
import re
import sqlite3
from typing import Any

from ocpp_hub_core.prototype.managed_debt_evidence import ATTESTATIONS
from ocpp_hub_core.prototype.managed_delivery_recovery import DeliveryAttestation, delivery_recovery_digest
from ocpp_hub_core.prototype.managed_denial_faults import (
    DELIVERY_MAPPINGS,
    PROOFS,
    archived_start_matches,
    proof_headers_match,
)
from ocpp_hub_core.prototype.managed_native_repair import AUDITS
from ocpp_hub_core.prototype.managed_release_recovery import PROOF as RELEASE_PROOF
from ocpp_hub_core.prototype.managed_start_delivery import RETRIES
from ocpp_hub_core.prototype.managed_stop_delivery import KINDS as STOP_KINDS
from ocpp_hub_core.prototype.managed_stop_delivery import REJECTION, WIRE_ACK
from ocpp_hub_core.prototype.store import fingerprint

RECOVERY_METADATA = frozenset({DELIVERY_MAPPINGS, PROOFS, AUDITS, RELEASE_PROOF, RETRIES, REJECTION, WIRE_ACK})


# An archived charge must still identify the exact closed local and vendor lifecycle.
def closed_pair(db: sqlite3.Connection, local: object, vendor: object, start: object, stop: object) -> bool:
    row = db.execute(
        "SELECT start,stop FROM transactions WHERE local=? AND vendor=? AND stop IS NOT NULL", (local, vendor)
    ).fetchone()
    return row is not None and fingerprint(json.loads(row[0])) == start and fingerprint(json.loads(row[1])) == stop


# A wire receipt identifies its exact original queued report.
def wire_acknowledged(ack: object, token: object) -> bool:
    return (
        isinstance(ack, dict)
        and isinstance(token, str)
        and re.fullmatch(r"e-[0-9a-f]{32}", token) is not None
        and ack.get("delivery_token") == token
        and isinstance(ack.get("reply_digest"), str)
        and re.fullmatch(r"[0-9a-f]{64}", ack["reply_digest"]) is not None
    )


# The exact receipt proves verification even when Pause was submitted from an older dashboard.
def verified_pause(db: sqlite3.Connection, identifier: object) -> bool:
    if not isinstance(identifier, str):
        return False
    row = db.execute(
        "SELECT result FROM managed_requests WHERE id=? AND action='pause' AND state='completed'",
        (identifier,),
    ).fetchone()
    result = json.loads(row[0]) if row is not None else None
    return isinstance(result, dict) and result.get("pause_verified") is True


# Validate the two reports belonging to the closed session recorded in a terminal operator proof.
def operator_closed(db: sqlite3.Connection, proof: dict[str, Any]) -> bool:
    kind = proof["kind"]
    if kind not in (*STOP_KINDS, "start-delivery"):
        return False
    evidence, resolved = proof["attestation"], proof.get("resolved")
    ATTESTATIONS[kind].parse(evidence, recent=False)
    transaction = resolved.get("transaction") if isinstance(resolved, dict) else None
    local = evidence.get("local_transaction_id")
    if (
        not isinstance(transaction, dict)
        or transaction.get("local") != local
        or not isinstance(transaction.get("start"), str)
        or not isinstance(transaction.get("stop"), str)
        or fingerprint(json.loads(transaction["start"])) != evidence.get("start_digest")
        or fingerprint(json.loads(transaction["stop"])) != evidence.get("stop_digest")
        or not closed_pair(
            db, local, transaction.get("vendor"), evidence.get("start_digest"), evidence.get("stop_digest")
        )
    ):
        return False
    if kind in STOP_KINDS:
        return (
            transaction.get("vendor") == evidence.get("vendor_transaction_id")
            and resolved.get("retained") is False
            and resolved.get("wire_ack") == proof.get("wire_ack")
            and wire_acknowledged(proof.get("wire_ack"), evidence.get("delivery_token"))
        )
    if resolved.get("retry") is not None or resolved.get("retained") != []:
        return False
    outbox = proof["prior"].get("outbox")
    if not isinstance(outbox, list) or not all(isinstance(event, dict) for event in outbox):
        return False
    stops = [
        event
        for event in outbox
        if event.get("action") == "StopTransaction"
        and event.get("local") == local
        and event.get("payload") == transaction.get("stop")
    ]
    return (
        len(stops) == 1
        and archived_start_matches(proof, evidence.get("delivery_token"), local, evidence.get("start_digest"))
        and wire_acknowledged(proof.get("start_wire_ack"), evidence.get("delivery_token"))
        and wire_acknowledged(proof.get("stop_wire_ack"), stops[0].get("token"))
    )


# Only wire-settled supersession with a matching successor can retire an older operator proof.
def superseded_operator(operators: dict[str, Any], identifier: str, proof: dict[str, Any], marker: object) -> bool:
    if not isinstance(marker, dict) or marker.get("reason") != "later_start_retry":
        return False
    successor_id = marker.get("proof")
    if not isinstance(successor_id, str):
        return False
    successor = operators.get(successor_id)
    if not isinstance(successor, dict) or not isinstance(successor.get("proof"), dict):
        return False
    prior = successor["proof"].get("prior")
    return (
        bool(successor.get("completed"))
        and isinstance(prior, dict)
        and prior.get("previous_operator") == {"id": identifier, "digest": fingerprint(proof)}
    )


# Retain historical proofs only after their wire work and owner acknowledgement have both settled.
def recovery_history_settled(db: sqlite3.Connection, meta: dict[str, Any], revision: int) -> bool:
    if meta.get(AUDITS, {}) != {} or meta.get(RETRIES, {}) != {}:
        return False
    if not (RECOVERY_METADATA - {AUDITS, RETRIES}).intersection(meta):
        return True
    mappings, operators, release = (meta.get(key, {}) for key in (DELIVERY_MAPPINGS, PROOFS, RELEASE_PROOF))
    if not all(isinstance(value, dict) for value in (mappings, operators, release)):
        return False
    pause, pause_revision = release.get("pause_request"), release.get("revision")
    if (
        not verified_pause(db, pause)
        or type(pause_revision) is not int
        or not 0 < pause_revision <= revision
        or release.get("zero_hold_retained") is not True
        or release.get("charging_started") is not False
    ):
        return False
    latest_mappings, latest_operators = [], []
    for local, entry in sorted(mappings.items()):
        if not isinstance(entry, dict):
            return False
        proof, completed = entry.get("proof"), entry.get("completed")
        if not isinstance(proof, dict) or proof.get("version") != 2 or not isinstance(completed, dict):
            return False
        attestation = DeliveryAttestation.parse(proof.get("attestation"), recent=False)
        if (
            local != str(attestation.local)
            or fingerprint(proof["attestation"]) != proof.get("input_digest")
            or not verified_pause(db, completed.get("pause"))
            or not closed_pair(
                db, attestation.local, attestation.vendor, attestation.start_digest, attestation.stop_digest
            )
            or not wire_acknowledged(proof.get("stop_wire_ack"), proof.get("stop_token"))
        ):
            return False
        if completed["pause"] == pause:
            latest_mappings.append(proof)
    for identifier, entry in operators.items():
        if not isinstance(entry, dict):
            return False
        proof, completed = entry.get("proof"), entry.get("completed")
        if not isinstance(proof, dict) or not proof_headers_match(proof, identifier) or not operator_closed(db, proof):
            return False
        if isinstance(completed, dict):
            if not verified_pause(db, completed.get("pause")) or entry.get("superseded"):
                return False
            if completed["pause"] == pause:
                latest_operators.append(proof)
        elif completed is not None or not superseded_operator(operators, identifier, proof, entry.get("superseded")):
            return False
    for key in (REJECTION, WIRE_ACK):
        if key not in meta:
            continue
        witness = meta[key]
        if (
            not isinstance(witness, dict)
            or not wire_acknowledged(witness, witness.get("delivery_token"))
            or witness.get("payload_digest") != witness.get("stop_digest")
            or not closed_pair(
                db,
                witness.get("local_transaction_id"),
                witness.get("vendor_transaction_id"),
                witness.get("start_digest"),
                witness.get("stop_digest"),
            )
        ):
            return False
    mapping_digest = delivery_recovery_digest(latest_mappings) if latest_mappings else None
    return (
        release.get("delivery_reconciliation") == mapping_digest
        and len(latest_operators) <= 1
        and release.get("operator_reconciliation") == (fingerprint(latest_operators[0]) if latest_operators else None)
        and (not latest_operators or release.get("prior_fault") == latest_operators[0].get("installed_fault"))
    )
