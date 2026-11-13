"""One independently settled vendor Start and its actual closed cloud lifecycle recover together."""

from __future__ import annotations

import json
import time
from typing import Any

from .store import Delivery, Journal, RecoveryRequiredError, fingerprint

KIND = "vendor-start-mapping"
PROOFS = "managed_operator_debt_reconciliations"
REPORTS = ("StatusNotification", "MeterValues", "FirmwareStatusNotification", "DiagnosticsStatusNotification")


# Admit only one exact uncertain command and one real closed unmapped pair, without interpreting optional profiles.
def combined_candidate(j: Journal, snapshot: dict[str, Any]) -> dict[str, Any]:
    rows, marker, head = snapshot["mutations"], snapshot["marker"], snapshot["head"]
    if (
        len(rows) != 1
        or rows[0]["origin"] != "vendor"
        or rows[0]["action"] != "RemoteStartTransaction"
        or rows[0]["state"] not in ("pending", "uncertain")
        or snapshot["grant"]
        or snapshot["requests"]
        or snapshot["deferred"]
        or head is None
        or head["action"] != "StartTransaction"
        or head["state"] != "ambiguous"
        or (snapshot["fault"] or {}).get("classification") != "owner_control_or_delivery_uncertain"
    ):
        raise RecoveryRequiredError("combined recovery requires one uncertain vendor Start and its ambiguous report")
    mutation = rows[0]
    if marker and (
        marker.get("mutation", mutation["token"]) != mutation["token"]
        or marker.get("action") != mutation["action"]
        or marker.get("digest") != mutation["digest"]
    ):
        raise RecoveryRequiredError("combined recovery cannot retire another command marker")
    local = head["transaction"]
    transaction = j.db.execute("SELECT * FROM transactions WHERE local=?", (local,)).fetchone()
    pair = [row for row in snapshot["outbox"] if row["action"] in ("StartTransaction", "StopTransaction")]
    if (
        type(local) is not int
        or transaction is None
        or transaction["stop"] is None
        or transaction["vendor"] is not None
        or json.loads(transaction["start"]) != head["payload"]
        or len(pair) != 2
        or pair[0]["token"] != head["token"]
        or pair[0]["action"] != "StartTransaction"
        or pair[1]["action"] != "StopTransaction"
        or pair[1]["local"] != local
        or pair[1]["payload"] != transaction["stop"]
        or pair[1]["state"] != "queued"
        or any(row["local"] != local and row["vendor"] is None for row in snapshot["transactions"])
        or any(row["stop"] is None for row in snapshot["transactions"])
        or any(
            row["token"] != head["token"]
            and (
                row["state"] != "queued"
                or row["local"] not in (None, local)
                or row["action"] not in (*REPORTS, "StopTransaction")
            )
            for row in snapshot["outbox"]
        )
    ):
        raise RecoveryRequiredError("combined recovery retains every unrelated or unbound lifecycle")
    start = head["payload"]
    canonical = {"idTag": start["idTag"]}
    if mutation["digest"] not in (
        fingerprint(canonical),
        fingerprint(canonical | {"connectorId": start["connectorId"]}),
    ):
        raise RecoveryRequiredError("command does not match the exact canonical Start association")
    return {
        "mutation_token": mutation["token"],
        "action": mutation["action"],
        "payload_digest": mutation["digest"],
        "local_transaction_id": local,
        "delivery_token": head["token"],
        "stop_delivery_token": pair[1]["token"],
        "start_digest": fingerprint(start),
        "stop_digest": fingerprint(json.loads(transaction["stop"])),
    }


# Proof postimages permit routine report drain but pin the retired command, actual mapping and exact Stop delivery.
def combined_resolution(j: Journal, evidence: dict[str, Any], wire_ack: dict[str, Any] | None) -> dict[str, Any]:
    mutation = j.db.execute("SELECT * FROM mutations WHERE token=?", (evidence["mutation_token"],)).fetchone()
    transaction = j.db.execute(
        "SELECT local,start,stop,vendor FROM transactions WHERE local=?", (evidence["local_transaction_id"],)
    ).fetchone()
    return {
        "mutation": dict(mutation) if mutation is not None else None,
        "marker": j.get("uncertain_vendor_command"),
        "grant": j.get("vendor_start_grant"),
        "transaction": dict(transaction) if transaction is not None else None,
        "start_retained": j.db.execute("SELECT 1 FROM outbox WHERE token=?", (evidence["delivery_token"],)).fetchone()
        is not None,
        "stop_retained": j.db.execute(
            "SELECT 1 FROM outbox WHERE token=?", (evidence["stop_delivery_token"],)
        ).fetchone()
        is not None,
        "wire_ack": wire_ack,
    }


# A failed composite Stop cannot detach another genuinely denied lifecycle from its original fault.
def retains_denial_fault(j: Journal, proof: dict[str, Any]) -> bool:
    fault = proof.get("installed_fault")
    return proof.get("kind") == KIND and any(
        denial.get("fault") == fault and denial.get("completed") is None
        for denial in j.get("managed_start_denials", {}).values()
    )


# No foreign Stop, even under the same fault, can complete or supersede this composite proof.
def combined_stop_matches(proof: dict[str, Any], identity: dict[str, Any]) -> bool:
    e = proof["attestation"]
    return (
        proof["kind"] == KIND
        and all(
            identity.get(name) == e[name]
            for name in ("local_transaction_id", "vendor_transaction_id", "start_digest", "stop_digest")
        )
        and identity.get("delivery_token") == e["stop_delivery_token"]
    )


# This callback runs only inside the exact successful Stop ACK/dequeue transaction, never on permission to retry.
def record_combined_stop_ack(j: Journal, item: Delivery, response: dict[str, Any], identity: dict[str, Any]) -> None:
    if not j.db.in_transaction:
        raise RecoveryRequiredError("combined Stop evidence requires its actual acknowledgement transaction")
    records, changed = j.get(PROOFS, {}), False
    for key, entry in records.items():
        proof = entry["proof"]
        if (
            proof["kind"] != KIND
            or entry.get("completed") is not None
            or entry.get("superseded")
            or j.get("managed_control_fault") != proof["installed_fault"]
            or not combined_stop_matches(proof, identity)
        ):
            continue
        if proof["resolved"] != combined_resolution(j, proof["attestation"], proof.get("wire_ack")):
            raise RecoveryRequiredError("combined recovery changed before its exact Stop acknowledgement")
        wire = {**identity, "reply_digest": fingerprint(response), "at_epoch": time.time()}
        resolved = combined_resolution(j, proof["attestation"], wire)
        resolved["stop_retained"] = False
        records[key] = {**entry, "proof": {**proof, "wire_ack": wire, "resolved": resolved}}
        changed = True
    if changed:
        j.put(PROOFS, records)
