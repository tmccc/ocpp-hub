"""A retained Stop needs an exact independent retry approval and a genuine correlated cloud acknowledgment."""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from typing import Any

from .managed_combined_start_recovery import KIND as COMBINED_KIND
from .managed_combined_start_recovery import combined_stop_matches, record_combined_stop_ack, retains_denial_fault
from .store import Delivery, Journal, RecoveryRequiredError, fingerprint

REJECTION = "managed_stop_reply_rejection"
WIRE_ACK = "managed_stop_wire_acknowledgment"
PROOFS = "managed_operator_debt_reconciliations"
KINDS = ("stop-delivery", "stop-fault")
NO_REPLY = fingerprint(None)


@dataclass(frozen=True)
class StopIdentity:
    """One immutable local Stop and its actual cloud mapping, without publishing any user tag."""

    delivery_token: str
    local_transaction_id: int
    vendor_transaction_id: int
    payload_digest: str
    start_digest: str
    stop_digest: str


# An arbitrary closed session cannot replace the exact retained event or its established cloud transaction.
def stop_identity(j: Journal, item: Delivery) -> dict[str, Any]:
    row = j.db.execute("SELECT start,stop,vendor FROM transactions WHERE local=?", (item.transaction,)).fetchone()
    if (
        item.action != "StopTransaction"
        or type(item.transaction) is not int
        or row is None
        or row["stop"] is None
        or row["vendor"] is None
        or json.loads(row["stop"]) != item.payload
    ):
        raise RecoveryRequiredError("Stop reconciliation requires its exact closed mapped transaction and event")
    return asdict(
        StopIdentity(
            item.token,
            item.transaction,
            row["vendor"],
            fingerprint(item.payload),
            fingerprint(json.loads(row["start"])),
            fingerprint(json.loads(row["stop"])),
        )
    )


# Record the delivery transition before reporting failure; never strand an unexplained inflight lifecycle head.
def reject_stop_reply(j: Journal, item: Delivery, response: Any, *, reason: str) -> None:
    identity = stop_identity(j, item)
    rejection = {
        **identity,
        "reply_digest": fingerprint(response),
        "reason": reason,
        "observed_at_epoch": time.time(),
    }
    with j.atomic():
        current = j.next_delivery()
        if (
            current is None
            or current.token != item.token
            or current.state != "inflight"
            or stop_identity(j, current) != identity
        ):
            raise RecoveryRequiredError("rejected Stop no longer identifies the exact sent delivery")
        records = j.get(PROOFS, {})
        changed = False
        for key, entry in records.items():
            proof = entry["proof"]
            if (
                entry.get("completed") is None
                and not entry.get("superseded")
                and proof["kind"] in (*KINDS, "start-delivery", COMBINED_KIND)
                and j.get("managed_control_fault") == proof["installed_fault"]
                and (
                    proof["attestation"]["delivery_token"] == item.token
                    or combined_stop_matches(proof, identity)
                    or proof["kind"] == "start-delivery"
                    and proof["attestation"]["local_transaction_id"] == item.transaction
                    and proof.get("start_wire_ack") is not None
                    or proof.get("wire_ack") is not None
                    and j.db.execute(
                        "SELECT 1 FROM outbox WHERE token=?", (proof["attestation"]["delivery_token"],)
                    ).fetchone()
                    is None
                )
            ):
                records[key] = {
                    **entry,
                    "superseded": {"reply_digest": rejection["reply_digest"], "at_epoch": time.time()},
                }
                if not retains_denial_fault(j, proof):
                    j.put(
                        "managed_control_fault",
                        {"classification": "owner_control_or_delivery_uncertain", "at_epoch": time.time()},
                    )
                changed = True
        if changed:
            j.put(PROOFS, records)
        j.put(REJECTION, rejection)
        j.db.execute("UPDATE outbox SET state='rejected' WHERE token=?", (item.token,))


# Approval applies only to this complete malformed Stop response, never an error or another event's metadata.
def approved_stop_metadata(j: Journal, item: Delivery, response: dict[str, Any]) -> bool:
    identity = stop_identity(j, item)
    for entry in j.get(PROOFS, {}).values():
        proof = entry["proof"]
        if (
            entry.get("completed") is not None
            or entry.get("superseded")
            or proof["kind"] not in KINDS
            or j.get("managed_control_fault") != proof["installed_fault"]
        ):
            continue
        evidence = proof["attestation"]
        if (
            all(evidence.get(key) == value for key, value in identity.items())
            and evidence.get("reply_digest") == fingerprint(response)
            and evidence.get("cloud_stop_independently_verified") is True
        ):
            return True
    return False


# Postimages distinguish real wire acknowledgment from retry permission while tolerating queued/inflight transitions.
def stop_resolution(j: Journal, evidence: dict[str, Any], wire_ack: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "rejection": j.get(REJECTION),
        "retained": j.db.execute("SELECT 1 FROM outbox WHERE token=?", (evidence["delivery_token"],)).fetchone()
        is not None,
        "wire_ack": wire_ack,
        "transaction": dict(row)
        if (
            row := j.db.execute(
                "SELECT local,start,stop,vendor FROM transactions WHERE local=?", (evidence["local_transaction_id"],)
            ).fetchone()
        )
        is not None
        else None,
    }


# Called inside Journal.acknowledge: proof and actual dequeue commit together or both roll back.
def record_stop_ack(j: Journal, item: Delivery, response: dict[str, Any]) -> None:
    if item.action != "StopTransaction":
        return
    if not j.db.in_transaction:
        raise RecoveryRequiredError("Stop wire acknowledgment must share its actual journal commit")
    identity = stop_identity(j, item)
    record_combined_stop_ack(j, item, response, identity)
    denials = j.get("managed_start_denials", {})
    denial = denials.get(str(item.transaction))
    if denial is not None:
        if (
            denial["local"] != identity["local_transaction_id"]
            or denial["vendor"] != identity["vendor_transaction_id"]
            or denial["start_digest"] != identity["start_digest"]
        ):
            raise RecoveryRequiredError("denied session Stop does not match its immutable Start")
        wire = {
            **identity,
            "fault": denial.get("fault"),
            "reply_digest": fingerprint(response),
            "at_epoch": time.time(),
        }
        j.put("managed_start_denials", {**denials, str(item.transaction): {**denial, "wire_ack": wire}})
    records, changed = j.get(PROOFS, {}), False
    for key, entry in records.items():
        proof = entry["proof"]
        if (
            entry.get("completed") is not None
            or entry.get("superseded")
            or proof["kind"] not in KINDS
            or j.get("managed_control_fault") != proof["installed_fault"]
            or not all(proof["attestation"].get(name) == value for name, value in identity.items())
        ):
            continue
        wire = {"delivery_token": item.token, "reply_digest": fingerprint(response), "at_epoch": time.time()}
        proof = {**proof, "wire_ack": wire}
        # Journal deletes this exact event after the callback; any deletion failure rolls this projection back.
        proof["resolved"] = {**stop_resolution(j, proof["attestation"], wire), "retained": False}
        records[key] = {**entry, "proof": proof}
        changed = True
    if changed:
        j.put(PROOFS, records)
    else:
        fault = j.get("managed_control_fault", {})
        prior = j.get(WIRE_ACK, {})
        if fault.get("classification") == "owner_control_or_delivery_uncertain" and prior.get("fault") != fault:
            j.put(
                WIRE_ACK, {**identity, "fault": fault, "reply_digest": fingerprint(response), "at_epoch": time.time()}
            )
