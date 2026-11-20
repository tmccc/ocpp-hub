"""A retained Stop needs an exact independent retry approval and a genuine correlated cloud acknowledgment."""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from typing import Any

from .managed_combined_start_recovery import KIND as COMBINED_KIND
from .managed_combined_start_recovery import combined_stop_matches, record_combined_stop_ack
from .managed_denial_faults import DELIVERY_MAPPINGS, operator_wire_settled, retained_denial_fault
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


# Latch only a rejection that still identifies the exact retained Stop, including on upgrade.
def latch_rejected_stop(j: Journal, item: Delivery) -> None:
    if j.get("managed_mode") is None or j.fault.latched() or item.state != "rejected":
        return
    rejection = j.get(REJECTION, {})
    if rejection.get("delivery_token") != item.token:
        return
    identity = stop_identity(j, item)
    if all(rejection.get(key) == value for key, value in identity.items()):
        j.fault.replace(
            {"classification": "owner_control_or_delivery_uncertain", "at_epoch": rejection["observed_at_epoch"]}
        )
        j.phase.fault(j.get("control", {}))


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
                and j.fault.read() == proof["installed_fault"]
                and (
                    proof["attestation"]["delivery_token"] == item.token
                    or combined_stop_matches(proof, identity)
                    or proof["kind"] == "start-delivery"
                    and proof["attestation"]["local_transaction_id"] == item.transaction
                    and proof.get("start_wire_ack") is not None
                    or operator_wire_settled(proof)
                    and j.db.execute(
                        "SELECT 1 FROM outbox WHERE token=?", (proof["attestation"]["delivery_token"],)
                    ).fetchone()
                    is None
                )
            ):
                retained_fault = retained_denial_fault(j, proof)
                records[key] = {
                    **entry,
                    "superseded": {"reply_digest": rejection["reply_digest"], "at_epoch": time.time()},
                }
                j.fault.replace(
                    retained_fault
                    or {"classification": "owner_control_or_delivery_uncertain", "at_epoch": time.time()},
                )
                changed = True
        if changed:
            j.put(PROOFS, records)
        j.put(REJECTION, rejection)
        j.db.execute("UPDATE outbox SET state='rejected' WHERE token=?", (item.token,))
        rejected = j.next_delivery()
        assert rejected is not None
        latch_rejected_stop(j, rejected)


# Approval applies only to this complete malformed Stop response, never an error or another event's metadata.
def approved_stop_metadata(j: Journal, item: Delivery, response: dict[str, Any]) -> bool:
    identity = stop_identity(j, item)
    for entry in j.get(PROOFS, {}).values():
        proof = entry["proof"]
        if (
            entry.get("completed") is not None
            or entry.get("superseded")
            or proof["kind"] not in KINDS
            or j.fault.read() != proof["installed_fault"]
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


# A mapped Start keeps its own real Stop acknowledgement, even if more Starts are later mapped.
def record_mapped_stop_ack(j: Journal, identity: dict[str, Any], response: dict[str, Any]) -> None:
    records = j.get(DELIVERY_MAPPINGS, {})
    key = str(identity["local_transaction_id"])
    entry = records.get(key)
    if entry is None or entry.get("completed") is not None or entry["proof"].get("version") != 2:
        return
    proof = entry["proof"]
    evidence = proof["attestation"]
    if (
        proof.get("stop_wire_ack") is not None
        or proof["stop_token"] != identity["delivery_token"]
        or evidence["vendor"] != identity["vendor_transaction_id"]
        or evidence["start_digest"] != identity["start_digest"]
        or evidence["stop_digest"] != identity["stop_digest"]
    ):
        raise RecoveryRequiredError("mapped Stop does not match the independently confirmed Start and Stop pair")
    ack = {"delivery_token": identity["delivery_token"], "reply_digest": fingerprint(response), "at_epoch": time.time()}
    records[key] = {**entry, "proof": {**proof, "stop_wire_ack": ack}}
    j.put(DELIVERY_MAPPINGS, records)


# Called inside Journal.acknowledge: proof and actual dequeue commit together or both roll back.
def record_stop_ack(j: Journal, item: Delivery, response: dict[str, Any]) -> None:
    if item.action != "StopTransaction":
        return
    if not j.db.in_transaction:
        raise RecoveryRequiredError("Stop wire acknowledgment must share its actual journal commit")
    identity = stop_identity(j, item)
    record_mapped_stop_ack(j, identity, response)
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
            or j.fault.read() != proof["installed_fault"]
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
        fault = j.fault.read({})
        prior = j.get(WIRE_ACK, {})
        if fault.get("classification") == "owner_control_or_delivery_uncertain" and prior.get("fault") != fault:
            j.put(
                WIRE_ACK, {**identity, "fault": fault, "reply_digest": fingerprint(response), "at_epoch": time.time()}
            )
