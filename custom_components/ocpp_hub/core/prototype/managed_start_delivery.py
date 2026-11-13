"""Separate explicit Start refusals, unknown delivery and denied-but-correlated cloud lifecycles."""

from __future__ import annotations

import json
import math
import time
from typing import TYPE_CHECKING, Any

from .local_control import local_start_receipt
from .store import Delivery, Journal, RecoveryRequiredError, fingerprint

if TYPE_CHECKING:
    from .managed import ManagedController

RETRIES = "managed_start_delivery_retries"
DENIALS = "managed_start_denials"
PROOFS = "managed_operator_debt_reconciliations"
ATTEMPTS = 3
RETRY_INTERVAL = 60.0
KIND = "start-delivery"


# Quarantine must install its own recovery gate even when native reconciliation prevents a supervisor poll.
def latch_start_delivery_fault(j: Journal) -> None:
    if not j.db.in_transaction:
        raise RecoveryRequiredError("Start delivery fault must share its quarantine transaction")
    if j.get("managed_mode") is not None and not j.get("managed_control_fault"):
        control = j.get("control", {})
        j.put("control", {**control, "owner": "local", "inhibit": True, "uncertain": True})
        j.put(
            "managed_control_fault", {"classification": "owner_control_or_delivery_uncertain", "at_epoch": time.time()}
        )


# An interrupted approved retry needs new exact evidence; its old retry permission cannot block all later tooling.
def supersede_unknown_start_retry(j: Journal, item: Delivery) -> None:
    if not j.db.in_transaction:
        raise RecoveryRequiredError("unknown Start retry normalization must share its journal transition")
    if item.action != "StartTransaction" or item.state != "ambiguous":
        return
    latch_start_delivery_fault(j)
    records, changed = j.get(PROOFS, {}), False
    for key, entry in records.items():
        proof = entry["proof"]
        evidence = proof["attestation"]
        if (
            proof["kind"] == KIND
            and entry.get("completed") is None
            and not entry.get("superseded")
            and proof.get("start_wire_ack") is None
            and j.get("managed_control_fault") == proof["installed_fault"]
            and evidence["delivery_token"] == item.token
            and evidence["local_transaction_id"] == item.transaction
            and evidence["payload_digest"] == fingerprint(item.payload)
        ):
            records[key] = {
                **entry,
                "superseded": {"reason": "approved_start_retry_outcome_unknown", "at_epoch": time.time()},
            }
            changed = True
    if changed:
        j.put(PROOFS, records)
        j.put(
            "managed_control_fault", {"classification": "owner_control_or_delivery_uncertain", "at_epoch": time.time()}
        )


# Only the exact bound ordinary owner request may settle metadata alongside its denied closed lifecycle.
def denied_normal_identity(j: Journal, local: int) -> dict[str, Any] | None:
    state, owner = j.get("managed_test_charge", {}), local_start_receipt(j)
    published = state.get("start_rpc_accepted") is True
    unpublished = (
        state.get("start_rpc_accepted") is False
        and state.get("start_attempted") is True
        and owner.get("request_id") == state.get("id")
        and owner.get("stage") in ("issued", "accepted")
    )
    request = j.db.execute(
        "SELECT action,revision,state FROM managed_requests WHERE id=?", (state.get("id"),)
    ).fetchone()
    transaction = j.db.execute("SELECT start FROM transactions WHERE local=?", (local,)).fetchone()
    if (
        state.get("kind") != "normal"
        or state.get("source") != "local"
        or state.get("local_transaction") not in (None, local)
        or state.get("local_transaction") is None
        and owner.get("request_id") != state.get("id")
        or not (published or unpublished)
        or owner.get("local") != local
        or published
        and owner.get("stage") != "accepted"
        or owner.get("digest") != state.get("tag_digest")
        or transaction is None
        or fingerprint(json.loads(transaction["start"])["idTag"].casefold()) != state.get("tag_digest")
        or owner.get("request_id") not in (None, state.get("id"))
        or request is None
        or request["action"] != "start"
        or request["state"] not in ("accepted", "running", "uncertain", "completed", "failed")
        or request["state"] in ("accepted", "running", "uncertain")
        and owner.get("request_id") != state.get("id")
        or request["revision"] + int(published) != state.get("revision")
    ):
        return None
    return {key: state[key] for key in ("id", "revision", "tag_digest")} | {
        "local_transaction": local,
        "request_revision": request["revision"],
    }


# A persisted retry deadline avoids a tight loop and survives process replacement without resetting its budget.
def retry_deferred(j: Journal, item: Delivery, waits: dict[str, tuple[str, float]]) -> bool:
    records = j.get(RETRIES, {})
    for token in tuple(waits):
        if token not in records:
            waits.pop(token)
    record = records.get(item.token)
    if item.action != "StartTransaction" or record is None:
        return False
    if record.get("payload_digest") != fingerprint(item.payload) or record.get("local") != item.transaction:
        raise RecoveryRequiredError("Start retry does not identify its original delivery")
    if (
        type(record.get("errors")) is not int
        or not 0 <= record["errors"] < ATTEMPTS
        or type(record.get("next_epoch")) not in (int, float)
        or not math.isfinite(record["next_epoch"])
        or record["next_epoch"] < 0
    ):
        raise RecoveryRequiredError("Start retry budget or deadline is invalid")
    digest = fingerprint(record)
    previous = waits.get(item.token)
    if previous is not None and previous[0] == digest:
        return time.monotonic() < previous[1]
    # A wall-clock rollback cannot turn a short persisted backoff into an indefinite queue hold after restart.
    remaining = min(max(0.0, float(record["next_epoch"]) - time.time()), RETRY_INTERVAL * int(record["errors"]))
    waits[item.token] = (digest, time.monotonic() + remaining)
    return remaining > 0


# A correlated CALLERROR allows bounded retransmission; a lost reply remains ambiguous and never reaches this path.
def record_start_error(j: Journal, item: Delivery, answer: Any) -> bool:
    records = j.get(RETRIES, {})
    previous = records.get(item.token, {})
    errors = previous.get("errors", 0)
    if type(errors) is not int or not 0 <= errors < ATTEMPTS:
        raise RecoveryRequiredError("Start refusal has no valid remaining retry budget")
    count = errors + 1
    exhausted = count >= ATTEMPTS
    record = {
        "local": item.transaction,
        "payload_digest": fingerprint(item.payload),
        "reply_digest": fingerprint(answer),
        "errors": count,
        "next_epoch": time.time() + RETRY_INTERVAL * count,
    }
    with j.atomic():
        head = j.next_delivery()
        if head is None or head.token != item.token or head.state != "inflight" or head.payload != item.payload:
            raise RecoveryRequiredError("Start refusal does not identify the exact sent delivery")
        j.put(RETRIES, {**records, item.token: record})
        j.db.execute("UPDATE outbox SET state=? WHERE token=?", ("rejected" if exhausted else "queued", item.token))
        if exhausted:
            proofs, changed = j.get(PROOFS, {}), False
            for key, entry in proofs.items():
                proof = entry["proof"]
                if (
                    proof["kind"] == KIND
                    and entry.get("completed") is None
                    and not entry.get("superseded")
                    and proof["attestation"]["delivery_token"] == item.token
                ):
                    proofs[key] = {**entry, "superseded": {"reply_digest": record["reply_digest"]}}
                    changed = True
            if changed:
                j.put(PROOFS, proofs)
                j.put(
                    "managed_control_fault",
                    {"classification": "owner_control_or_delivery_uncertain", "at_epoch": time.time()},
                )
            latch_start_delivery_fault(j)
    return exhausted


# Mapping and denied authorization share the actual Start acknowledgment, without authorizing that session to draw.
def record_start_denial(j: Journal, item: Delivery, response: dict[str, Any]) -> None:
    if not j.db.in_transaction:
        raise RecoveryRequiredError("Start denial must share its mapping transaction")
    records = j.get(DENIALS, {})
    fault = j.get("managed_control_fault")
    created_fault = fault is None
    if created_fault:
        fault = {
            "classification": "owner_control_or_delivery_uncertain",
            "at_epoch": time.time(),
            "denied_start": item.token,
        }
        j.put("managed_control_fault", fault)
    j.put(
        DENIALS,
        {
            **records,
            str(item.transaction): {
                "local": item.transaction,
                "vendor": response["transactionId"],
                "start_token": item.token,
                "start_digest": fingerprint(item.payload),
                "reply_digest": fingerprint(response),
                "denied_at": time.time(),
                "completed": None,
                "fault": fault if fault.get("classification") == "owner_control_or_delivery_uncertain" else None,
            },
        },
    )
    control = j.get("control", {})
    j.put("control", {**control, "owner": "local", "inhibit": True, "uncertain": True})


# Only the active exact Stop-tool proof can bridge its original generic fault; no native or historic null row qualifies.
def denial_faults(j: Journal, fault: dict[str, Any]) -> list[dict[str, Any]]:
    if fault.get("classification") == "owner_control_or_delivery_uncertain":
        return [fault]
    if fault.get("classification") != "operator_debt_reconciled_at_zero":
        return []
    identifier = fault.get("proof")
    entry = j.get(PROOFS, {}).get(identifier)
    if not entry or entry.get("completed") is not None or entry.get("superseded"):
        return []
    proof = entry["proof"]
    prior = proof.get("prior", {}).get("fault")
    if (
        proof.get("installed_fault") != fault
        or proof.get("kind") not in ("stop-delivery", "stop-fault")
        or fingerprint(proof.get("attestation")) != identifier
        or not isinstance(prior, dict)
        or prior.get("classification") != "owner_control_or_delivery_uncertain"
    ):
        return []
    return [prior]


# Each associated denial needs its own genuine Stop; the legacy singleton can identify only one exact session.
def denied_start_evidence(c: ManagedController, fault: dict[str, Any]) -> dict[str, Any] | None:
    j = c.session.journal
    sources = denial_faults(j, fault)
    if len(sources) != 1 or j.next_delivery() is not None:
        return None
    bridged = {}
    if sources[0] != fault:
        # Import at the function boundary to avoid a module cycle; readiness never calls denial evidence.
        from .managed_debt_recovery import operator_recovery_evidence

        operator = operator_recovery_evidence(c, fault)
        if operator is None:
            return None
        bridged["operator_reconciliation"] = fingerprint(operator)
    fault = sources[0]
    members = []
    for denial in sorted(j.get(DENIALS, {}).values(), key=lambda value: value["local"]):
        if denial.get("fault") != fault or denial.get("completed") is not None:
            continue
        wire = denial.get("wire_ack") or j.get("managed_stop_wire_acknowledgment", {})
        row = j.db.execute("SELECT start,stop,vendor FROM transactions WHERE local=?", (denial["local"],)).fetchone()
        if (
            wire.get("fault") != fault
            or wire.get("local_transaction_id") != denial["local"]
            or wire.get("vendor_transaction_id") != denial["vendor"]
            or wire.get("start_digest") != denial["start_digest"]
            or row is None
            or row["stop"] is None
            or row["vendor"] != denial["vendor"]
            or fingerprint(json.loads(row["start"])) != denial["start_digest"]
            or fingerprint(json.loads(row["stop"])) != wire.get("stop_digest")
        ):
            return None
        members.append({"denial": denial, "wire": wire, "normal": denied_normal_identity(j, denial["local"])})
    if not members or sum(member["normal"] is not None for member in members) > 1:
        return None
    primary = next((member for member in members if member["normal"] is not None), members[0])
    return {**primary, "fault": fault, **bridged, **({"denials": members} if len(members) > 1 else {})}


# Consume each captured denial separately; a shared fault cannot substitute another session's Stop receipt.
def complete_denied_start(j: Journal, proof: dict[str, Any], pause: str) -> None:
    for member in proof.get("denials", [proof]):
        complete_one_denial(j, member, pause)


# Preserve terminal owner result bytes while completing only this exact denied lifecycle's stored evidence.
def complete_one_denial(j: Journal, proof: dict[str, Any], pause: str) -> None:
    records = j.get(DENIALS)
    key = str(proof["denial"]["local"])
    if records.get(key) != proof["denial"]:
        raise RecoveryRequiredError("Start denial proof no longer identifies its lifecycle")
    if proof.get("normal") is not None:
        if denied_normal_identity(j, proof["denial"]["local"]) != proof["normal"]:
            raise RecoveryRequiredError("denied owner metadata no longer identifies the closed lifecycle")
        j.put(
            "managed_test_charge",
            {
                **j.get("managed_test_charge"),
                "state": "completed",
                "charging": False,
                "local_transaction": proof["denial"]["local"],
                "recovery_reason": "denied_owned_session_closed_at_verified_zero",
            },
        )
        j.db.execute(
            "UPDATE managed_requests SET state='completed',result=? WHERE id=? AND action='start' "
            "AND revision=? AND state IN ('accepted','running','uncertain')",
            (
                json.dumps({"local_session_verified": True, "charging": False, "timed": False}, sort_keys=True),
                proof["normal"]["id"],
                proof["normal"]["request_revision"],
            ),
        )
    j.put(DENIALS, {**records, key: {**records[key], "completed": {"pause": pause, "at_epoch": time.time()}}})


# Postimages keep actual wire acknowledgments separate from the operator's permission to retry one exact event.
def start_resolution(j: Journal, evidence: dict[str, Any]) -> dict[str, Any]:
    local = evidence["local_transaction_id"]
    row = j.db.execute("SELECT local,start,stop,vendor FROM transactions WHERE local=?", (local,)).fetchone()
    return {
        "retry": j.get(RETRIES, {}).get(evidence["delivery_token"]),
        "retained": [
            dict(r)
            for r in j.db.execute(
                "SELECT token,action,payload,local FROM outbox WHERE local=? "
                "AND action IN ('StartTransaction','StopTransaction') ORDER BY seq",
                (local,),
            )
        ],
        "transaction": dict(row) if row is not None else None,
    }


# Repeated refusals or unknown replies cannot masquerade as actual successful Start and Stop delivery.
def record_start_wire_ack(j: Journal, item: Delivery, response: dict[str, Any]) -> None:
    if item.action not in ("StartTransaction", "StopTransaction"):
        return
    records, changed = j.get(PROOFS, {}), False
    if item.action == "StartTransaction":
        retries = j.get(RETRIES, {})
        if item.token in retries:
            j.put(RETRIES, {key: value for key, value in retries.items() if key != item.token})
    for key, entry in records.items():
        proof = entry["proof"]
        if (
            proof["kind"] != KIND
            or entry.get("completed") is not None
            or entry.get("superseded")
            or j.get("managed_control_fault") != proof["installed_fault"]
            or proof["attestation"]["local_transaction_id"] != item.transaction
        ):
            continue
        name = "start_wire_ack" if item.action == "StartTransaction" else "stop_wire_ack"
        wire = {"delivery_token": item.token, "reply_digest": fingerprint(response), "at_epoch": time.time()}
        proof = {**proof, name: wire}
        projection = start_resolution(j, proof["attestation"])
        projection["retained"] = [r for r in projection["retained"] if r["token"] != item.token]
        records[key] = {**entry, "proof": {**proof, "resolved": projection}}
        changed = True
    if changed:
        j.put(PROOFS, records)
