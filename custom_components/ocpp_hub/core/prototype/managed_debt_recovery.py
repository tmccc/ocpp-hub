"""Exclusive offline reconciliation retires one independently attested debt while retaining physical zero."""

from __future__ import annotations

import json
import math
import re
import time
from dataclasses import asdict
from typing import TYPE_CHECKING, Any

from .events import OBSERVATION_ACTIONS as REPORTS
from .local_control import local_start_receipt
from .managed_combined_start_recovery import KIND as COMBINED_KIND
from .managed_combined_start_recovery import combined_candidate, combined_resolution
from .managed_debt_evidence import ATTESTATIONS, DebtAttestation
from .managed_delivery_recovery import closed_normal_delivery, delivery_anchor
from .managed_denial_faults import (
    current_operator_proof,
    observation_resolution,
    operator_for_queued_lifecycle,
    operator_wire_settled,
    pending_mapping_headers,
    proof_headers_match,
)
from .managed_intent import unfinished_owner_requests
from .managed_native_repair import repair_pending, repair_revoked
from .managed_recovery_zero import require_original_zero
from .managed_start_delivery import KIND as START_KIND
from .managed_start_delivery import RETRIES, start_resolution
from .managed_stop_delivery import KINDS as STOP_KINDS
from .managed_stop_delivery import NO_REPLY, REJECTION, WIRE_ACK, stop_identity, stop_resolution
from .managed_storage_recovery import storage_candidate, storage_resolution
from .native_bounded import NativeBoundedSpec
from .store import Delivery, Journal, RecoveryRequiredError, fingerprint
from .trial_handover_evidence import HandoverEvidence
from .trial_resources import ResourceBook

if TYPE_CHECKING:
    from .managed import ManagedController

KEY = "managed_operator_debt_reconciliations"
FAULT = "operator_debt_reconciled_at_zero"
VENDOR_KINDS = ("vendor-write", "vendor-start")
LATER_VENDOR_DEBT = "later_vendor_debt"


# A later lifecycle may take over only after the earlier proof's own work settled.
def retry_predecessor(j: Journal, spec: NativeBoundedSpec, kind: str) -> dict[str, Any] | None:
    head = j.next_delivery()
    if head is None:
        return None
    if kind == START_KIND:
        if head.action != "StartTransaction" or head.state not in ("ambiguous", "rejected"):
            return None
    elif head.action != "StopTransaction" or head.state != "rejected":
        return None
    previous = operator_for_queued_lifecycle(j, j.fault.read({}), head)
    if (
        previous is None
        or previous["anchor"] != delivery_anchor(j, spec)
        or previous["resolved"] != resolved_debt(j, previous["kind"], previous)
    ):
        return None
    return previous


# Preserve exact private preimages in the durable proof without publishing raw tags, native bodies or credentials.
def debt_snapshot(j: Journal, spec: NativeBoundedSpec, kind: str) -> dict[str, Any]:
    grant = j.get("vendor_start_grant")
    row = j.db.execute("SELECT * FROM mutations WHERE token=?", ((grant or {}).get("mutation"),)).fetchone()
    mappings = pending_mapping_headers(j)
    if kind in VENDOR_KINDS:
        previous = vendor_predecessor(j, spec)
    else:
        previous = retry_predecessor(j, spec, kind) if kind in (START_KIND, "stop-delivery") else None
    order = "rowid" if kind == "vendor-write" else "token"
    mutations = [dict(r) for r in j.db.execute("SELECT * FROM mutations WHERE state!='confirmed' ORDER BY " + order)]
    separate_grant = kind == "vendor-write" and row is not None and row["state"] == "confirmed"
    snapshot = {
        "anchor": delivery_anchor(j, spec),
        "intent": j.get("managed_control_intent"),
        "control": j.get("control"),
        "fault": previous["prior"]["fault"] if previous is not None else j.fault.read(),
        "mutations": mutations,
        "marker": j.get("uncertain_vendor_command"),
        "grant": grant,
        "grant_mutation": dict(row) if row is not None and (kind == "vendor-start" or separate_grant) else None,
        "stop_rejection": j.get(REJECTION),
        "stop_wire_ack": j.get(WIRE_ACK),
        "start_retries": j.get(RETRIES),
        "denials": j.get("managed_start_denials"),
        "test": j.get("managed_test_charge"),
        "receipt": j.get("local_start_receipt"),
        "deferred": j.get("managed_deferred_start"),
        "requests": [dict(row) for row in sorted(unfinished_owner_requests(j), key=lambda row: row["id"])],
        "head": asdict(head) if (head := j.next_delivery()) is not None else None,
        "outbox": [dict(row) for row in j.db.execute("SELECT * FROM outbox ORDER BY seq")],
        "transactions": [dict(row) for row in j.db.execute("SELECT * FROM transactions ORDER BY local")],
        "kind": kind,
        **(
            {"previous_operator": {"id": previous["installed_fault"]["proof"], "digest": fingerprint(previous)}}
            if previous is not None
            else {}
        ),
        **({"delivery_mappings": mappings} if mappings else {}),
        **({"storage_resolution": storage_resolution(j)} if kind == "storage-fault" else {}),
    }
    if kind == "vendor-write" and (len(mutations) > 1 or previous is not None):
        snapshot["vendor_command_scopes"] = [
            dict(scope)
            for mutation in mutations
            if (
                scope := j.db.execute(
                    "SELECT * FROM vendor_command_scope WHERE mutation=?", (mutation["token"],)
                ).fetchone()
            )
            is not None
        ]
    if kind == "local-start" and len(snapshot["mutations"]) == 1:
        scope = j.db.execute(
            "SELECT * FROM vendor_command_scope WHERE mutation=?", (snapshot["mutations"][0]["token"],)
        ).fetchone()
        if scope is not None:
            snapshot["vendor_stop_scope"] = dict(scope)
        if stop := closed_vendor_stop(snapshot):
            snapshot["closed_vendor_stop"] = stop
    return snapshot


# A genuine closed mapped Stop with a drained lifecycle leaves no target for the exact restrictive command.
def closed_vendor_stop(snapshot: dict[str, Any]) -> dict[str, Any] | None:
    mutations, marker = snapshot["mutations"], snapshot["marker"]
    if (
        len(mutations) != 1
        or snapshot["outbox"]
        or snapshot["grant"]
        or snapshot["start_retries"]
        or snapshot.get("delivery_mappings")
        or any(entry.get("completed") is None for entry in (snapshot["denials"] or {}).values())
    ):
        return None
    mutation = mutations[0]
    if (
        mutation["origin"] != "vendor"
        or mutation["action"] != "RemoteStopTransaction"
        or mutation["state"] not in ("pending", "uncertain")
        or marker
        and (
            not isinstance(marker, dict)
            or marker.get("action") != mutation["action"]
            or marker.get("digest") != mutation["digest"]
            or marker.get("mutation", mutation["token"]) != mutation["token"]
        )
    ):
        return None
    scope = snapshot.get("vendor_stop_scope")
    if scope is not None and (
        type(scope["local"]) is not int
        or scope["local"] < 0
        or scope["wire_result"] not in ("dispatched", "call_error")
    ):
        return None
    matches = [
        row
        for row in snapshot["transactions"]
        if row["vendor"] is not None
        and (scope is None or row["local"] == scope["local"])
        and fingerprint({"transactionId": row["vendor"]}) == mutation["digest"]
    ]
    if len(matches) != 1 or matches[0]["stop"] is None:
        return None
    transaction = matches[0]
    stop = json.loads(transaction["stop"])
    if not isinstance(stop, dict) or stop.get("transactionId") != transaction["local"]:
        return None
    # Journal.stop enqueues this lifecycle atomically. Only a real cloud acknowledgment can drain it.
    return {
        "mutation_token": mutation["token"],
        "local_transaction_id": transaction["local"],
        "vendor_transaction_id": transaction["vendor"],
        "stop_digest": fingerprint(stop),
        "cloud_stop_acknowledged": True,
        "command_reply": "unknown",
    }


# Current work, other owner requests and original-session uncertainty cannot be waived by another debt's proof.
def require_common_debt_idle(j: Journal, kind: str, snapshot: dict[str, Any]) -> None:
    predecessor = snapshot.get("previous_operator")
    closed_stop = kind == "local-start" and snapshot.get("closed_vendor_stop") is not None
    if (
        any(
            entry.get("completed") is None
            and not entry.get("superseded")
            and (predecessor is None or key != predecessor["id"])
            for key, entry in j.get(KEY, {}).items()
        )
        or (snapshot["fault"] or {}).get("classification") not in (None, "owner_control_or_delivery_uncertain")
        or j.db.execute("SELECT 1 FROM transactions WHERE stop IS NULL LIMIT 1").fetchone()
        or kind not in ("observation", "stop-delivery", START_KIND, COMBINED_KIND)
        and j.db.execute("SELECT 1 FROM transactions WHERE vendor IS NULL LIMIT 1").fetchone()
        or not j.current_change.read().settled_or_missing
        or j.get("managed_current_prepare_intent", {}).get("stage") not in (None, "complete")
        or kind != "local-start"
        and j.get("control", {}).get("start_pending")
        or kind != "local-start"
        and j.get("managed_test_charge", {}).get("state") not in (None, "completed")
        and not (kind == START_KIND and closed_normal_delivery(j))
        or kind not in ("observation", "stop-delivery", START_KIND, COMBINED_KIND)
        and j.next_delivery() is not None
        or kind not in ("vendor-write", COMBINED_KIND)
        and not closed_stop
        and j.get("uncertain_vendor_command")
        or kind not in ("vendor-write", "vendor-start")
        and j.get("vendor_start_grant")
        or kind not in ("vendor-write", COMBINED_KIND)
        and not closed_stop
        and j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
    ):
        raise RecoveryRequiredError("operator reconciliation cannot waive another unsettled operation")


# Removing a legacy report may unblock exact queued closed lifecycle pairs; orphan or ambiguous tails stay quarantined.
def require_observation_tail(j: Journal) -> None:
    for row in j.db.execute("SELECT local,start,stop FROM transactions WHERE vendor IS NULL"):
        pair = j.db.execute(
            "SELECT action,payload,state FROM outbox WHERE local=? "
            "AND action IN ('StartTransaction','StopTransaction') "
            "ORDER BY seq",
            (row["local"],),
        ).fetchall()
        if (
            len(pair) != 2
            or pair[0]["action"] != "StartTransaction"
            or pair[1]["action"] != "StopTransaction"
            or pair[0]["payload"] != row["start"]
            or pair[1]["payload"] != row["stop"]
            or any(item["state"] != "queued" for item in pair)
        ):
            raise RecoveryRequiredError("legacy report recovery cannot waive orphaned or ambiguous lifecycle debt")
    if j.db.execute(
        "SELECT 1 FROM outbox WHERE action IN ('StartTransaction','StopTransaction') AND state!='queued' LIMIT 1"
    ).fetchone():
        raise RecoveryRequiredError("legacy report recovery retains ambiguous or rejected lifecycle quarantine")


# The visibility marker can belong to a later command. A legacy digest must identify just one retained row.
def vendor_marker_token(snapshot: dict[str, Any]) -> str | None:
    marker = snapshot["marker"]
    if not marker:
        return None
    matches = [
        row["token"]
        for row in snapshot["mutations"]
        if isinstance(marker, dict)
        and marker.get("action") == row["action"]
        and marker.get("digest") == row["digest"]
        and marker.get("mutation", row["token"]) == row["token"]
    ]
    if len(matches) != 1:
        raise RecoveryRequiredError("vendor visibility marker belongs to another write")
    return str(matches[0])


# Attest one exact vendor write at a time, in the order in which its durable command was recorded.
def vendor_candidate(snapshot: dict[str, Any]) -> dict[str, Any]:
    rows = snapshot["mutations"]
    if not rows or any(row["origin"] != "vendor" or row["state"] not in ("pending", "uncertain") for row in rows):
        raise RecoveryRequiredError("operator reconciliation requires unresolved vendor writes only")
    vendor_marker_token(snapshot)
    grant = snapshot["grant"]
    if grant:
        starts = [row for row in rows if row["token"] == grant.get("mutation")]
        if not starts and snapshot["grant_mutation"] is not None:
            starts = [snapshot["grant_mutation"]]
        if (
            grant.get("local") is not None
            or len(starts) != 1
            or starts[0]["origin"] != "vendor"
            or starts[0]["action"] != "RemoteStartTransaction"
        ):
            raise RecoveryRequiredError("a bound or unrelated vendor Start grant cannot be retired")
    row = rows[0]
    return {"mutation_token": row["token"], "action": row["action"], "payload_digest": row["digest"]}


# A closed mapped session can no longer consume this exact cloud grant through its deferred binding.
def closed_vendor_start_binding(snapshot: dict[str, Any], grant: dict[str, Any]) -> bool:
    deferred = snapshot["deferred"]
    if (
        not isinstance(deferred, dict)
        or deferred.get("owner") is not False
        or deferred.get("vendor") != grant["token"]
        or type(deferred.get("local")) is not int
    ):
        return False
    for transaction in snapshot["transactions"]:
        if transaction["local"] != deferred["local"]:
            continue
        if transaction["stop"] is None or transaction["vendor"] is None:
            return False
        tag = json.loads(transaction["start"]).get("idTag")
        return isinstance(tag, str) and fingerprint(tag.casefold()) == grant["digest"]
    return False


# Confirmed delivery is not command consumption; pin its actual still-unbound grant and mutation separately.
def vendor_start_candidate(snapshot: dict[str, Any]) -> dict[str, Any]:
    grant, row = snapshot["grant"] or {}, snapshot["grant_mutation"] or {}
    if (
        not grant
        or grant.get("local") is not None
        or not isinstance(grant.get("token"), str)
        or re.fullmatch(r"[a-f0-9]{32}", grant["token"]) is None
        or not isinstance(grant.get("digest"), str)
        or re.fullmatch(r"[a-f0-9]{64}", grant["digest"]) is None
        or type(grant.get("expires")) not in (int, float)
        or not math.isfinite(grant["expires"])
        or row.get("token") != grant.get("mutation")
        or row.get("origin") != "vendor"
        or row.get("action") != "RemoteStartTransaction"
        or row.get("state") != "confirmed"
        or snapshot["mutations"]
        or snapshot["marker"]
        or snapshot["deferred"]
        and not closed_vendor_start_binding(snapshot, grant)
    ):
        raise RecoveryRequiredError("vendor Start requires its exact accepted, unbound command and no other debt")
    return {
        "mutation_token": row["token"],
        "action": row["action"],
        "payload_digest": row["digest"],
        "grant_token": grant["token"],
        "grant_digest": fingerprint(grant),
    }


# A retained exact Stop permits only retry; a prior drained Stop needs its actual fault-bound wire receipt.
def require_stop_tail(j: Journal, head: Delivery) -> None:
    rows = j.db.execute("SELECT * FROM outbox ORDER BY seq").fetchall()
    if not rows or rows[0]["token"] != head.token:
        raise RecoveryRequiredError("Stop recovery no longer identifies its exact outbox head")
    for row in j.db.execute("SELECT local,start,stop FROM transactions WHERE vendor IS NULL"):
        pair = [
            item
            for item in rows[1:]
            if item["local"] == row["local"] and item["action"] in ("StartTransaction", "StopTransaction")
        ]
        if (
            row["stop"] is None
            or len(pair) != 2
            or pair[0]["action"] != "StartTransaction"
            or pair[1]["action"] != "StopTransaction"
            or pair[0]["payload"] != row["start"]
            or pair[1]["payload"] != row["stop"]
            or any(item["state"] != "queued" for item in pair)
        ):
            raise RecoveryRequiredError("Stop recovery retains ambiguous or orphaned queued lifecycle debt")
    for item in rows[1:]:
        if item["state"] != "queued":
            raise RecoveryRequiredError("Stop recovery cannot waive another ambiguous or rejected delivery")
        transaction = j.db.execute("SELECT * FROM transactions WHERE local=?", (item["local"],)).fetchone()
        if item["action"] in REPORTS:
            if item["local"] is not None and transaction is None:
                raise RecoveryRequiredError("queued report has no original transaction")
            if (
                transaction is not None
                and transaction["vendor"] is None
                and not any(
                    prior["seq"] < item["seq"]
                    and prior["local"] == item["local"]
                    and prior["action"] == "StartTransaction"
                    for prior in rows[1:]
                )
            ):
                raise RecoveryRequiredError("queued report precedes its unmapped Start")
        elif item["action"] == "StopTransaction" and transaction is not None and transaction["vendor"] is not None:
            stop_identity(
                j, Delivery(item["token"], item["action"], json.loads(item["payload"]), item["local"], item["state"])
            )
        elif (
            item["action"] not in ("StartTransaction", "StopTransaction")
            or transaction is None
            or transaction["vendor"] is not None
        ):
            raise RecoveryRequiredError("Stop recovery retains an unrecognized lifecycle tail")


# An exact retained Stop may be retried without dropping any later definitely queued closed lifecycle pair.
def stop_candidate(j: Journal, snapshot: dict[str, Any], kind: str) -> dict[str, Any]:
    fault = snapshot["fault"] or {}
    item: Delivery | None = None
    if fault.get("classification") != "owner_control_or_delivery_uncertain":
        raise RecoveryRequiredError("Stop recovery requires the exact latched delivery fault")
    if kind == "stop-fault":
        wire = snapshot["stop_wire_ack"] or {}
        if wire.get("fault") != fault or snapshot["head"] is not None:
            raise RecoveryRequiredError("drained Stop fault requires its preserved actual wire acknowledgment")
        row = j.db.execute(
            "SELECT stop FROM transactions WHERE local=?", (wire.get("local_transaction_id"),)
        ).fetchone()
        if row is None or row["stop"] is None:
            raise RecoveryRequiredError("wire receipt no longer has its original Stop")
        item = Delivery(
            wire["delivery_token"], "StopTransaction", json.loads(row["stop"]), wire["local_transaction_id"], "queued"
        )
        identity = stop_identity(j, item)
        if not all(wire.get(key) == value for key, value in identity.items()):
            raise RecoveryRequiredError("wire receipt does not identify the exact original Stop")
        reply = wire["reply_digest"]
    else:
        item = j.next_delivery()
        if item is None or item.action != "StopTransaction" or item.state not in ("queued", "inflight", "rejected"):
            raise RecoveryRequiredError("Stop retry requires the exact retained closed lifecycle head")
        if j.db.execute("SELECT 1 FROM delivery_replies WHERE token=?", (item.token,)).fetchone():
            raise RecoveryRequiredError("Stop retry requires ordinary settlement of its saved reply first")
        identity = stop_identity(j, item)
        require_stop_tail(j, item)
        rejection = snapshot["stop_rejection"] or {}
        if rejection and rejection.get("delivery_token") == item.token:
            if not all(rejection.get(key) == value for key, value in identity.items()):
                raise RecoveryRequiredError("Stop rejection belongs to another lifecycle")
            reply = rejection["reply_digest"]
        else:
            reply = NO_REPLY
    return {**identity, "reply_digest": reply, "fault_digest": fingerprint(fault)}


# Time, zero and closed sessions alone never identify the accepted owner Start; its exact receipts remain mandatory.
def local_start_candidate(j: Journal, snapshot: dict[str, Any]) -> dict[str, Any]:
    state, receipt = snapshot["test"] or {}, local_start_receipt(j)
    requests = snapshot["requests"]
    deadline = state.get("hard_deadline_epoch")
    published = state.get("start_rpc_accepted") is True
    unpublished = (
        state.get("start_rpc_accepted") is False
        and state.get("start_attempted") is True
        and receipt.get("stage") in ("issued", "accepted")
        and receipt.get("request_id") == state.get("id")
    )
    if (
        state.get("kind") != "normal"
        or state.get("source") != "local"
        or not (published or unpublished)
        or state.get("local_transaction") is not None
        or state.get("state") not in ("expired", "interrupted", "stopping", "awaiting_power")
        or not isinstance(deadline, (int, float))
        or isinstance(deadline, bool)
        or not 0 < deadline <= time.time()
        or len(requests) != 1
        or requests[0]["id"] != state.get("id")
        or requests[0]["action"] != "start"
        or state.get("revision") != requests[0]["revision"] + int(published)
        or published
        and receipt.get("stage") != "accepted"
        or receipt.get("request_id") not in (None, state.get("id"))
        or receipt.get("local") is not None
        or not receipt.get("ticket")
        or receipt.get("digest") != state.get("tag_digest")
        or not isinstance(state.get("tag_digest"), str)
        or len(state["tag_digest"]) != 64
        or j.get("control", {}).get("start_pending") is not True
        or snapshot["grant"]
        or snapshot["mutations"]
        and snapshot.get("closed_vendor_stop") is None
    ):
        raise RecoveryRequiredError("owner Start needs its exact accepted, unbound and expired command receipts")
    deferred = snapshot["deferred"]
    if deferred:
        row = j.db.execute(
            "SELECT start,stop,vendor FROM transactions WHERE local=?", (deferred.get("local"),)
        ).fetchone()
        if (
            set(deferred) != {"local", "owner", "ticket", "vendor"}
            or deferred["owner"] is not True
            or deferred["ticket"] != receipt["ticket"]
            or deferred["vendor"] is not None
            or row is None
            or row["stop"] is None
            or row["vendor"] is None
            or fingerprint(json.loads(row["start"])["idTag"].casefold()) != state["tag_digest"]
        ):
            raise RecoveryRequiredError("deferred Start does not belong to this receipt and a genuine closed session")
    return {
        "request_id": requests[0]["id"],
        "request_digest": fingerprint(requests[0]),
        "state_digest": fingerprint(state),
        "receipt_digest": fingerprint(receipt),
    }


# Exact independent nonacceptance can authorize retry, never erase a Start or fabricate its cloud mapping.
def start_retry_candidate(j: Journal, snapshot: dict[str, Any]) -> dict[str, Any]:
    head, fault = j.next_delivery(), snapshot["fault"] or {}
    if (
        head is None
        or head.action != "StartTransaction"
        or head.state not in ("rejected", "ambiguous")
        or fault.get("classification") != "owner_control_or_delivery_uncertain"
    ):
        raise RecoveryRequiredError("Start retry needs its exact quarantined delivery head and fault")
    if j.db.execute("SELECT 1 FROM delivery_replies WHERE token=?", (head.token,)).fetchone():
        raise RecoveryRequiredError("Start retry requires ordinary settlement of its saved reply first")
    row = j.db.execute("SELECT start,stop,vendor FROM transactions WHERE local=?", (head.transaction,)).fetchone()
    rows = snapshot["outbox"]
    if row is None or row["stop"] is None or row["vendor"] is not None or json.loads(row["start"]) != head.payload:
        raise RecoveryRequiredError("Start retry requires its genuine closed unmapped lifecycle")
    for transaction in j.db.execute("SELECT local,start,stop FROM transactions WHERE vendor IS NULL"):
        pair = [
            r
            for r in rows
            if r["local"] == transaction["local"] and r["action"] in ("StartTransaction", "StopTransaction")
        ]
        if (
            transaction["stop"] is None
            or len(pair) != 2
            or [r["action"] for r in pair] != ["StartTransaction", "StopTransaction"]
            or pair[0]["payload"] != transaction["start"]
            or pair[1]["payload"] != transaction["stop"]
            or any(r["state"] != "queued" for r in pair if r["token"] != head.token)
        ):
            raise RecoveryRequiredError("Start retry cannot waive another ambiguous or orphaned lifecycle")
    for queued in rows[1:]:
        if queued["state"] != "queued" or queued["action"] not in (*REPORTS, "StartTransaction", "StopTransaction"):
            raise RecoveryRequiredError("Start retry retains unrelated delivery uncertainty")
        if (
            queued["local"] is not None
            and j.db.execute("SELECT 1 FROM transactions WHERE local=?", (queued["local"],)).fetchone() is None
        ):
            raise RecoveryRequiredError("queued report has no original transaction")
    retry = (snapshot["start_retries"] or {}).get(head.token, {})
    payload_digest = fingerprint(head.payload)
    reply_key = "quarantined_reply_digest" if head.state == "ambiguous" else "reply_digest"
    if retry and (retry.get("local"), retry.get("payload_digest")) != (head.transaction, payload_digest):
        raise RecoveryRequiredError("Start retry reply does not identify its original delivery")
    return {
        "delivery_token": head.token,
        "local_transaction_id": head.transaction,
        "payload_digest": payload_digest,
        "start_digest": fingerprint(json.loads(row["start"])),
        "stop_digest": fingerprint(json.loads(row["stop"])),
        "reply_digest": retry.get(reply_key, NO_REPLY),
        "fault_digest": fingerprint(fault),
    }


# Inspection returns only correlation digests; a fresh independent observer and exclusive Journal owner are required.
def debt_candidate(
    j: Journal, book: ResourceBook, spec: NativeBoundedSpec, n: HandoverEvidence, kind: str
) -> dict[str, Any]:
    if kind not in ATTESTATIONS:
        raise RecoveryRequiredError("unknown operator reconciliation kind")
    require_original_zero(j, book, spec, n)
    snapshot = debt_snapshot(j, spec, kind)
    require_common_debt_idle(j, kind, snapshot)
    if kind == COMBINED_KIND:
        specific = combined_candidate(j, snapshot)
    elif kind == "local-start":
        specific = local_start_candidate(j, snapshot)
    elif kind == "storage-fault":
        specific = storage_candidate(j, snapshot)
    elif kind == START_KIND:
        if snapshot["requests"]:
            raise RecoveryRequiredError("an unrelated management request still requires reconciliation")
        specific = start_retry_candidate(j, snapshot)
    else:
        if snapshot["requests"]:
            raise RecoveryRequiredError("an unrelated management request still requires reconciliation")
        if kind == "vendor-write":
            specific = vendor_candidate(snapshot)
        elif kind == "vendor-start":
            specific = vendor_start_candidate(snapshot)
        elif kind in STOP_KINDS:
            specific = stop_candidate(j, snapshot, kind)
        else:
            require_observation_tail(j)
            head = j.next_delivery()
            if (
                head is None
                or head.action not in REPORTS
                or head.state != "rejected"
                or (snapshot["fault"] or {}).get("classification") != "owner_control_or_delivery_uncertain"
            ):
                raise RecoveryRequiredError("legacy recovery requires one exact rejected routine delivery head")
            specific = {
                "observation_token": head.token,
                "action": head.action,
                "payload_digest": fingerprint(head.payload),
            }
    return {
        "identity_digest": fingerprint(j.identity),
        "spec_digest": spec.digest,
        "candidate_digest": fingerprint(snapshot),
        **specific,
    }


# This postimage excludes later Pause revisions and routine queue drain, but pins every debt retired by the tool.
def resolved_debt(j: Journal, kind: str, proof: dict[str, Any]) -> dict[str, Any]:
    e = proof["attestation"]
    if kind == COMBINED_KIND:
        return combined_resolution(j, e, proof.get("wire_ack"))
    if kind == "storage-fault":
        return storage_resolution(j)
    if kind in STOP_KINDS:
        return stop_resolution(j, e, proof.get("wire_ack"))
    if kind == START_KIND:
        return start_resolution(j, e)
    if kind in ("vendor-write", "vendor-start"):
        row = j.db.execute("SELECT * FROM mutations WHERE token=?", (e["mutation_token"],)).fetchone()
        resolution = {
            "mutation": dict(row) if row is not None else None,
            "marker": j.get("uncertain_vendor_command"),
            "grant": j.get("vendor_start_grant"),
        }
        if kind == "vendor-start" and proof["prior"].get("deferred"):
            resolution["deferred"] = j.get("managed_deferred_start")
        return resolution
    if kind == "local-start":
        row = j.db.execute("SELECT * FROM managed_requests WHERE id=?", (e["request_id"],)).fetchone()
        resolution = {
            "request": dict(row) if row is not None else None,
            "test": j.get("managed_test_charge"),
            "receipt": j.get("local_start_receipt"),
            "deferred": j.get("managed_deferred_start"),
        }
        if stop := proof["prior"].get("closed_vendor_stop"):
            # Confirmed rows may be pruned, but a surviving unresolved row would contradict this proof.
            resolution["closed_vendor_stop_pending"] = (
                j.db.execute(
                    "SELECT 1 FROM mutations WHERE token=? AND state!='confirmed'", (stop["mutation_token"],)
                ).fetchone()
                is not None
            )
        return resolution
    return observation_resolution(j, e["observation_token"])


# The exact command row stays immutable even after later steps change the visibility marker or Start grant.
def settled_vendor_write(j: Journal, proof: dict[str, Any], identifier: str) -> bool:
    if proof.get("kind") != "vendor-write" or not proof_headers_match(proof, identifier):
        return False
    token = proof["attestation"]["mutation_token"]
    row = j.db.execute("SELECT * FROM mutations WHERE token=? AND state='confirmed'", (token,)).fetchone()
    return row is not None and dict(row) == proof["resolved"]["mutation"]


# Follow only linked, still-retained vendor attestations back to the first command.
def vendor_predecessors(j: Journal, proof: dict[str, Any], records: dict[str, Any]) -> list[str] | None:
    seen: list[str] = []
    anchor = proof["anchor"]
    while (link := proof["prior"].get("previous_operator")) is not None:
        prior_id = link["id"]
        entry = records.get(prior_id, {})
        prior = entry.get("proof")
        marker = entry.get("superseded", {})
        if (
            prior_id in seen
            or prior is None
            or prior["anchor"] != anchor
            or link["digest"] != fingerprint(prior)
            or marker.get("reason") != LATER_VENDOR_DEBT
            or marker.get("proof") != fingerprint(proof["attestation"])
            or not settled_vendor_write(j, prior, prior_id)
        ):
            return None
        seen.append(prior_id)
        proof = prior
    return seen


# Take over only after the current proof and all earlier command outcomes still agree with the stopped journal.
def vendor_predecessor(j: Journal, spec: NativeBoundedSpec) -> dict[str, Any] | None:
    fault = j.fault.read({})
    proof = current_operator_proof(j, fault)
    if (
        proof is None
        or proof["kind"] != "vendor-write"
        or proof["anchor"] != delivery_anchor(j, spec)
        or repair_pending(j, fault)
        or repair_revoked(j, fault)
        or proof["resolved"] != resolved_debt(j, proof["kind"], proof)
        or vendor_predecessors(j, proof, j.get(KEY, {})) is None
    ):
        return None
    return proof


# Identify the exact next vendor debt, or its still-unacknowledged proof, without auditing or changing history.
def vendor_history_recovery_scope(
    j: Journal, spec: NativeBoundedSpec, *, retiring_history_fault: bool
) -> dict[str, Any] | None:
    fault = j.fault.read({})
    write = j.db.execute("SELECT 1 FROM mutations WHERE origin='vendor' AND state!='confirmed' LIMIT 1").fetchone()
    grant = j.get("vendor_start_grant")
    if not write and not grant:
        proof = settled_operator_recovery(j, spec, fault)
        if proof is None or proof["kind"] not in VENDOR_KINDS:
            return None
        kind = proof["kind"]
    else:
        proof = None
        kind = "vendor-write" if write else "vendor-start"
    if fault and (repair_pending(j, fault) or repair_revoked(j, fault)):
        raise RecoveryRequiredError("vendor history recovery requires settled native repair evidence")
    snapshot = debt_snapshot(j, spec, kind)
    if proof is not None:
        identifier = fingerprint(proof["attestation"])
        snapshot["previous_operator"] = {"id": identifier}
        snapshot["fault"] = proof["prior"]["fault"]
    elif retiring_history_fault:
        # The history owner may retire its own exact fault in the same transaction as the archive.
        snapshot["fault"] = None
    require_common_debt_idle(j, kind, snapshot)
    if snapshot["requests"]:
        raise RecoveryRequiredError("an unrelated management request still requires reconciliation")
    if write and any(row["state"] == "pending" for row in snapshot["mutations"]):
        # Journal startup marks interrupted recorded dispatches uncertain. A bare intent has no such evidence.
        raise RecoveryRequiredError("history repair requires a recorded uncertain vendor outcome")
    if proof is not None:
        return {"kind": kind, "proof": identifier}
    candidate = vendor_candidate(snapshot) if write else vendor_start_candidate(snapshot)
    return {"kind": kind, **candidate}


# Reconciliation records an operator-verified resolution, never successful execution, replay, Start, Stop or Return.
def reconcile_debt(
    j: Journal, book: ResourceBook, spec: NativeBoundedSpec, n: HandoverEvidence, kind: str, evidence: DebtAttestation
) -> dict[str, Any]:
    cls = ATTESTATIONS.get(kind)
    if cls is None or type(evidence) is not cls:
        raise RecoveryRequiredError("operator evidence does not match the requested debt kind")
    e = asdict(cls.parse(asdict(evidence), recent=False))
    require_original_zero(j, book, spec, n)
    records, identifier = j.get(KEY, {}), fingerprint(e)
    if identifier in records:
        proof = records[identifier]["proof"]
        if records[identifier].get("superseded"):
            if (
                kind == "vendor-write"
                and records[identifier]["superseded"].get("reason") == LATER_VENDOR_DEBT
                and proof["anchor"] == delivery_anchor(j, spec)
                and settled_vendor_write(j, proof, identifier)
            ):
                return {"already_recorded": True, "zero_hold_retained": True, "charging_started": False}
            raise RecoveryRequiredError("operator proof was superseded by later delivery recovery")
        if (
            proof["kind"] != kind
            or proof["anchor"] != delivery_anchor(j, spec)
            or proof["resolved"] != resolved_debt(j, kind, proof)
        ):
            raise RecoveryRequiredError("previous operator debt proof no longer matches its recorded resolution")
        return {"already_recorded": True, "zero_hold_retained": True, "charging_started": False}
    cls.parse(e)
    candidate = debt_candidate(j, book, spec, n, kind)
    if any(e.get(key) != value for key, value in candidate.items()):
        raise RecoveryRequiredError("operator evidence does not identify the exact current debt")
    snapshot = debt_snapshot(j, spec, kind)
    closed_start = kind == START_KIND and closed_normal_delivery(j)
    fault = (
        snapshot["fault"]
        if kind == COMBINED_KIND
        else {"classification": FAULT, "proof": identifier, "at_epoch": time.time()}
    )
    proof = {
        "kind": kind,
        "attestation": e,
        "prior": snapshot,
        "anchor": delivery_anchor(j, spec),
        "native_digest": n.digest,
        "native_observed_at": n.physical.observed_at,
        "installed_fault": fault,
        "recorded_at": time.time(),
    }
    if kind == "observation":
        # Journal owns the exact dequeue transaction; capture the proof there too via the caller's transaction below.
        head = j.next_delivery()
        assert head is not None
    with j.atomic():
        if snapshot != debt_snapshot(j, spec, kind) or records != j.get(KEY, {}):
            raise RecoveryRequiredError("operator reconciliation authority changed")
        if kind == COMBINED_KIND:
            j.reconcile_mutation(e["mutation_token"])
            j.db.execute("DELETE FROM meta WHERE key='uncertain_vendor_command'")
            current = j.next_delivery()
            assert current is not None
            j.acknowledge_in_transaction(
                current, {"transactionId": e["vendor_transaction_id"], "idTagInfo": {"status": "Accepted"}}
            )
            proof["wire_ack"] = None
        elif kind == "vendor-write":
            j.reconcile_mutation(e["mutation_token"])
            if vendor_marker_token(snapshot) == e["mutation_token"]:
                j.db.execute("DELETE FROM meta WHERE key='uncertain_vendor_command'")
            if (snapshot["grant"] or {}).get("mutation") == e["mutation_token"]:
                j.db.execute("DELETE FROM meta WHERE key='vendor_start_grant'")
        elif kind == "vendor-start":
            j.db.execute("DELETE FROM meta WHERE key='vendor_start_grant'")
            if snapshot["deferred"]:
                j.put("managed_deferred_start", {})
        elif kind == "stop-delivery":
            j.db.execute("UPDATE outbox SET state='queued' WHERE token=?", (e["delivery_token"],))
            proof["wire_ack"] = None
        elif kind == "stop-fault":
            proof["wire_ack"] = snapshot["stop_wire_ack"]
        elif kind == START_KIND:
            retries = j.get(RETRIES, {})
            j.put(
                RETRIES,
                {
                    **retries,
                    e["delivery_token"]: {
                        "local": e["local_transaction_id"],
                        "payload_digest": e["payload_digest"],
                        "reply_digest": e["reply_digest"],
                        "errors": 0,
                        "next_epoch": 0,
                    },
                },
            )
            j.db.execute("UPDATE outbox SET state='queued' WHERE token=?", (e["delivery_token"],))
            if closed_start:
                j.starts.complete_metadata(snapshot["test"], reason="operator_verified_closed_start_delivery")
        elif kind == "local-start":
            j.starts.operator_no_pending(snapshot["receipt"], snapshot["test"], identifier, e["request_id"])
            if snapshot["deferred"]:
                j.put("managed_deferred_start", {})
            if stop := snapshot.get("closed_vendor_stop"):
                j.reconcile_mutation(stop["mutation_token"])
                j.db.execute("DELETE FROM meta WHERE key='uncertain_vendor_command'")
        elif kind != "storage-fault":
            assert head is not None
            j.db.execute(
                "INSERT INTO rejected_observations VALUES(?,?,?,?,?,?,?)",
                (head.token, head.action, fingerprint(head.payload), head.transaction, None, "rejected", time.time()),
            )
            j.db.execute("DELETE FROM outbox WHERE token=?", (head.token,))
        control = j.get("control")
        j.starts.operator_control(control)
        if predecessor := snapshot.get("previous_operator"):
            key = predecessor["id"]
            reason = (
                LATER_VENDOR_DEBT
                if kind in VENDOR_KINDS
                else ("later_start_retry" if kind == START_KIND else "later_stop_retry")
            )
            records[key] = {
                **records[key],
                "superseded": {
                    "reason": reason,
                    "proof": identifier,
                    "at_epoch": time.time(),
                },
            }
        j.fault.replace(fault)
        proof["resolved"] = resolved_debt(j, kind, proof)
        j.put(KEY, {**records, identifier: {"proof": proof, "completed": None}})
    return {"already_recorded": False, "zero_hold_retained": True, "charging_started": False}


# A new Pause can acknowledge one exact proof only after its retained lifecycle and all unrelated debts settle.
def operator_recovery_evidence(c: ManagedController, fault: dict[str, Any]) -> dict[str, Any] | None:
    return settled_operator_recovery(c.session.journal, c.native.spec, fault)


# Read the durable outcome independently of the running service or the Pause that will acknowledge it.
def settled_operator_recovery(j: Journal, spec: NativeBoundedSpec, fault: dict[str, Any]) -> dict[str, Any] | None:
    records = j.get(KEY, {})
    if fault.get("classification") == "owner_control_or_delivery_uncertain":
        matches = [
            (identifier, entry)
            for identifier, entry in records.items()
            if entry.get("completed") is None
            and not entry.get("superseded")
            and entry["proof"]["kind"] == COMBINED_KIND
            and entry["proof"]["installed_fault"] == fault
        ]
        if len(matches) != 1:
            return None
        identifier, entry = matches[0]
    elif fault.get("classification") == FAULT:
        if current_operator_proof(j, fault) is None:
            return None
        identifier = fault.get("proof")
        entry = records.get(identifier)
    else:
        return None
    if not entry or entry.get("completed") is not None or entry.get("superseded"):
        return None
    proof: dict[str, Any] = entry["proof"]
    if not isinstance(identifier, str) or not proof_headers_match(proof, identifier):
        return None
    if proof["kind"] == COMBINED_KIND and proof["prior"].get("fault") != fault:
        return None
    if proof["kind"] in VENDOR_KINDS and (
        j.get("uncertain_vendor_command")
        or j.get("vendor_start_grant")
        or vendor_predecessors(j, proof, records) is None
    ):
        return None
    if (
        not operator_wire_settled(proof)
        or proof["installed_fault"] != fault
        or proof["anchor"] != delivery_anchor(j, spec)
        or proof["resolved"] != resolved_debt(j, proof["kind"], proof)
        or j.next_delivery() is not None
        or j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
        or j.db.execute("SELECT 1 FROM transactions WHERE stop IS NULL OR vendor IS NULL LIMIT 1").fetchone()
        or j.get("managed_control_intent", {}).get("mode") != "paused"
    ):
        return None
    return proof


# Consume the operator proof inside the same atomic commit which acknowledges the new Pause at physical zero.
def complete_operator_recovery(j: Journal, proof: dict[str, Any], pause: str) -> None:
    records = j.get(KEY)
    identifier = fingerprint(proof["attestation"])
    if records[identifier] != {"proof": proof, "completed": None}:
        raise RecoveryRequiredError("operator reconciliation acknowledgement was already consumed")
    completed = {"pause": pause, "at_epoch": time.time()}
    if proof["kind"] in VENDOR_KINDS:
        predecessors = vendor_predecessors(j, proof, records)
        if predecessors is None:
            raise RecoveryRequiredError("vendor command acknowledgement no longer matches its earlier outcomes")
        for previous in predecessors:
            records[previous] = {**records[previous], "completed": completed}
    j.put(KEY, {**records, identifier: {"proof": proof, "completed": completed}})
