"""Exclusive offline reconciliation retires one independently attested debt while retaining physical zero."""

from __future__ import annotations

import json
import time
from dataclasses import asdict
from typing import TYPE_CHECKING, Any

from .local_control import local_start_receipt
from .managed_cutoffs import supervised_timer
from .managed_debt_evidence import ATTESTATIONS, DebtAttestation
from .managed_delivery_recovery import delivery_anchor
from .managed_occurrences import occurrences
from .managed_recovery_zero import require_original_zero
from .native_bounded import NativeBoundedSpec
from .store import Journal, RecoveryRequiredError, encoded, fingerprint
from .trial_handover_evidence import HandoverEvidence
from .trial_resources import ResourceBook

if TYPE_CHECKING:
    from .managed import ManagedController

KEY = "managed_operator_debt_reconciliations"
FAULT = "operator_debt_reconciled_at_zero"
REPORTS = ("StatusNotification", "MeterValues", "FirmwareStatusNotification", "DiagnosticsStatusNotification")


# Preserve exact private preimages in the durable proof without publishing raw tags, native bodies or credentials.
def debt_snapshot(j: Journal, spec: NativeBoundedSpec, kind: str) -> dict[str, Any]:
    return {
        "anchor": delivery_anchor(j, spec),
        "intent": j.get("managed_control_intent"),
        "control": j.get("control"),
        "fault": j.get("managed_control_fault"),
        "mutations": [dict(r) for r in j.db.execute("SELECT * FROM mutations WHERE state!='confirmed' ORDER BY token")],
        "marker": j.get("uncertain_vendor_command"),
        "grant": j.get("vendor_start_grant"),
        "test": j.get("managed_test_charge"),
        "receipt": j.get("local_start_receipt"),
        "deferred": j.get("managed_deferred_start"),
        "requests": [
            dict(r)
            for r in j.db.execute(
                "SELECT * FROM managed_requests WHERE action!='pause' AND state IN ('accepted','running','uncertain') "
                "ORDER BY id"
            )
        ],
        "head": asdict(head) if (head := j.next_delivery()) is not None else None,
        "outbox": [dict(row) for row in j.db.execute("SELECT * FROM outbox ORDER BY seq")],
        "transactions": [dict(row) for row in j.db.execute("SELECT * FROM transactions ORDER BY local")],
        "kind": kind,
    }


# Current/timer work, other owner requests and original-session uncertainty cannot be waived by another debt's proof.
def require_common_debt_idle(j: Journal, kind: str) -> None:
    if (
        any(entry.get("completed") is None for entry in j.get(KEY, {}).values())
        or j.get("managed_control_fault", {}).get("classification") not in (None, "owner_control_or_delivery_uncertain")
        or j.db.execute("SELECT 1 FROM transactions WHERE stop IS NULL LIMIT 1").fetchone()
        or kind != "observation"
        and j.db.execute("SELECT 1 FROM transactions WHERE vendor IS NULL LIMIT 1").fetchone()
        or j.get("managed_current_change", {}).get("state") not in (None, "completed", "cancelled")
        or j.get("managed_current_prepare_intent", {}).get("stage") not in (None, "complete")
        or any(supervised_timer(lease) or lease.get("state") == "requested" for lease in occurrences(j).values())
        or kind != "local-start"
        and j.get("control", {}).get("start_pending")
        or kind != "local-start"
        and j.get("managed_test_charge", {}).get("state") not in (None, "completed")
        or kind != "observation"
        and j.next_delivery() is not None
        or kind != "vendor-write"
        and j.get("uncertain_vendor_command")
        or kind != "vendor-write"
        and j.get("vendor_start_grant")
        or kind != "vendor-write"
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


# Only one exact vendor-origin write and its own optional unbound RemoteStart grant may be attested here.
def vendor_candidate(j: Journal, snapshot: dict[str, Any]) -> dict[str, Any]:
    rows = snapshot["mutations"]
    if len(rows) != 1 or rows[0]["origin"] != "vendor" or rows[0]["state"] not in ("pending", "uncertain"):
        raise RecoveryRequiredError("operator reconciliation requires exactly one unresolved vendor write")
    row, marker, grant = rows[0], snapshot["marker"], snapshot["grant"]
    if marker and (
        marker.get("action") != row["action"]
        or marker.get("digest") != row["digest"]
        or marker.get("mutation", row["token"]) != row["token"]
    ):
        raise RecoveryRequiredError("vendor visibility marker belongs to another write")
    if grant and (
        row["action"] != "RemoteStartTransaction"
        or grant.get("mutation") != row["token"]
        or grant.get("local") is not None
    ):
        raise RecoveryRequiredError("a bound or unrelated vendor Start grant cannot be retired")
    return {"mutation_token": row["token"], "action": row["action"], "payload_digest": row["digest"]}


# Time, zero and closed sessions alone never identify the accepted owner Start; its exact receipts remain mandatory.
def local_start_candidate(j: Journal, snapshot: dict[str, Any]) -> dict[str, Any]:
    state, receipt = snapshot["test"] or {}, local_start_receipt(j)
    requests = snapshot["requests"]
    deadline = state.get("hard_deadline_epoch")
    if (
        state.get("kind") != "normal"
        or state.get("source") != "local"
        or state.get("start_rpc_accepted") is not True
        or state.get("local_transaction") is not None
        or state.get("state") not in ("expired", "interrupted", "stopping", "awaiting_power")
        or not isinstance(deadline, (int, float))
        or isinstance(deadline, bool)
        or not 0 < deadline <= time.time()
        or len(requests) != 1
        or requests[0]["id"] != state.get("id")
        or requests[0]["action"] != "start"
        or state.get("revision") != requests[0]["revision"] + 1
        or receipt.get("stage") != "accepted"
        or receipt.get("local") is not None
        or not receipt.get("ticket")
        or receipt.get("digest") != state.get("tag_digest")
        or not isinstance(state.get("tag_digest"), str)
        or len(state["tag_digest"]) != 64
        or j.get("control", {}).get("start_pending") is not True
        or snapshot["grant"]
        or snapshot["mutations"]
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


# Inspection returns only correlation digests; a fresh independent observer and exclusive Journal owner are required.
def debt_candidate(
    j: Journal, book: ResourceBook, spec: NativeBoundedSpec, n: HandoverEvidence, kind: str
) -> dict[str, Any]:
    if kind not in ATTESTATIONS:
        raise RecoveryRequiredError("unknown operator reconciliation kind")
    require_original_zero(j, book, spec, n)
    require_common_debt_idle(j, kind)
    snapshot = debt_snapshot(j, spec, kind)
    if kind == "local-start":
        specific = local_start_candidate(j, snapshot)
    else:
        if snapshot["requests"]:
            raise RecoveryRequiredError("an unrelated management request still requires reconciliation")
        if kind == "vendor-write":
            specific = vendor_candidate(j, snapshot)
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
    if kind == "vendor-write":
        row = j.db.execute("SELECT * FROM mutations WHERE token=?", (e["mutation_token"],)).fetchone()
        return {
            "mutation": dict(row) if row is not None else None,
            "marker": j.get("uncertain_vendor_command"),
            "grant": j.get("vendor_start_grant"),
        }
    if kind == "local-start":
        row = j.db.execute("SELECT * FROM managed_requests WHERE id=?", (e["request_id"],)).fetchone()
        return {
            "request": dict(row) if row is not None else None,
            "test": j.get("managed_test_charge"),
            "receipt": j.get("local_start_receipt"),
            "deferred": j.get("managed_deferred_start"),
        }
    row = j.db.execute("SELECT * FROM rejected_observations WHERE token=?", (e["observation_token"],)).fetchone()
    return {
        "rejection": dict(row) if row is not None else None,
        "retained": j.db.execute("SELECT 1 FROM outbox WHERE token=?", (e["observation_token"],)).fetchone()
        is not None,
    }


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
        if proof["anchor"] != delivery_anchor(j, spec) or proof["resolved"] != resolved_debt(j, kind, proof):
            raise RecoveryRequiredError("previous operator debt proof no longer matches its recorded resolution")
        return {"already_recorded": True, "zero_hold_retained": True, "charging_started": False}
    cls.parse(e)
    candidate = debt_candidate(j, book, spec, n, kind)
    if any(e.get(key) != value for key, value in candidate.items()):
        raise RecoveryRequiredError("operator evidence does not identify the exact current debt")
    snapshot = debt_snapshot(j, spec, kind)
    fault = {"classification": FAULT, "proof": identifier, "at_epoch": time.time()}
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
        if kind == "vendor-write":
            j.reconcile_mutation(e["mutation_token"])
            j.db.execute("DELETE FROM meta WHERE key='uncertain_vendor_command'")
            if snapshot["grant"]:
                j.db.execute("DELETE FROM meta WHERE key='vendor_start_grant'")
        elif kind == "local-start":
            j.put("local_start_receipt", {**snapshot["receipt"], "stage": "reconciled", "proof": identifier})
            j.put(
                "managed_test_charge",
                {
                    **snapshot["test"],
                    "state": "completed",
                    "charging": False,
                    "recovery_reason": "operator_verified_no_pending_start",
                },
            )
            if snapshot["deferred"]:
                j.put("managed_deferred_start", {})
            j.db.execute(
                "UPDATE managed_requests SET state='failed',result=? WHERE id=?",
                (
                    encoded({"classification": "operator_reconciled_at_zero", "charging_started": False}),
                    e["request_id"],
                ),
            )
        else:
            assert head is not None
            j.db.execute(
                "INSERT INTO rejected_observations VALUES(?,?,?,?,?,?,?)",
                (head.token, head.action, fingerprint(head.payload), head.transaction, None, "rejected", time.time()),
            )
            j.db.execute("DELETE FROM outbox WHERE token=?", (head.token,))
        control = j.get("control")
        j.put("control", {**control, "owner": "local", "inhibit": True, "uncertain": True, "start_pending": False})
        j.put("managed_control_fault", fault)
        proof["resolved"] = resolved_debt(j, kind, proof)
        j.put(KEY, {**records, identifier: {"proof": proof, "completed": None}})
    return {"already_recorded": False, "zero_hold_retained": True, "charging_started": False}


# A new Pause can acknowledge one exact proof only after its retained lifecycle and all unrelated debts settle.
def operator_recovery_evidence(c: ManagedController, fault: dict[str, Any]) -> dict[str, Any] | None:
    j = c.session.journal
    entry = j.get(KEY, {}).get(fault.get("proof"))
    if fault.get("classification") != FAULT or not entry or entry.get("completed") is not None:
        return None
    proof: dict[str, Any] = entry["proof"]
    if (
        proof["installed_fault"] != fault
        or proof["anchor"] != delivery_anchor(j, c.native.spec)
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
    j.put(KEY, {**records, identifier: {"proof": proof, "completed": {"pause": pause, "at_epoch": time.time()}}})
