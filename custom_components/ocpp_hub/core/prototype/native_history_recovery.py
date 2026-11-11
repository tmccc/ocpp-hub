"""Explicit offline reconciliation of a historical-retention fault, retaining zero and every other guard."""

from __future__ import annotations

import json
import math
import time
from typing import Any

from ..control import HUB_PROFILE_ID
from .managed_current import managed_current_ceiling
from .native_bounded import NativeBoundedSpec
from .native_history import NativeHistoryArchive, audit_history, history_policy
from .native_start_attempts import split_attempts
from .store import Journal, RecoveryRequiredError, fingerprint
from .trial_handover_evidence import HandoverEvidence
from .trial_resources import ResourceBook


# An unsuccessful timed test keeps its original uncertain reply after its effect has been physically stopped.
def _superseded_tests(journal: Journal, revision: int, fault_at: float) -> list[dict[str, str]]:
    proofs = []
    rows = journal.db.execute(
        "SELECT rowid,* FROM managed_requests WHERE state IN ('accepted','running','uncertain') ORDER BY rowid"
    )
    for row in rows:
        if (
            row["action"] != "start_test"
            or row["state"] != "uncertain"
            or json.loads(row["result"]) != {"classification": "start_not_confirmed"}
            or journal.get("managed_test_charge", {}).get("id") == row["id"]
            or not 0 <= row["revision"] < revision
        ):
            raise RecoveryRequiredError("a management request still needs reconciliation")
        pauses = journal.db.execute(
            "SELECT id,result FROM managed_requests WHERE action='pause' AND state='completed' "
            "AND rowid>? AND revision>? AND revision<? ORDER BY rowid",
            (row["rowid"], row["revision"], revision),
        )
        for pause in pauses:
            result = json.loads(pause["result"])
            at = result.get("completed_at")
            if (
                result.get("pause_verified") is True
                and type(at) in (int, float)
                and math.isfinite(at)
                and 0 < at <= fault_at
            ):
                proofs.append({"request_id": row["id"], "verified_pause_id": pause["id"]})
                break
        else:
            raise RecoveryRequiredError("unconfirmed timed start has no subsequent verified Pause")
    return proofs


# The caller owns the exclusive journal lock after stopping the service; this function has no charger writer.
def acknowledge_history_policy(
    journal: Journal, book: ResourceBook, spec: NativeBoundedSpec, native: HandoverEvidence
) -> dict[str, Any]:
    return _reconcile(journal, book, spec, native, attempts=False)


# Repair the parser fault under the installed capability and preserve the existing archive policy.
def acknowledge_start_attempts(
    journal: Journal, book: ResourceBook, spec: NativeBoundedSpec, native: HandoverEvidence
) -> dict[str, Any]:
    return _reconcile(journal, book, spec, native, attempts=True)


# Both operator actions verify the existing owned zero before atomically recording what cleared the fault.
def _reconcile(
    journal: Journal, book: ResourceBook, spec: NativeBoundedSpec, native: HandoverEvidence, *, attempts: bool
) -> dict[str, Any]:
    fault, failure = journal.get("managed_control_fault", {}), journal.get("managed_native_failure", {})
    policy = history_policy(journal, spec.digest)
    detail = (
        "complete connected-handover native evidence is required"
        if attempts
        else "native bounded handover must preserve every original history record"
    )
    binding = native.physical.configuration_digest
    if attempts and managed_current_ceiling(journal, spec) == 32:
        spec.require_managed(native, journal.identity, closed=True, allow_unplugged=True)
        binding = fingerprint(native.configuration | {"AmpLmt": "6"})
    else:
        spec.require(native, journal.identity, "trial", closed=True, allow_unplugged=True)
    if (
        fault.get("classification") != "native_authority_or_preservation_failed"
        or failure.get("type") != "SecurityError"
        or failure.get("detail") != detail
        or type(fault.get("at_epoch")) not in (int, float)
        or type(failure.get("at_epoch")) not in (int, float)
        or not 0 <= fault["at_epoch"] - failure["at_epoch"] <= 2
        or journal.get("trial_phase") not in ("blocked", "recovering")
        or journal.get("native_bounded_handover", {}).get("stage") != "admitted"
        or journal.get("trial_preservation_fault")
        or journal.get("trial_inventory_fault")
        or journal.get("uncertain_vendor_command")
        or journal.get("vendor_start_grant", {}).get("local") is None
        and bool(journal.get("vendor_start_grant", {}))
        or journal.get("trial_binding", {}).get("configuration_digest") != binding
        or journal.active() is not None
        or journal.db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone()
        or journal.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
        or (
            not attempts
            and journal.db.execute(
                "SELECT 1 FROM managed_requests WHERE state IN ('accepted','running','uncertain') LIMIT 1"
            ).fetchone()
        )
    ):
        raise RecoveryRequiredError("history reconciliation requires the matching native fault and a settled journal")
    spec.require_boot(journal.get("boot"))
    control = journal.get("control", {})
    if control.get("owner") != "local" or not control.get("inhibit") or control.get("start_pending"):
        raise RecoveryRequiredError("history acknowledgement requires the existing inhibited zero hold")
    with journal.atomic():
        present = book.owned_present(native.physical)
        receipt = journal.db.execute(
            "SELECT state,confirmed FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
        ).fetchone()
        station = [resource for resource in present if resource.identifier == HUB_PROFILE_ID]
        if (
            len(station) != 1
            or station[0].canonical.get("limit") != 0
            or receipt is None
            or receipt["state"] != "confirmed"
            or receipt["confirmed"] is None
            or json.loads(receipt["confirmed"]) != station[0].canonical
        ):
            raise RecoveryRequiredError("history acknowledgement requires actual confirmed owned zero")
        if attempts:
            numbered, unassigned = split_attempts(list(native.history))
            plans = {r.body.get("idTag") for r in native.physical.resources if r.kind == "plan"}
            intent = journal.get("managed_control_intent", {})
            if (
                journal.get("native_history_policy") != policy
                or journal.get("vendor_start_grant")
                or journal.get("managed_current_prepare_intent", {}).get("stage") not in (None, "complete")
                or journal.get("managed_current_change", {}).get("state") not in (None, "completed")
                or journal.get("managed_test_charge", {}).get("state") not in (None, "completed")
                or intent.get("mode") not in ("app", "paused")
                or type(intent.get("revision")) is not int
                or not 0 <= intent["revision"] < 2**53 - 1
                or journal.db.execute(
                    "SELECT 1 FROM transactions WHERE stop IS NULL OR vendor IS NULL LIMIT 1"
                ).fetchone()
                or not any(
                    row["idTagStart"] in plans and 0 <= failure["at_epoch"] - row["startTime"] <= 30
                    for row in unassigned.values()
                )
            ):
                raise RecoveryRequiredError("the fault does not match a reconciled scheduled start attempt")
            known = {r[0] for r in journal.db.execute("SELECT transaction_id FROM native_history_records")}
            if any(row["transactionId"] not in known for row in numbered):
                raise RecoveryRequiredError("unexpected numbered history needs reconciliation")
            superseded = _superseded_tests(journal, intent["revision"], fault["at_epoch"])
            journal.require_allocation()
            audit_history(journal, spec.digest, list(native.history), False)
            summary = {"unassigned_attempts": len(unassigned)}
            journal.put("managed_control_intent", {"mode": "paused", "revision": intent["revision"] + 1})
            journal.put(
                "native_start_attempt_recovery",
                {
                    "prior_fault": fault,
                    "prior_failure": failure,
                    "prior_intent": intent,
                    "superseded_test_proofs": superseded,
                    "native_digest": native.digest,
                    "at_epoch": time.time(),
                    **summary,
                },
            )
        else:
            summary = NativeHistoryArchive(journal).observe(
                journal.get("native_bounded_handover")["original_history"], list(native.history), allow_missing=True
            )
            if not summary["missing_records"]:
                raise RecoveryRequiredError("no missing historical records were independently observed")
            journal.put("native_history_policy", policy)
            journal.put(
                "native_history_policy_acknowledgement",
                {
                    "policy": policy,
                    "prior_fault": fault,
                    "prior_failure": failure,
                    "at_epoch": time.time(),
                    "native_digest": fingerprint(list(native.history)),
                    "summary": summary,
                    "charging_started": False,
                },
            )
        journal.db.execute("DELETE FROM meta WHERE key='managed_control_fault'")
        journal.put("trial_phase", "recovering")
        journal.put("control", {**control, "owner": "local", "inhibit": True, "uncertain": True})
    return {**summary, "fault_cleared": True, "charging_started": False}
