"""Explicit offline reconciliation of a historical-retention fault, retaining zero and every other guard."""

from __future__ import annotations

import json
import time
from typing import Any

from ..control import HUB_PROFILE_ID
from .control_state import ControlState
from .managed_current import managed_current_ceiling
from .managed_debt_recovery import vendor_history_recovery_scope
from .managed_intent import ControlIntent, unfinished_owner_requests, verified_pause_after, write_intent
from .managed_native_repair import retire_history_omission_revocations
from .managed_recovery_zero import require_paused_owned_zero
from .native_bounded import NativeBoundedSpec
from .native_history import (
    ORIGINAL_HISTORY_OMISSION,
    ORIGINAL_HISTORY_REFUSAL,
    STARTUP_HISTORY_OMISSION,
    MissingOriginalHistoryError,
    NativeHistoryArchive,
    audit_history,
    history_policy,
    inspect_history,
)
from .native_start_attempts import split_attempts
from .store import Journal, RecoveryRequiredError, fingerprint
from .trial_handover_evidence import HandoverEvidence
from .trial_resources import ResourceBook

HISTORY_ACK = "native_history_policy_acknowledgement"


# A history repair owns only history, so its latch must leave unrelated pending work to its existing owner.
def history_recovery_idle(
    journal: Journal, *, check_owner_requests: bool = True, retain_queued_status: bool = False
) -> bool:
    grant = journal.get("vendor_start_grant")
    waiting = "SELECT 1 FROM outbox WHERE state!='queued' OR action!='StatusNotification' OR local IS NOT NULL LIMIT 1"
    outbox = waiting if retain_queued_status else "SELECT 1 FROM outbox LIMIT 1"
    return not (
        journal.get("uncertain_vendor_command")
        or grant
        and grant.get("local") is None
        or journal.active() is not None
        or journal.db.execute(outbox).fetchone()
        or journal.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
        or check_owner_requests
        and journal.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='managed_requests'").fetchone()
        and unfinished_owner_requests(journal, include_pause=True)
    )


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
            "SELECT id,revision,result FROM managed_requests WHERE action='pause' AND state='completed' "
            "AND rowid>? ORDER BY rowid",
            (row["rowid"],),
        )
        for pause in pauses:
            if verified_pause_after(
                json.loads(pause["result"]), row["revision"], pause["revision"], revision, fault_at
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
    fault, failure = journal.fault.read({}), journal.get("managed_native_failure", {})
    owns_fault = matches_history_fault(fault, failure, ORIGINAL_HISTORY_REFUSAL)
    receipt = journal.get(HISTORY_ACK, {})
    if receipt.get("vendor_recovery") and journal.get("native_history_policy") is not None:
        require_paused_owned_zero(journal, book, spec, native)
        inspect_history(journal, spec.digest, list(native.history), False)
        return {**receipt["summary"], "already_recorded": True, "fault_cleared": False, "charging_started": False}
    if not owns_fault or not history_recovery_idle(journal, retain_queued_status=True):
        retire_fault = (
            owns_fault
            and failure.get("source") in (ORIGINAL_HISTORY_OMISSION, STARTUP_HISTORY_OMISSION)
            and fault.get("native_failure_digest") == fingerprint(failure)
        )
        with journal.atomic():
            revocations = retire_history_omission_revocations(journal, fault)
            scope = vendor_history_recovery_scope(journal, spec, retiring_history_fault=retire_fault)
            if scope is not None:
                if revocations:
                    scope["history_revocations"] = revocations
                return acknowledge_history_with_vendor_debt(journal, book, spec, native, scope, retire_fault)
            if revocations:
                raise RecoveryRequiredError("no matching vendor recovery owns the historical refusal")
    return _reconcile(journal, book, spec, native, attempts=False)


# Require an independent current omission and the same paused original zero used by vendor recovery.
def acknowledge_history_with_vendor_debt(
    journal: Journal,
    book: ResourceBook,
    spec: NativeBoundedSpec,
    native: HandoverEvidence,
    scope: dict[str, Any],
    retire_fault: bool,
) -> dict[str, Any]:
    require_paused_owned_zero(journal, book, spec, native)
    current = list(native.history)
    try:
        inspect_history(journal, spec.digest, current, False)
    except MissingOriginalHistoryError:
        summary = record_history_policy(journal, history_policy(journal, spec.digest), current, vendor_recovery=scope)
    else:
        raise RecoveryRequiredError("no missing original historical records were independently observed")
    if retire_fault:
        journal.fault.clear()
    return {**summary, "fault_cleared": retire_fault, "charging_started": False}


# The receipt binds independent history evidence without settling any command the operator still owns.
def record_history_policy(
    journal: Journal,
    policy: dict[str, Any] | None,
    current: list[dict[str, Any]],
    *,
    vendor_recovery: dict[str, Any] | None = None,
) -> dict[str, int]:
    original = journal.get("native_bounded_handover")["original_history"]
    summary = NativeHistoryArchive(journal).observe(original, current, allow_missing=policy is not None)
    if policy is not None:
        if not (summary["missing_records"] or summary.get("missing_attempts")):
            raise RecoveryRequiredError("no missing historical records were independently observed")
        journal.put("native_history_policy", policy)
    journal.put(
        HISTORY_ACK,
        {
            "policy": policy,
            "prior_fault": journal.fault.read({}),
            "prior_failure": journal.get("managed_native_failure", {}),
            "at_epoch": time.time(),
            "native_digest": fingerprint(current),
            "summary": summary,
            "charging_started": False,
            **({"vendor_recovery": vendor_recovery} if vendor_recovery is not None else {}),
        },
    )
    return summary


# Identify the particular native refusal whose own latch the offline tool is allowed to retire.
def matches_history_fault(fault: dict[str, Any], failure: dict[str, Any], detail: str) -> bool:
    if "native_failure_digest" in fault:
        # The scheduled-attempt parser repair applies only to legacy faults. Require exact source for current faults.
        if detail != ORIGINAL_HISTORY_REFUSAL or failure.get("source") not in (
            ORIGINAL_HISTORY_OMISSION,
            STARTUP_HISTORY_OMISSION,
        ):
            return False
        if fault["native_failure_digest"] != fingerprint(failure):
            return False
    return (
        fault.get("classification") == "native_authority_or_preservation_failed"
        and failure.get("type") == "SecurityError"
        and failure.get("detail") == detail
        and type(fault.get("at_epoch")) in (int, float)
        and type(failure.get("at_epoch")) in (int, float)
        and 0 <= fault["at_epoch"] - failure["at_epoch"] <= 2
    )


# Repair the parser fault under the installed capability and preserve the existing archive policy.
def acknowledge_start_attempts(
    journal: Journal, book: ResourceBook, spec: NativeBoundedSpec, native: HandoverEvidence
) -> dict[str, Any]:
    return _reconcile(journal, book, spec, native, attempts=True)


# Both operator actions verify the existing owned zero before atomically recording what cleared the fault.
def _reconcile(
    journal: Journal, book: ResourceBook, spec: NativeBoundedSpec, native: HandoverEvidence, *, attempts: bool
) -> dict[str, Any]:
    fault, failure = journal.fault.read({}), journal.get("managed_native_failure", {})
    policy = history_policy(journal, spec.digest)
    detail = "complete connected-handover native evidence is required" if attempts else ORIGINAL_HISTORY_REFUSAL
    binding = native.physical.configuration_digest
    if managed_current_ceiling(journal, spec) == 32:
        spec.require_managed(native, journal.identity, closed=True, allow_unplugged=True)
        binding = fingerprint(native.configuration | {"AmpLmt": "6"})
    else:
        spec.require(native, journal.identity, "trial", closed=True, allow_unplugged=True)
    if (
        not matches_history_fault(fault, failure, detail)
        or not journal.phase.read().held
        or journal.get("native_bounded_handover", {}).get("stage") != "admitted"
        or journal.get("trial_preservation_fault")
        or journal.get("trial_inventory_fault")
        or journal.get("trial_binding", {}).get("configuration_digest") != binding
        or not history_recovery_idle(journal, check_owner_requests=not attempts, retain_queued_status=not attempts)
    ):
        raise RecoveryRequiredError("history reconciliation requires the matching native fault and a settled journal")
    spec.require_boot(journal.get("boot"))
    control = journal.get("control", {})
    if not ControlState(control).local_hold or control.get("start_pending"):
        raise RecoveryRequiredError("history acknowledgement requires the existing inhibited zero hold")
    with journal.atomic():
        present = book.owned_present(native.physical)
        receipt = journal.db.execute(
            "SELECT state,confirmed FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
        ).fetchone()
        station = [resource for resource in present if resource.identifier == HUB_PROFILE_ID]
        if not book.confirmed_station_zero(station, receipt):
            raise RecoveryRequiredError("history acknowledgement requires actual confirmed owned zero")
        if attempts:
            numbered, unassigned = split_attempts(list(native.history))
            plans = {r.body.get("idTag") for r in native.physical.resources if r.kind == "plan"}
            intent = journal.get("managed_control_intent", {})
            # The older parser could only fail on attempts it had never successfully audited.
            NativeHistoryArchive(journal)
            archived_names = {
                json.loads(r[0])["filename"] for r in journal.db.execute("SELECT body FROM native_start_attempts")
            }
            if (
                journal.get("native_history_policy") != policy
                or journal.get("native_start_attempt_recovery") is not None
                or journal.get("vendor_start_grant")
                or journal.get("managed_current_prepare_intent", {}).get("stage") not in (None, "complete")
                or journal.current_change.read().state not in (None, "completed")
                or journal.get("managed_test_charge", {}).get("state") not in (None, "completed")
                or not isinstance(intent, dict)
                or set(intent) != {"mode", "revision"}
                or intent.get("mode") not in ("app", "paused")
                or type(intent.get("revision")) is not int
                or not 0 <= intent["revision"] < 2**53 - 1
                or journal.db.execute(
                    "SELECT 1 FROM transactions WHERE stop IS NULL OR vendor IS NULL LIMIT 1"
                ).fetchone()
                or not any(
                    row["idTagStart"] in plans
                    and 0 <= failure["at_epoch"] - row["startTime"] <= 30
                    and row["filename"] not in archived_names
                    for row in unassigned.values()
                )
            ):
                raise RecoveryRequiredError("the fault does not match a reconciled scheduled start attempt")
            known = {r[0] for r in journal.db.execute("SELECT digest FROM native_history_records")}
            if any(fingerprint(row) not in known for row in numbered):
                raise RecoveryRequiredError("unexpected numbered history needs reconciliation")
            superseded = _superseded_tests(journal, intent["revision"], fault["at_epoch"])
            journal.require_allocation()
            audit_history(journal, spec.digest, list(native.history), False)
            summary = {"unassigned_attempts": len(unassigned)}
            write_intent(journal, ControlIntent(intent["mode"], intent["revision"]), "paused")
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
            current = list(native.history)
            startup = failure.get("source") == STARTUP_HISTORY_OMISSION and fault.get(
                "native_failure_digest"
            ) == fingerprint(failure)
            allow_missing = True
            if startup:
                try:
                    inspect_history(journal, spec.digest, current, False)
                except MissingOriginalHistoryError:
                    pass
                else:
                    allow_missing = False
            summary = record_history_policy(journal, policy if allow_missing else None, current)
        journal.fault.clear()
        journal.phase.recover()
        journal.control.hold(control)
    return {**summary, "fault_cleared": True, "charging_started": False}
