"""Explicit offline reconciliation of a historical-retention fault, retaining zero and every other guard."""

from __future__ import annotations

import json
import time
from typing import Any

from ..control import HUB_PROFILE_ID
from .native_bounded import NativeBoundedSpec
from .native_history import NativeHistoryArchive, history_policy
from .store import Journal, RecoveryRequiredError, fingerprint
from .trial_handover_evidence import HandoverEvidence
from .trial_resources import ResourceBook


# The caller owns the exclusive journal lock after stopping the service; this function has no charger writer.
def acknowledge_history_policy(
    journal: Journal, book: ResourceBook, spec: NativeBoundedSpec, native: HandoverEvidence
) -> dict[str, Any]:
    fault, failure = journal.get("managed_control_fault", {}), journal.get("managed_native_failure", {})
    policy = history_policy(journal, spec.digest)
    if (
        fault.get("classification") != "native_authority_or_preservation_failed"
        or failure.get("type") != "SecurityError"
        or failure.get("detail") != "native bounded handover must preserve every original history record"
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
        or journal.get("trial_binding", {}).get("configuration_digest") != native.physical.configuration_digest
        or journal.active() is not None
        or journal.db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone()
        or journal.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
        or journal.db.execute(
            "SELECT 1 FROM managed_requests WHERE state IN ('accepted','running','uncertain') LIMIT 1"
        ).fetchone()
    ):
        raise RecoveryRequiredError("only independently verified historical absence can be acknowledged")
    spec.require(native, journal.identity, "trial", closed=True, allow_unplugged=True)
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
