"""Independently attested recovery of a legacy storage latch after every actual debt has settled."""

import math
from typing import Any

from .managed_current_recovery import INTERRUPTED
from .store import Journal, RecoveryRequiredError, fingerprint


# Pin the diagnostic and immutable local history without including the new Pause revision or installed fault.
def storage_resolution(j: Journal) -> dict[str, Any]:
    return {
        "diagnostic": j.get("managed_native_failure"),
        "transactions": [dict(row) for row in j.db.execute("SELECT * FROM transactions ORDER BY local")],
        "receipt": j.get("local_start_receipt"),
        "test": j.get("managed_test_charge"),
        "deferred": j.get("managed_deferred_start"),
    }


# Zero and an empty queue never establish cause; the separate operator statement must verify storage provenance.
def storage_candidate(j: Journal, snapshot: dict[str, Any]) -> dict[str, str]:
    fault = snapshot["fault"] or {}
    at = fault.get("at_epoch")
    if (
        set(fault) != {"classification", "at_epoch"}
        or fault.get("classification") != "owner_control_or_delivery_uncertain"
        or not isinstance(at, (int, float))
        or isinstance(at, bool)
        or not math.isfinite(at)
        or at <= 0
        or snapshot["requests"]
        or snapshot["outbox"]
        or snapshot["deferred"]
        or snapshot["stop_rejection"]
        or (snapshot["stop_wire_ack"] or {}).get("fault") == fault
        or snapshot["receipt"]
        and snapshot["receipt"].get("stage") in ("prepared", "issued", "accepted")
        and snapshot["receipt"].get("local") is None
        or any(
            entry.get("completed") is None and entry["proof"].get("prior_fault") == fault
            for entry in j.get("managed_delivery_reconciliations", {}).values()
        )
        or any(
            denial.get("completed") is None and denial.get("fault") == fault
            for denial in j.get("managed_start_denials", {}).values()
        )
        or j.get(INTERRUPTED)
    ):
        raise RecoveryRequiredError("storage recovery requires an independently explained plain fault and no debt")
    return {"fault_digest": fingerprint(fault), "diagnostic_digest": fingerprint(storage_resolution(j))}
