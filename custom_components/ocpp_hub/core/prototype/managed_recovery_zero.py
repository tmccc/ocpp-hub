"""Physical and commissioned-original proof shared by explicit offline debt reconciliations."""

from __future__ import annotations

import json

from ..control import HUB_PROFILE_ID
from .managed_current import managed_current_ceiling
from .native_bounded import NativeBoundedSpec
from .native_history import audit_history
from .store import Journal, RecoveryRequiredError, fingerprint
from .trial_handover_evidence import HandoverEvidence
from .trial_resources import ResourceBook


# Software debt predicates stay with each typed recovery; this helper never exempts or edits a pending command.
def require_original_zero(j: Journal, book: ResourceBook, spec: NativeBoundedSpec, n: HandoverEvidence) -> None:
    ceiling = managed_current_ceiling(j, spec)
    if ceiling == 32:
        spec.require_managed(n, j.identity, closed=True, allow_unplugged=True)
        binding = fingerprint(n.configuration | {"AmpLmt": "6"})
    else:
        spec.require(n, j.identity, "trial", closed=True, allow_unplugged=True)
        binding = n.physical.configuration_digest
    spec.require_boot(j.get("boot"))
    j.require_allocation()
    intent, control = j.get("managed_control_intent", {}), j.get("control", {})
    if (
        j.get("managed_mode") != {"mode": "app_schedule", "spec_digest": spec.digest, "local_start": False}
        or intent.get("mode") != "paused"
        or type(intent.get("revision")) is not int
        or not 0 <= intent["revision"] < 2**53
        or control.get("owner") != "local"
        or control.get("inhibit") is not True
        or not control.get("ticket")
        or type(control.get("resume_limit")) not in (int, float)
        or not 6 <= control["resume_limit"] <= ceiling
        or int(control["resume_limit"]) != control["resume_limit"]
        or j.get("trial_phase") not in ("blocked", "recovering", "active")
        or j.get("native_bounded_handover", {}).get("stage") != "admitted"
        or j.get("native_bounded_handover", {}).get("fault")
        or j.get("trial_preservation_fault")
        or j.get("trial_inventory_fault")
        or j.get("trial_binding", {}).get("configuration_digest") != binding
        or j.get("trial_binding", {}).get("boot_digest") != spec.boot_digest
        or n.saved_schedule_digest != spec.saved_schedule_digest
        or n.physical.transaction_open
        or any(v != 0 for v in n.currents_a + n.powers_w)
    ):
        raise RecoveryRequiredError("operator reconciliation requires the existing paused original closed zero hold")
    audit_history(j, spec.digest, list(n.history), False)
    station = [r for r in book.owned_present(n.physical) if r.kind == "profile" and r.identifier == HUB_PROFILE_ID]
    row = j.db.execute(
        "SELECT state,confirmed FROM trial_resources WHERE key=?", (f"profile:{HUB_PROFILE_ID}",)
    ).fetchone()
    if (
        len(station) != 1
        or station[0].canonical.get("limit") != 0
        or row is None
        or row["state"] != "confirmed"
        or row["confirmed"] is None
        or json.loads(row["confirmed"]) != station[0].canonical
    ):
        raise RecoveryRequiredError("operator reconciliation requires independently confirmed owned station zero")
