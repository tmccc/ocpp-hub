"""Explicit owner Pause can reconcile one unconfirmed release while retaining physical zero."""

from __future__ import annotations

import json
import math
import time
from typing import TYPE_CHECKING, Any

from ..control import HUB_PROFILE_ID
from .managed_current import managed_current_ceiling
from .managed_current_recovery import INTERRUPTED, legacy_current_evidence
from .managed_cutoffs import supervised_timer
from .managed_debt_recovery import complete_operator_recovery, operator_recovery_evidence
from .managed_delivery_recovery import complete_delivery_recovery, delivery_recovery_evidence
from .managed_native_repair import repair_pending, repair_revoked
from .managed_occurrences import occurrences
from .managed_start_delivery import DENIALS, complete_denied_start, denial_faults, denied_start_evidence
from .native_faults import MEASURED_FAULT, measurement_cause
from .store import RecoveryRequiredError, fingerprint
from .trial_rollback import require_lifecycle_idle

if TYPE_CHECKING:
    from .managed import ManagedController

ACK = "managed_release_pause_acknowledgement"
PROOF = "managed_release_reconciliation"
FAULT = "owner_release_unconfirmed"


# Capture every applicable proof together; selecting one cannot acknowledge another proof on the same fault.
def release_evidence(c: ManagedController, fault: dict[str, Any]) -> dict[str, Any] | None:
    if repair_revoked(c.session.journal, fault):
        return None
    evidence: dict[str, Any] = {}
    operator = operator_recovery_evidence(c, fault)
    active = [
        entry
        for entry in c.session.journal.get("managed_operator_debt_reconciliations", {}).values()
        if entry.get("completed") is None
        and not entry.get("superseded")
        and entry["proof"].get("installed_fault") == fault
    ]
    if active and (len(active) != 1 or operator is None):
        return None
    denied = denied_start_evidence(c, fault)
    sources = denial_faults(c.session.journal, fault)
    if denied is None and any(
        denial.get("fault") in sources and denial.get("completed") is None
        for denial in c.session.journal.get(DENIALS, {}).values()
    ):
        return None
    current = legacy_current_evidence(c, fault)
    if current is not None:
        evidence["current_failure_evidence"] = current
    for key, proof in (
        ("delivery_reconciliation", delivery_recovery_evidence(c, fault)),
        ("operator_reconciliation", operator),
        ("denied_start_reconciliation", denied),
    ):
        if proof is not None:
            evidence[key] = fingerprint(proof)
    if evidence or fault.get("classification") == FAULT:
        return evidence
    if fault.get("classification") == MEASURED_FAULT:
        failure = c.session.journal.get("managed_native_failure", {})
        if (
            failure.get("type") == "MeasuredOutputLimitError"
            and fault.get("native_failure_digest") == fingerprint(failure)
            and measurement_cause(
                failure.get("cause"),
                c.native.spec.digest,
                c.current.selected(),
                managed_current_ceiling(c.session.journal, c.native.spec),
            )
        ):
            return {"native_failure": failure}
        return None
    if fault.get("classification") != "native_authority_or_preservation_failed":
        return None
    failure = c.session.journal.get("managed_native_failure", {})
    at, observed = fault.get("at_epoch"), failure.get("at_epoch")
    eligible = (
        failure.get("type") in ("NativeTransportUnavailableError", "StaleEvidenceError")
        and isinstance(at, (int, float))
        and not isinstance(at, bool)
        and isinstance(observed, (int, float))
        and not isinstance(observed, bool)
        and 0 < at < 2**53
        and 0 < observed < 2**53
        and math.isfinite(at)
        and math.isfinite(observed)
        and 0 <= at - observed <= 2
    )
    return {"native_failure": failure} if eligible else None


# Generic authority faults still require independently identified evidence, even at an apparently idle zero.
def recoverable_fault(c: ManagedController, fault: dict[str, Any]) -> bool:
    return release_evidence(c, fault) is not None


# Call inside the existing Pause acceptance transaction after its new paused revision has been recorded.
def record_release_pause(c: ManagedController, identifier: str) -> None:
    j = c.session.journal
    fault = j.get("managed_control_fault", {})
    evidence = release_evidence(c, fault)
    if evidence is None:
        return
    intent = c.requests.intent()
    if intent.mode != "paused":
        raise RecoveryRequiredError("release acknowledgement requires an explicit paused intent")
    j.put(ACK, {"fault": fault, "pause_request": identifier, "revision": intent.revision, **evidence})


# Every post-await check retains the exact diagnostic/request snapshot acknowledged by this particular Pause.
def acknowledged_evidence(c: ManagedController, fault: dict[str, Any], ack: dict[str, Any]) -> bool:
    expected = release_evidence(c, fault)
    actual = {key: value for key, value in ack.items() if key not in ("fault", "pause_request", "revision")}
    return expected is not None and actual == expected


# Prove only this fault is recoverable; never clear vendor uncertainty or release positive current here.
async def reconcile_owner_release(c: ManagedController) -> bool:
    s, j, g = c.session, c.session.journal, c.session.gateway
    if c.native.repair_audits is not None:
        c.native.repair_audits.flush()
    fault = j.get("managed_control_fault", {})
    ack = j.get(ACK, {})
    intent = c.requests.intent()
    if (
        not recoverable_fault(c, fault)
        or ack.get("fault") != fault
        or not acknowledged_evidence(c, fault, ack)
        or intent.mode != "paused"
        or ack.get("revision") != intent.revision
    ):
        return False
    request = j.db.execute(
        "SELECT action,state FROM managed_requests WHERE id=?", (ack.get("pause_request"),)
    ).fetchone()
    if (
        request is None
        or request["action"] != "pause"
        or request["state"] not in ("accepted", "uncertain", "completed")
    ):
        return False
    epoch, revision = g._charger_epoch, g.controls._revision
    boot = g.boot_observation
    ticket = j.get("control", {}).get("ticket")
    current = legacy_current_evidence(c, fault)
    delivery = delivery_recovery_evidence(c, fault)
    operator = operator_recovery_evidence(c, fault)
    denied = denied_start_evidence(c, fault)

    # Any newer Pause, Boot, local operation, request or unrelated write debt revokes this snapshot.
    def guard() -> None:
        control = j.get("control", {})
        if (
            c.stopping
            or repair_pending(j, fault)
            or repair_revoked(j, fault)
            or c.requests.intent() != intent
            or j.get(ACK) != ack
            or j.get("managed_control_fault") != fault
            or not recoverable_fault(c, fault)
            or not acknowledged_evidence(c, fault, ack)
            or g._charger_epoch != epoch
            or g.boot_observation != boot
            or boot is None
            or g.controls._revision != revision
            or not g.current_boot_seen(epoch)
            or g.charger.sender is None
            or g.vendor.sender is None
            or g.vendor_registration != "Accepted"
            or not s.runtime.status.vendor_connected
            or j.get("trial_phase") not in ("active", "blocked", "recovering")
            or j.get("native_bounded_handover", {}).get("stage") != "admitted"
            or j.get("native_bounded_handover", {}).get("fault")
            or j.get("trial_preservation_fault")
            or j.get("trial_inventory_fault")
            or j.get("uncertain_vendor_command")
            or j.get("vendor_start_grant")
            or control.get("ticket") != ticket
            or not ticket
            or control.get("owner") != "local"
            or control.get("inhibit") is not True
            or control.get("start_pending")
            or not g.controls.ready
            or g.controls.desired() != 0
            or c.pause_task is not None
            and not c.pause_task.done()
            or g.controls._operations
            or g.charger._pending
            or g.vendor._pending
            or j.get("managed_current_change", {}).get("state") in ("pending", "recovering", "interrupted", "uncertain")
            and current is None
            or j.get("managed_current_prepare_intent", {}).get("stage") not in (None, "complete")
            or j.get("managed_test_charge", {}).get("state") not in (None, "completed")
            and not (denied_start_evidence(c, fault) or {}).get("normal")
            or any(supervised_timer(lease) for lease in occurrences(j).values())
            or any(lease.get("state") == "requested" for lease in occurrences(j).values())
            or j.db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone()
            or j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
            or j.db.execute("SELECT 1 FROM transactions WHERE stop IS NULL OR vendor IS NULL LIMIT 1").fetchone()
            or j.db.execute(
                "SELECT 1 FROM managed_requests WHERE action!='pause' AND state IN ('accepted','running') "
                "AND id IS NOT ? LIMIT 1",
                (((denied_start_evidence(c, fault) or {}).get("normal") or {}).get("id"),),
            ).fetchone()
        ):
            raise RecoveryRequiredError("release reconciliation awaits the exact paused zero and settled journal")
        c.native.spec.require_boot(j.get("boot"))
        require_lifecycle_idle(g)

    guard()
    # Production native read verifies commissioned native6/native32, original history, freshness and read generation.
    native = await c.native.read(closed=True)
    guard()
    c.native.require(native, closed=True)
    c.native.require_known_session(native)
    if (
        native.physical.transaction_open
        or any(v != 0 for v in native.currents_a + native.powers_w)
        or native.saved_schedule_digest != c.native.spec.saved_schedule_digest
        or j.get("trial_binding")
        != {
            "configuration_digest": c.native.binding_digest(native),
            "station": c.native.choice.value(),
            "boot_digest": c.native.spec.boot_digest,
        }
    ):
        raise RecoveryRequiredError("release reconciliation awaits closed native zero and original configuration")
    present = s.book.owned_present(native.physical)
    station = [r for r in present if r.kind == "profile" and r.identifier == HUB_PROFILE_ID]
    row = j.db.execute(
        "SELECT state,confirmed FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
    ).fetchone()
    if (
        len(station) != 1
        or station[0].canonical.get("limit") != 0
        or row is None
        or row["state"] != "confirmed"
        or row["confirmed"] is None
        or json.loads(row["confirmed"]) != station[0].canonical
    ):
        raise RecoveryRequiredError("release reconciliation awaits independently confirmed owned station zero")
    with j.atomic():
        guard()
        control = j.get("control")
        j.put(
            PROOF,
            {
                "prior_fault": fault,
                "prior_native_failure": ack.get("native_failure"),
                "prior_current_failure": current,
                "delivery_reconciliation": fingerprint(delivery) if delivery is not None else None,
                "operator_reconciliation": fingerprint(operator) if operator is not None else None,
                "denied_start_reconciliation": fingerprint(denied) if denied is not None else None,
                "pause_request": ack["pause_request"],
                "revision": intent.revision,
                "native_digest": native.digest,
                "boot_observation": boot,
                "at_epoch": time.time(),
                "zero_hold_retained": True,
                "charging_started": False,
            },
        )
        j.db.execute("DELETE FROM meta WHERE key='managed_control_fault'")
        j.db.execute("DELETE FROM meta WHERE key=?", (ACK,))
        if delivery is not None:
            complete_delivery_recovery(j, delivery, ack["pause_request"])
        if operator is not None:
            complete_operator_recovery(j, operator, ack["pause_request"])
        if denied is not None:
            complete_denied_start(j, denied, ack["pause_request"])
        if current is not None:
            control = {**control, "resume_limit": current["previous_a"]}
            j.put(
                "managed_current_change",
                {
                    **current["change"],
                    "state": "cancelled",
                    "previous_a": current["previous_a"],
                    "recovery_reason": "explicit_pause_verified_zero",
                },
            )
            # This outcome confirms only reconciliation at zero; the requested target was never completed.
            j.db.execute(
                "UPDATE managed_requests SET state='failed',result=? WHERE id=? AND state='uncertain'",
                (
                    json.dumps(
                        {
                            "classification": "current_unconfirmed",
                            "reconciled_at_zero": True,
                            "requested_current_a": current["change"]["current_a"],
                            "previous_current_a": current["previous_a"],
                        }
                    ),
                    current["change"]["id"],
                ),
            )
            j.db.execute("DELETE FROM meta WHERE key=?", (INTERRUPTED,))
        j.put("control", {**control, "owner": "local", "inhibit": True, "uncertain": True})
        j.put("trial_phase", "recovering")
        j.put("trial_starting", False)
    # fault() invalidated the old revision. This complete closed proof re-establishes only current zero introduction.
    # Never assign a newer revision before every binding/Boot/debt/closed-zero check and the durable commit succeed.
    c.native.introduced_epoch = epoch
    c.native.introduced_revision = revision
    c.released_epoch = None
    return True
