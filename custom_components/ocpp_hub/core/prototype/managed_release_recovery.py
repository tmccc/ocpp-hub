"""Explicit owner Pause can reconcile one unconfirmed release while retaining physical zero."""

from __future__ import annotations

import math
import time
from typing import TYPE_CHECKING, Any

from .control_state import ControlState
from .managed_current import managed_current_ceiling
from .managed_current_recovery import legacy_current_evidence
from .managed_debt_recovery import complete_operator_recovery, operator_recovery_evidence
from .managed_delivery_recovery import (
    complete_delivery_recovery,
    delivery_recovery_digest,
    delivery_recovery_evidence,
    pending_delivery_proofs,
)
from .managed_intent import RELEASE_PAUSE_ACK, retire_pause_ack, saved_pause_ack
from .managed_native_repair import repair_pending, repair_revoked
from .managed_settlement import charge_test_candidate, complete_record, current_candidate, require_record
from .managed_start_delivery import DENIALS, complete_denied_start, denial_faults, denied_start_evidence
from .native_faults import MEASURED_FAULT, measurement_cause
from .store import RecoveryRequiredError, fingerprint
from .trial_rollback import require_lifecycle_idle

if TYPE_CHECKING:
    from .managed import ManagedController
    from .trial_handover_evidence import HandoverEvidence

ACK = RELEASE_PAUSE_ACK
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
    delivery = delivery_recovery_evidence(c, fault, operator)
    if pending_delivery_proofs(c.session.journal) and delivery is None:
        return None
    if delivery is not None:
        evidence["delivery_reconciliation"] = delivery_recovery_digest(delivery)
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
        failure.get("type") in ("NativeTransportUnavailableError", "StaleEvidenceError", "NativeReadinessError")
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


# Capture the current fault evidence inside the Pause acceptance transaction.
def release_pause_ack(c: ManagedController, identifier: str) -> dict[str, Any] | None:
    j = c.session.journal
    fault = j.fault.read({})
    if not fault:
        return None
    try:
        evidence = release_evidence(c, fault)
    except AttributeError, KeyError, TypeError, ValueError:
        return None  # Malformed historical proof cannot delay the basic Pause.
    if evidence is None:
        return None
    intent = c.requests.intent()
    if intent.mode != "paused":
        raise RecoveryRequiredError("release acknowledgement requires an explicit paused intent")
    return {"fault": fault, "pause_request": identifier, "revision": intent.revision, **evidence}


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
    fault = j.fault.read({})
    ack = saved_pause_ack(j) or {}
    intent = c.requests.intent()
    if (
        not recoverable_fault(c, fault)
        or ack.get("fault") != fault
        or not acknowledged_evidence(c, fault, ack)
        or intent.mode != "paused"
        or ack.get("revision") != intent.revision
    ):
        return False
    epoch, revision = g._charger_epoch, g.controls._revision
    boot = g.boot_observation
    ticket = j.get("control", {}).get("ticket")
    current = legacy_current_evidence(c, fault)
    terminal_current = current_candidate(c) if current is None else None
    terminal_test = charge_test_candidate(c)
    terminal = [record for record in (terminal_current, terminal_test) if record is not None]
    operator = operator_recovery_evidence(c, fault)
    delivery = delivery_recovery_evidence(c, fault, operator)
    denied = denied_start_evidence(c, fault)

    # Any newer Pause, Boot, local operation, request or unrelated write debt revokes this snapshot.
    def common_guard() -> None:
        for record in terminal:
            require_record(c, record)
        control = j.get("control", {})
        if (
            c.stopping
            or c.native.introduction_pending
            or repair_pending(j, fault)
            or repair_revoked(j, fault)
            or c.requests.intent() != intent
            or saved_pause_ack(j) != ack
            or j.fault.read() != fault
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
            or not j.phase.read().supervised
            or j.get("native_bounded_handover", {}).get("stage") != "admitted"
            or j.get("native_bounded_handover", {}).get("fault")
            or j.get("trial_preservation_fault")
            or j.get("trial_inventory_fault")
            or g.vendor_write_evidence().unresolved
            or j.get("vendor_start_grant")
            or control.get("ticket") != ticket
            or not ticket
            or not ControlState(control).explicit_local_hold
            or control.get("start_pending")
            or not g.controls.ready
            or g.controls.desired() != 0
            or c.pause_task is not None
            and not c.pause_task.done()
            or g.controls._operations
            or g.charger._pending
            or g.vendor._pending
            or j.current_change.read().unresolved
            and current is None
            and terminal_current is None
            or j.get("managed_current_prepare_intent", {}).get("stage") not in (None, "complete")
            or j.get("managed_test_charge", {}).get("state") not in (None, "completed")
            and not (denied_start_evidence(c, fault) or {}).get("normal")
            and terminal_test is None
            or j.db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone()
            or j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
            or j.db.execute("SELECT 1 FROM transactions WHERE stop IS NULL OR vendor IS NULL LIMIT 1").fetchone()
            or any(
                row["id"] != ((denied_start_evidence(c, fault) or {}).get("normal") or {}).get("id")
                and row["id"] != (terminal_test.identifier if terminal_test is not None else None)
                for row in j.db.execute(
                    "SELECT id,action,revision FROM managed_requests "
                    "WHERE action!='pause' AND state IN ('accepted','running')"
                )
            )
        ):
            raise RecoveryRequiredError("release reconciliation awaits the exact paused zero and settled journal")
        c.native.spec.require_boot(j.get("boot"))
        require_lifecycle_idle(g)

    # Original timetable, closed native zero and the exact current Pause must agree at every completion boundary.
    def require_original_zero(native: HandoverEvidence) -> None:
        common_guard()
        if native.saved_schedule_digest != c.native.spec.saved_schedule_digest or j.get("trial_binding") != {
            "configuration_digest": c.native.binding_digest(native),
            "station": c.native.choice.value(),
            "boot_digest": c.native.spec.boot_digest,
        }:
            raise RecoveryRequiredError("release reconciliation awaits closed native zero and exact configuration")
        if c.native.stop_zero_refusal(native) is not None:
            raise RecoveryRequiredError("release reconciliation awaits independently confirmed owned station zero")

    common_guard()
    # Two fresh native proofs retain the original source-publication and final-publication boundaries.
    native = await c.native.read(closed=True)
    c.commands.decide(lambda: require_original_zero(native))
    common_guard()
    native = await c.native.read(closed=True)

    # Apply the second verified zero and its fault/receipt changes before publishing the recovered revision.
    def settle() -> bool:
        require_original_zero(native)
        with j.atomic():
            require_original_zero(native)
            control = j.get("control")
            for record in terminal:
                complete_record(c, record)
            j.put(
                PROOF,
                {
                    "prior_fault": fault,
                    "prior_native_failure": ack.get("native_failure"),
                    "prior_current_failure": current,
                    "delivery_reconciliation": delivery_recovery_digest(delivery) if delivery is not None else None,
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
            j.fault.clear()
            retire_pause_ack(j, fault)
            if delivery is not None:
                complete_delivery_recovery(j, delivery, ack["pause_request"])
            if operator is not None:
                complete_operator_recovery(j, operator, ack["pause_request"])
            if denied is not None:
                complete_denied_start(j, denied, ack["pause_request"])
            if current is not None:
                control = {**control, "resume_limit": current["previous_a"]}
                j.current_change.reconcile_at_zero(current["change"], current["previous_a"])
            j.phase.recover_introduced(control)
        # Publish the current zero introduction after its evidence checks and durable commit.
        c.native.audited_epoch = epoch
        c.native.introduced_epoch, c.native.introduced_revision = epoch, revision
        c.released_epoch = None
        if terminal_test is not None:
            c.session.journal.starts.set_lease(None)
            c.charging.changed.set()
        return True

    return c.commands.decide(settle)
