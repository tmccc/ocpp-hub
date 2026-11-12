"""Explicit owner Pause can reconcile one unconfirmed release while retaining physical zero."""

from __future__ import annotations

import json
import math
import time
from typing import TYPE_CHECKING, Any

from ..control import HUB_PROFILE_ID
from .managed_current_recovery import INTERRUPTED, legacy_current_evidence
from .managed_cutoffs import supervised_timer
from .managed_occurrences import occurrences
from .store import RecoveryRequiredError
from .trial_rollback import require_lifecycle_idle

if TYPE_CHECKING:
    from .managed import ManagedController

ACK = "managed_release_pause_acknowledgement"
PROOF = "managed_release_reconciliation"
FAULT = "owner_release_unconfirmed"


# Legacy ordinary transport latches require their exact stored availability diagnostic, never generic authority drift.
def recoverable_fault(c: ManagedController, fault: dict[str, Any]) -> bool:
    if fault.get("classification") == FAULT:
        return True
    if legacy_current_evidence(c, fault) is not None:
        return True
    if fault.get("classification") != "native_authority_or_preservation_failed":
        return False
    failure = c.session.journal.get("managed_native_failure", {})
    at, observed = fault.get("at_epoch"), failure.get("at_epoch")
    return (
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


# Call inside the existing Pause acceptance transaction after its new paused revision has been recorded.
def record_release_pause(c: ManagedController, identifier: str) -> None:
    j = c.session.journal
    fault = j.get("managed_control_fault", {})
    if not recoverable_fault(c, fault):
        return
    intent = c.requests.intent()
    if intent.mode != "paused":
        raise RecoveryRequiredError("release acknowledgement requires an explicit paused intent")
    acknowledgement = {"fault": fault, "pause_request": identifier, "revision": intent.revision}
    current = legacy_current_evidence(c, fault)
    if current is not None:
        acknowledgement["current_failure_evidence"] = current
    elif fault.get("classification") != FAULT:
        acknowledgement["native_failure"] = j.get("managed_native_failure")
    j.put(ACK, acknowledgement)


# Every post-await check retains the exact diagnostic/request snapshot acknowledged by this particular Pause.
def acknowledged_evidence(c: ManagedController, fault: dict[str, Any], ack: dict[str, Any]) -> bool:
    current = legacy_current_evidence(c, fault)
    if current is not None:
        return bool(ack.get("current_failure_evidence") == current)
    return bool(
        "current_failure_evidence" not in ack
        and (
            fault.get("classification") == FAULT
            or ack.get("native_failure") == c.session.journal.get("managed_native_failure")
        )
    )


# Prove only this fault is recoverable; never clear vendor uncertainty or release positive current here.
async def reconcile_owner_release(c: ManagedController) -> bool:
    s, j, g = c.session, c.session.journal, c.session.gateway
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

    # Any newer Pause, Boot, local operation, request or unrelated write debt revokes this snapshot.
    def guard() -> None:
        control = j.get("control", {})
        if (
            c.stopping
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
            or any(supervised_timer(lease) for lease in occurrences(j).values())
            or any(lease.get("state") == "requested" for lease in occurrences(j).values())
            or j.db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone()
            or j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
            or j.db.execute("SELECT 1 FROM transactions WHERE stop IS NULL OR vendor IS NULL LIMIT 1").fetchone()
            or j.db.execute(
                "SELECT 1 FROM managed_requests WHERE action!='pause' AND state IN ('accepted','running') LIMIT 1"
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
