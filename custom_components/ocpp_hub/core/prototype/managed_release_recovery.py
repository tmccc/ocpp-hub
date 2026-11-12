"""Explicit owner Pause can reconcile one unconfirmed release while retaining physical zero."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING

from ..control import HUB_PROFILE_ID
from .managed_cutoffs import supervised_timer
from .managed_occurrences import occurrences
from .store import RecoveryRequiredError
from .trial_rollback import require_lifecycle_idle

if TYPE_CHECKING:
    from .managed import ManagedController

ACK = "managed_release_pause_acknowledgement"
PROOF = "managed_release_reconciliation"
FAULT = "owner_release_unconfirmed"


# Call inside the existing Pause acceptance transaction after its new paused revision has been recorded.
def record_release_pause(c: ManagedController, identifier: str) -> None:
    j = c.session.journal
    fault = j.get("managed_control_fault", {})
    if fault.get("classification") != FAULT:
        return
    intent = c.requests.intent()
    if intent.mode != "paused":
        raise RecoveryRequiredError("release acknowledgement requires an explicit paused intent")
    j.put(ACK, {"fault": fault, "pause_request": identifier, "revision": intent.revision})


# Prove only this fault is recoverable; never clear vendor uncertainty or release positive current here.
async def reconcile_owner_release(c: ManagedController) -> bool:
    s, j, g = c.session, c.session.journal, c.session.gateway
    fault = j.get("managed_control_fault", {})
    ack = j.get(ACK, {})
    intent = c.requests.intent()
    if (
        fault.get("classification") != FAULT
        or ack.get("fault") != fault
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

    # Any newer Pause, Boot, local operation, request or unrelated write debt revokes this snapshot.
    def guard() -> None:
        control = j.get("control", {})
        if (
            c.stopping
            or c.requests.intent() != intent
            or j.get(ACK) != ack
            or j.get("managed_control_fault") != fault
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
        j.put("control", {**control, "owner": "local", "inhibit": True, "uncertain": True})
        j.put("trial_phase", "recovering")
        j.put("trial_starting", False)
    # fault() invalidated the old revision. This complete closed proof re-establishes only current zero introduction.
    # Never assign a newer revision before every binding/Boot/debt/closed-zero check and the durable commit succeed.
    c.native.introduced_epoch = epoch
    c.native.introduced_revision = revision
    c.released_epoch = None
    return True
