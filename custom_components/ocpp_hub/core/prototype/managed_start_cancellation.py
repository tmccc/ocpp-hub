"""Retire an exact cancelled, unprepared Start without borrowing authority over another charging session."""

from __future__ import annotations

from enum import Enum, auto
from typing import TYPE_CHECKING

from .start_settlement import RetirementCandidate, RetirementRule
from .store import Journal, fingerprint

if TYPE_CHECKING:
    from .managed_test_charge import ManagedChargeTests, TestLease


class CancellationRetirement(Enum):
    """Waiting for owned I/O or durable settlement must never become permission for a deadline Stop."""

    NOT_APPLICABLE = auto()
    PENDING = auto()
    RETIRED = auto()


# Cancellation selects with its explicit live lease and retains its original metadata/request/raw-receipt preimage.
def unprepared_start(j: Journal, lease: TestLease) -> RetirementCandidate | None:
    return j.starts.retirement(RetirementRule.CANCELLED, lease=lease)


# Only the exact initial zero may request ordinary re-verification of the still-current original app authority.
def initial_zero_recovery(owner: ManagedChargeTests, lease: TestLease) -> bool:
    c, j, g = owner.controller, owner.journal, owner.controller.session.gateway
    control, intent = j.get("control", {}), c.requests.intent()
    return (
        lease.initial_zero_ticket is not None
        and control.get("ticket") == lease.initial_zero_ticket
        and control.get("stop_epoch") == lease.initial_zero_stop_epoch
        and control.get("owner") == "local"
        and control.get("inhibit") is True
        and not control.get("start_pending")
        and lease.mode == intent.mode == "app"
        and lease.revision == intent.revision
        and g._charger_epoch == lease.epoch
        and g.controls._revision == lease.control_revision
        and g.boot_observation == lease.initial_zero_boot
        and g.current_boot_seen(lease.epoch)
        and fingerprint(j.identity) == lease.identity
        and c.native.spec.digest == lease.spec
        and c.current.selected() == lease.current_a
        and not c.current.pending()
        and not c.stopping
        and not c.requests.return_in_progress()
        and j.get("trial_phase") == "active"
        and not j.get("managed_control_fault")
        and not j.get("uncertain_vendor_command")
        and not j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
    )


# Settle an actually cancelled no-dispatch request; any initial zero requires fresh ordinary recovery before release.
def retire_cancelled_start(owner: ManagedChargeTests, lease: TestLease) -> CancellationRetirement:
    worker = owner.worker
    if worker is None or not worker.done() or not worker.cancelled() or owner.lease is not lease:
        return CancellationRetirement.NOT_APPLICABLE
    j, g = owner.journal, owner.controller.session.gateway
    candidate = unprepared_start(j, lease)
    if candidate is None:
        return CancellationRetirement.NOT_APPLICABLE
    if g.controls._operations or g.charger._pending:
        return CancellationRetirement.PENDING
    with j.atomic():
        if (
            owner.worker is not worker
            or owner.lease is not lease
            or unprepared_start(j, lease) != candidate
            or g.controls._operations
            or g.charger._pending
        ):
            return CancellationRetirement.PENDING
        j.starts.retire(candidate)
        recover = initial_zero_recovery(owner, lease)
        if recover:
            j.put("trial_phase", "recovering")
    if recover:
        owner.controller.released_epoch = None
    j.starts.set_lease(None)
    owner.changed.set()
    return CancellationRetirement.RETIRED
