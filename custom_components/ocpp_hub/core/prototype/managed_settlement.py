"""Exact terminal records may retire under a caller's fresh zero proof, without forgiving charging debt."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .managed_records import TerminalCurrent
from .start_settlement import RetirementCandidate
from .store import RecoveryRequiredError, fingerprint

if TYPE_CHECKING:
    from .managed import ManagedController


# The current outcome owner checks the saved selection and its original receipt.
def current_candidate(c: ManagedController) -> TerminalCurrent | None:
    worker = c.current.worker
    return c.session.journal.current_change.terminal(
        worker_pending=worker is not None and not worker.done(), selected=c.current.selected
    )


# Closed lifecycle or exact known-unsent provenance distinguishes obsolete test metadata from unbound Start debt.
def charge_test_candidate(c: ManagedController, *, timed_only: bool = False) -> RetirementCandidate | None:
    worker = c.charging.worker
    return c.session.journal.starts.delivered_at_zero(
        worker_pending=worker is not None and not worker.done(), timed_only=timed_only
    )


# A candidate may never outlive replacement of its exact metadata, request, receipt, lifecycle or live lease.
def require_record(c: ManagedController, record: TerminalCurrent | RetirementCandidate) -> None:
    if isinstance(record, RetirementCandidate):
        c.session.journal.starts.require(record)
        return
    c.session.journal.current_change.require_terminal(record)


# The caller owns the fresh physical proof and atomic transaction. Keep the original reported failure.
def complete_record(c: ManagedController, record: TerminalCurrent | RetirementCandidate) -> None:
    if isinstance(record, RetirementCandidate):
        c.session.journal.starts.retire(record)
        return
    c.session.journal.current_change.settle_terminal(record, c.current.selected)


# Only closed ordinary charging debt permits automatic metadata retirement or internal Pause completion.
def require_settled_debt(c: ManagedController) -> None:
    from .trial_rollback import require_lifecycle_idle

    j = c.session.journal
    receipt = j.starts.receipt()
    require_lifecycle_idle(c.session.gateway)
    if (
        j.get("vendor_start_grant")
        or j.get("control", {}).get("start_pending")
        or receipt.get("local") is None
        and receipt.get("stage") in ("prepared", "issued", "accepted")
        or j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
        or j.db.execute("SELECT 1 FROM transactions WHERE stop IS NULL OR vendor IS NULL LIMIT 1").fetchone()
    ):
        raise RecoveryRequiredError("terminal settlement awaits actual Start and lifecycle reconciliation")


# This restrictive claim can settle on the same connection after fresh zero proof of the current commissioned Boot.
def internal_pause_matches(c: ManagedController) -> bool:
    g, j = c.session.gateway, c.session.journal
    marker = j.get("managed_internal_pause", {})
    source = marker.get("source")
    if source not in {"current", "test"}:
        return False
    if source == "current":
        identifier = j.current_change.read().identifier
    else:
        identifier = j.get("managed_test_charge", {}).get("id")
    return bool(
        marker.get("id") == identifier
        and marker.get("ticket") == j.get("control", {}).get("ticket")
        and marker.get("revision") == c.requests.intent().revision
        and c.requests.intent().mode == "paused"
        and marker.get("epoch") == g._charger_epoch
        and g.current_boot_seen(g._charger_epoch)
        and fingerprint(j.get("boot")) == c.native.spec.boot_digest
    )
