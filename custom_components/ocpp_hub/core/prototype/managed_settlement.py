"""Exact terminal records may retire under a caller's fresh zero proof, without forgiving charging debt."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .local_control import local_start_receipt
from .start_settlement import RetirementCandidate, RetirementRule
from .store import RecoveryRequiredError, encoded

if TYPE_CHECKING:
    from .managed import ManagedController
    from .start_settlement import RetirementCandidate, RetirementRule
from .store import Journal


@dataclass(frozen=True)
class TerminalRecord:
    """Immutable metadata, receipt and lifecycle snapshots grant no physical authority by themselves."""

    key: str
    body: str
    request: tuple[Any, ...]
    current_target: int | None = None

    # Bind the only request this candidate may exempt from the pending-request fence.
    @property
    def identifier(self) -> str:
        return str(self.request[0])


# Read all durable request fields, including the original result that must survive terminal metadata cleanup.
def request_snapshot(j: Journal, identifier: Any) -> tuple[Any, ...] | None:
    row = j.db.execute("SELECT * FROM managed_requests WHERE id=?", (identifier,)).fetchone()
    return tuple(row) if row is not None else None


# A failed selection retains the previous verified current and its truthful original failure receipt.
def current_candidate(c: ManagedController) -> TerminalRecord | None:
    from .managed_current import current_failure

    j = c.session.journal
    change = j.get("managed_current_change", {})
    request = request_snapshot(j, change.get("id"))
    target = j.db.execute("SELECT current_a FROM managed_current_requests WHERE id=?", (change.get("id"),)).fetchone()
    if (
        c.current.worker is not None
        and not c.current.worker.done()
        or change.get("state") not in ("uncertain", "interrupted")
        or change.get("mode") not in ("app", "paused")
        or type(change.get("previous_a")) is not int
        or change["previous_a"] != c.current.selected()
        or change["previous_a"] != j.get("control", {}).get("resume_limit")
        or request is None
        or request[1] != "set_current"
        or request[3] not in ("uncertain", "failed")
        or request[2] >= c.requests.intent().revision
        or target is None
        or type(change.get("current_a")) is not int
        or target[0] != change["current_a"]
    ):
        return None
    result = json.loads(request[4])
    if "failure" in change:
        if current_failure(j, change) is None or result != {
            "classification": "current_unconfirmed",
            "requested_current_a": change["current_a"],
            "previous_current_a": change["previous_a"],
        }:
            return None
    elif result != {"classification": "process_replaced"}:
        return None
    return TerminalRecord("managed_current_change", encoded(change), request, current_target=target[0])


# Closed lifecycle or exact known-unsent provenance distinguishes obsolete test metadata from unbound Start debt.
def charge_test_candidate(c: ManagedController) -> RetirementCandidate | None:
    worker = c.charging.worker
    return c.session.journal.starts.retirement(
        RetirementRule.GENERIC, worker_pending=worker is not None and not worker.done()
    )


# A candidate may never outlive replacement of its exact metadata, request, receipt, lifecycle or live lease.
def require_record(c: ManagedController, record: TerminalRecord | RetirementCandidate) -> None:
    if isinstance(record, RetirementCandidate):
        c.session.journal.starts.require(record)
        return
    j = c.session.journal
    if encoded(j.get(record.key, {})) != record.body or request_snapshot(j, record.identifier) != record.request:
        raise RecoveryRequiredError("terminal record changed before settlement")
    if record.current_target is not None:
        target = j.db.execute(
            "SELECT current_a FROM managed_current_requests WHERE id=?", (record.identifier,)
        ).fetchone()
        if target is None or target[0] != record.current_target:
            raise RecoveryRequiredError("terminal current target changed before settlement")


# The caller owns the fresh physical proof and atomic transaction; terminal receipts remain byte for byte intact.
def complete_record(c: ManagedController, record: TerminalRecord | RetirementCandidate) -> None:
    if isinstance(record, RetirementCandidate):
        c.session.journal.starts.retire(record)
        return
    require_record(c, record)
    j, state = c.session.journal, json.loads(record.body)
    if record.key == "managed_current_change":
        if state["previous_a"] != c.current.selected() or state["previous_a"] != j.get("control", {}).get(
            "resume_limit"
        ):
            raise RecoveryRequiredError("previous current selection changed before settlement")
        j.put(record.key, {**state, "state": "cancelled", "recovery_reason": "previous_selection_at_verified_zero"})
        return


# Only closed ordinary charging debt permits automatic metadata retirement or internal Pause completion.
def require_settled_debt(c: ManagedController) -> None:
    from .trial_rollback import require_lifecycle_idle

    j = c.session.journal
    receipt = local_start_receipt(j)
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


# Record an internal restrictive winner in the same transaction as its intent and control claim.
def claim_internal_pause(c: ManagedController, source: str, identifier: str) -> None:
    g, j = c.session.gateway, c.session.journal
    _, ticket = g.controls._claim(True, stopping=True)
    j.put(
        "managed_internal_pause",
        {
            "ticket": ticket,
            "revision": c.requests.intent().revision,
            "source": source,
            "id": identifier,
            "epoch": g._charger_epoch,
            "boot": g.boot_observation,
        },
    )


# Only this source's still-current paused claim may converge; a new Boot, request or owner ticket revokes it.
def internal_pause_matches(c: ManagedController) -> bool:
    g, j = c.session.gateway, c.session.journal
    marker = j.get("managed_internal_pause", {})
    key = {"current": "managed_current_change", "test": "managed_test_charge"}.get(marker.get("source"))
    return bool(
        key is not None
        and marker.get("id") == j.get(key, {}).get("id")
        and marker.get("ticket") == j.get("control", {}).get("ticket")
        and marker.get("revision") == c.requests.intent().revision
        and c.requests.intent().mode == "paused"
        and marker.get("epoch") == g._charger_epoch
        and marker.get("boot") == g.boot_observation
    )
