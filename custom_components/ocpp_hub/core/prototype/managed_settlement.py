"""Exact terminal records may retire under a caller's fresh zero proof, without forgiving charging debt."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .local_control import local_start_receipt
from .store import RecoveryRequiredError, encoded, fingerprint

if TYPE_CHECKING:
    from .managed import ManagedController
    from .store import Journal


@dataclass(frozen=True)
class TerminalRecord:
    """Immutable metadata, receipt and lifecycle snapshots grant no physical authority by themselves."""

    key: str
    body: str
    request: tuple[Any, ...]
    receipt: str | None = None
    transaction: tuple[Any, ...] | None = None
    lease: object | None = None
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
def charge_test_candidate(c: ManagedController) -> TerminalRecord | None:
    j, tests = c.session.journal, c.tests
    state, lease = tests.state(), tests.lease
    request = request_snapshot(j, state.get("id"))
    if (
        request is None
        or tests.worker is not None
        and not tests.worker.done()
        or state.get("kind") not in ("normal", "test")
        or state.get("source") not in ("local", "cloud")
        or state.get("state") not in ("interrupted", "expired", "stopping", "running", "charging", "awaiting_power")
        or lease is not None
        and (lease.identifier != state.get("id") or lease.source != state.get("source"))
        or j.active() is not None
        or j.get("control", {}).get("start_pending")
        or j.get("vendor_start_grant")
        or j.get("uncertain_vendor_command")
        or j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
    ):
        return None
    local, receipt = state.get("local_transaction"), local_start_receipt(j)
    normal, cloud = state["kind"] == "normal", state["source"] == "cloud"
    action = "arm_cloud_test" if cloud else "start" if normal else "start_test"
    if request[1] != action or request[2] + int(normal and state.get("start_rpc_accepted") is True) != state.get(
        "revision"
    ):
        return None
    if cloud:
        if normal or local is None and (lease is not None or state.get("state") != "interrupted"):
            return None
        if local is not None and state.get("start_source_verified") is not True:
            return None
    else:
        if local is None and receipt.get("request_id") == state.get("id"):
            local = receipt.get("local")
        if local is None:
            if state.get("start_rpc_accepted") is not False or not (
                state.get("start_attempted") is False
                or receipt.get("request_id") == state.get("id")
                and receipt.get("digest") == state.get("tag_digest")
                and receipt.get("stage") in ("unsent", "rejected")
            ):
                return None
        elif (
            receipt.get("local") != local
            or receipt.get("request_id") not in (None, state.get("id"))
            or receipt.get("stage") not in ("issued", "accepted")
            or receipt.get("digest") != state.get("tag_digest")
            or receipt.get("stage") == "issued"
            and (state.get("start_attempted") is not True or state.get("start_rpc_accepted") is not False)
        ):
            return None
    transaction = None
    if local is not None:
        if type(local) is not int:
            return None
        row = j.db.execute("SELECT local,start,stop,vendor FROM transactions WHERE local=?", (local,)).fetchone()
        if row is None or row["stop"] is None:
            return None
        if not cloud and fingerprint(json.loads(row["start"])["idTag"].casefold()) != state.get("tag_digest"):
            return None
        transaction = tuple(row)
    return TerminalRecord("managed_test_charge", encoded(state), request, encoded(receipt), transaction, lease)


# A candidate may never outlive replacement of its exact metadata, request, receipt, lifecycle or live lease.
def require_record(c: ManagedController, record: TerminalRecord) -> None:
    j = c.session.journal
    if encoded(j.get(record.key, {})) != record.body or request_snapshot(j, record.identifier) != record.request:
        raise RecoveryRequiredError("terminal record changed before settlement")
    if record.current_target is not None:
        target = j.db.execute(
            "SELECT current_a FROM managed_current_requests WHERE id=?", (record.identifier,)
        ).fetchone()
        if target is None or target[0] != record.current_target:
            raise RecoveryRequiredError("terminal current target changed before settlement")
    if record.receipt is not None and (
        encoded(local_start_receipt(j)) != record.receipt or c.tests.lease is not record.lease
    ):
        raise RecoveryRequiredError("terminal Start receipt changed before settlement")
    if record.transaction is not None:
        row = j.db.execute(
            "SELECT local,start,stop,vendor FROM transactions WHERE local=?", (record.transaction[0],)
        ).fetchone()
        if row is None or tuple(row) != record.transaction:
            raise RecoveryRequiredError("terminal lifecycle changed before settlement")


# The caller owns the fresh physical proof and atomic transaction; terminal receipts remain byte for byte intact.
def complete_record(c: ManagedController, record: TerminalRecord) -> None:
    require_record(c, record)
    j, state = c.session.journal, json.loads(record.body)
    if record.key == "managed_current_change":
        if state["previous_a"] != c.current.selected() or state["previous_a"] != j.get("control", {}).get(
            "resume_limit"
        ):
            raise RecoveryRequiredError("previous current selection changed before settlement")
        j.put(record.key, {**state, "state": "cancelled", "recovery_reason": "previous_selection_at_verified_zero"})
        return
    local = record.transaction[0] if record.transaction is not None else None
    unissued = local is None and state["source"] == "local"
    result = (
        {"classification": "start_not_accepted_at_verified_zero"}
        if unissued
        else {"closed_at_verified_zero": True, "charging": False}
    )
    j.db.execute(
        "UPDATE managed_requests SET state=?,result=? WHERE id=? AND action=? AND revision=? "
        "AND state IN ('accepted','running','uncertain')",
        ("failed" if unissued else "completed", encoded(result), *record.request[:3]),
    )
    j.put(
        record.key,
        {
            **state,
            "state": "completed",
            "charging": False,
            "local_transaction": local,
            "recovery_reason": "start_not_accepted_at_verified_zero"
            if unissued
            else "owned_operation_settled_at_verified_zero",
        },
    )


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
    from .managed_occurrences import occurrences

    if any(v.get("temporary_attempts") for v in occurrences(j).values()):
        raise RecoveryRequiredError("terminal settlement awaits genuine temporary Start attempts")


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
