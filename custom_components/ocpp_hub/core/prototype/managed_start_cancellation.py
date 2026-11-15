"""Retire an exact cancelled, unprepared Start without borrowing authority over another charging session."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

from .store import Journal, RecoveryRequiredError, encoded, fingerprint

if TYPE_CHECKING:
    from .managed_test_charge import ManagedChargeTests, TestLease


class CancellationRetirement(Enum):
    """Waiting for owned I/O or durable settlement must never become permission for a deadline Stop."""

    NOT_APPLICABLE = auto()
    PENDING = auto()
    RETIRED = auto()


@dataclass(frozen=True)
class UnpreparedStart:
    """Immutable preimages bind only this management row and leave every earlier receipt unchanged."""

    identifier: str
    action: str
    revision: int
    metadata: str
    request: str
    receipt: str


# A separate settled historical command cannot be confused with preparation of the current managed request.
def previous_receipt(j: Journal, value: Any, identifier: str) -> bool:
    if value is None:
        return True
    if not isinstance(value, dict):
        return False
    prior = value.get("request_id")
    if (
        not isinstance(prior, str)
        or re.fullmatch(r"[a-f0-9]{32}", prior) is None
        or prior == identifier
        or not isinstance(value.get("ticket"), str)
        or re.fullmatch(r"[a-f0-9]{32}", value["ticket"]) is None
        or not isinstance(value.get("digest"), str)
        or re.fullmatch(r"[a-f0-9]{64}", value["digest"]) is None
    ):
        return False
    local, stage = value.get("local"), value.get("stage")
    if local is None:
        return stage in ("unsent", "rejected", "reconciled")
    if type(local) is not int or local <= 0 or stage not in ("issued", "accepted", "reconciled"):
        return False
    row = j.db.execute("SELECT stop FROM transactions WHERE local=?", (local,)).fetchone()
    return row is not None and row["stop"] is not None


# Explicit current-version false markers prove no Start dispatch, while initial restrictive work may have occurred.
def unprepared_start(j: Journal, lease: TestLease) -> UnpreparedStart | None:
    state = j.get("managed_test_charge")
    if (
        lease.source != "local"
        or not isinstance(state, dict)
        or state.get("id") != lease.identifier
        or state.get("source") != "local"
        or state.get("kind") != ("normal" if lease.normal else "test")
        or state.get("state") != "starting"
        or state.get("start_attempted") is not False
        or state.get("start_rpc_accepted") is not False
        or state.get("local_transaction") is not None
        or state.get("tag_digest") is not None
        or type(state.get("revision")) is not int
    ):
        return None
    row = j.db.execute("SELECT * FROM managed_requests WHERE id=?", (lease.identifier,)).fetchone()
    if (
        row is None
        or row["action"] != ("start" if lease.normal else "start_test")
        or row["revision"] != state["revision"]
        or row["state"] not in ("accepted", "running")
    ):
        return None
    receipt = j.get("local_start_receipt")
    if not previous_receipt(j, receipt, lease.identifier):
        return None
    return UnpreparedStart(
        lease.identifier, row["action"], row["revision"], encoded(state), encoded(dict(row)), encoded(receipt)
    )


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
        changed = j.db.execute(
            "UPDATE managed_requests SET state='failed',result=? WHERE id=? AND action=? AND revision=? "
            "AND state IN ('accepted','running')",
            (
                encoded({"classification": "cancelled_before_start_dispatch"}),
                candidate.identifier,
                candidate.action,
                candidate.revision,
            ),
        ).rowcount
        if changed != 1:
            raise RecoveryRequiredError("cancelled Start request changed before retirement")
        j.put(
            "managed_test_charge",
            {
                **json.loads(candidate.metadata),
                "state": "completed",
                "charging": False,
                "recovery_reason": "cancelled_before_start_dispatch",
            },
        )
        recover = initial_zero_recovery(owner, lease)
        if recover:
            j.put("trial_phase", "recovering")
    if recover:
        owner.controller.released_epoch = None
    owner.lease = None
    owner.changed.set()
    return CancellationRetirement.RETIRED
