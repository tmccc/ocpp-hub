"""Shared lifecycle prerequisites for native cleanup and managed recovery."""

from __future__ import annotations

import json

from .gateway import Gateway
from .managed_sources import awaiting_owner_start
from .managed_start_debt import preserved_start_debt
from .recovery_contracts import ALLOW, GateDecision, Pending, Prerequisite
from .store import RecoveryRequiredError


# Both unplugged and independently protected cleanup require actual lifecycle and vendor-write reconciliation.
def require_lifecycle_idle(
    gateway: Gateway, *, continuing_local: int | None = None, preserve_expired_start: bool = False
) -> None:
    decision = lifecycle_decision(
        gateway, continuing_local=continuing_local, preserve_expired_start=preserve_expired_start
    )
    if isinstance(decision, Pending):
        raise RecoveryRequiredError(decision.detail)


# Lifecycle closure, quarantined writes and unbound grants remain separate duties at the actual completion boundary.
def lifecycle_decision(
    gateway: Gateway, *, continuing_local: int | None = None, preserve_expired_start: bool = False
) -> GateDecision:
    journal = gateway.journal
    lifecycle_pending = journal.db.execute(
        "SELECT 1 FROM outbox WHERE action IN ('StartTransaction','StopTransaction') LIMIT 1"
    ).fetchone()
    if journal.active() != continuing_local or lifecycle_pending:
        return Pending(
            Prerequisite.LIFECYCLE, "actual Stop, pending start or queued history still requires reconciliation"
        )
    if journal.db.execute("SELECT 1 FROM mutations WHERE origin='vendor' AND state!='confirmed' LIMIT 1").fetchone():
        return Pending(Prerequisite.RECONCILIATION, "an earlier vendor write requires explicit reconciliation")
    row = journal.db.execute(
        "SELECT start FROM transactions WHERE local=? AND stop IS NULL", (continuing_local,)
    ).fetchone()
    if awaiting_owner_start(journal, json.loads(row["start"])["idTag"] if row is not None else None) and not (
        preserve_expired_start and preserved_start_debt(journal, continuing_local=continuing_local)
    ):
        return Pending(Prerequisite.LIFECYCLE, "an unbound start still requires independent reconciliation")
    return ALLOW
