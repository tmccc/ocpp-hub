"""Scoped, resumable cleanup; restoring the original backend is a separately verified final step."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any

from .. import commands
from ..control import HUB_PROFILE_ID
from .gateway import Gateway
from .managed_schedule import awaiting_owner_start
from .managed_start_debt import preserved_start_debt
from .rpc import UnavailableError
from .security import SecurityError
from .store import RecoveryRequiredError
from .trial_evidence import Evidence
from .trial_resources import ResourceBook

Observe = Callable[[], Awaitable[Evidence]]


# Both unplugged and independently protected cleanup require actual lifecycle and vendor-write reconciliation.
def require_lifecycle_idle(
    gateway: Gateway, *, continuing_local: int | None = None, preserve_expired_start: bool = False
) -> None:
    journal = gateway.journal
    lifecycle_pending = journal.db.execute(
        "SELECT 1 FROM outbox WHERE action IN ('StartTransaction','StopTransaction') LIMIT 1"
    ).fetchone()
    if journal.active() != continuing_local or lifecycle_pending:
        raise RecoveryRequiredError("actual Stop, pending start or queued history still requires reconciliation")
    journal.require_vendor_writes_reconciled()
    row = journal.db.execute(
        "SELECT start FROM transactions WHERE local=? AND stop IS NULL", (continuing_local,)
    ).fetchone()
    if awaiting_owner_start(journal, json.loads(row["start"])["idTag"] if row is not None else None) and not (
        preserve_expired_start and preserved_start_debt(journal, continuing_local=continuing_local)
    ):
        raise RecoveryRequiredError("an unbound start still requires independent reconciliation")


class Rollback:
    """An ACK is never deletion proof, and a URL change never proves that a persistent hold disappeared."""

    # The controller owns no socket or file credentials; synthetic and native observers share this interface.
    def __init__(self, gateway: Gateway, book: ResourceBook, observe: Observe) -> None:
        self.gateway, self.book, self.observe = gateway, book, observe

    # Physical unplugged evidence and actual lifecycle completion are required, even when a Stop RPC was Accepted.
    def require_idle(self, evidence: Evidence) -> None:
        evidence.require_unplugged(self.gateway.journal.identity)
        require_lifecycle_idle(self.gateway)

    # Return an exact cleanup count and blockers without changing resources or the backend connection.
    async def plan(self) -> dict[str, Any]:
        evidence = await self.observe()
        self.require_idle(evidence)
        present = self.book.owned_present(evidence)
        return {
            "owned_profiles_to_clear": len(present),
            "station_ceiling_cleared_last": True,
            "backend_restore_automatic": False,
            "automatic_clear_blocked": bool(present and self.book.original_profile_count),
        }

    # Freeze first, then inspect before each exact-id clear; the station ceiling is always the last resource removed.
    async def run(self, quiesce: Callable[[], Awaitable[None]]) -> dict[str, Any]:
        journal = self.gateway.journal
        if journal.get("trial_phase") == "complete":
            raise RecoveryRequiredError("this rollback is already complete")
        if journal.get("trial_phase") not in ("rollback", "backend_restore_pending"):
            # Freeze charging-capable work while retaining shutdown Stop and cloud history if the idle audit fails.
            journal.put("trial_phase", "blocked")
        evidence = await self.observe()
        self.require_idle(evidence)
        self.book.owned_present(evidence)
        journal.put("trial_phase", "rollback")
        self.gateway.controls.invalidate()
        await quiesce()
        epoch = self.gateway._charger_epoch
        async with self.gateway.charger.sequence(0, lambda: self.gateway._require_epoch("charger", epoch)) as call:
            while True:
                evidence = await self.observe()
                self.require_idle(evidence)
                present = self.book.owned_present(evidence)
                if not present:
                    break
                self.book.require_profile_free_baseline()
                resource = present[0]
                if resource.kind != "profile":
                    raise SecurityError("first trial cannot automatically remove native plans")
                journal.put("rollback_pending", {"key": resource.key, "digest": resource.digest})
                answer = await call("ClearChargingProfile", commands.clear_charging_profile(resource.identifier))
                # A lost ACK remains resumable: a later complete native inventory can establish actual absence.
                if answer[0] != 3 or answer[2].get("status") not in ("Accepted", "Unknown"):
                    raise UnavailableError("scoped profile cleanup was not confirmed")
                verification = await self.observe()
                self.require_idle(verification)
                if any(r.key == resource.key for r in self.book.owned_present(verification)):
                    raise UnavailableError("charger acknowledged cleanup but retained the physical profile")
                journal.put("rollback_pending", {})
                if resource.identifier == HUB_PROFILE_ID and self.book.owned_present(verification):
                    raise SecurityError("station ceiling was removed before another owned resource")
        journal.put("rollback_pending", {})
        journal.put("trial_phase", "backend_restore_pending")
        return {"profiles_restored": True, "original_backend_restore_required": True, "complete": False}

    # Share the unchanged strong unplugged verification without losing a separate vehicle-permission obligation.
    async def verify_original_backend(self) -> None:
        journal = self.gateway.journal
        if journal.get("trial_phase") != "backend_restore_pending":
            raise RecoveryRequiredError("resource cleanup has not reached the backend restoration step")
        if self.gateway.charger.sender is not None:
            raise SecurityError("charger is still attached to the proxy; original backend reconnection is unproven")
        epoch = self.gateway._charger_epoch
        evidence = await self.observe()
        if self.gateway.charger.sender is not None or self.gateway._charger_epoch != epoch:
            raise SecurityError("proxy charger connection changed during original-backend verification")
        self.require_idle(evidence)
        if self.book.owned_present(evidence):
            raise SecurityError("trial profiles are still installed")
        if evidence.configuration_digest != self.book.baseline.configuration_digest or not evidence.upstream_connected:
            raise SecurityError("original configuration and native backend connection are not independently verified")

    # Complete the ordinary unplugged flow only after all its original native checks pass.
    async def confirm_original_backend(self) -> dict[str, Any]:
        await self.verify_original_backend()
        self.gateway.journal.put("trial_phase", "complete")
        return {"original_configuration_restored": True, "native_backend_connected": True, "complete": True}
