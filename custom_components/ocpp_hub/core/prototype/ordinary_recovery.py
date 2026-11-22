"""Restore only positively identified ordinary owner authority after an admitted station-zero recovery."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .command_processor import CommandPriority
from .managed_sources import ORDINARY_SOURCE
from .native_bounded import NativeReadinessError
from .recovery_contracts import AuthorityStamp, GateDecision, Pending, Prerequisite, require_allowed
from .store import fingerprint

if TYPE_CHECKING:
    from .managed import ManagedController
    from .trial_handover_evidence import HandoverEvidence

SOURCE = ORDINARY_SOURCE


@dataclass(frozen=True)
class OrdinaryCompletion:
    """One completed physical release awaiting its exact bookkeeping write."""

    hold: dict[str, Any]
    authority: AuthorityStamp
    completed_at: float


class OrdinaryRecovery:
    """A station hold never creates a Start, guesses a Stop, or borrows another lifecycle's continuation receipt."""

    # The existing controller retains all native/resource/zero and outgoing ownership guards.
    def __init__(self, controller: ManagedController) -> None:
        self.c = controller
        self.control = controller.session.gateway.controls.decisions.recovery
        self.source_published = False
        self.completion: OrdinaryCompletion | None = None

    # Use the shared owner's exact normal-Start provenance.
    def verified_normal(self) -> bool:
        return self.control.verified_normal(self)

    # Check that ordinary recovery owns the current app intent.
    def ordinary(self) -> bool:
        return self.control.ordinary(self)

    # Check whether the saved hold still belongs to this app intent.
    def active(self) -> bool:
        return self.control.ordinary_active(self)

    # The immutable accepted Start binds identity; a connector number or native session number alone cannot do so.
    def start_digest(self, local: int | None) -> str | None:
        if local is None:
            return None
        row = self.c.session.journal.db.execute(
            "SELECT start FROM transactions WHERE local=? AND stop IS NULL", (local,)
        ).fetchone()
        return fingerprint(json.loads(row["start"])) if row is not None else None

    # Apply the shared owner's source proof without changing the ordinary recovery record.
    def source(self, native: HandoverEvidence) -> dict[str, Any] | None:
        return self.c.session.gateway.controls.decisions.schedule.ordinary_source(self, native)

    # Publish or revoke the latest source proof through the shared owner.
    def observe(self, native: HandoverEvidence) -> None:
        return self.control.observe_ordinary(self, native)

    # Check whether this introduction or an existing hold needs ordinary recovery.
    def needed(self) -> bool:
        return self.control.ordinary_needed(self)

    # Check whether the two reports describe the same session closing.
    def closure_gap(self, native: HandoverEvidence) -> bool:
        return self.control.ordinary_closure_gap(self, native)

    # Execute the zero proof under the shared owner's exact restrictive claim.
    async def hold(self, reason: str) -> None:
        guard = self.c.commands.decide(
            lambda: self.control.claim_ordinary_hold(self, reason), priority=CommandPriority.RESTRICT
        )
        await self.c.session.gateway.controls.ensure_ceiling(guard=guard)

    # This guard also runs inside the real positive RPC gate after all native HTTP waits.
    def require_authority(self, local: int | None, *, introduced: bool = True) -> None:
        require_allowed(self.authority_decision(local, introduced=introduced))

    # Check the exact source and owner at the positive send gate.
    def authority_decision(self, local: int | None, *, introduced: bool = True) -> GateDecision:
        return self.control.ordinary_authority(self, local, introduced=introduced)

    # Repair a superseded Boot's zero introduction only after the full current zero and source proof.
    def confirm_zero_introduction(self, native: HandoverEvidence, local: int | None) -> None:
        require_allowed(self.zero_introduction_decision(native, local))

    # A zero-only introduction acknowledges current proof without replaying Start, Stop or another domain's duty.
    def zero_introduction_decision(self, native: HandoverEvidence, local: int | None) -> GateDecision:
        return self.c.commands.decide(lambda: self.control.confirm_ordinary_zero(self, native, local))

    # Check that native and ledger observations still identify the same session.
    def session_decision(self, native: HandoverEvidence, local: int) -> GateDecision:
        return self.control.ordinary_session(self, native, local)

    # Retry the journal note under the exact release that already completed, without touching the charger.
    def finish_recovery(self) -> bool:
        return self.c.commands.decide(lambda: self.control.settle_ordinary(self))

    # Fresh zero/source proof restores only the original intent and never emits a Start or Stop.
    async def recover(self) -> Pending | None:
        c = self.c
        await self.hold("restricted_introduction")
        try:
            native = await c.native.read_result()
        except NativeReadinessError as error:
            if not error.unplugged_at_zero or not self.closure_gap(error.native):
                raise
            return Pending(Prerequisite.LIFECYCLE, "ordinary zero hold awaits exact native closure")
        if isinstance(native, Pending):
            return native
        ready = self.control.ordinary_preflight(self, native)
        if isinstance(ready, Pending):
            return ready
        hold, local = ready
        progress = await c.recovery.restore(continuing_local=local)
        if not progress.shared:
            return progress.pending
        assert progress.operation is not None and progress.operation.published_authority is not None
        self.completion = OrdinaryCompletion(hold, progress.operation.published_authority, time.time())
        self.finish_recovery()
        return None
