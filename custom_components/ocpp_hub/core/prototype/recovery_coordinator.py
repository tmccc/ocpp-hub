"""One owned recovery service coordinates prerequisites, physical release and guarded authority publication."""

from __future__ import annotations

import asyncio
import sqlite3
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .connection_scope import control_scope_matches, current_control_scope
from .managed_current import current_source_decision
from .managed_intent import ControlIntent
from .policy import DeniedError
from .recovery_contracts import (
    ALLOW,
    AuthorityStamp,
    GateDecision,
    Pending,
    Prerequisite,
    ReleaseHandoff,
    RestoreProgress,
    RestoreState,
)
from .recovery_policy import (
    PreflightDisposition,
    RecoveryFacts,
    RecoveryObligation,
    RecoveryWork,
    ReleaseDisposition,
    next_recovery_work,
    preflight_disposition,
    release_disposition,
    restore_progress,
)
from .release_attempt import ReleaseAttempt
from .rpc import UnavailableError
from .security import SecurityError
from .store import RecoveryRequiredError

if TYPE_CHECKING:
    from .managed import ManagedController


@dataclass(frozen=True)
class ReturnSettlement:
    """One known request's journal update, independent of its physical work."""

    identifier: str
    intent: ControlIntent
    unconfirmed: bool = False
    await_cloud: bool = False


class RecoveryCoordinator:
    """Domain ledgers remain authoritative; this service owns the common recovery decision and completion path."""

    # This service shares existing domain lifecycles and never holds a global lock across their I/O.
    def __init__(self, controller: ManagedController) -> None:
        self.c = controller
        self._task: asyncio.Task[RestoreProgress] | None = None
        self._return_settlement: ReturnSettlement | None = None

    # Concurrent polls observe the existing owned completion instead of preparing or settling the same work twice.
    def pending(self) -> bool:
        return self._return_settlement is not None or self._task is not None and not self._task.done()

    # A failed receipt update can be retried without repeating native checks or physical commands.
    def settle_for_poll(self) -> bool:
        if self._return_settlement is not None:
            self._settle_return(self._return_settlement, retry=True)
        # A receipt waiting for storage must still allow supervision to maintain the physical hold.
        return self._task is None or self._task.done()

    # Keep the exact disposition until the receipt update commits or a newer Pause supersedes the request.
    def _settle_return(self, settlement: ReturnSettlement, *, retry: bool = False) -> None:
        try:
            requests = self.c.requests
            if not retry or requests.return_current(settlement.identifier, settlement.intent):
                requests.return_waiting(
                    settlement.identifier,
                    settlement.intent,
                    unconfirmed=settlement.unconfirmed,
                    await_cloud=settlement.await_cloud,
                )
        except sqlite3.Error:
            self._return_settlement = settlement
        else:
            self._return_settlement = None

    # Project simultaneous domain duties without replacing their authoritative durable records with one phase.
    def obligations(self) -> RecoveryFacts:
        c, j, g = self.c, self.c.session.journal, self.c.session.gateway
        intent = c.requests.intent()
        change, start = j.current_change.read().value, c.charging.state()
        pending_return = c.requests.pending_return()
        obligations = []
        if change.get("state") == "recovering":
            obligations.append(RecoveryObligation(RecoveryWork.CURRENT, str(change.get("id"))))
        if c.charging.scoped_normal_stop():
            obligations.append(RecoveryObligation(RecoveryWork.EXPIRED_START, str(start.get("id"))))
        if intent.mode == "paused":
            if pending_return is not None:
                obligations.append(RecoveryObligation(RecoveryWork.RETURN, pending_return[0]))
            obligations.append(RecoveryObligation(RecoveryWork.PAUSE, str(intent.revision)))
        if c.ordinary.needed():
            obligations.append(RecoveryObligation(RecoveryWork.ORDINARY, str(intent.revision)))
        if c.released_epoch != g._charger_epoch:
            obligations.append(RecoveryObligation(RecoveryWork.CONNECTION, str(g._charger_epoch)))
        return RecoveryFacts(tuple(obligations), intent.mode == "paused")

    # Run one bounded recovery pass and rebuild obligations after every await.
    async def advance(self) -> Pending | None:
        c, g = self.c, self.c.session.gateway
        visited: frozenset[RecoveryWork] = frozenset()
        while (work := next_recovery_work(self.obligations(), visited)) is not None:
            visited = visited | {work}
            if work is RecoveryWork.CURRENT:
                pending = await c.current.recover_overlap()
                if pending is not None:
                    return pending
            elif work is RecoveryWork.EXPIRED_START:
                if await c.charging.reconcile_expired() and c.requests.intent().mode == "app":
                    return Pending(Prerequisite.LIFECYCLE, "expired Start recovery remains pending")
            if work is RecoveryWork.RETURN:
                request = c.requests.pending_return()
                if request is not None and c.requests.intent().mode == "paused":
                    await c.return_requested(*request)
            elif work is RecoveryWork.PAUSE:
                await c.maintain_pause()
            elif work is RecoveryWork.ORDINARY:
                if not c.session.runtime.status.vendor_connected or g.vendor_registration != "Accepted":
                    await c.ordinary.hold("restricted_introduction")
                    return Pending(Prerequisite.CLOUD, "ordinary recovery awaits the current cloud connection")
                pending = await c.ordinary.recover()
                if pending is not None:
                    return pending
            elif work is RecoveryWork.CONNECTION:
                pending = await self.connection()
                if pending is not None:
                    return pending
        return None

    # Restore the current connection only after its cloud acknowledgement and the common recovery checks.
    async def connection(self) -> Pending | None:
        c, g = self.c, self.c.session.gateway
        if not c.session.runtime.status.vendor_connected or g.vendor_registration != "Accepted":
            return Pending(Prerequisite.CLOUD, "connection restoration awaits the current cloud acknowledgement")
        if c.released_epoch != g._charger_epoch:
            progress = await self.restore()
            if not progress.shared:
                return progress.pending
        return None

    # Caller cancellation cannot discard result interpretation, fault retention or guarded final publication.
    async def restore(
        self,
        expected_intent: ControlIntent | None = None,
        *,
        continuing_local: int | None = None,
        return_request: str | None = None,
        claim_return: bool = False,
    ) -> RestoreProgress:
        launched = await self.c.commands.run(
            lambda: self._begin_restore(expected_intent, continuing_local, return_request, claim_return)
        )
        if isinstance(launched, RestoreProgress):
            return launched
        progress = await asyncio.shield(launched)
        if progress.shared:
            operation = progress.operation
            if (
                operation is None
                or operation.published_authority != self.c.authority()
                or self.c.stopping
                or self.c.session.journal.fault.latched()
            ):
                # A newer owner event can run after the completion exits but before this waiter resumes.
                return RestoreProgress(
                    RestoreState.YIELDED,
                    Pending(Prerequisite.SUPERSEDED, "completed release authority retired before its caller resumed"),
                    operation,
                )
        return progress

    # Claim and launch one exact release in the command processor before any native or cloud waits.
    def _begin_restore(
        self,
        expected_intent: ControlIntent | None,
        continuing_local: int | None,
        return_request: str | None,
        claim_return: bool,
    ) -> asyncio.Task[RestoreProgress] | RestoreProgress:
        if self.c.stopping:
            return RestoreProgress(
                RestoreState.WAITING, Pending(Prerequisite.SUPERSEDED, "managed retirement retains its hold")
            )
        if self.pending():
            return RestoreProgress(
                RestoreState.WAITING,
                Pending(Prerequisite.OWNED_OPERATION, "the current owned recovery completion remains pending"),
            )
        expected = expected_intent or self.c.requests.intent()
        # The request driver claims running; the existing direct restore API retains accepted until publication.
        if claim_return and return_request is not None:
            try:
                claimed = self.c.requests.return_started(return_request, expected)
            except sqlite3.Error:
                return RestoreProgress(
                    RestoreState.WAITING, Pending(Prerequisite.RECONCILIATION, "Return awaits journal storage")
                )
            if not claimed:
                return RestoreProgress(
                    RestoreState.WAITING, Pending(Prerequisite.SUPERSEDED, "Return no longer owns its accepted request")
                )
        try:
            self._task = self.c.session.gateway.controls.launch_completion(
                self._restore_owned(expected, continuing_local=continuing_local, return_request=return_request)
            )
        except DeniedError:
            if claim_return and return_request is not None:
                self._settle_return(ReturnSettlement(return_request, expected, unconfirmed=True))
            raise
        return self._task

    # This completion boundary owns request disposition even if the original poll or HTTP client disappears.
    async def _restore_owned(
        self, expected_intent: ControlIntent, *, continuing_local: int | None, return_request: str | None
    ) -> RestoreProgress:
        try:
            progress = await self._run_restore(
                expected_intent, continuing_local=continuing_local, return_request=return_request
            )
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError, sqlite3.Error:
            if return_request is not None:
                self._settle_return(ReturnSettlement(return_request, expected_intent, unconfirmed=True))
            raise
        if return_request is not None and not progress.shared:
            operation = progress.operation
            await_cloud = False
            if (operation is None or not operation.claimed) and progress.pending is not None:
                disposition = preflight_disposition("explicit", progress.pending)
                if disposition is PreflightDisposition.REQUEST_UNCONFIRMED:
                    self._settle_return(ReturnSettlement(return_request, expected_intent, unconfirmed=True))
                    return RestoreProgress(RestoreState.REQUEST_UNCONFIRMED, progress.pending, operation)
                await_cloud = progress.pending.reason is Prerequisite.CLOUD_ACK
            self._settle_return(ReturnSettlement(return_request, expected_intent, await_cloud=await_cloud))
        return progress

    # All restoration callers consume this same prerequisite, operation-result and publication contract.
    async def _run_restore(
        self,
        expected_intent: ControlIntent,
        *,
        continuing_local: int | None,
        return_request: str | None,
    ) -> RestoreProgress:
        c = self.c
        g, j = c.session.gateway, c.session.journal
        vendor_epoch = g._vendor_epoch

        def cloud_current() -> bool:
            return (
                g._vendor_epoch == vendor_epoch
                and g.vendor.sender is not None
                and g.vendor_registration == "Accepted"
                and c.session.runtime.status.vendor_connected
            )

        can_wait_for_cloud = cloud_current() or (
            return_request is not None and c.requests.return_awaiting_cloud(return_request, expected_intent)
        )

        # This guard also runs before LocalControl claims and again inside each actual outgoing RPC gate.
        def guard() -> GateDecision:
            if c.stopping or not c.requests.return_current(return_request, expected_intent):
                return Pending(Prerequisite.SUPERSEDED, "release awaits its exact current owner Return")
            if not cloud_current():
                reason = (
                    Prerequisite.CLOUD
                    if return_request is None
                    else (Prerequisite.CLOUD_ACK if can_wait_for_cloud else Prerequisite.ZERO_PROOF)
                )
                return Pending(reason, "release awaits its current registered cloud connection")
            if c.ordinary.active():
                decision = c.ordinary.authority_decision(continuing_local)
                if isinstance(decision, Pending):
                    return decision
            current_recovery = j.current_change.read().recovering
            if current_recovery:
                decision = current_source_decision(g, continuing_local)
                if isinstance(decision, Pending):
                    return decision
            if (
                continuing_local is not None
                and not current_recovery
                and not c.ordinary.active()
                and not (c.charging.scoped_normal_stop() and c.charging.current_daily(continuing_local))
            ):
                return Pending(Prerequisite.SOURCE, "source-specific continuation was superseded")
            return ALLOW

        decision = guard()
        if isinstance(decision, Pending):
            return RestoreProgress(RestoreState.WAITING, decision)
        connection = current_control_scope(g)
        if connection is None:
            return RestoreProgress(
                RestoreState.WAITING, Pending(Prerequisite.NATIVE, "release awaits an authenticated Boot")
            )
        epoch, revision = g._charger_epoch, g.controls._revision
        native = await c.native.read_result()
        decision = guard()
        if isinstance(decision, Pending):
            return RestoreProgress(RestoreState.WAITING, decision)
        if isinstance(native, Pending):
            return RestoreProgress(RestoreState.WAITING, native)
        decision = c.native.session_decision(native)
        if isinstance(decision, Pending):
            return RestoreProgress(RestoreState.WAITING, decision)
        if native.physical.transaction_open and continuing_local is None:
            return RestoreProgress(
                RestoreState.WAITING, Pending(Prerequisite.LIFECYCLE, "owner release awaits actual transaction closure")
            )
        decision = c.zero_decision(native, continuing_local=continuing_local)
        if isinstance(decision, Pending):
            return RestoreProgress(RestoreState.WAITING, decision)
        if j.phase.read().value == "recovering":
            resumed = await c.native.resume_result(
                continuing_local=continuing_local, preserve_expired_start=c.charging.preserve_debt
            )
            if isinstance(resumed, Pending):
                return RestoreProgress(RestoreState.WAITING, resumed)
        ticket = j.get("control", {}).get("ticket")
        native = await c.native.read_result(closed=continuing_local is None)
        decision = guard()
        if isinstance(decision, Pending):
            return RestoreProgress(RestoreState.WAITING, decision)
        if isinstance(native, Pending):
            return RestoreProgress(RestoreState.WAITING, native)
        decision = c.zero_decision(native, continuing_local=continuing_local)
        if isinstance(decision, Pending):
            return RestoreProgress(RestoreState.WAITING, decision)
        if (
            c.stopping
            or not control_scope_matches(g, connection)
            or epoch != g._charger_epoch
            or revision != g.controls._revision
            or ticket != j.get("control", {}).get("ticket")
            or not j.phase.read().active
            or j.get("control", {}).get("uncertain")
        ):
            raise UnavailableError("managed release authority changed during its native check")
        decision = guard()
        if isinstance(decision, Pending):
            return RestoreProgress(RestoreState.WAITING, decision)
        if expected_intent.mode == "paused":
            c.complete_pause_observation(native, expected_intent, connection)
            if return_request is not None:
                if not isinstance(ticket, str):
                    raise RecoveryRequiredError("Return requires its current zero hold ticket")
                c.requests.bind_return(return_request, expected_intent, ticket, connection[0].boot)
        scope = connection[0]
        attempt = ReleaseAttempt(
            "automatic" if expected_intent.mode == "app" else "explicit",
            expected_intent,
            scope,
            revision,
            guard,
            before_ticket=ticket,
        )

        # Restrictive rollback keeps its exact Boot proof even when the positive release permission is revoked.
        def rollback_guard() -> None:
            if not control_scope_matches(g, (scope, revision)):
                raise UnavailableError("release rollback belongs to a superseded Boot observation")

        # Final SQL joins the wire child's transaction; this callback grants no permission across an await.
        def publish(authority: AuthorityStamp) -> AuthorityStamp | Pending:
            if not c.publication_is_current(authority):
                return Pending(Prerequisite.SUPERSEDED, "release publication was superseded")
            decision = guard()
            if isinstance(decision, Pending):
                return decision
            j.put("managed_owner_epoch", epoch)
            if return_request is not None:
                c.requests.returned(return_request, expected_intent, c.current.returned_to_previous)
            published = c.authority()
            if published is None:
                raise RecoveryRequiredError("release publication lost its exact Boot")
            return published

        # Only the successful child COMMIT can publish connection completion in memory.
        def committed(authority: AuthorityStamp) -> None:
            c.released_epoch = epoch

        result = await g.controls.release_result(
            attempt,
            guard=guard,
            context=c.session.book.positive_release_guard(guard),
            rollback_guard=rollback_guard,
            handoff=ReleaseHandoff(publish, committed),
        )
        decision = ALLOW if result.published else guard()
        disposition = release_disposition(
            attempt.purpose,
            result,
            c.authority(),
            zero_ready=g.controls.ready and g.controls.desired() == 0,
            faulted=j.fault.latched(),
            publication_allowed=not isinstance(decision, Pending),
        )
        if disposition is ReleaseDisposition.WAIT and result.claimed:
            with j.atomic():
                if c.authority() != result.authority or not g.controls.ready or g.controls.desired() != 0:
                    raise RecoveryRequiredError("owned release zero acknowledgement was superseded")
                assert attempt.ticket is not None
                g.controls._complete(attempt.ticket, uncertain=False)
        if disposition is ReleaseDisposition.FAULT:
            if not c.stopping and c.requests.intent() == expected_intent:
                c.fault("owner_release_unconfirmed")
                failure = result.failure
                try:
                    j.put(
                        "managed_release_failure",
                        {
                            "type": type(failure).__name__ if failure is not None else "prerequisite_after_release",
                            "detail": str(failure)[:1024]
                            if failure is not None
                            else result.pending.detail
                            if result.pending
                            else "release unconfirmed",
                            "at_epoch": time.time(),
                            "charger_rpc": g.charger.diagnostics.snapshot(),
                        },
                    )
                except sqlite3.Error:
                    pass  # The required fault already committed and the command still reports its actual failure.
            if result.failure is not None:
                raise result.failure
            raise RecoveryRequiredError("owned release did not establish shared authority")
        return restore_progress(disposition, result)
