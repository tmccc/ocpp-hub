"""One owned recovery service coordinates prerequisites, physical release and guarded authority publication."""

from __future__ import annotations

import asyncio
import sqlite3
from functools import partial
from typing import TYPE_CHECKING

from .connection_scope import current_control_scope
from .managed_intent import ControlIntent, ReturnSettlement
from .policy import DeniedError
from .recovery_contracts import (
    AuthorityStamp,
    Pending,
    Prerequisite,
    ReleaseHandoff,
    RestoreProgress,
    RestoreState,
)
from .recovery_policy import (
    RecoveryWork,
    next_recovery_work,
)
from .release_attempt import ReleaseAttempt
from .rpc import UnavailableError
from .security import SecurityError
from .store import RecoveryRequiredError

if TYPE_CHECKING:
    from .managed import ManagedController


class RecoveryCoordinator:
    """Run the owned recovery task and retry saving its exact Return result."""

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
        self.c.commands.decide(
            lambda: self.c.session.gateway.controls.decisions.write_return_settlement(self, settlement, retry)
        )

    # Run one bounded recovery pass and rebuild obligations after every await.
    async def advance(self) -> Pending | None:
        c = self.c
        decisions = c.session.gateway.controls.decisions.recovery
        # A refused note must not rerun the already released command or interrupt ordinary monitoring.
        visited = frozenset({RecoveryWork.ORDINARY}) if c.ordinary.finish_recovery() else frozenset()
        while (work := next_recovery_work(decisions.obligations(c), visited)) is not None:
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
                    if c.session.journal.fault.latched():
                        return Pending(Prerequisite.RECONCILIATION, "Return retained its protective fault")
            elif work is RecoveryWork.PAUSE:
                await c.maintain_pause()
                intent = decisions.offline_restore_intent(c)
                if intent is not None:
                    await self.restore(intent, offline_pause=True)
            elif work is RecoveryWork.ORDINARY:
                if not decisions.cloud_ready(c):
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
        c = self.c
        needed = c.session.gateway.controls.decisions.recovery.connection_restore(c)
        if isinstance(needed, Pending):
            return needed
        if needed:
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
        offline_pause: bool = False,
    ) -> RestoreProgress:
        launched = await self.c.commands.run(
            lambda: self._begin_restore(expected_intent, continuing_local, return_request, claim_return, offline_pause)
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
        offline_pause: bool,
    ) -> asyncio.Task[RestoreProgress] | RestoreProgress:
        expected = self.c.session.gateway.controls.decisions.claim_restore_request(
            self, expected_intent, return_request, claim_return, offline_pause
        )
        if isinstance(expected, RestoreProgress):
            return expected
        try:
            self._task = self.c.session.gateway.controls.launch_completion(
                self._restore_owned(
                    expected,
                    continuing_local=continuing_local,
                    return_request=return_request,
                    offline_pause=offline_pause,
                )
            )
        except DeniedError:
            if claim_return and return_request is not None:
                self._settle_return(ReturnSettlement.failed(return_request, expected, possibly_sent=False))
            raise
        return self._task

    # This completion boundary owns request disposition even if the original poll or HTTP client disappears.
    async def _restore_owned(
        self,
        expected_intent: ControlIntent,
        *,
        continuing_local: int | None,
        return_request: str | None,
        offline_pause: bool,
    ) -> RestoreProgress:
        attempt: ReleaseAttempt | None = None
        try:
            try:
                prepared = await self._prepare_restore(expected_intent, continuing_local, return_request, offline_pause)
            except sqlite3.Error as error:
                c = self.c
                g, j = c.session.gateway, c.session.journal
                repair = c.native.repair_audits
                if (
                    return_request is None
                    or j.db.in_transaction
                    or not g.controls.ready
                    or g.controls.desired() != 0
                    or repair is not None
                    and repair.pending_storage_error is error
                ):
                    raise
                # Preparation cannot send a positive profile. This request can fail at already verified zero.
                prepared = RestoreProgress(
                    RestoreState.WAITING,
                    Pending(Prerequisite.RECONCILIATION, "Return proof could not be saved before sending", error),
                )
            if isinstance(prepared, ReleaseAttempt):
                attempt = prepared
                progress = await self._run_release(attempt, offline_pause)
            else:
                progress = prepared
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError, sqlite3.Error:
            if return_request is not None:
                self._settle_return(
                    ReturnSettlement.failed(
                        return_request,
                        expected_intent,
                        possibly_sent=bool(attempt and attempt.positive_may_have_dispatched),
                    )
                )
            raise
        if return_request is not None:
            settlement, progress = ReturnSettlement.observed(return_request, expected_intent, progress)
            if settlement is not None:
                self._settle_return(settlement)
        return progress

    # Check the current facts and bind the accepted request before a positive command can start.
    async def _prepare_restore(
        self,
        expected_intent: ControlIntent,
        continuing_local: int | None,
        return_request: str | None,
        offline_pause: bool,
    ) -> RestoreProgress | ReleaseAttempt:
        c = self.c
        g, j = c.session.gateway, c.session.journal
        decisions = g.controls.decisions
        guard = decisions.release_guard(c, expected_intent, continuing_local, return_request, offline_pause)

        decision = guard()
        if isinstance(decision, Pending):
            return RestoreProgress(RestoreState.WAITING, decision)
        connection = current_control_scope(g)
        if connection is None:
            return RestoreProgress(
                RestoreState.WAITING, Pending(Prerequisite.NATIVE, "release awaits an authenticated Boot")
            )
        revision = connection[1]
        native = await c.native.read_result()
        decision = decisions.recovery.release_observation(c, guard, native, continuing_local)
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

        attempt = ReleaseAttempt(
            "automatic" if expected_intent.mode == "app" or offline_pause else "explicit",
            expected_intent,
            connection[0],
            revision,
            guard,
            before_ticket=ticket,
            request_id=return_request,
            continuing_local=continuing_local,
        )
        return c.commands.decide(lambda: decisions.prepare_release_attempt(c, attempt, native))

    # The wire child records possible dispatch and publishes the result under the original scope.
    async def _run_release(self, attempt: ReleaseAttempt, offline_pause: bool) -> RestoreProgress:
        c = self.c
        g = c.session.gateway
        decisions = g.controls.decisions
        guard, epoch = attempt.positive_guard, attempt.scope.epoch

        # Only the successful child COMMIT can publish connection completion in memory.
        def committed(authority: AuthorityStamp) -> None:
            c.released_epoch = epoch

        result = await g.controls.release_result(
            attempt,
            guard=guard,
            context=c.session.book.positive_release_guard(guard),
            rollback_guard=partial(decisions.require_release_boot, g, attempt),
            handoff=ReleaseHandoff(partial(decisions.publish_release, c, attempt, offline_pause), committed),
        )

        return c.commands.decide(lambda: decisions.settle_release(c, attempt, result))
