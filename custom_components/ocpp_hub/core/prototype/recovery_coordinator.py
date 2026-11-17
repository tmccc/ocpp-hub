"""One owned recovery service coordinates prerequisites, physical release and guarded authority publication."""

from __future__ import annotations

import asyncio
import sqlite3
import time
from typing import TYPE_CHECKING

from .connection_scope import current_scope
from .managed_current import current_source_decision
from .managed_intent import ControlIntent
from .managed_schedule import KEY, current_source, supervised_timer, temporary_continuation
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


class RecoveryCoordinator:
    """Domain ledgers remain authoritative; this service owns the common recovery decision and completion path."""

    # This service shares existing domain lifecycles and never holds a global lock across their I/O.
    def __init__(self, controller: ManagedController) -> None:
        self.c = controller
        self._task: asyncio.Task[RestoreProgress] | None = None

    # Concurrent polls observe the existing owned completion instead of preparing or settling the same work twice.
    def pending(self) -> bool:
        return self._task is not None and not self._task.done()

    # Rebuild the current exact identity at each send/publication boundary; an earlier snapshot is not permission.
    def authority(self) -> AuthorityStamp | None:
        c, g = self.c, self.c.session.gateway
        scope = current_scope(g)
        if scope is None:
            return None
        return AuthorityStamp(
            c.requests.intent(), scope, g.controls._revision, c.session.journal.get("control", {}).get("ticket")
        )

    # Project simultaneous domain duties without replacing their authoritative durable records with one phase.
    def obligations(self) -> RecoveryFacts:
        c, j, g = self.c, self.c.session.journal, self.c.session.gateway
        intent = c.requests.intent()
        change, start, timer = j.get("managed_current_change", {}), c.charging.state(), j.get(KEY, {})
        pending_return = c.requests.pending_return()
        obligations = []
        if change.get("state") == "recovering":
            obligations.append(RecoveryObligation(RecoveryWork.CURRENT, str(change.get("id"))))
        if c.charging.scoped_normal_stop():
            obligations.append(RecoveryObligation(RecoveryWork.EXPIRED_START, str(start.get("id"))))
        if supervised_timer(timer):
            obligations.append(RecoveryObligation(RecoveryWork.TIMER_DEADLINE, str(timer.get("id"))))
        if intent.mode == "paused":
            if pending_return is not None:
                obligations.append(RecoveryObligation(RecoveryWork.RETURN, pending_return[0]))
            obligations.append(RecoveryObligation(RecoveryWork.PAUSE, str(intent.revision)))
        if c.ordinary.needed():
            obligations.append(RecoveryObligation(RecoveryWork.ORDINARY, str(intent.revision)))
        if c.released_epoch != g._charger_epoch:
            obligations.append(RecoveryObligation(RecoveryWork.CONNECTION, str(g._charger_epoch)))
        return RecoveryFacts(tuple(obligations), intent.mode == "paused")

    # Run one bounded recovery pass; rebuild facts after each await and leave urgent deadline zero independent.
    async def advance(self) -> Pending | None:
        c, j, g = self.c, self.c.session.journal, self.c.session.gateway
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
            elif work is RecoveryWork.TIMER_DEADLINE:
                await c.schedule.protect_deadline(j.get(KEY, {}))
            elif work is RecoveryWork.RETURN:
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

    # Connection restoration retains timer-specific physical duties before using the same common restore service.
    async def connection(self) -> Pending | None:
        c, j, g = self.c, self.c.session.journal, self.c.session.gateway
        timer = j.get(KEY, {})
        pending_timer = supervised_timer(timer)
        cutoff_hold = pending_timer and "hold_since" in timer
        if cutoff_hold:
            pending = await c.schedule.hold_until_closed()
            if pending is not None:
                return pending
            if c.schedule.awaiting_start() and j.active() is None:
                native = await c.native.read_result(closed=True)
                if isinstance(native, Pending):
                    return native
                c.require_paused(native, timer_cleanup=True)
                await c.schedule.observe(native)
        elif pending_timer and j.active() is not None and c.native.introduced_epoch == g._charger_epoch:
            native = await c.native.read_result()
            if isinstance(native, Pending):
                return native
            await c.schedule.observe(native)
        if not c.session.runtime.status.vendor_connected or g.vendor_registration != "Accepted":
            if cutoff_hold and j.get("trial_phase") == "active":
                native = await c.native.read_result(closed=True)
                if isinstance(native, Pending):
                    return native
                c.require_paused(native)
                decision = await c.schedule.before_release_result(native)
                if isinstance(decision, Pending):
                    return decision
            return Pending(Prerequisite.CLOUD, "connection restoration awaits the current cloud acknowledgement")
        if j.get(KEY, {}).get("idle_cutoff_hold"):
            if j.get("trial_phase") == "recovering":
                resumed = await c.native.resume_result()
                if isinstance(resumed, Pending):
                    return resumed
        elif c.released_epoch != g._charger_epoch:
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
            self.c.requests.return_started(return_request)
        try:
            self._task = self.c.session.gateway.controls.launch_completion(
                self._restore_owned(expected, continuing_local=continuing_local, return_request=return_request)
            )
        except DeniedError:
            if claim_return and return_request is not None:
                self.c.requests.return_waiting(return_request, expected, unconfirmed=True)
            raise
        progress = await asyncio.shield(self._task)
        if progress.shared:
            operation = progress.operation
            if (
                operation is None
                or operation.published_authority != self.authority()
                or self.c.stopping
                or self.c.session.journal.get("managed_control_fault")
            ):
                # A newer owner event can run after the completion exits but before this waiter resumes.
                return RestoreProgress(
                    RestoreState.YIELDED,
                    Pending(Prerequisite.SUPERSEDED, "completed release authority retired before its caller resumed"),
                    operation,
                )
        return progress

    # This completion boundary owns request disposition even if the original poll or HTTP client disappears.
    async def _restore_owned(
        self, expected_intent: ControlIntent, *, continuing_local: int | None, return_request: str | None
    ) -> RestoreProgress:
        c = self.c
        try:
            progress = await self._run_restore(
                expected_intent, continuing_local=continuing_local, return_request=return_request
            )
            if return_request is not None and not progress.shared:
                operation = progress.operation
                if (operation is None or not operation.claimed) and progress.pending is not None:
                    disposition = preflight_disposition("explicit", progress.pending)
                    if disposition is PreflightDisposition.REQUEST_UNCONFIRMED:
                        c.requests.return_waiting(return_request, expected_intent, unconfirmed=True)
                        return RestoreProgress(RestoreState.REQUEST_UNCONFIRMED, progress.pending, operation)
                c.requests.return_waiting(return_request, expected_intent)
            return progress
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, TimeoutError, sqlite3.Error:
            if return_request is not None:
                c.requests.return_waiting(return_request, expected_intent, unconfirmed=True)
            raise

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

        # This guard also runs before LocalControl claims and again inside each actual outgoing RPC gate.
        def guard() -> GateDecision:
            if not c.requests.return_current(return_request, expected_intent):
                return Pending(Prerequisite.SUPERSEDED, "release awaits its exact current owner Return")
            timer = j.get(KEY, {})
            if c.ordinary.active():
                decision = c.ordinary.authority_decision(continuing_local)
                if isinstance(decision, Pending):
                    return decision
            current_recovery = j.get("managed_current_change", {}).get("state") == "recovering"
            if current_recovery:
                decision = current_source_decision(g, continuing_local)
                if isinstance(decision, Pending):
                    return decision
            if (
                continuing_local is not None
                and not current_recovery
                and not c.ordinary.active()
                and not (c.charging.scoped_normal_stop() and c.charging.current_daily(continuing_local))
                and (
                    not current_source(g, timer)
                    or c.schedule.needs_source(timer)
                    or j.active() != continuing_local
                    or timer.get("other_local") != continuing_local
                    and (
                        timer.get("unidentified_local") != continuing_local
                        or not temporary_continuation(j, timer, continuing_local)
                    )
                    or timer.get("unreported_native")
                )
            ):
                return Pending(Prerequisite.SOURCE, "source-specific continuation was superseded")
            if c.stopping or c.requests.intent() != expected_intent:
                return Pending(Prerequisite.SUPERSEDED, "a newer requested hold superseded managed release")
            return ALLOW

        decision = guard()
        if isinstance(decision, Pending):
            return RestoreProgress(RestoreState.WAITING, decision)
        epoch, revision = g._charger_epoch, g.controls._revision
        native = await c.native.read_result()
        if isinstance(native, Pending):
            return RestoreProgress(RestoreState.WAITING, native)
        decision = await c.schedule.closure_decision(native)
        if isinstance(decision, Pending):
            return RestoreProgress(RestoreState.WAITING, decision)
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
        if j.get("trial_phase") == "recovering":
            resumed = await c.native.resume_result(
                continuing_local=continuing_local, preserve_expired_start=c.charging.preserve_debt
            )
            if isinstance(resumed, Pending):
                return RestoreProgress(RestoreState.WAITING, resumed)
        ticket = j.get("control", {}).get("ticket")
        native = await c.native.read_result(closed=continuing_local is None)
        if isinstance(native, Pending):
            return RestoreProgress(RestoreState.WAITING, native)
        decision = c.zero_decision(native, continuing_local=continuing_local)
        if isinstance(decision, Pending):
            return RestoreProgress(RestoreState.WAITING, decision)
        if continuing_local is None and j.get("managed_temporary_schedule", {}).get("state") in (
            "installing",
            "armed",
            "clearing",
        ):
            decision = await c.schedule.before_release_result(native)
            if isinstance(decision, Pending):
                return RestoreProgress(RestoreState.WAITING, decision)
            native = await c.native.read_result(closed=continuing_local is None)
            if isinstance(native, Pending):
                return RestoreProgress(RestoreState.WAITING, native)
            decision = c.zero_decision(native, continuing_local=continuing_local)
            if isinstance(decision, Pending):
                return RestoreProgress(RestoreState.WAITING, decision)
        if (
            c.stopping
            or epoch != g._charger_epoch
            or revision != g.controls._revision
            or ticket != j.get("control", {}).get("ticket")
            or j.get("trial_phase") != "active"
            or j.get("control", {}).get("uncertain")
        ):
            raise UnavailableError("managed release authority changed during its native check")
        release_guard = c.schedule.release_decision if continuing_local is None and not c.ordinary.active() else guard
        decision = release_guard()
        if isinstance(decision, Pending):
            return RestoreProgress(RestoreState.WAITING, decision)
        if expected_intent.mode == "paused":
            decision = guard()
            if isinstance(decision, Pending):
                return RestoreProgress(RestoreState.WAITING, decision)
            c.require_paused(native)
            c.requests.confirm_pauses()
        scope = current_scope(g)
        if scope is None:
            raise UnavailableError("managed release awaits a current actual Boot")
        attempt = ReleaseAttempt(
            "automatic" if expected_intent.mode == "app" else "explicit",
            expected_intent,
            scope,
            revision,
            release_guard,
            before_ticket=ticket,
        )

        # Restrictive rollback keeps its exact Boot proof even when the positive release permission is revoked.
        def rollback_guard() -> None:
            if current_scope(g) != scope or g.controls._revision != revision:
                raise UnavailableError("release rollback belongs to a superseded Boot observation")

        # Final SQL joins the wire child's transaction; this callback grants no permission across an await.
        def publish(authority: AuthorityStamp) -> AuthorityStamp | Pending:
            if not j.db.in_transaction or self.authority() != authority:
                return Pending(Prerequisite.SUPERSEDED, "release publication was superseded")
            decision = guard()
            if isinstance(decision, Pending):
                return decision
            decision = release_guard()
            if isinstance(decision, Pending):
                return decision
            j.put("managed_owner_epoch", epoch)
            timer = j.get(KEY, {})
            if timer.get("state") == "complete" and not timer.get("idle_cutoff_hold") and j.active() is None:
                # Inventory cleanup can release this verified idle profile while its separate cutoff remains pending.
                j.put(
                    KEY,
                    {
                        k: v
                        for k, v in timer.items()
                        if k not in ("hold_since", "next_cap", "next_stop", "unidentified_local")
                    },
                )
            if continuing_local is not None:
                c.schedule.clear_idle_cutoff("verified_session")
            elif expected_intent.mode == "paused" and c.schedule.owner_returning():
                c.schedule.clear_idle_cutoff("owner_return")
            timer = j.get(KEY, {})
            if (
                timer.get("state") == "requested"
                and timer.get("idle_cutoff_replacement")
                and timer["revision"] == expected_intent.revision
                and timer["epoch"] == epoch
            ):
                # This newly authorized one-off was accepted under zero; bind its installation to this verified release.
                j.put(KEY, {**timer, "ticket": j.get("control", {}).get("ticket"), "idle_cutoff_replacement": False})

            if return_request is not None:
                c.requests.returned(return_request, expected_intent, c.current.returned_to_previous)
            published = self.authority()
            if published is None:
                raise RecoveryRequiredError("release publication lost its exact Boot")
            return published

        # Only the successful child COMMIT can publish connection completion in memory.
        def committed(authority: AuthorityStamp) -> None:
            c.released_epoch = epoch

        result = await g.controls.release_result(
            attempt,
            guard=guard,
            context=c.session.book.positive_release_guard(release_guard),
            rollback_guard=rollback_guard,
            handoff=ReleaseHandoff(publish, committed),
        )
        decision = ALLOW if result.published else guard()
        disposition = release_disposition(
            attempt.purpose,
            result,
            self.authority(),
            zero_ready=g.controls.ready and g.controls.desired() == 0,
            faulted=bool(j.get("managed_control_fault")),
            timer_superseded=c.schedule.release_superseded(attempt),
            publication_allowed=not isinstance(decision, Pending),
        )
        if disposition is ReleaseDisposition.WAIT and result.claimed:
            with j.atomic():
                if self.authority() != result.authority or not g.controls.ready or g.controls.desired() != 0:
                    raise RecoveryRequiredError("owned release zero acknowledgement was superseded")
                assert attempt.ticket is not None
                g.controls._complete(attempt.ticket, uncertain=False)
        if disposition is ReleaseDisposition.FAULT:
            if not c.stopping and c.requests.intent() == expected_intent:
                failure = result.failure
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
                c.fault("owner_release_unconfirmed")
            if result.failure is not None:
                raise result.failure
            raise RecoveryRequiredError("owned release did not establish shared authority")
        return restore_progress(disposition, result)
