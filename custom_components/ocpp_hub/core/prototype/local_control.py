"""Durable local ownership with a physical station ceiling and an explicit zero-current hold."""

from __future__ import annotations

import asyncio
import sqlite3
import sys
import uuid
from collections.abc import Awaitable, Callable, Coroutine
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .. import commands
from ..control import HUB_PROFILE_ID
from ..frames import Frame
from .composite import FutureScheduleError, verify_composite
from .connection_scope import CloudAttemptScope
from .policy import DeniedError, Policy, number
from .recovery_contracts import (
    ALLOW,
    AuthorityStamp,
    CapPending,
    FailureKind,
    GateDecision,
    Guard,
    NotSent,
    OwnedReleaseResult,
    Pending,
    ReleaseHandoff,
    Reply,
    ResultCall,
    WireReply,
    require_allowed,
)
from .release_attempt import ReleaseAttempt
from .rpc import Peer, UnavailableError, ValidationPreemptedError
from .security import SecurityError
from .start_handoff import (
    OwnedStartResult,
    StartAttempt,
    StartHandoff,
    StartHandoffContext,
)
from .station import StationChoice
from .store import Journal, RecoveryRequiredError, ReleaseDeferredError, fingerprint

Call = Callable[[str, dict[str, Any]], Awaitable[Frame]]


class ControlSupersededError(DeniedError):
    """A specific newer durable control ticket retired this operation's ownership."""


# Translate genuine technical failures once at the executor boundary; ordinary prerequisites never enter here.
def failure_kind(error: BaseException | None) -> FailureKind | None:
    if error is None:
        return None
    if type(error) in (ControlSupersededError, ValidationPreemptedError):
        return FailureKind.SUPERSEDED
    if isinstance(error, sqlite3.Error):
        return FailureKind.STORAGE
    if isinstance(error, SecurityError):
        return FailureKind.STRICT
    if isinstance(error, (UnavailableError, TimeoutError, OSError)):
        return FailureKind.TRANSPORT
    if isinstance(error, (DeniedError, RecoveryRequiredError)):
        return FailureKind.CONTROL
    return FailureKind.UNKNOWN


# Retirement must finish before caller cancellation is re-raised, because its children can still use the journal.
async def join_retirement[T](task: asyncio.Task[T]) -> T:
    cancellation: asyncio.CancelledError | None = None
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError as error:
            if task.cancelled():
                raise
            cancellation = cancellation or error
        except Exception as failure:
            # This retirement boundary retains cleanup failure as the cause of a pending caller cancellation.
            if cancellation is not None:
                raise cancellation from failure
            raise
    if cancellation is not None:
        raise cancellation
    return result


@dataclass(frozen=True)
class StopAcknowledgement:
    """An actual accepted Stop remains tied to its exact live control claim and local session."""

    epoch: int
    revision: int
    ticket: str
    local: int
    generation: int


# Read compatibility evidence through the journal's sole Start lifecycle owner.
def local_start_receipt(j: Journal) -> dict[str, Any]:
    return j.starts.receipt()


# Retire only the exact prepared receipt while preserving later owner claims.
def retire_unsent_local_start(j: Journal, ticket: str | None) -> None:
    j.starts.retire_prepared(ticket)


# Normalize old unbound evidence without replaying the Start command.
def recover_unreported_local_start(j: Journal) -> None:
    j.starts.recover_legacy()


class LocalControl:
    """A stopped software session cannot be restarted by an already installed native timer."""

    # Confirmation belongs to a socket generation; only the desired limit and hold survive restart.
    def __init__(self, journal: Journal, peer: Peer, policy: Policy, epoch: Callable[[], int]) -> None:
        self.journal, self.peer, self.policy, self.epoch = journal, peer, policy, epoch
        self._confirmed_epoch = -1
        self._confirmed_limit: float | None = None
        self._revision = 0
        self._operations: set[asyncio.Task[Any]] = set()
        self._completions: set[asyncio.Task[Any]] = set()
        self._retirement: asyncio.Task[None] | None = None
        self._stopping: asyncio.Task[Frame] | None = None
        self._stop_ticket: str | None = None
        self._release_ticket: str | None = None
        self.release_attempt: ReleaseAttempt | None = None
        self._stop_ack: StopAcknowledgement | None = None
        self._stop_generation = 0
        self.authorize: Callable[[str], bool] | None = None
        self.prepare_station: Callable[[ResultCall, int], Awaitable[StationChoice | NotSent]] | None = None

    # Client cancellation cannot abandon a safety sequence halfway through its physical effects.
    async def _owned[T](self, operation: Coroutine[Any, Any, T]) -> T:
        if len(self._operations) >= 32:
            operation.close()
            raise DeniedError("local operation queue is full")
        return await asyncio.shield(self._launch(operation))

    # Ordinary operations share a bounded pool; Stop additionally has one coalesced reserved slot.
    def _launch[T](self, operation: Coroutine[Any, Any, T]) -> asyncio.Task[T]:
        task = asyncio.create_task(operation)
        self._operations.add(task)
        task.add_done_callback(self._finished)
        return task

    # Completion ownership includes preflight and publication without pretending those tasks are in-flight wire writes.
    def launch_completion[T](self, operation: Coroutine[Any, Any, T]) -> asyncio.Task[T]:
        if len(self._completions) >= 32:
            operation.close()
            raise DeniedError("local completion queue is full")
        task = asyncio.create_task(operation)
        self._completions.add(task)
        task.add_done_callback(self._finished)
        return task

    # Retain operations until completion and retrieve failures even when their original caller has gone away.
    def _finished(self, task: asyncio.Task[Any]) -> None:
        self._operations.discard(task)
        self._completions.discard(task)
        if not task.cancelled():
            task.exception()

    # The gateway detaches transport first, allowing bounded RPCs to fail without leaving a journal user behind.
    async def close(self) -> None:
        if self._retirement is None:
            self._retirement = asyncio.create_task(self._retire())
        await join_retirement(self._retirement)

    # Each owned action already bounds its native/RPC work; never close its store merely because a waiter expired.
    async def _retire(self) -> None:
        while self._completions:
            await asyncio.gather(*tuple(self._completions), return_exceptions=True)
        while self._operations:
            await asyncio.gather(*tuple(self._operations), return_exceptions=True)

    # Return the persisted bounded ceiling without ever raising a previous lower local limit on start.
    def desired(self) -> float:
        state = self.journal.get("control", {})
        return (
            0.0
            if state.get("inhibit")
            else number(state.get("resume_limit", self.policy.max_current_a), self.policy.max_current_a)
        )

    # A real Accepted result on the current transport is necessary before charging-capable vendor commands.
    @property
    def ready(self) -> bool:
        if self.journal.phase.read().value is not None:
            receipt = self.journal.db.execute(
                "SELECT state FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
            ).fetchone()
            if receipt is None or receipt["state"] != "confirmed":
                return False
        return (
            not self.journal.get("trial_preservation_fault")
            and not self.journal.get("trial_inventory_fault")
            and self._confirmed_epoch == self.epoch()
            and self._confirmed_limit == self.desired()
        )

    # A changed device introduction cannot inherit a confirmation or complete an older in-flight verification.
    def invalidate(self) -> None:
        self._revision += 1
        self._confirmed_epoch, self._confirmed_limit = -1, None

    # Validate both transport generation and a newer stop/operation before every RPC in a sequence.
    def _check(self, epoch: int, ticket: str | None = None) -> None:
        if epoch != self.epoch():
            raise UnavailableError("local control belongs to a retired charger")
        if ticket is not None and self.journal.get("control", {}).get("ticket") != ticket:
            raise ControlSupersededError("a newer local operation superseded this request")

    # Record ownership and uncertainty before anything can be sent to the charger.
    def _claim(self, inhibit: bool, stopping: bool = False) -> tuple[int, str]:
        phase = self.journal.phase.read()
        previous = self.journal.get("control", {})
        if stopping and phase.value in ("rollback", "backend_restore_pending"):
            if self.journal.active() is None and not previous.get("start_pending"):
                raise DeniedError("trial cleanup remains frozen without a known session to stop")
        elif stopping and phase.held:
            self.journal.phase.recover()
        elif not phase.legacy_active:
            raise DeniedError("trial local controls are frozen")
        ticket = uuid.uuid4().hex
        self.journal.control.claim(previous, ticket, inhibit, stopping, self.policy.max_current_a)
        return self.epoch(), ticket

    # Complete only this operation; a retired response cannot release a newer stop's hold.
    def _complete(self, ticket: str, **changes: Any) -> None:
        self.journal.control.complete(ticket, **changes)

    # The journal's Start owner binds evidence inside the actual lifecycle transaction.
    def bind_start(self, tag: str, local: int) -> None:
        self.journal.starts.bind(tag, local)

    # Install the reserved station profile; Accepted confirms receipt, not yet real-device execution or persistence.
    async def _cap(self, call: Call, limit: float, epoch: int, *, guard: Callable[[], None] | None = None) -> Frame:
        # Legacy commissioning and Stop callers share the result-aware verifier without changing their Frame API.
        async def result_call(action: str, payload: dict[str, Any]) -> Reply:
            return Reply(await call(action, payload))

        result = await self._cap_result(result_call, limit, epoch, guard=guard)
        if isinstance(result, CapPending):
            require_allowed(result.pending)
            raise AssertionError("pending cap did not stop the legacy caller")
        return result.frame

    # A normal source wait can stop preparation, the actual Set, or a later composite without erasing prior effects.
    async def _cap_result(
        self, call: ResultCall, limit: float, epoch: int, *, guard: Guard | None = None
    ) -> Reply | CapPending:
        if limit > 0 and self.journal.phase.starting:
            raise DeniedError("initial native admission has not completed")
        self._confirmed_epoch = -1
        revision = self._revision
        ticket = self.journal.get("control", {}).get("ticket")
        desired = self.desired()

        # Boot may change on the same socket while a capability or composite RPC awaits its reply.
        def check() -> GateDecision:
            self._check(epoch)
            if guard is not None:
                decision = guard()
                if isinstance(decision, Pending):
                    return decision
            if revision != self._revision:
                raise UnavailableError("device introduction changed during station verification")
            if self.journal.get("control", {}).get("ticket") != ticket or self.desired() != desired:
                raise DeniedError("a newer local operation superseded station verification")
            return ALLOW

        choice = (
            await self.prepare_station(call, epoch)
            if self.prepare_station is not None
            else StationChoice("Relative", self.policy.local_stack_level)
        )
        if isinstance(choice, NotSent):
            return CapPending(choice.pending)
        decision = check()
        if isinstance(decision, Pending):
            return CapPending(decision)
        answer = await call(
            "SetChargingProfile",
            choice.payload(limit),
        )
        if isinstance(answer, NotSent):
            return CapPending(answer.pending)
        decision = check()
        if isinstance(decision, Pending):
            return CapPending(decision)
        if answer.frame[0] != 3 or answer.frame[2].get("status") != "Accepted":
            self._confirmed_epoch = -1
            raise UnavailableError("charger did not confirm the station ceiling")
        verification = await self._verify_ceiling_result(call, limit, check)
        if verification is not None:
            return CapPending(verification)
        self._confirmed_epoch, self._confirmed_limit = epoch, limit
        return answer

    # Each composite and retry revalidates current ownership and may return an ordinary prerequisite as data.
    async def _verify_ceiling_result(self, call: ResultCall, limit: float, check: Guard) -> Pending | None:
        loop = asyncio.get_running_loop()
        retry_deadline: float | None = None
        for attempt in range(3):
            decision = check()
            if isinstance(decision, Pending):
                return decision
            if retry_deadline is None:
                verification = await call("GetCompositeSchedule", commands.get_composite_schedule(1, 60))
            else:
                remaining = retry_deadline - loop.time()
                if remaining <= 0:
                    raise UnavailableError("composite verification retry budget expired")
                try:
                    async with asyncio.timeout(remaining):
                        verification = await call("GetCompositeSchedule", commands.get_composite_schedule(1, 60))
                except TimeoutError:
                    raise UnavailableError("composite verification retry budget expired") from None
            completed_at = datetime.now(UTC)
            if isinstance(verification, NotSent):
                return verification.pending
            decision = check()
            if isinstance(decision, Pending):
                return decision
            try:
                verify_composite(verification.frame, limit, completed_at)
            except FutureScheduleError as error:
                if error.ahead_seconds > 1 or attempt == 2:
                    raise UnavailableError("composite schedule remains outside present coverage") from None
                if retry_deadline is None:
                    retry_deadline = loop.time() + 1
                wait_until = min(loop.time() + 0.25, retry_deadline)
                while loop.time() < wait_until:
                    decision = check()
                    if isinstance(decision, Pending):
                        return decision
                    await asyncio.sleep(min(0.025, max(0, wait_until - loop.time())))
                decision = check()
                if isinstance(decision, Pending):
                    return decision
            else:
                return None
        raise UnavailableError("composite schedule remains outside present coverage")

    # Establish or restore the desired ceiling before a start, native plan or new authenticated connection is used.
    async def ensure_ceiling(self, *, guard: Callable[[], None] | None = None) -> None:
        if guard is not None:
            guard()
        phase = self.journal.phase.read()
        recovery_hold = phase.held and self.journal.get("control", {}).get("inhibit")
        if not phase.legacy_active and not recovery_hold:
            raise UnavailableError("trial ceiling reapplication is disabled outside active mode")
        if self.ready:
            return
        epoch = self.epoch()
        limit = self.desired()

        # Capability reads may await a peer; a newer Stop or lower desired limit must win before the eventual write.
        def check() -> None:
            self._check(epoch)
            if guard is not None:
                guard()
            if self.desired() != limit:
                raise DeniedError("desired ceiling changed during preparation")

        async with self.peer.sequence(0 if limit == 0 else 1, check) as call:
            if not self.ready:
                await self._cap(call, limit, epoch, guard=check)

    # A positive slider update cannot silently release an explicit zero-current hold.
    async def set_limit(
        self,
        value: Any,
        *,
        guard: Callable[[], None] | None = None,
        shared: bool = False,
        on_restrictive_claim: Callable[[str | None, str], None] | None = None,
    ) -> Frame:
        return await self._owned(
            self._set_limit(value, guard=guard, shared=shared, on_restrictive_claim=on_restrictive_claim)
        )

    # Keep the complete claimed slider operation alive independently of an individual API client.
    async def _set_limit(
        self,
        value: Any,
        *,
        guard: Callable[[], None] | None = None,
        shared: bool = False,
        on_restrictive_claim: Callable[[str | None, str], None] | None = None,
    ) -> Frame:
        if guard is not None:
            guard()
        limit = number(value, self.policy.max_current_a)
        limit = 0.0 if limit < 6 else limit
        if limit > 0 and self.journal.phase.starting:
            raise DeniedError("initial native admission has not completed")
        previous = self.journal.get("control", {})
        if previous.get("inhibit") and limit > 0:
            raise DeniedError("resume or release explicitly before raising a stopped limit")
        epoch, ticket = self._claim(previous.get("inhibit", False) or limit == 0)
        if limit == 0 and on_restrictive_claim is not None:
            on_restrictive_claim(previous.get("ticket"), ticket)
        if limit > 0:
            self._complete(ticket, resume_limit=limit)

        # The managed selector retains app authority only while its durable request generation still matches.
        def check() -> None:
            self._check(epoch, ticket)
            if guard is not None:
                guard()

        async with self.peer.sequence(0 if limit == 0 else 1, check) as call:
            answer = await self._cap(call, limit, epoch)
        check()
        self._complete(ticket, uncertain=False, **({"owner": "shared"} if shared else {}))
        return answer

    # Every actual Boot retires Stop acknowledgements even when its metadata is unchanged.
    def forget_stop_ack(self) -> None:
        self._stop_generation += 1
        self._stop_ack = None

    # An accepted known-session Stop waits for its genuine closure instead of repeating after each native poll.
    def stop_acknowledged(self, local: int | None) -> bool:
        ack = self._stop_ack
        return (
            ack is not None
            and local is not None
            and ack.local == local
            and ack.epoch == self.epoch()
            and ack.revision == self._revision
            and ack.generation == self._stop_generation
            and ack.ticket == self.journal.get("control", {}).get("ticket")
        )

    # Pause first, then stop only the known session, without allowing queued cloud traffic between them.
    async def stop(self, *, expected_local: int | None = None, guard: Callable[[], None] | None = None) -> Frame:
        if guard is not None:
            guard()
        if expected_local is not None and self.journal.active() not in (None, expected_local):
            raise RecoveryRequiredError("scheduled Stop no longer owns the active transaction")
        previous = self.journal.get("control", {})
        if self._stopping is None or self._stopping.done() or previous.get("ticket") != self._stop_ticket:
            previous_stop_ticket, committed = self._stop_ticket, False
            try:
                # Synchronous guards may bind the claim; both durable facts must commit before a child can send.
                with self.journal.atomic():
                    epoch, ticket = self._claim(True, stopping=True)
                    self._stop_ticket = ticket
                    if guard is not None:
                        guard()
                committed = True
            finally:
                if not committed:
                    self._stop_ticket = previous_stop_ticket
            self._stopping = self._launch(
                self._stop(epoch, ticket, previous.get("start_pending", False), expected_local, guard)
            )
        return await asyncio.shield(self._stopping)

    # Finish the zero-current hold and known-session stop even if the initiating client disconnects.
    async def _stop(
        self,
        epoch: int,
        ticket: str,
        pending_start: bool,
        expected_local: int | None = None,
        guard: Callable[[], None] | None = None,
    ) -> Frame:
        revision, generation = self._revision, self._stop_generation

        # Recheck generation and optional scoped authority at each physical RPC.
        def check() -> None:
            self._check(epoch, ticket)
            if guard is not None:
                guard()

        cap_error: UnavailableError | None = None
        async with self.peer.sequence(0, check) as call:
            try:
                answer = await self._cap(call, 0, epoch)
            except (UnavailableError, DeniedError, RecoveryRequiredError) as error:
                self._check(epoch, ticket)
                cap_error = (
                    error
                    if isinstance(error, UnavailableError)
                    else UnavailableError("station hold could not be confirmed")
                )
            local = self.journal.active()
            if local is not None and expected_local in (None, local):
                answer = await call("RemoteStopTransaction", commands.remote_stop_transaction(local))
                if answer[0] != 3 or answer[2].get("status") != "Accepted":
                    raise UnavailableError("transaction stop was not confirmed")
                check()
                self._stop_ack = StopAcknowledgement(epoch, revision, ticket, local, generation)
            if cap_error is not None:
                raise cap_error
        self._complete(ticket, uncertain=bool(pending_start))
        return answer

    # Keep any previous lower ceiling; a rejected or uncertain start reasserts zero before returning failure.
    async def start(self, tag: str) -> Frame:
        return await self._owned(self._start(tag))

    # Managed parents consume wire acceptance and publication separately after the owned child finishes.
    async def start_managed_result(
        self,
        tag: str,
        guard: Callable[[], None],
        *,
        request_id: str | None = None,
        on_prepared: Callable[[], None] | None = None,
        scope_guard: Callable[[], None] | None = None,
        scope: CloudAttemptScope | None = None,
        handoff: StartHandoff | None = None,
    ) -> OwnedStartResult:
        if self.policy.allow_local_start or not 6 <= self.policy.max_current_a <= 32:
            raise DeniedError("managed Start requires the restricted installation policy")
        attempt = StartAttempt()
        try:
            await self._owned(
                self._start_owned(tag, guard, request_id, on_prepared, scope_guard, scope, handoff, attempt)
            )
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, sqlite3.Error:
            if not attempt.finished:
                raise
        return attempt.result()

    # The child retains the publisher and actual reply even after cancellation of its parent waiter.
    async def _start_owned(
        self,
        tag: str,
        guard: Callable[[], None],
        request_id: str | None,
        on_prepared: Callable[[], None] | None,
        scope_guard: Callable[[], None] | None,
        scope: CloudAttemptScope | None,
        handoff: StartHandoff | None,
        attempt: StartAttempt,
    ) -> Frame:
        try:
            return await self._start(
                tag,
                guard=guard,
                shared=True,
                request_id=request_id,
                on_prepared=on_prepared,
                scope_guard=scope_guard,
                scope=scope,
                handoff=handoff,
                attempt=attempt,
            )
        finally:
            attempt.failure = sys.exception()
            attempt.finished = True

    # A claimed start either completes its bounded sequence or reasserts the physical hold on failure.
    async def _start(
        self,
        tag: str,
        *,
        guard: Callable[[], None] | None = None,
        shared: bool = False,
        request_id: str | None = None,
        on_prepared: Callable[[], None] | None = None,
        scope_guard: Callable[[], None] | None = None,
        scope: CloudAttemptScope | None = None,
        handoff: StartHandoff | None = None,
        attempt: StartAttempt | None = None,
    ) -> Frame:
        if scope_guard is not None:
            scope_guard()
        if guard is not None:
            guard()
        if not self.policy.allow_local_start and guard is None:
            raise DeniedError("local Start is disabled; charging remains under the app and charger schedule")
        if self.journal.phase.starting:
            raise DeniedError("initial native admission has not completed")
        previous = self.journal.get("control", {})
        if previous.get("uncertain") or previous.get("start_pending"):
            raise RecoveryRequiredError("previous local outcome requires reconciliation")
        if (
            not any(tag.casefold() == approved.casefold() for approved in self.policy.allowed_tags)
            or self.journal.active() is not None
        ):
            raise DeniedError("tag is not allowed or a transaction is unresolved")
        with self.journal.atomic():
            epoch, ticket = self._claim(previous.get("inhibit", False))
            self.journal.starts.prepare(ticket, fingerprint(tag.casefold()), request_id)
            if on_prepared is not None:
                on_prepared()

        restoring_zero = False

        # Positive permission may be revoked; the same exact ticket and Boot still permit its restrictive rollback.
        def check() -> None:
            self._check(epoch, ticket)
            if scope_guard is not None:
                scope_guard()
            if not restoring_zero and guard is not None:
                guard()

        # Write dispatch and ACK independently of the control ticket a later Pause can supersede.
        def record_stage(stage: str) -> None:
            self.journal.starts.record(ticket, stage)

        # The native and resource gates both run before the transport can send this command.
        def dispatch(action: str) -> None:
            if action == "RemoteStartTransaction":
                if self.journal.active() is not None:
                    raise DeniedError("an actual transaction superseded the pending Start before dispatch")
                record_stage("issued")
                if attempt is not None:
                    attempt.remote_start_dispatched = True

        # Receipt recording precedes fallible resource/result publication and never claims observed charging.
        def received(action: str, frame: Frame) -> None:
            if attempt is not None and action == "RemoteStartTransaction":
                attempt.answer = frame

        async with self.peer.sequence(1, check, on_dispatch=dispatch, on_reply=received) as call:
            try:
                limit = number(previous.get("resume_limit", self.policy.max_current_a), self.policy.max_current_a)
                await self._cap(call, limit, epoch)
                if self.authorize is not None and not self.authorize(tag):
                    raise DeniedError("authorization changed before forwarding a local start")
                with self.journal.atomic():
                    self.journal.starts.armed(ticket)
                answer = await call("RemoteStartTransaction", commands.remote_start_transaction(tag, 1))
                if attempt is not None:
                    attempt.answer = answer
                if answer[0] != 3 or answer[2].get("status") != "Accepted":
                    if answer[0] == 3 and answer[2].get("status") == "Rejected":
                        record_stage("rejected")
                    raise UnavailableError("transaction start was not confirmed")
                record_stage("accepted")
                check()
                publication = None
                with self.journal.atomic():
                    check()
                    if handoff is not None:
                        if scope is None:
                            raise RecoveryRequiredError("managed Start publication lacks its exact Boot scope")
                        publication = handoff.write_in_transaction(
                            StartHandoffContext(request_id, ticket, scope, self._revision, fingerprint(tag.casefold()))
                        )
                    self._complete(ticket, uncertain=False, **({"owner": "shared"} if shared else {}))
                if publication is not None:
                    assert handoff is not None
                    handoff.committed(publication)
                    if attempt is not None:
                        attempt.publication = publication
            except UnavailableError, DeniedError, RecoveryRequiredError, SecurityError, sqlite3.Error:
                retire_unsent_local_start(self.journal, ticket)
                self._complete(ticket, inhibit=True)
                self._check(epoch, ticket)
                restoring_zero = True
                await self._cap(call, 0, epoch)
                check()
                if attempt is not None:
                    attempt.zero_verified = True
                raise
        return answer

    # Releasing local ownership is a physical operation, never removal of the station safety profile.
    async def release(self, acknowledge_uncertainty: bool = False) -> None:
        pending = await self._owned(self._release_owned(acknowledge_uncertainty, None, None, None, None))
        require_allowed(pending)

    # The managed coordinator consumes the completed child result, never a guessed exception-chain outcome.
    async def release_result(
        self,
        attempt: ReleaseAttempt,
        *,
        guard: Guard,
        context: AbstractContextManager[None],
        rollback_guard: Callable[[], None],
        handoff: ReleaseHandoff,
    ) -> OwnedReleaseResult:
        try:
            await self._owned(self._release_owned(False, guard, attempt, context, rollback_guard, handoff))
        except DeniedError, SecurityError, RecoveryRequiredError, UnavailableError, sqlite3.Error:
            if not attempt.finished:
                raise
        return attempt.result()

    # Completion and guard lifetime belong to this shielded child, including its rollback and cancellation cleanup.
    async def _release_owned(
        self,
        acknowledge_uncertainty: bool,
        guard: Guard | None,
        attempt: ReleaseAttempt | None,
        context: AbstractContextManager[None] | None,
        rollback_guard: Callable[[], None] | None,
        handoff: ReleaseHandoff | None = None,
    ) -> Pending | None:
        try:
            if self.release_attempt is not None:
                raise RecoveryRequiredError("another owned release is still running")
            with context if context is not None else nullcontext():
                if attempt is not None:
                    self.release_attempt = attempt
                pending = await self._release(
                    acknowledge_uncertainty, guard, rollback_guard, attempt=attempt, handoff=handoff
                )
                if attempt is not None:
                    attempt.pending = pending
                return pending
        finally:
            if attempt is not None:
                attempt.failure = sys.exception()
                attempt.failure_kind = failure_kind(attempt.failure)
                attempt.cancelled = isinstance(attempt.failure, asyncio.CancelledError)
                attempt.cleanup_failed = attempt.failure is not None and attempt.failure is not attempt.positive_failure
                attempt.finished = True
                if self.release_attempt is attempt:
                    self.release_attempt = None

    # A failed physical release reinstalls zero instead of retaining only a misleading software hold.
    async def _release(
        self,
        acknowledge_uncertainty: bool,
        guard: Guard | None = None,
        rollback_guard: Callable[[], None] | None = None,
        *,
        attempt: ReleaseAttempt | None = None,
        handoff: ReleaseHandoff | None = None,
    ) -> Pending | None:
        if guard is not None:
            pending = guard()  # A newer Pause must be checked before this child claims any ownership.
            if isinstance(pending, Pending):
                return pending
        if self.journal.phase.starting:
            raise DeniedError("initial native admission has not completed")
        previous = self.journal.get("control", {})
        if previous.get("uncertain") and not acknowledge_uncertainty:
            raise RecoveryRequiredError("previous local outcome requires reconciliation")
        epoch, ticket = self._claim(previous.get("inhibit", False))
        if attempt is not None:
            attempt.ticket = ticket
        self._release_ticket = ticket
        try:
            restoring_zero = False

            # The positive guard can revoke release; the same ticket and Boot still authorize rollback to zero.
            def check() -> GateDecision | None:
                self._check(epoch, ticket)
                if rollback_guard is not None:
                    rollback_guard()
                if not restoring_zero and guard is not None:
                    return guard()
                return ALLOW

            # Record possible physical effects at actual sender invocation and retain them through later reads.
            def dispatched(action: str) -> None:
                if attempt is not None and action == "SetChargingProfile":
                    if restoring_zero:
                        attempt.zero_may_have_dispatched = True
                    else:
                        attempt.positive_may_have_dispatched = True

            # Record the actual reply before resource ACK persistence, which can itself fail.
            def received(action: str, frame: Frame) -> None:
                if attempt is not None and not restoring_zero and action == "SetChargingProfile":
                    attempt.positive_reply = (
                        WireReply.ACCEPTED
                        if frame[0] == 3 and frame[2].get("status") == "Accepted"
                        else WireReply.REJECTED
                        if frame[0] == 3 and frame[2].get("status") == "Rejected"
                        else WireReply.ERROR
                    )

            async with self.peer.sequence_results(1, check, on_dispatch=dispatched, on_reply=received) as call:
                # Both failure and pending rollback retain the same ticket, Boot and observed zero sequence.
                async def rollback_zero() -> None:
                    nonlocal restoring_zero
                    self._complete(ticket, inhibit=True)
                    self._check(epoch, ticket)
                    restoring_zero = True
                    rollback = await self._cap_result(call, 0, epoch)
                    if isinstance(rollback, CapPending):
                        raise RecoveryRequiredError("owned release rollback did not reach verified zero")
                    check()
                    if attempt is not None:
                        attempt.zero_confirmed = True

                try:
                    result = await self._cap_result(
                        call,
                        number(previous.get("resume_limit", self.policy.max_current_a), self.policy.max_current_a),
                        epoch,
                    )
                    pending = result.pending if isinstance(result, CapPending) else check()
                    if attempt is not None and isinstance(result, Reply):
                        attempt.positive_confirmed = True
                    if not isinstance(pending, Pending):
                        publication = None
                        with self.journal.atomic():
                            pending = check()
                            if not isinstance(pending, Pending) and handoff is not None:
                                if attempt is None:
                                    raise RecoveryRequiredError("managed release publication lacks owned identity")
                                publication = handoff.write_in_transaction(
                                    AuthorityStamp(attempt.intent, attempt.scope, attempt.control_revision, ticket)
                                )
                                if isinstance(publication, Pending):
                                    pending = publication
                            if not isinstance(pending, Pending):
                                self._complete(ticket, owner="shared", inhibit=False, uncertain=False)
                        if not isinstance(pending, Pending):
                            if attempt is not None:
                                attempt.published = True
                                attempt.published_authority = (
                                    publication if isinstance(publication, AuthorityStamp) else None
                                )
                            if handoff is not None:
                                assert isinstance(publication, AuthorityStamp)
                                handoff.committed(publication)
                            return None
                    if attempt is not None:
                        attempt.pending = pending
                except (UnavailableError, DeniedError, RecoveryRequiredError, SecurityError, sqlite3.Error) as error:
                    if attempt is not None:
                        attempt.positive_failure = error
                        attempt.initial_failure_kind = failure_kind(error)
                    await rollback_zero()
                    if attempt is None and isinstance(error, ReleaseDeferredError):
                        check()
                        self._complete(ticket, uncertain=False)
                    raise
                await rollback_zero()
                return pending
        finally:
            if self._release_ticket == ticket:
                self._release_ticket = None
