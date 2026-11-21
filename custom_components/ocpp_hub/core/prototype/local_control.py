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
from functools import partial
from typing import Any

from .. import commands
from ..control import HUB_PROFILE_ID
from ..frames import Frame
from .command_processor import CommandPriority, CommandProcessor
from .composite import ExcessiveCompositeError, FutureScheduleError, verify_composite
from .connection_scope import CloudAttemptScope
from .incident_observation import IncidentObservation, ObservationKind, ObservedReply
from .managed_intent import decode_intent, read_pause_claim
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
    Prerequisite,
    ReleaseHandoff,
    Reply,
    ResultCall,
    WireReply,
    require_allowed,
)
from .release_attempt import ReleaseAttempt
from .rpc import Peer, UnavailableError, ValidationPreemptedError, profile_status
from .security import SecurityError
from .start_handoff import (
    OwnedStartResult,
    StartAttempt,
    StartHandoff,
    StartHandoffContext,
)
from .station import StationChoice
from .store import Journal, RecoveryRequiredError, ReleaseDeferredError, StationZeroStorageError, fingerprint

Call = Callable[[str, dict[str, Any]], Awaitable[Frame]]
type StopProof = Callable[[int, bool], Awaitable[Callable[[], None]]]


class ControlSupersededError(DeniedError):
    """A specific newer durable control ticket retired this operation's ownership."""


class StopReplyUnconfirmedError(UnavailableError):
    """The station confirmed zero but the transaction Stop reply did not establish acceptance."""


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

    # Confirmation belongs to an exact Boot; only the desired limit and hold survive restart.
    def __init__(
        self, journal: Journal, peer: Peer, policy: Policy, epoch: Callable[[], int], boot: Callable[[], str | None]
    ) -> None:
        self.journal, self.peer, self.policy, self.epoch = journal, peer, policy, epoch
        self.boot = boot
        self.command_processor: CommandProcessor | None = None
        self._confirmed: tuple[int, str | None, float] | None = None
        self._revision = 0
        self._operations: set[asyncio.Task[Any]] = set()
        self._completions: set[asyncio.Task[Any]] = set()
        self._retirement: asyncio.Task[None] | None = None
        self._stopping: asyncio.Task[Frame] | None = None
        self._stop_ticket: str | None = None
        self._stop_owner: tuple[object, ...] | None = None
        self._release_ticket: str | None = None
        self._unsent_release: tuple[str, dict[str, Any]] | None = None
        self.release_attempt: ReleaseAttempt | None = None
        self._stop_ack: StopAcknowledgement | None = None
        self._stop_generation = 0
        self.authorize: Callable[[str], bool] | None = None
        self.prepare_station: Callable[[ResultCall, int], Awaitable[StationChoice | NotSent]] | None = None
        self.require_retained_station: Callable[[float], Awaitable[None]] | None = None
        self.retained_ceiling_exceeded: Callable[[], None] | None = None
        self.station_missing: Callable[[], bool] = lambda: False
        self.incident_observer: Callable[[IncidentObservation], None] | None = None

    # Offer small facts to optional reporting and keep its reads and failures outside control decisions.
    def observe_incident(
        self, kind: ObservationKind, request: str | None, ticket: str, reply: ObservedReply | None = None
    ) -> None:
        observer = self.incident_observer
        if observer is None or request is None:
            return
        pause_ticket = None
        try:
            intent = decode_intent(self.journal.get("managed_control_intent"))
            if intent.mode == "paused":
                claim = read_pause_claim(self.journal)
                if claim.complete and claim.revision == intent.revision:
                    pause_ticket = claim.ticket
        except OSError, ValueError, TypeError, RecursionError, sqlite3.Error, SecurityError:
            intent = None
        try:
            observer(IncidentObservation(kind, request, ticket, intent, pause_ticket, reply))
        except Exception:
            pass  # The observer can fail independently while the owned charger sequence must continue.

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
        for tasks in (self._completions, self._operations):
            while tasks:
                await asyncio.gather(*tuple(tasks), return_exceptions=True)
                # An already-completed gather can return before the task-done callbacks run.
                await asyncio.sleep(0)

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
            not self.station_missing()
            and not self.journal.get("trial_preservation_fault")
            and not self.journal.get("trial_inventory_fault")
            and self._confirmed == (self.epoch(), self.boot(), self.desired())
        )

    # A newer Boot on the same transport needs a fresh read of the existing profile and effective schedule.
    def ceiling_needs_recheck(self) -> bool:
        if self._confirmed is None:
            return False
        epoch, boot, limit = self._confirmed
        return epoch == self.epoch() and boot != self.boot() and limit == self.desired()

    # A changed device introduction cannot inherit a confirmation or complete an older in-flight verification.
    def invalidate(self) -> None:
        self._revision += 1
        self._confirmed = None

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
        boot = self.boot()
        if limit > 0 and self.journal.phase.starting:
            raise DeniedError("initial native admission has not completed")
        if limit > 0 and boot is None and self.journal.phase.read().supervised:
            raise UnavailableError("positive station verification awaits a saved Boot")
        self._confirmed = None
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
            if boot != self.boot() or revision != self._revision:
                raise UnavailableError("device introduction changed during station verification")
            if self.journal.get("control", {}).get("ticket") != ticket or self.desired() != desired:
                raise DeniedError("a newer local operation superseded station verification")
            return ALLOW

        try:
            choice = (
                await self.prepare_station(call, epoch)
                if self.prepare_station is not None
                else StationChoice("Relative", self.policy.local_stack_level)
            )
        except UnavailableError as error:
            if boot != self.boot() and revision != self._revision:
                raise RecoveryRequiredError("device introduction changed during capability discovery") from error
            raise
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

        # Check that the actual Set reply still belongs to this ceiling before asking for its composite.
        def accept_profile() -> Pending | None:
            decision = check()
            if isinstance(decision, Pending):
                return decision
            accepted = answer.frame[0] == 3 and answer.frame[2].get("status") == "Accepted"
            if not accepted or (
                self.journal.phase.read().supervised
                and profile_status("SetChargingProfile", answer.frame) != "Accepted"
            ):
                raise UnavailableError("charger did not confirm the station ceiling")
            return None

        pending = self.peer.decide(accept_profile)
        if pending is not None:
            return CapPending(pending)
        verification = await self._verify_ceiling_result(call, limit, check)
        if verification is not None:
            return CapPending(verification)
        pending = self._confirm_ceiling(epoch, boot, limit, check)
        return CapPending(pending) if pending is not None else answer

    # Publish verified current only while both its wire result and its original owner still apply.
    def _confirm_ceiling(self, epoch: int, boot: str | None, limit: float, check: Guard) -> Pending | None:
        # Confirming zero is part of restrictive work.
        def confirm() -> Pending | None:
            pending = check()
            if isinstance(pending, Pending):
                return pending
            self._confirmed = (epoch, boot, limit)
            return None

        priority = CommandPriority.RESTRICT if limit == 0 else CommandPriority.NORMAL
        return self.peer.decide(confirm, priority=priority)

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

            # Use the schedule and exact owner together, with the receipt time captured before the decision.
            def accept_composite(verification: Reply) -> Pending | None:
                decision = check()
                if isinstance(decision, Pending):
                    return decision
                verify_composite(verification.frame, limit, completed_at)
                return None

            try:
                priority = CommandPriority.RESTRICT if limit == 0 else CommandPriority.NORMAL
                return self.peer.decide(lambda: accept_composite(verification), priority=priority)
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
        boot = self.boot()
        limit = self.desired()
        if boot is None and phase.supervised:
            raise UnavailableError("station verification awaits a saved Boot")
        ticket = self.journal.get("control", {}).get("ticket")

        # Capability reads may await a peer; a newer Stop or lower desired limit must win before the eventual write.
        def check() -> GateDecision:
            self._check(epoch)
            if guard is not None:
                guard()
            if boot != self.boot():
                raise UnavailableError("station verification belongs to an earlier Boot")
            if self.desired() != limit or self.journal.get("control", {}).get("ticket") != ticket:
                raise DeniedError("desired ceiling changed during preparation")
            return ALLOW

        async with self.peer.sequence_results(0 if limit == 0 else 1, check) as call:
            check()
            if self.ready:
                return
            if self.ceiling_needs_recheck() and self.require_retained_station is not None:
                await self.require_retained_station(limit)
                try:
                    pending = await self._verify_ceiling_result(call, limit, check)
                except ExcessiveCompositeError:
                    if self.retained_ceiling_exceeded is not None:
                        self.peer.decide(self.retained_ceiling_exceeded, priority=CommandPriority.RESTRICT)
                    raise
                if pending is None:
                    pending = self._confirm_ceiling(epoch, boot, limit, check)
                if pending is not None:
                    require_allowed(pending)
            else:
                result = await self._cap_result(call, limit, epoch, guard=check)
                if isinstance(result, CapPending):
                    require_allowed(result.pending)

    # A positive slider update cannot silently release an explicit zero-current hold.
    async def set_limit(
        self,
        value: Any,
        *,
        guard: Callable[[], None] | None = None,
        shared: bool = False,
        on_restrictive_claim: Callable[[str | None, str], None] | None = None,
        on_claim: Callable[[str], None] | None = None,
    ) -> Frame:
        return await self._owned(
            self._set_limit(
                value, guard=guard, shared=shared, on_restrictive_claim=on_restrictive_claim, on_claim=on_claim
            )
        )

    # Keep the complete claimed slider operation alive independently of an individual API client.
    async def _set_limit(
        self,
        value: Any,
        *,
        guard: Callable[[], None] | None = None,
        shared: bool = False,
        on_restrictive_claim: Callable[[str | None, str], None] | None = None,
        on_claim: Callable[[str], None] | None = None,
    ) -> Frame:
        limit = number(value, self.policy.max_current_a)
        limit = 0.0 if limit < 6 else limit
        priority = CommandPriority.RESTRICT if limit == 0 else CommandPriority.NORMAL

        # Claim the slider request and bind its managed receipt together.
        def claim() -> tuple[int, str]:
            if guard is not None:
                guard()
            if limit > 0 and self.journal.phase.starting:
                raise DeniedError("initial native admission has not completed")
            previous = self.journal.get("control", {})
            if previous.get("inhibit") and limit > 0:
                raise DeniedError("resume or release explicitly before raising a stopped limit")
            with self.journal.atomic() if on_claim is not None else nullcontext():
                epoch, ticket = self._claim(previous.get("inhibit", False) or limit == 0)
                if on_claim is not None:
                    on_claim(ticket)
                if limit == 0 and on_restrictive_claim is not None:
                    on_restrictive_claim(previous.get("ticket"), ticket)
                if limit > 0:
                    self._complete(ticket, resume_limit=limit)
            return epoch, ticket

        epoch, ticket = self.peer.decide(claim, priority=priority)

        # The managed selector retains app authority only while its durable request generation still matches.
        def check() -> None:
            self._check(epoch, ticket)
            if guard is not None:
                guard()

        async with self.peer.sequence(0 if limit == 0 else 1, check) as call:
            answer = await self._cap(call, limit, epoch)

        # Leave the result with the ticket that produced the verified ceiling.
        def settle() -> None:
            check()
            self._complete(ticket, uncertain=False, **({"owner": "shared"} if shared else {}))

        self.peer.decide(settle, priority=priority)
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
    async def stop(
        self,
        *,
        expected_local: int | None = None,
        guard: Callable[[], None] | None = None,
        station_absent: bool = False,
        prove_stop: StopProof | None = None,
        on_claim: Callable[[str], None] | None = None,
        owner: tuple[object, ...] | None = None,
        priority: CommandPriority = CommandPriority.RESTRICT,
    ) -> Frame:
        claim = partial(self._claim_stop, expected_local, guard, station_absent, prove_stop, on_claim, owner)
        task = claim() if self.command_processor is None else await self.command_processor.run(claim, priority=priority)
        return await asyncio.shield(task)

    # Reserve the exact Stop and its child together before yielding to another command or a charger report.
    def _claim_stop(
        self,
        expected_local: int | None,
        guard: Callable[[], None] | None,
        station_absent: bool,
        prove_stop: StopProof | None,
        on_claim: Callable[[str], None] | None,
        owner: tuple[object, ...] | None,
    ) -> asyncio.Task[Frame]:
        if station_absent and (expected_local is None or guard is None):
            raise RecoveryRequiredError("an absent station ceiling permits only an independently guarded Stop")
        if guard is not None:
            guard()
        if expected_local is not None and self.journal.active() not in (None, expected_local):
            raise RecoveryRequiredError("scheduled Stop no longer owns the active transaction")
        previous = self.journal.get("control", {})
        if (
            self._stopping is None
            or self._stopping.done()
            or previous.get("ticket") != self._stop_ticket
            or owner != self._stop_owner
        ):
            previous_stop_ticket, committed = self._stop_ticket, False
            try:
                # Synchronous guards may bind the claim; both durable facts must commit before a child can send.
                with self.journal.atomic():
                    epoch, ticket = self._claim(True, stopping=True)
                    self._stop_ticket = ticket
                    if on_claim is not None:
                        on_claim(ticket)
                    if guard is not None:
                        guard()
                committed = True
            finally:
                if not committed:
                    self._stop_ticket = previous_stop_ticket
            self._stop_owner = owner
            self._stopping = self._launch(
                self._stop(
                    epoch,
                    ticket,
                    previous.get("start_pending", False),
                    expected_local,
                    guard,
                    station_absent,
                    prove_stop,
                )
            )
        return self._stopping

    # Finish the zero-current hold and known-session stop even if the initiating client disconnects.
    async def _stop(
        self,
        epoch: int,
        ticket: str,
        pending_start: bool,
        expected_local: int | None = None,
        guard: Callable[[], None] | None = None,
        station_absent: bool = False,
        prove_stop: StopProof | None = None,
    ) -> Frame:
        revision, generation = self._revision, self._stop_generation
        stop_guard: Callable[[], None] | None = None

        # Recheck generation and optional scoped authority at each physical RPC.
        def check() -> None:
            self._check(epoch, ticket)
            if guard is not None:
                guard()
            if stop_guard is not None:
                stop_guard()

        cap_error: UnavailableError | StationZeroStorageError | None = None
        answer: Frame | None = None
        local: int | None = None

        # A real accepted Stop can arrive before the outgoing send finishes draining.
        def received_stop(action: str, frame: Frame) -> None:
            if (
                action == "RemoteStopTransaction"
                and local is not None
                and frame[0] == 3
                and frame[2] == {"status": "Accepted"}
            ):
                self._stop_ack = StopAcknowledgement(epoch, revision, ticket, local, generation)

        async with self.peer.sequence(0, check, on_receive=received_stop) as call:
            if station_absent:
                cap_error = UnavailableError("the missing station ceiling cannot establish a zero hold")
            else:
                try:
                    answer = await self._cap(call, 0, epoch)
                except StationZeroStorageError as error:
                    # The station write can fail separately from the committed, independently proved Stop.
                    if expected_local is None or self.journal.db.in_transaction or guard is None and prove_stop is None:
                        raise
                    check()
                    cap_error = error
                except (UnavailableError, DeniedError, RecoveryRequiredError) as error:
                    self._check(epoch, ticket)
                    cap_error = (
                        error
                        if isinstance(error, UnavailableError)
                        else UnavailableError("station hold could not be confirmed")
                    )
            local = self.journal.active()
            # Managed shutdown pins the first attempt's transaction, then handles any newly reported Start.
            if local is not None and (local == expected_local or expected_local is None and prove_stop is None):
                if prove_stop is not None:
                    stop_guard = await prove_stop(local, cap_error is not None)
                try:
                    answer = await call("RemoteStopTransaction", commands.remote_stop_transaction(local))
                except UnavailableError as error:
                    if cap_error is not None:
                        raise cap_error from error
                    raise StopReplyUnconfirmedError("transaction stop reply was unavailable") from error

                # Keep an actual acknowledgement for the exact transaction it addressed.
                def accept_stop(answer: Frame) -> None:
                    nonlocal stop_guard
                    if answer[0] != 3 or answer[2] != {"status": "Accepted"}:
                        if cap_error is not None:
                            raise cap_error
                        raise StopReplyUnconfirmedError("transaction stop was not confirmed")
                    # The StopTransaction report may already have closed this session.
                    stop_guard = None
                    check()

                self.peer.decide(lambda: accept_stop(answer), priority=CommandPriority.RESTRICT)
            if cap_error is not None:
                raise cap_error
        assert answer is not None
        self.peer.decide(
            lambda: self._complete(ticket, uncertain=bool(pending_start)), priority=CommandPriority.RESTRICT
        )
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
        # Claim and prepare this Start before waiting for its independent evidence and wire result.
        def claim() -> tuple[int, str, dict[str, Any]]:
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
            return epoch, ticket, previous

        epoch, ticket, previous = self.peer.decide(claim)
        restoring_zero = False
        saved_reply: bool | None = None

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

        # This boundary follows actual transport send completion, which is later than the durable issued marker.
        def sent(action: str) -> None:
            if action == "RemoteStartTransaction":
                self.observe_incident("start_sent", request_id, ticket)

        # Receipt recording precedes fallible resource/result publication and never claims observed charging.
        def received(action: str, frame: Frame) -> None:
            nonlocal saved_reply
            if action != "RemoteStartTransaction":
                return
            if attempt is not None:
                attempt.answer = frame
            try:
                saved_reply = self.journal.starts.record_reply(ticket, frame)
            except sqlite3.Error:
                if self.journal.db.in_transaction:
                    raise
            if self.incident_observer is None or request_id is None:
                return
            if frame[0] == 4:
                reply: ObservedReply = "call_error"
            elif frame[2] == {"status": "Accepted"}:
                reply = "accepted"
            elif frame[2] == {"status": "Rejected"}:
                reply = "rejected"
            else:
                reply = "other"
            self.observe_incident("start_reply", request_id, ticket, reply)

        async with self.peer.sequence(
            1,
            check,
            on_dispatch=dispatch,
            on_receive=received,
            on_sent=sent,
            late_reply_action="RemoteStartTransaction",
        ) as call:
            try:
                limit = number(previous.get("resume_limit", self.policy.max_current_a), self.policy.max_current_a)
                await self._cap(call, limit, epoch)

                # Arm the exact attempt only after its ceiling and current authorization agree.
                def arm() -> None:
                    check()
                    if self.authorize is not None and not self.authorize(tag):
                        raise DeniedError("authorization changed before forwarding a local start")
                    with self.journal.atomic():
                        self.journal.starts.armed(ticket)

                self.peer.decide(arm)
                answer = await call("RemoteStartTransaction", commands.remote_start_transaction(tag, 1))

                # Save the wire fact first, then grant control only if the same owner can still publish.
                def settle(answer: Frame) -> None:
                    if attempt is not None:
                        attempt.answer = answer
                    accepted = self.journal.starts.record_reply(ticket, answer) if saved_reply is None else saved_reply
                    if not accepted:
                        raise UnavailableError("transaction start was not confirmed")
                    check()
                    publication = None
                    with self.journal.atomic():
                        check()
                        if handoff is not None:
                            if scope is None:
                                raise RecoveryRequiredError("managed Start publication lacks its exact Boot scope")
                            publication = handoff.write_in_transaction(
                                StartHandoffContext(
                                    request_id, ticket, scope, self._revision, fingerprint(tag.casefold())
                                )
                            )
                        self._complete(ticket, uncertain=False, **({"owner": "shared"} if shared else {}))
                    if publication is not None:
                        assert handoff is not None
                        handoff.committed(publication)
                        if attempt is not None:
                            attempt.publication = publication

                self.peer.decide(lambda: settle(answer))
            except UnavailableError, DeniedError, RecoveryRequiredError, SecurityError, sqlite3.Error:
                # A failed Start can restore only the zero hold owned by its original ticket.
                def hold() -> None:
                    nonlocal restoring_zero
                    retire_unsent_local_start(self.journal, ticket)
                    self._complete(ticket, inhibit=True)
                    self._check(epoch, ticket)
                    restoring_zero = True

                self.peer.decide(hold, priority=CommandPriority.RESTRICT)
                await self._cap(call, 0, epoch)

                # Retain verification only for the attempt whose rollback completed.
                def confirm_rollback() -> None:
                    check()
                    if attempt is not None:
                        attempt.zero_verified = True

                self.peer.decide(confirm_rollback, priority=CommandPriority.RESTRICT)
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

    # Retire an unsent release's claim without replacing any observations or the newer owner's control.
    def retry_unsent_release(self) -> None:
        if self._unsent_release is None:
            return
        ticket, previous = self._unsent_release
        j = self.journal
        try:
            with j.atomic():
                j.control.retire_unsent_claim(ticket, previous)
        except sqlite3.Error:
            if j.db.in_transaction:
                raise
            return
        self._unsent_release = None

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
        # Claim Release only while its preflight still describes this exact hold and connection.
        def claim() -> tuple[int, str, dict[str, Any]] | Pending:
            if guard is not None:
                pending = guard()  # A newer Pause must be checked before this child claims any ownership.
                if isinstance(pending, Pending):
                    return pending
            if self.journal.phase.starting:
                raise DeniedError("initial native admission has not completed")
            previous = self.journal.get("control", {})
            if attempt is not None and rollback_guard is not None:
                # A task can start after a new Boot or hold has replaced the verified preflight.
                try:
                    rollback_guard()
                except UnavailableError as error:
                    return Pending(Prerequisite.SUPERSEDED, "release Boot changed before the claim", error)
                if previous.get("ticket") != attempt.before_ticket:
                    return Pending(Prerequisite.SUPERSEDED, "release hold changed before the claim")
            if previous.get("uncertain") and not acknowledge_uncertainty:
                raise RecoveryRequiredError("previous local outcome requires reconciliation")
            try:
                epoch, ticket = self._claim(previous.get("inhibit", False))
            except sqlite3.Error as error:
                if attempt is None or self.journal.db.in_transaction:
                    raise
                # The coordinator still compares the original verified zero, Boot and unchanged ticket.
                return Pending(Prerequisite.RECONCILIATION, "release claim could not be saved", error)
            if attempt is not None:
                attempt.ticket = ticket
            self._release_ticket = ticket
            return epoch, ticket, previous

        claimed = self.peer.decide(claim)
        if isinstance(claimed, Pending):
            return claimed
        epoch, ticket, previous = claimed
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

            async with self.peer.sequence_results(1, check, on_dispatch=dispatched, on_receive=received) as call:
                # Both failure and pending rollback retain the same ticket, Boot and observed zero sequence.
                async def rollback_zero() -> None:
                    # Only the current ticket may withdraw positive control before the zero write.
                    def hold() -> None:
                        nonlocal restoring_zero
                        self._complete(ticket, inhibit=True)
                        self._check(epoch, ticket)
                        restoring_zero = True

                    self.peer.decide(hold, priority=CommandPriority.RESTRICT)
                    rollback = await self._cap_result(call, 0, epoch)
                    if isinstance(rollback, CapPending):
                        raise RecoveryRequiredError("owned release rollback did not reach verified zero")

                    # Own and publish this rollback's actual verified zero.
                    def confirm_zero() -> None:
                        check()
                        if attempt is not None:
                            attempt.zero_confirmed = True

                    self.peer.decide(confirm_zero, priority=CommandPriority.RESTRICT)

                try:
                    result = await self._cap_result(
                        call,
                        number(previous.get("resume_limit", self.policy.max_current_a), self.policy.max_current_a),
                        epoch,
                    )

                    # The verified ceiling can publish shared ownership only in its guarded handoff.
                    def settle(result: Reply | CapPending) -> Pending | None:
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
                        return pending

                    pending = self.peer.decide(lambda: settle(result))
                    if pending is None:
                        return None
                except (UnavailableError, DeniedError, RecoveryRequiredError, SecurityError, sqlite3.Error) as error:
                    if attempt is not None:
                        attempt.positive_failure = error
                        attempt.initial_failure_kind = failure_kind(error)
                        if not attempt.positive_may_have_dispatched and rollback_guard is not None:
                            try:
                                rollback_guard()
                            except UnavailableError as retired:
                                self._unsent_release = ticket, previous
                                self.peer.decide(self.retry_unsent_release, priority=CommandPriority.RESTRICT)
                                return Pending(Prerequisite.SUPERSEDED, "release Boot changed before dispatch", retired)
                    await rollback_zero()
                    if attempt is None and isinstance(error, ReleaseDeferredError):
                        # A deferred legacy release still settles only its own verified zero.
                        def settle_deferred() -> None:
                            check()
                            self._complete(ticket, uncertain=False)

                        self.peer.decide(settle_deferred, priority=CommandPriority.RESTRICT)
                    raise
                await rollback_zero()
                return pending
        finally:
            if self._release_ticket == ticket:
                self._release_ticket = None
