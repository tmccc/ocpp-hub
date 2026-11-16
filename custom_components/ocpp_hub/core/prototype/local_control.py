"""Durable local ownership with a physical station ceiling and an explicit zero-current hold."""

from __future__ import annotations

import asyncio
import json
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
from .policy import DeniedError, Policy, number
from .release_attempt import ReleaseAttempt, native_availability
from .rpc import Peer, UnavailableError
from .security import SecurityError
from .station import StationChoice
from .store import Journal, RecoveryRequiredError, ReleaseDeferredError, fingerprint

Call = Callable[[str, dict[str, Any]], Awaitable[Frame]]


class ControlSupersededError(DeniedError):
    """A specific newer durable control ticket retired this operation's ownership."""


@dataclass(frozen=True)
class StopAcknowledgement:
    """An actual accepted Stop remains tied to its exact live control claim and local session."""

    epoch: int
    revision: int
    ticket: str
    local: int
    generation: int


# Older managed normal Starts saved their tag with the owner's request before dispatching the physical command.
def local_start_receipt(j: Journal) -> dict[str, Any]:
    receipt = j.get("local_start_receipt", {})
    if receipt:
        return dict(receipt)
    old = j.get("managed_test_charge", {})
    if old.get("tag_digest") and old.get("source") == "local":
        return {
            "digest": old["tag_digest"],
            "stage": "accepted" if old.get("start_rpc_accepted") else "prepared",
            "local": old.get("local_transaction"),
        }
    return {}


# A crash or revoked validation before final dispatch cannot leave a real transaction for this command to report.
def retire_unsent_local_start(j: Journal, ticket: str | None) -> None:
    receipt = j.get("local_start_receipt", {})
    if not ticket or receipt.get("ticket") != ticket or receipt.get("stage") != "prepared":
        return
    control = j.get("control", {})
    with j.atomic():
        intent = j.get("managed_control_intent", {})
        j.put(
            "local_start_receipt",
            {
                **receipt,
                "stage": "unsent",
                "settled_ticket": control.get("ticket"),
                "pause_revision": intent.get("revision") if intent.get("mode") == "paused" else None,
            },
        )
        j.put("control", {**control, "start_pending": False})


# A pre-upgrade ordinary Start can lose its pending flag when a different tag is accepted first.
def recover_unreported_local_start(j: Journal) -> None:
    existing = j.get("local_start_receipt", {})
    if existing:
        retire_unsent_local_start(j, existing.get("ticket"))
        return
    owner = j.get("managed_test_charge", {})
    if (
        owner.get("source") != "local"
        or owner.get("kind") != "normal"
        or owner.get("start_rpc_accepted") is not True
        or owner.get("local_transaction") is not None
        or owner.get("state") not in ("awaiting_power", "stopping", "interrupted")
        or not isinstance(owner.get("tag_digest"), str)
        or len(owner["tag_digest"]) != 64
    ):
        return
    control = j.get("control", {})
    # The old ledger never recorded a Start-command floor. A matching historical tag alone has no ordering proof.
    if not control.get("start_pending") and any(
        fingerprint(json.loads(row["start"])["idTag"].casefold()) == owner["tag_digest"]
        for row in j.db.execute("SELECT start FROM transactions")
    ):
        return
    with j.atomic():
        j.put("local_start_receipt", {"digest": owner["tag_digest"], "stage": "accepted", "local": None})
        j.put("control", {**control, "start_pending": True})


class LocalControl:
    """A stopped software session cannot be restarted by an already installed native timer."""

    # Confirmation belongs to a socket generation; only the desired limit and hold survive restart.
    def __init__(self, journal: Journal, peer: Peer, policy: Policy, epoch: Callable[[], int]) -> None:
        self.journal, self.peer, self.policy, self.epoch = journal, peer, policy, epoch
        self._confirmed_epoch = -1
        self._confirmed_limit: float | None = None
        self._revision = 0
        self._operations: set[asyncio.Task[Any]] = set()
        self._stopping: asyncio.Task[Frame] | None = None
        self._stop_ticket: str | None = None
        self._release_ticket: str | None = None
        self.release_attempt: ReleaseAttempt | None = None
        self._stop_ack: StopAcknowledgement | None = None
        self._stop_generation = 0
        self.authorize: Callable[[str], bool] | None = None
        self.prepare_station: Callable[[Call, int], Awaitable[StationChoice]] | None = None

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

    # Retain operations until completion and retrieve failures even when their original caller has gone away.
    def _finished(self, task: asyncio.Task[Any]) -> None:
        self._operations.discard(task)
        if not task.cancelled():
            task.exception()

    # The gateway detaches transport first, allowing bounded RPCs to fail without leaving a journal user behind.
    async def close(self) -> None:
        if self._operations:
            done, pending = await asyncio.wait(self._operations, timeout=self.peer.timeout * 4 + 1)
            for task in done:
                if not task.cancelled():
                    task.exception()
            if pending:
                raise RuntimeError("local operation retirement exceeded its deadline")

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
        if self.journal.get("trial_phase") is not None:
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
        phase = self.journal.get("trial_phase")
        previous = self.journal.get("control", {})
        if stopping and phase in ("rollback", "backend_restore_pending"):
            if self.journal.active() is None and not previous.get("start_pending"):
                raise DeniedError("trial cleanup remains frozen without a known session to stop")
        elif stopping and phase in ("blocked", "recovering"):
            self.journal.put("trial_phase", "recovering")
        elif phase not in (None, "active"):
            raise DeniedError("trial local controls are frozen")
        ticket = uuid.uuid4().hex
        state = {
            **previous,
            "owner": "local",
            "inhibit": inhibit,
            "uncertain": True,
            "ticket": ticket,
            "stop_epoch": previous.get("stop_epoch", 0) + int(stopping),
            "resume_limit": previous.get("resume_limit", self.policy.max_current_a),
        }
        self.journal.put("control", state)
        return self.epoch(), ticket

    # Complete only this operation; a retired response cannot release a newer stop's hold.
    def _complete(self, ticket: str, **changes: Any) -> None:
        state = self.journal.get("control", {})
        if state.get("ticket") == ticket:
            self.journal.put("control", {**state, **changes})

    # Bind only the issued local command with this tag, inside the actual transaction's durable commit.
    def bind_start(self, tag: str, local: int) -> None:
        control = self.journal.get("control", {})
        if not control.get("start_pending"):
            return
        receipt = local_start_receipt(self.journal)
        if (
            receipt.get("local") is None
            and receipt.get("digest") == fingerprint(tag.casefold())
            and receipt.get("stage") in ("issued", "accepted")
        ):
            intent = self.journal.get("managed_control_intent", {})
            self.journal.put(
                "local_start_receipt",
                {
                    **receipt,
                    "local": local,
                    "bound_ticket": control.get("ticket"),
                    "pause_revision": intent.get("revision") if intent.get("mode") == "paused" else None,
                },
            )
            self.journal.put("control", {**control, "start_pending": False})

    # Install the reserved station profile; Accepted confirms receipt, not yet real-device execution or persistence.
    async def _cap(self, call: Call, limit: float, epoch: int, *, guard: Callable[[], None] | None = None) -> Frame:
        if limit > 0 and self.journal.get("trial_starting"):
            raise DeniedError("initial native admission has not completed")
        self._confirmed_epoch = -1
        revision = self._revision
        ticket = self.journal.get("control", {}).get("ticket")
        desired = self.desired()

        # Boot may change on the same socket while a capability or composite RPC awaits its reply.
        def check() -> None:
            self._check(epoch)
            if guard is not None:
                guard()
            if revision != self._revision:
                raise UnavailableError("device introduction changed during station verification")
            if self.journal.get("control", {}).get("ticket") != ticket or self.desired() != desired:
                raise DeniedError("a newer local operation superseded station verification")

        choice = (
            await self.prepare_station(call, epoch)
            if self.prepare_station is not None
            else StationChoice("Relative", self.policy.local_stack_level)
        )
        check()
        answer = await call(
            "SetChargingProfile",
            choice.payload(limit),
        )
        check()
        if answer[0] != 3 or answer[2].get("status") != "Accepted":
            self._confirmed_epoch = -1
            raise UnavailableError("charger did not confirm the station ceiling")
        await self._verify_ceiling(call, limit, check)
        self._confirmed_epoch, self._confirmed_limit = epoch, limit
        return answer

    # Retry only a small future clock offset; every extra request is read-only and shares one monotonic deadline.
    async def _verify_ceiling(self, call: Call, limit: float, check: Callable[[], None]) -> None:
        loop = asyncio.get_running_loop()
        retry_deadline: float | None = None
        for attempt in range(3):
            check()
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
            check()
            try:
                verify_composite(verification, limit, completed_at)
            except FutureScheduleError as error:
                if error.ahead_seconds > 1 or attempt == 2:
                    raise UnavailableError("composite schedule remains outside present coverage") from None
                if retry_deadline is None:
                    retry_deadline = loop.time() + 1
                wait_until = min(loop.time() + 0.25, retry_deadline)
                while loop.time() < wait_until:
                    check()
                    await asyncio.sleep(min(0.025, max(0, wait_until - loop.time())))
                check()
            else:
                return
        raise UnavailableError("composite schedule remains outside present coverage")

    # Establish or restore the desired ceiling before a start, native plan or new authenticated connection is used.
    async def ensure_ceiling(self, *, guard: Callable[[], None] | None = None) -> None:
        if guard is not None:
            guard()
        phase = self.journal.get("trial_phase")
        recovery_hold = phase in ("blocked", "recovering") and self.journal.get("control", {}).get("inhibit")
        if phase not in (None, "active") and not recovery_hold:
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
        if limit > 0 and self.journal.get("trial_starting"):
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

    # A managed test supplies a live per-request guard without enabling the unrestricted local Start policy.
    async def start_managed(
        self,
        tag: str,
        guard: Callable[[], None],
        *,
        request_id: str | None = None,
        on_prepared: Callable[[], None] | None = None,
        scope_guard: Callable[[], None] | None = None,
    ) -> Frame:
        if self.policy.allow_local_start or not 6 <= self.policy.max_current_a <= 32:
            raise DeniedError("managed Start requires the restricted installation policy")
        return await self._owned(
            self._start(
                tag, guard=guard, shared=True, request_id=request_id, on_prepared=on_prepared, scope_guard=scope_guard
            )
        )

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
    ) -> Frame:
        if scope_guard is not None:
            scope_guard()
        if guard is not None:
            guard()
        if not self.policy.allow_local_start and guard is None:
            raise DeniedError("local Start is disabled; charging remains under the app and charger schedule")
        if self.journal.get("trial_starting"):
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
            self.journal.put(
                "local_start_receipt",
                {
                    "ticket": ticket,
                    "digest": fingerprint(tag.casefold()),
                    "stage": "prepared",
                    "local": None,
                    "request_id": request_id,
                },
            )
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
            receipt = self.journal.get("local_start_receipt", {})
            if receipt.get("ticket") == ticket:
                with self.journal.atomic():
                    control = self.journal.get("control", {})
                    settled = stage == "rejected" and receipt.get("local") is None and control.get("ticket") == ticket
                    self.journal.put(
                        "local_start_receipt",
                        {**receipt, "stage": stage, **({"settled_ticket": ticket} if settled else {})},
                    )
                    if settled:
                        self.journal.put("control", {**control, "start_pending": False})

        # The native and resource gates both run before the transport can send this command.
        def dispatch(action: str) -> None:
            if action == "RemoteStartTransaction":
                if self.journal.active() is not None:
                    raise DeniedError("an actual transaction superseded the pending Start before dispatch")
                record_stage("issued")

        async with self.peer.sequence(1, check, on_dispatch=dispatch) as call:
            try:
                limit = number(previous.get("resume_limit", self.policy.max_current_a), self.policy.max_current_a)
                await self._cap(call, limit, epoch)
                if self.authorize is not None and not self.authorize(tag):
                    raise DeniedError("authorization changed before forwarding a local start")
                with self.journal.atomic():
                    self._complete(ticket, inhibit=False, start_pending=True)
                answer = await call("RemoteStartTransaction", commands.remote_start_transaction(tag, 1))
                if answer[0] != 3 or answer[2].get("status") != "Accepted":
                    if answer[0] == 3 and answer[2].get("status") == "Rejected":
                        record_stage("rejected")
                    raise UnavailableError("transaction start was not confirmed")
                record_stage("accepted")
                check()
                self._complete(ticket, uncertain=False, **({"owner": "shared"} if shared else {}))
            except UnavailableError, DeniedError, RecoveryRequiredError, SecurityError:
                retire_unsent_local_start(self.journal, ticket)
                self._complete(ticket, inhibit=True)
                self._check(epoch, ticket)
                restoring_zero = True
                await self._cap(call, 0, epoch)
                check()
                raise
        return answer

    # Releasing local ownership is a physical operation, never removal of the station safety profile.
    async def release(
        self,
        acknowledge_uncertainty: bool = False,
        *,
        guard: Callable[[], None] | None = None,
        attempt: ReleaseAttempt | None = None,
        context: AbstractContextManager[None] | None = None,
        rollback_guard: Callable[[], None] | None = None,
    ) -> None:
        await self._owned(self._release_owned(acknowledge_uncertainty, guard, attempt, context, rollback_guard))

    # Completion and guard lifetime belong to this shielded child, including its rollback and cancellation cleanup.
    async def _release_owned(
        self,
        acknowledge_uncertainty: bool,
        guard: Callable[[], None] | None,
        attempt: ReleaseAttempt | None,
        context: AbstractContextManager[None] | None,
        rollback_guard: Callable[[], None] | None,
    ) -> None:
        try:
            if self.release_attempt is not None:
                raise RecoveryRequiredError("another owned release is still running")
            with context if context is not None else nullcontext():
                if attempt is not None:
                    self.release_attempt = attempt
                await self._release(acknowledge_uncertainty, guard, rollback_guard, attempt=attempt)
        finally:
            if attempt is not None:
                attempt.failure = sys.exception()
                attempt.cancelled = isinstance(attempt.failure, asyncio.CancelledError)
                attempt.finished = True
                if self.release_attempt is attempt:
                    self.release_attempt = None

    # A failed physical release reinstalls zero instead of retaining only a misleading software hold.
    async def _release(
        self,
        acknowledge_uncertainty: bool,
        guard: Callable[[], None] | None = None,
        rollback_guard: Callable[[], None] | None = None,
        *,
        attempt: ReleaseAttempt | None = None,
    ) -> None:
        if guard is not None:
            guard()  # Check before claiming, including when a newer Pause ran before this owned task started.
        if self.journal.get("trial_starting"):
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
            def check() -> None:
                self._check(epoch, ticket)
                if rollback_guard is not None:
                    rollback_guard()
                if not restoring_zero and guard is not None:
                    guard()

            # Record possible physical effects at actual sender invocation and retain them through later reads.
            def dispatched(action: str) -> None:
                if attempt is not None and action == "SetChargingProfile":
                    if restoring_zero:
                        attempt.zero_may_have_dispatched = True
                    else:
                        attempt.positive_may_have_dispatched = True

            async with self.peer.sequence(1, check, on_dispatch=dispatched) as call:
                try:
                    await self._cap(
                        call,
                        number(previous.get("resume_limit", self.policy.max_current_a), self.policy.max_current_a),
                        epoch,
                    )
                    check()
                    if attempt is not None:
                        attempt.positive_confirmed = True
                except (UnavailableError, DeniedError, RecoveryRequiredError) as error:
                    if attempt is not None:
                        attempt.positive_failure = error
                    self._complete(ticket, inhibit=True)
                    self._check(epoch, ticket)
                    restoring_zero = True
                    await self._cap(call, 0, epoch)
                    check()
                    if attempt is not None:
                        attempt.zero_confirmed = True
                    if isinstance(error, ReleaseDeferredError) and (
                        attempt is None
                        or not attempt.positive_may_have_dispatched
                        and native_availability(error)
                        and rollback_guard is not None
                    ):
                        check()
                        self._complete(ticket, uncertain=False)
                    raise
                # Changing the station ceiling cannot bind a pending command to a transaction.
                self._complete(ticket, owner="shared", inhibit=False, uncertain=False)
        finally:
            if self._release_ticket == ticket:
                self._release_ticket = None
