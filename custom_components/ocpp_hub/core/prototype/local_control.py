"""Durable local ownership with a physical station ceiling and an explicit zero-current hold."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Coroutine
from datetime import UTC, datetime
from typing import Any

from .. import commands
from ..control import HUB_PROFILE_ID
from ..frames import Frame
from .composite import FutureScheduleError, verify_composite
from .policy import DeniedError, Policy, number
from .rpc import Peer, UnavailableError
from .station import StationChoice
from .store import Journal, RecoveryRequiredError, ReleaseDeferredError

Call = Callable[[str, dict[str, Any]], Awaitable[Frame]]


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
            raise DeniedError("a newer local operation superseded this request")

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

    # Install the reserved station profile; Accepted confirms receipt, not yet real-device execution or persistence.
    async def _cap(self, call: Call, limit: float, epoch: int) -> Frame:
        if limit > 0 and self.journal.get("trial_starting"):
            raise DeniedError("initial native admission has not completed")
        self._confirmed_epoch = -1
        revision = self._revision
        ticket = self.journal.get("control", {}).get("ticket")
        desired = self.desired()

        # Boot may change on the same socket while a capability or composite RPC awaits its reply.
        def check() -> None:
            self._check(epoch)
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
    async def ensure_ceiling(self) -> None:
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
            if self.desired() != limit:
                raise DeniedError("desired ceiling changed during preparation")

        async with self.peer.sequence(0, check) as call:
            if not self.ready:
                await self._cap(call, limit, epoch)

    # A positive slider update cannot silently release an explicit zero-current hold.
    async def set_limit(self, value: Any, *, guard: Callable[[], None] | None = None, shared: bool = False) -> Frame:
        return await self._owned(self._set_limit(value, guard=guard, shared=shared))

    # Keep the complete claimed slider operation alive independently of an individual API client.
    async def _set_limit(self, value: Any, *, guard: Callable[[], None] | None = None, shared: bool = False) -> Frame:
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
        if limit > 0:
            self._complete(ticket, resume_limit=limit)

        # The managed selector retains app authority only while its durable request generation still matches.
        def check() -> None:
            self._check(epoch, ticket)
            if guard is not None:
                guard()

        async with self.peer.sequence(1, check) as call:
            answer = await self._cap(call, limit, epoch)
        check()
        self._complete(ticket, uncertain=False, **({"owner": "shared"} if shared else {}))
        return answer

    # Pause first, then stop only the known session, without allowing queued cloud traffic between them.
    async def stop(self, *, expected_local: int | None = None) -> Frame:
        if expected_local is not None and self.journal.active() not in (None, expected_local):
            raise RecoveryRequiredError("scheduled Stop no longer owns the active transaction")
        previous = self.journal.get("control", {})
        if self._stopping is None or self._stopping.done() or previous.get("ticket") != self._stop_ticket:
            epoch, ticket = self._claim(True, stopping=True)
            self._stop_ticket = ticket
            self._stopping = self._launch(
                self._stop(epoch, ticket, previous.get("start_pending", False), expected_local)
            )
        return await asyncio.shield(self._stopping)

    # Finish the zero-current hold and known-session stop even if the initiating client disconnects.
    async def _stop(self, epoch: int, ticket: str, pending_start: bool, expected_local: int | None = None) -> Frame:
        cap_error: UnavailableError | None = None
        async with self.peer.sequence(0, lambda: self._check(epoch, ticket)) as call:
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
            if cap_error is not None:
                raise cap_error
        self._complete(ticket, uncertain=bool(pending_start))
        return answer

    # Keep any previous lower ceiling; a rejected or uncertain start reasserts zero before returning failure.
    async def start(self, tag: str) -> Frame:
        return await self._owned(self._start(tag))

    # A managed test supplies a live per-request guard without enabling the unrestricted local Start policy.
    async def start_managed(self, tag: str, guard: Callable[[], None]) -> Frame:
        if self.policy.allow_local_start or not 6 <= self.policy.max_current_a <= 32:
            raise DeniedError("managed Start requires the restricted installation policy")
        return await self._owned(self._start(tag, guard=guard, shared=True))

    # A claimed start either completes its bounded sequence or reasserts the physical hold on failure.
    async def _start(self, tag: str, *, guard: Callable[[], None] | None = None, shared: bool = False) -> Frame:
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
        epoch, ticket = self._claim(previous.get("inhibit", False))

        # The request generation is checked again after native validation and immediately before each actual send.
        def check() -> None:
            self._check(epoch, ticket)
            if guard is not None:
                guard()

        async with self.peer.sequence(1, check) as call:
            try:
                limit = number(previous.get("resume_limit", self.policy.max_current_a), self.policy.max_current_a)
                await self._cap(call, limit, epoch)
                if self.authorize is not None and not self.authorize(tag):
                    raise DeniedError("authorization changed before forwarding a local start")
                self._complete(ticket, inhibit=False, start_pending=True)
                answer = await call("RemoteStartTransaction", commands.remote_start_transaction(tag, 1))
                if answer[0] != 3 or answer[2].get("status") != "Accepted":
                    raise UnavailableError("transaction start was not confirmed")
            except UnavailableError, DeniedError, RecoveryRequiredError:
                self._complete(ticket, inhibit=True)
                self._check(epoch, ticket)
                await self._cap(call, 0, epoch)
                raise
        if guard is not None:
            guard()
        self._complete(ticket, uncertain=False, **({"owner": "shared"} if shared else {}))
        return answer

    # Releasing local ownership is a physical operation, never removal of the station safety profile.
    async def release(self, acknowledge_uncertainty: bool = False, *, guard: Callable[[], None] | None = None) -> None:
        await self._owned(self._release(acknowledge_uncertainty, guard))

    # A failed physical release reinstalls zero instead of retaining only a misleading software hold.
    async def _release(self, acknowledge_uncertainty: bool, guard: Callable[[], None] | None = None) -> None:
        if guard is not None:
            guard()  # Check before claiming, including when a newer Pause ran before this owned task started.
        if self.journal.get("trial_starting"):
            raise DeniedError("initial native admission has not completed")
        previous = self.journal.get("control", {})
        if previous.get("uncertain") and not acknowledge_uncertainty:
            raise RecoveryRequiredError("previous local outcome requires reconciliation")
        epoch, ticket = self._claim(previous.get("inhibit", False))

        # A mode generation supplements the ticket through asynchronous native validation and actual send.
        def check() -> None:
            self._check(epoch, ticket)
            if guard is not None:
                guard()

        async with self.peer.sequence(1, check) as call:
            try:
                await self._cap(
                    call,
                    number(previous.get("resume_limit", self.policy.max_current_a), self.policy.max_current_a),
                    epoch,
                )
            except (UnavailableError, DeniedError, RecoveryRequiredError) as error:
                self._complete(ticket, inhibit=True)
                self._check(epoch, ticket)
                await self._cap(call, 0, epoch)
                if isinstance(error, ReleaseDeferredError):
                    self._complete(ticket, uncertain=False)
                raise
            self._complete(ticket, owner="shared", inhibit=False, uncertain=False, start_pending=False)
