"""Bounded bidirectional requests with priority and exact peer-error preservation."""

from __future__ import annotations

import asyncio
import heapq
import sqlite3
import time
import uuid
from collections import OrderedDict
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from ..frames import Frame, build_call, build_call_error, build_call_result
from .command_processor import CommandPriority, CommandProcessor
from .diagnostics import RpcDiagnostics
from .recovery_contracts import CallResult, GateDecision, Guard, NotSent, Pending, Reply, ResultCall, require_reply
from .store import Journal, RecoveryRequiredError, fingerprint

if TYPE_CHECKING:
    from .command_outcomes import OwnerWire, OwnerWireScope

Sender = Callable[[str], Awaitable[None]]
ReplyHandler = Callable[[str, Frame], bool | None]


class UnavailableError(RuntimeError):
    """The peer did not give a confirmed outcome."""


class ValidationPreemptedError(UnavailableError):
    """A genuinely queued restrictive operation retired validation before dispatch."""


class IntroductionDeferredError(UnavailableError):
    """Introduction needs fresh evidence or its first restrictive zero; no Stop or positive command was sent."""


class RegistrationDeferredError(UnavailableError):
    """The backend requires a bounded wait before the next registration attempt."""

    # Retain only timing, never the backend response body or identity.
    def __init__(self, retry_after: float) -> None:
        super().__init__("vendor registration deferred")
        self.retry_after = retry_after


class InboundCache:
    """Bound live handlers while durable connection receipts preserve exact replies without evicting writes."""

    # Only one Gateway owns a journal; prior-process connections cannot receive replies or retain live authority.
    def __init__(self, capacity: int = 32, *, journal: Journal | None = None) -> None:
        self.capacity = capacity
        self.command_processor: CommandProcessor | None = None
        self.db = journal.db if journal is not None else sqlite3.connect(":memory:", isolation_level=None)
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS inbound_receipts("
            "side TEXT NOT NULL,epoch INTEGER NOT NULL,token TEXT NOT NULL,digest TEXT NOT NULL,response TEXT,"
            "PRIMARY KEY(side,epoch,token))"
        )
        self.db.execute("DELETE FROM inbound_receipts")
        self._items: dict[tuple[str, int, str], tuple[str, asyncio.Future[str]]] = {}
        self._reads: OrderedDict[tuple[str, int, str], tuple[str, str]] = OrderedDict()
        self._awaiting_write: OrderedDict[tuple[str, int, str], tuple[str, str]] = OrderedDict()
        self._write_capacity = capacity if journal is None else journal.capacity
        self._epochs: dict[str, int] = {}

    # An interrupted handler may have transmitted a mutation; its duplicate receives failure and never replays it.
    @staticmethod
    def unavailable(token: str) -> str:
        return build_call_error(token, "InternalError", "request outcome unavailable; request was not replayed")

    # A failed cache write leaves the handler's outcome intact and its pre-send receipt prevents redispatch.
    def complete(
        self, key: tuple[str, int, str], digest: str, future: asyncio.Future[str], text: str, read: bool
    ) -> str:
        side, epoch, _ = key
        if self._epochs.get(side, epoch) == epoch:
            if read:
                self._reads[key] = (digest, text)
                if len(self._reads) > self.capacity:
                    self._reads.popitem(last=False)
            elif not self._save_response(key, digest, text):
                self._awaiting_write[key] = (digest, text)
                if len(self._awaiting_write) > self._write_capacity:
                    self._awaiting_write.popitem(last=False)
        future.set_result(text)
        self._items.pop(key, None)
        return text

    # Only amend the existing receipt with the same connection, request and body.
    def _save_response(self, key: tuple[str, int, str], digest: str, text: str) -> bool:
        try:
            self.db.execute(
                "UPDATE inbound_receipts SET response=? WHERE side=? AND epoch=? AND token=? AND digest=?",
                (text, *key, digest),
            )
        except sqlite3.Error:
            return False
        self._awaiting_write.pop(key, None)
        return True

    # Response storage can recover independently of the physical outcome and the original caller.
    def retry(self) -> None:
        for key, (digest, text) in tuple(self._awaiting_write.items()):
            self._save_response(key, digest, text)

    # Preserve mutation receipts for the whole connection; an evicted read can safely run again.
    async def response(
        self,
        side: str,
        epoch: int,
        frame: Frame,
        handler: Callable[[], Awaitable[str]],
        *,
        stop: bool = False,
        read: bool = False,
    ) -> str:
        key = side, epoch, frame[1]
        digest = fingerprint(frame[2:])
        priority = CommandPriority.RESTRICT if stop else CommandPriority.NORMAL
        processor = self.command_processor

        # Keep receipt admission and completion in the same order as other decisions.
        def decide[T](decision: Callable[[], T]) -> T:
            return decision() if processor is None else processor.decide(decision, priority=priority)

        # Yield before claiming the receipt so cancellation always has a clear owner.
        if processor is not None:
            await processor.yield_to_restrictions(priority)
        receipt = decide(lambda: self._admit(key, digest, stop, read))
        if isinstance(receipt, str):
            return receipt
        future, owner = receipt
        if not owner:
            return await asyncio.shield(future)
        try:
            text = await handler()
        except BaseException:
            # Keep a terminal receipt even on cancellation after possible dispatch.
            decide(lambda: self.complete(key, digest, future, self.unavailable(key[2]), read))
            raise
        return decide(lambda: self.complete(key, digest, future, text, read))

    # Admit one owner for a new request or return the existing receipt for a duplicate.
    def _admit(
        self, key: tuple[str, int, str], digest: str, stop: bool, read: bool
    ) -> str | tuple[asyncio.Future[str], bool]:
        side, epoch, token = key
        if self._epochs.get(side, epoch) != epoch:
            return self.unavailable(token)
        previous = self._items.get(key)
        if previous is not None:
            if previous[0] != digest:
                return build_call_error(token, "FormationViolation", "CALL id has a different request body")
            return previous[1], False
        recent = self._reads.get(key) or self._awaiting_write.get(key)
        if recent is not None:
            if recent[0] != digest:
                return build_call_error(token, "FormationViolation", "CALL id has a different request body")
            return recent[1]
        try:
            stored = self.db.execute(
                "SELECT digest,response FROM inbound_receipts WHERE side=? AND epoch=? AND token=?", key
            ).fetchone()
            if stored is not None:
                if stored[0] != digest:
                    return build_call_error(token, "FormationViolation", "CALL id has a different request body")
                return stored[1] if isinstance(stored[1], str) else self.unavailable(token)
            if len(self._items) >= self.capacity + int(stop):
                return build_call_error(token, "OccurrenceConstraintViolation", "request queue is full")
            if not read:
                self.db.execute("INSERT INTO inbound_receipts VALUES(?,?,?,?,NULL)", (*key, digest))
        except sqlite3.Error:
            # A command without a durable receipt must not execute.
            return self.unavailable(token)
        future: asyncio.Future[str] = asyncio.get_running_loop().create_future()
        self._items[key] = digest, future
        return future, True

    # Replacement connections retire their own old results; in-flight old handlers cannot insert them again.
    def retire(self, side: str, epoch: int) -> None:
        self.db.execute("DELETE FROM inbound_receipts WHERE side=? AND epoch!=?", (side, epoch))
        self._epochs[side] = epoch
        for cache in (self._reads, self._awaiting_write):
            for key in tuple(cache):
                if key[0] == side and key[1] != epoch:
                    del cache[key]


# Preserve the actual peer's result or error while restoring the original requester id.
def reply_as(frame: Frame, unique_id: str) -> str:
    if frame[0] == 4:
        text: str = build_call_error(unique_id, frame[2], frame[3], frame[4])
    else:
        text = build_call_result(unique_id, frame[2])
    return text


# Resource confirmation and command replies use the same supported response for each profile action.
def profile_status(action: str, answer: Frame) -> str | None:
    if answer[0] != 3:
        return None
    supported = (
        ("Accepted", "Unknown") if action == "ClearChargingProfile" else ("Accepted", "Rejected", "NotSupported")
    )
    return next((status for status in supported if answer[2] == {"status": status}), None)


class PriorityGate:
    """One outgoing CALL per direction, with local stop ahead of queued cloud work."""

    # Allocate a small priority queue shared by local and cloud requests.
    def __init__(self) -> None:
        self._condition = asyncio.Condition()
        self._waiting: list[tuple[int, int]] = []
        self._serial = 0
        self._busy = False
        self.restrictive_waiting = asyncio.Event()

    # A nested authorization cannot wait behind the very command which is waiting for its response.
    @property
    def busy(self) -> bool:
        return self._busy

    # Only queued restrictive work may interrupt an audit; a dispatched command always retains its reply wait.
    def _update_restrictive_waiting(self) -> None:
        if any(priority == 0 for priority, _ in self._waiting):
            self.restrictive_waiting.set()
        else:
            self.restrictive_waiting.clear()

    # Enter in priority order; cancellation removes the waiting ticket.
    @asynccontextmanager
    async def enter(self, priority: int) -> AsyncIterator[None]:
        async with self._condition:
            self._serial += 1
            ticket = (priority, self._serial)
            heapq.heappush(self._waiting, ticket)
            self._update_restrictive_waiting()
            try:
                await self._condition.wait_for(lambda: not self._busy and self._waiting[0] == ticket)
            except BaseException:
                self._waiting.remove(ticket)
                heapq.heapify(self._waiting)
                self._update_restrictive_waiting()
                self._condition.notify_all()
                raise
            heapq.heappop(self._waiting)
            self._update_restrictive_waiting()
            self._busy = True
        try:
            yield
        finally:
            async with self._condition:
                self._busy = False
                self._condition.notify_all()


class Peer:
    """The receiver remains free to resolve replies while a request waits."""

    # Keep no socket or host: tests attach synthetic or loopback-only senders explicitly.
    def __init__(self, name: str, timeout: float) -> None:
        self.name = name
        self.timeout = timeout
        self.command_processor: CommandProcessor | None = None
        self.validation_timeout = 15.0
        self.sender: Sender | None = None
        self.gate = PriorityGate()
        self._pending: dict[str, asyncio.Future[Frame]] = {}
        self._receivers: dict[str, tuple[CommandPriority, Callable[[Frame], None]]] = {}
        self._late_receivers: OrderedDict[str, tuple[float, CommandPriority, Callable[[Frame], None]]] = OrderedDict()
        self.before_send: Callable[[str, dict[str, Any]], GateDecision | None] | None = None
        self.validate_send: Callable[[str, dict[str, Any]], Awaitable[Guard | GateDecision | None]] | None = None
        self.after_result: Callable[[str, dict[str, Any], Frame], None] | None = None
        self.diagnostics = RpcDiagnostics()

    # An active caller can still settle this exact reply within its normal deadline.
    def has_pending_call(self, wire: str | None) -> bool:
        return wire is not None and wire in self._pending

    # Use the shared queue for state decisions made from replies, including during exception cleanup.
    def decide[T](self, decision: Callable[[], T], *, priority: CommandPriority = CommandPriority.NORMAL) -> T:
        processor = self.command_processor
        return decision() if processor is None else processor.decide(decision, priority=priority)

    # A short additional window can still settle an issued command on this same connection.
    def receive(self, frame: Frame) -> None:
        wire = frame[1]
        future = self._pending.get(wire)
        waiting = future is not None and not future.done()
        receiver = self._receivers.pop(wire, None)
        late = self._late_receivers.pop(wire, None)
        if receiver is None and late is not None and late[0] > time.monotonic():
            receiver = (late[1], late[2])
            self.diagnostics.reply(len(wire), True, late=True)
        else:
            self.diagnostics.reply(len(wire), waiting or receiver is not None)
        if future is not None and waiting:
            future.set_result(frame)
        if receiver is not None:
            priority, record = receiver
            self.decide(lambda: record(frame), priority=priority)

    # Send a request only after rechecking current authority inside the gate.
    async def call(
        self,
        action: str,
        payload: dict[str, Any] | Callable[[], dict[str, Any]],
        priority: int = 10,
        check: Callable[[], None] | None = None,
        token: str | None = None,
        before_send: Callable[[], None] | None = None,
        on_dispatch: Callable[[str], None] | None = None,
        *,
        on_reply: ReplyHandler | None = None,
        on_receive: Callable[[str, Frame], None] | None = None,
        keep_late_reply: bool = False,
        prepare_call: Callable[[], Awaitable[None]] | None = None,
    ) -> Frame:
        return require_reply(
            await self.call_result(
                action,
                payload,
                priority,
                check,
                token,
                before_send,
                on_dispatch,
                on_reply=on_reply,
                on_receive=on_receive,
                keep_late_reply=keep_late_reply,
                prepare_call=prepare_call,
            )
        )

    # Managed operations consume an unsent prerequisite as data while sharing the sole send and cleanup path.
    async def call_result(
        self,
        action: str,
        payload: dict[str, Any] | Callable[[], dict[str, Any]],
        priority: int = 10,
        check: Guard | None = None,
        token: str | None = None,
        before_send: Guard | None = None,
        on_dispatch: Callable[[str], None] | None = None,
        *,
        on_reply: ReplyHandler | None = None,
        on_receive: Callable[[str, Frame], None] | None = None,
        keep_late_reply: bool = False,
        prepare_call: Callable[[], Awaitable[None]] | None = None,
    ) -> CallResult:
        async with self.gate.enter(priority):
            if prepare_call is not None:
                await prepare_call()
            return await self._call_locked(
                action,
                payload,
                check,
                token,
                before_send,
                on_dispatch,
                priority > 0,
                on_reply,
                on_receive=on_receive,
                keep_late_reply=keep_late_reply,
            )

    # Hold the gate across safety-cap and control RPCs so queued cloud work cannot interleave them.
    @asynccontextmanager
    async def sequence(
        self,
        priority: int,
        check: Callable[[], None],
        *,
        on_dispatch: Callable[[str], None] | None = None,
        on_reply: ReplyHandler | None = None,
        on_sent: Callable[[str], None] | None = None,
        on_receive: Callable[[str, Frame], None] | None = None,
        late_reply_action: str | None = None,
        owner_scope: OwnerWireScope | None = None,
        restricting: Callable[[], bool] | None = None,
    ) -> AsyncIterator[Callable[[str, dict[str, Any]], Awaitable[Frame]]]:
        async with self.sequence_results(
            priority,
            check,
            on_dispatch=on_dispatch,
            on_reply=on_reply,
            on_sent=on_sent,
            on_receive=on_receive,
            late_reply_action=late_reply_action,
            owner_scope=owner_scope,
            restricting=restricting,
        ) as result_call:
            # Recheck generation and ownership before every step, not merely on sequence entry.
            async def call(action: str, payload: dict[str, Any]) -> Frame:
                return require_reply(await result_call(action, payload))

            yield call

    # Retain one priority-gate owner for a typed multi-call operation, including its restrictive rollback.
    @asynccontextmanager
    async def sequence_results(
        self,
        priority: int,
        check: Guard,
        *,
        on_dispatch: Callable[[str], None] | None = None,
        on_reply: ReplyHandler | None = None,
        on_sent: Callable[[str], None] | None = None,
        on_receive: Callable[[str, Frame], None] | None = None,
        late_reply_action: str | None = None,
        owner_scope: OwnerWireScope | None = None,
        restricting: Callable[[], bool] | None = None,
    ) -> AsyncIterator[ResultCall]:
        async with self.gate.enter(priority):
            # Each call revalidates after native waits and before recording any resource intent.
            async def call(action: str, payload: dict[str, Any]) -> CallResult:
                return await self._call_locked(
                    action,
                    payload,
                    check,
                    None,
                    None,
                    on_dispatch,
                    priority > 0 and not (restricting is not None and restricting()),
                    on_reply,
                    on_sent,
                    on_receive=on_receive,
                    keep_late_reply=action == late_reply_action,
                    owner_scope=owner_scope,
                )

            yield call

    # Bound native preparation independently, retiring its sockets promptly on Stop, disconnect or cancellation.
    async def _validate(
        self, action: str, payload: dict[str, Any], future: asyncio.Future[Frame], preemptible: bool
    ) -> Guard | GateDecision | None:
        if self.validate_send is None:
            return None
        validation = asyncio.ensure_future(self.validate_send(action, payload))
        signal = self.gate.restrictive_waiting if preemptible else asyncio.Event()
        restrictive = asyncio.create_task(signal.wait())
        cancellation: asyncio.CancelledError | None = None
        try:
            async with asyncio.timeout(self.validation_timeout):
                done, _ = await asyncio.wait((validation, restrictive, future), return_when=asyncio.FIRST_COMPLETED)
                if validation.done():
                    validation.result()  # A completed strict/storage refusal precedes simultaneous preemption.
                if signal.is_set():
                    raise ValidationPreemptedError("request was not sent; restrictive control superseded validation")
                if future in done:
                    future.result()  # A retired peer's exception must terminate its audit before any dispatch.
                    raise UnavailableError("request was not sent; peer replied before dispatch")
                decision = await validation
        except asyncio.CancelledError as error:
            cancellation = error
            raise
        finally:
            validation.cancel()
            restrictive.cancel()
            cleanup = asyncio.gather(validation, restrictive, return_exceptions=True)
            while True:
                try:
                    outcomes = await asyncio.shield(cleanup)
                    break
                except asyncio.CancelledError as error:
                    cancellation = cancellation or error
            failure = next(
                (
                    outcome
                    for outcome in outcomes
                    if isinstance(outcome, BaseException) and not isinstance(outcome, asyncio.CancelledError)
                ),
                None,
            )
            if cancellation is not None:
                if failure is not None:
                    raise cancellation from failure
                raise cancellation
            if failure is not None:
                raise failure  # Native cancellation can itself fail required durable audit publication.
        if preemptible and self.gate.restrictive_waiting.is_set():
            raise ValidationPreemptedError("request was not sent; restrictive control superseded validation")
        return decision

    # Send one request only while the caller owns the outgoing gate.
    async def _call_locked(
        self,
        action: str,
        payload: dict[str, Any] | Callable[[], dict[str, Any]],
        check: Guard | None,
        token: str | None,
        before_send: Guard | None,
        on_dispatch: Callable[[str], None] | None = None,
        preemptible: bool = True,
        on_reply: ReplyHandler | None = None,
        on_sent: Callable[[str], None] | None = None,
        *,
        on_receive: Callable[[str, Frame], None] | None = None,
        keep_late_reply: bool = False,
        owner_scope: OwnerWireScope | None = None,
    ) -> CallResult:
        priority = CommandPriority.NORMAL if preemptible else CommandPriority.RESTRICT
        processor = self.command_processor
        if processor is not None:
            await processor.yield_to_restrictions(priority)
        if check is not None:
            decision = self.decide(check, priority=priority)
            if isinstance(decision, Pending):
                return NotSent(decision)
        sender = self.sender
        if sender is None:
            raise UnavailableError("peer is disconnected")
        unique_id = token if token is not None else self.name[:1] + "-" + uuid.uuid4().hex
        if not 1 <= len(unique_id) <= 36:
            raise RecoveryRequiredError("stored OCPP message identifier requires explicit reconciliation")
        future: asyncio.Future[Frame] = asyncio.get_running_loop().create_future()
        self._pending[unique_id] = future
        dispatched = False
        processing_reply = False
        owner_wire: OwnerWire | None = None
        try:
            actual = self.decide(payload, priority=priority) if callable(payload) else payload
            validation = await self._validate(action, actual, future, preemptible)
            if isinstance(validation, Pending):
                return NotSent(validation)

            # Guard and record the exact request immediately before its sender can transmit.
            def ready_to_send() -> str | Pending:
                nonlocal owner_wire
                if check is not None:
                    permitted = check()
                    if isinstance(permitted, Pending):
                        return permitted
                if self.sender is not sender:
                    raise UnavailableError("request was not sent; peer changed during outgoing validation")
                if preemptible and self.gate.restrictive_waiting.is_set():
                    raise ValidationPreemptedError("request was not sent; restrictive control superseded validation")
                if future.done():
                    future.result()
                    raise UnavailableError("request was not sent; peer replied before dispatch")
                if callable(validation):
                    permitted = validation()
                    if isinstance(permitted, Pending):
                        return permitted
                if before_send is not None:
                    permitted = before_send()
                    if isinstance(permitted, Pending):
                        return permitted
                if self.before_send is not None:
                    permitted = self.before_send(action, actual)
                    if isinstance(permitted, Pending):
                        return permitted
                message = build_call(unique_id, action, actual)
                self.diagnostics.sending(action, len(unique_id))
                if on_dispatch is not None:
                    on_dispatch(action)
                if owner_scope is not None:
                    owner_wire = owner_scope.sent(unique_id, action, actual)
                return message

            # Wire evidence is captured in the same receive turn as the action-specific callback.
            def received(answer: Frame) -> None:
                if owner_scope is not None and owner_wire is not None:
                    owner_scope.received(owner_wire, answer)
                if on_receive is not None:
                    on_receive(action, answer)

            message = self.decide(ready_to_send, priority=priority)
            if isinstance(message, Pending):
                return NotSent(message)
            async with asyncio.timeout(self.timeout):
                # Receipts can arrive while the original sender is still draining.
                dispatched = True
                if on_receive is not None or owner_wire is not None:
                    self._receivers[unique_id] = (priority, received)
                await sender(message)
                if on_sent is not None:
                    sent = on_sent
                    self.decide(lambda: sent(action), priority=priority)
                self.diagnostics.phase = "awaiting_reply"
                answer = await future
                processing_reply = True
                result = self.decide(lambda: self._accept_reply(action, actual, answer, on_reply), priority=priority)
                self.diagnostics.phase = "complete"
                return result
        except (TimeoutError, OSError, UnavailableError, asyncio.CancelledError) as error:
            if isinstance(error, asyncio.CancelledError):
                raise
            if (
                dispatched
                and not processing_reply
                and future.done()
                and not future.cancelled()
                and future.exception() is None
            ):
                # The receiver has the actual reply even if writing the request never returned.
                answer = future.result()
                result = self.decide(lambda: self._accept_reply(action, actual, answer, on_reply), priority=priority)
                self.diagnostics.phase = "complete"
                return result
            if isinstance(error, UnavailableError):
                raise
            detail = "peer outcome is unconfirmed" if dispatched else "request was not sent; outgoing validation failed"
            raise UnavailableError(detail) from error
        finally:
            self.diagnostics.interrupted()
            self._pending.pop(unique_id, None)
            receiver = self._receivers.pop(unique_id, None)
            if keep_late_reply and receiver is not None and self.sender is sender:
                self._late_receivers[unique_id] = (time.monotonic() + 30.0, *receiver)
                if len(self._late_receivers) > 32:
                    self._late_receivers.popitem(last=False)
            if future.done() and not future.cancelled():
                future.exception()
            elif not future.done():
                future.cancel()

    # Record the actual reply first, then let the resource owner interpret the result.
    def _accept_reply(
        self,
        action: str,
        payload: dict[str, Any],
        answer: Frame,
        on_reply: ReplyHandler | None,
    ) -> Reply:
        handled = on_reply is not None and on_reply(action, answer)
        if self.after_result is not None and not handled:
            self.after_result(action, payload, answer)
        return Reply(answer)

    # Fail pending work immediately on a disconnect without affecting the other peer.
    def detach(self) -> None:
        self.diagnostics.interrupted()
        self.sender = None
        self._receivers.clear()
        self._late_receivers.clear()
        for future in self._pending.values():
            if not future.done():
                future.set_exception(UnavailableError("peer disconnected"))
