"""Bounded bidirectional requests with priority and exact peer-error preservation."""

from __future__ import annotations

import asyncio
import heapq
import sqlite3
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

from ..frames import Frame, build_call, build_call_error, build_call_result
from .diagnostics import RpcDiagnostics
from .store import Journal, RecoveryRequiredError, fingerprint

Sender = Callable[[str], Awaitable[None]]


class UnavailableError(RuntimeError):
    """The peer did not give a confirmed outcome."""


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
        self.db = journal.db if journal is not None else sqlite3.connect(":memory:", isolation_level=None)
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS inbound_receipts("
            "side TEXT NOT NULL,epoch INTEGER NOT NULL,token TEXT NOT NULL,digest TEXT NOT NULL,response TEXT,"
            "PRIMARY KEY(side,epoch,token))"
        )
        self.db.execute("DELETE FROM inbound_receipts")
        self._items: dict[tuple[str, int, str], tuple[str, asyncio.Future[str]]] = {}

    # An interrupted handler may have transmitted a mutation; its duplicate receives failure and never replays it.
    @staticmethod
    def unavailable(token: str) -> str:
        return build_call_error(token, "InternalError", "request outcome unavailable; request was not replayed")

    # Commit a terminal result before waking duplicates; a failed commit leaves the durable pending no-replay marker.
    def complete(self, key: tuple[str, int, str], future: asyncio.Future[str], text: str) -> str:
        try:
            self.db.execute("UPDATE inbound_receipts SET response=? WHERE side=? AND epoch=? AND token=?", (text, *key))
        except sqlite3.Error:
            text = self.unavailable(key[2])  # The protocol boundary must still answer without repeating physical work.
        future.set_result(text)
        self._items.pop(key, None)
        return text

    # Share live work and persist completed replies so old harmless reads cannot exhaust the Stop acceptance path.
    async def response(self, side: str, epoch: int, frame: Frame, handler: Callable[[], Awaitable[str]]) -> str:
        key = side, epoch, frame[1]
        digest = fingerprint(frame[2:])
        previous = self._items.get(key)
        if previous is not None:
            if previous[0] != digest:
                return build_call_error(frame[1], "FormationViolation", "CALL id has a different request body")
            return await asyncio.shield(previous[1])
        try:
            stored = self.db.execute(
                "SELECT digest,response FROM inbound_receipts WHERE side=? AND epoch=? AND token=?", key
            ).fetchone()
            if stored is not None:
                if stored[0] != digest:
                    return build_call_error(frame[1], "FormationViolation", "CALL id has a different request body")
                return stored[1] if isinstance(stored[1], str) else self.unavailable(frame[1])
            if len(self._items) >= self.capacity:
                return build_call_error(frame[1], "OccurrenceConstraintViolation", "request queue is full")
            self.db.execute("INSERT INTO inbound_receipts VALUES(?,?,?,?,NULL)", (*key, digest))
        except sqlite3.Error:
            # Without a durable pre-handler receipt, no command may execute or be silently replayed.
            return self.unavailable(frame[1])
        future: asyncio.Future[str] = asyncio.get_running_loop().create_future()
        self._items[key] = digest, future
        try:
            text = await handler()
        except BaseException:
            # Preserve a bounded terminal answer even on cancellation after a possibly dispatched command.
            self.complete(key, future, self.unavailable(frame[1]))
            raise
        return self.complete(key, future, text)

    # Replacement connections retire their own old results; in-flight old handlers cannot insert them again.
    def retire(self, side: str, epoch: int) -> None:
        self.db.execute("DELETE FROM inbound_receipts WHERE side=? AND epoch!=?", (side, epoch))


# Preserve the actual peer's result or error while restoring the original requester id.
def reply_as(frame: Frame, unique_id: str) -> str:
    if frame[0] == 4:
        text: str = build_call_error(unique_id, frame[2], frame[3], frame[4])
    else:
        text = build_call_result(unique_id, frame[2])
    return text


class PriorityGate:
    """One outgoing CALL per direction, with local stop ahead of queued cloud work."""

    # Allocate a small priority queue shared by local and cloud requests.
    def __init__(self) -> None:
        self._condition = asyncio.Condition()
        self._waiting: list[tuple[int, int]] = []
        self._serial = 0
        self._busy = False

    # A nested authorization cannot wait behind the very command which is waiting for its response.
    @property
    def busy(self) -> bool:
        return self._busy

    # Enter in priority order; cancellation removes the waiting ticket.
    @asynccontextmanager
    async def enter(self, priority: int) -> AsyncIterator[None]:
        async with self._condition:
            self._serial += 1
            ticket = (priority, self._serial)
            heapq.heappush(self._waiting, ticket)
            try:
                await self._condition.wait_for(lambda: not self._busy and self._waiting[0] == ticket)
            except BaseException:
                self._waiting.remove(ticket)
                heapq.heapify(self._waiting)
                self._condition.notify_all()
                raise
            heapq.heappop(self._waiting)
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
        self.sender: Sender | None = None
        self.gate = PriorityGate()
        self._pending: dict[str, asyncio.Future[Frame]] = {}
        self.before_send: Callable[[str, dict[str, Any]], None] | None = None
        self.validate_send: Callable[[str, dict[str, Any]], Awaitable[None]] | None = None
        self.after_result: Callable[[str, dict[str, Any], Frame], None] | None = None
        self.diagnostics = RpcDiagnostics()

    # Correlate replies by fresh, namespaced ids; unknown replies are ignored.
    def receive(self, frame: Frame) -> None:
        future = self._pending.get(frame[1])
        self.diagnostics.reply(len(frame[1]), future is not None and not future.done())
        if future is not None and not future.done():
            future.set_result(frame)

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
    ) -> Frame:
        async with self.gate.enter(priority):
            return await self._call_locked(action, payload, check, token, before_send, on_dispatch)

    # Hold the gate across safety-cap and control RPCs so queued cloud work cannot interleave them.
    @asynccontextmanager
    async def sequence(
        self, priority: int, check: Callable[[], None], *, on_dispatch: Callable[[str], None] | None = None
    ) -> AsyncIterator[Callable[[str, dict[str, Any]], Awaitable[Frame]]]:
        async with self.gate.enter(priority):
            # Recheck generation and ownership before every step, not merely on sequence entry.
            async def call(action: str, payload: dict[str, Any]) -> Frame:
                return await self._call_locked(action, payload, check, None, None, on_dispatch)

            yield call

    # Send one request only while the caller owns the outgoing gate.
    async def _call_locked(
        self,
        action: str,
        payload: dict[str, Any] | Callable[[], dict[str, Any]],
        check: Callable[[], None] | None,
        token: str | None,
        before_send: Callable[[], None] | None,
        on_dispatch: Callable[[str], None] | None = None,
    ) -> Frame:
        if check is not None:
            check()
        sender = self.sender
        if sender is None:
            raise UnavailableError("peer is disconnected")
        unique_id = token if token is not None else self.name[:1] + "-" + uuid.uuid4().hex
        if not 1 <= len(unique_id) <= 36:
            raise RecoveryRequiredError("stored OCPP message identifier requires explicit reconciliation")
        future: asyncio.Future[Frame] = asyncio.get_running_loop().create_future()
        self._pending[unique_id] = future
        try:
            async with asyncio.timeout(self.timeout):
                actual = payload() if callable(payload) else payload
                if self.validate_send is not None:
                    await self.validate_send(action, actual)
                    if check is not None:
                        check()
                    if self.sender is not sender:
                        raise UnavailableError("peer changed during outgoing validation")
                if before_send is not None:
                    before_send()
                if self.before_send is not None:
                    self.before_send(action, actual)
                message = build_call(unique_id, action, actual)
                self.diagnostics.sending(action, len(unique_id))
                if on_dispatch is not None:
                    on_dispatch(action)
                await sender(message)
                self.diagnostics.phase = "awaiting_reply"
                answer = await future
                if self.after_result is not None:
                    self.after_result(action, actual, answer)
                self.diagnostics.phase = "complete"
                return answer
        except (TimeoutError, OSError) as error:
            raise UnavailableError("peer outcome is unconfirmed") from error
        finally:
            self.diagnostics.interrupted()
            self._pending.pop(unique_id, None)
            if future.done() and not future.cancelled():
                future.exception()
            elif not future.done():
                future.cancel()

    # Fail pending work immediately on a disconnect without affecting the other peer.
    def detach(self) -> None:
        self.diagnostics.interrupted()
        self.sender = None
        for future in self._pending.values():
            if not future.done():
                future.set_exception(UnavailableError("peer disconnected"))
