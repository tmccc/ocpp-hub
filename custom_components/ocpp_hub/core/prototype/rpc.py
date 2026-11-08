"""Bounded bidirectional requests with priority and exact peer-error preservation."""

from __future__ import annotations

import asyncio
import heapq
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

from ..frames import Frame, build_call, build_call_error, build_call_result
from .store import RecoveryRequiredError, fingerprint

Sender = Callable[[str], Awaitable[None]]


class UnavailableError(RuntimeError):
    """The peer did not give a confirmed outcome."""


class InboundCache:
    """Per-connection in-flight and completed replies; opaque bodies stay in memory only."""

    # Bound memory without evicting mutating requests and accidentally executing a retry again.
    def __init__(self, capacity: int = 4096) -> None:
        self.capacity = capacity
        self._items: dict[tuple[str, int, str], tuple[str, asyncio.Future[str]]] = {}

    # Share one execution between duplicate requests, including duplicates that arrive before its reply.
    async def response(self, side: str, epoch: int, frame: Frame, handler: Callable[[], Awaitable[str]]) -> str:
        key = side, epoch, frame[1]
        digest = fingerprint(frame[2:])
        previous = self._items.get(key)
        if previous is not None:
            if previous[0] != digest:
                return build_call_error(frame[1], "FormationViolation", "CALL id has a different request body")
            return await asyncio.shield(previous[1])
        if len(self._items) >= self.capacity:
            return build_call_error(frame[1], "OccurrenceConstraintViolation", "request receipt capacity reached")
        future: asyncio.Future[str] = asyncio.get_running_loop().create_future()
        self._items[key] = digest, future
        try:
            text = await handler()
        except BaseException:
            future.cancel()
            raise
        future.set_result(text)
        return text

    # Release completed opaque replies from a retired authenticated connection.
    def retire(self, side: str, epoch: int) -> None:
        for key in list(self._items):
            if key[0] == side and key[1] != epoch and self._items[key][1].done():
                del self._items[key]


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

    # Correlate replies by fresh, namespaced ids; unknown replies are ignored.
    def receive(self, frame: Frame) -> None:
        future = self._pending.get(frame[1])
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
    ) -> Frame:
        async with self.gate.enter(priority):
            if check is not None:
                check()
            sender = self.sender
            if sender is None:
                raise UnavailableError("peer is disconnected")
            unique_id = token or self.name[:1] + "-" + uuid.uuid4().hex
            if not 1 <= len(unique_id) <= 36:
                raise RecoveryRequiredError("stored OCPP message identifier requires explicit reconciliation")
            future: asyncio.Future[Frame] = asyncio.get_running_loop().create_future()
            self._pending[unique_id] = future
            try:
                async with asyncio.timeout(self.timeout):
                    actual = payload() if callable(payload) else payload
                    if before_send is not None:
                        before_send()
                    await sender(build_call(unique_id, action, actual))
                    return await future
            except (TimeoutError, OSError) as error:
                raise UnavailableError("peer outcome is unconfirmed") from error
            finally:
                self._pending.pop(unique_id, None)

    # Fail pending work immediately on a disconnect without affecting the other peer.
    def detach(self) -> None:
        self.sender = None
        for future in self._pending.values():
            if not future.done():
                future.set_exception(UnavailableError("peer disconnected"))
