"""Process one short charger command decision at a time."""

from __future__ import annotations

import asyncio
import heapq
from collections.abc import Awaitable, Callable
from enum import IntEnum


class CommandPriority(IntEnum):
    SHUTDOWN = -1
    RESTRICT = 0
    NORMAL = 10


class CommandProcessor:
    """Order state changes while network waits and incoming reports run independently."""

    # No background task is needed when there are no pending decisions.
    def __init__(self) -> None:
        self._waiting: list[tuple[CommandPriority, int, Callable[[], None]]] = []
        self._serial = 0
        self._scheduled = False
        self._deciding = False

    # Run a synchronous state change in priority order, with FIFO ordering at the same priority.
    async def run[T](self, decision: Callable[[], T], *, priority: CommandPriority = CommandPriority.NORMAL) -> T:
        loop = asyncio.get_running_loop()
        result = self._enqueue(loop, decision, priority)
        # Commit restrictions before yielding to a positive write already ready on the event loop.
        if priority < CommandPriority.NORMAL:
            while not result.done():
                self._process_next()
        self._schedule(loop)
        return await result

    # Settle a fact through the same queue before cancellation or another socket callback can lose it.
    def decide[T](self, decision: Callable[[], T], *, priority: CommandPriority = CommandPriority.NORMAL) -> T:
        if self._deciding:
            return decision()  # The calling turn already owns this entire synchronous change.
        if not self._waiting:
            self._deciding = True
            try:
                return decision()
            finally:
                self._deciding = False
        loop = asyncio.get_running_loop()
        result = self._enqueue(loop, decision, priority)
        while not result.done():
            self._process_next()
        self._schedule(loop)
        return result.result()

    # Let both synchronous settlement and waiting commands use the same priority and arrival order.
    def _enqueue[T](
        self, loop: asyncio.AbstractEventLoop, decision: Callable[[], T], priority: CommandPriority
    ) -> asyncio.Future[T]:
        result: asyncio.Future[T] = loop.create_future()

        # A cancelled caller that never got a turn has made no change.
        def process() -> None:
            if result.cancelled():
                return
            try:
                value = decision()
            except (Exception, asyncio.CancelledError) as error:
                # Return this decision's error to its caller and keep serving the other queued commands.
                result.set_exception(error)
            else:
                result.set_result(value)

        self._serial += 1
        heapq.heappush(self._waiting, (priority, self._serial, process))
        return result

    # Schedule only one callback for the current queue.
    def _schedule(self, loop: asyncio.AbstractEventLoop) -> None:
        if self._waiting and not self._scheduled:
            self._scheduled = True
            loop.call_soon(self._next)

    # Select the oldest decision at the highest priority.
    def _process_next(self) -> None:
        _, _, process = heapq.heappop(self._waiting)
        previous, self._deciding = self._deciding, True
        try:
            process()
        finally:
            self._deciding = previous

    # Run one turn and yield so arrivals and replies can be processed between decisions.
    def _next(self) -> None:
        self._scheduled = False
        if self._waiting:
            self._process_next()
        self._schedule(asyncio.get_running_loop())

    # Let ready restrictions join before ordinary work when there is already contention.
    async def yield_to_restrictions(self, priority: CommandPriority) -> None:
        if priority == CommandPriority.NORMAL and self._waiting:
            await self.turn(priority)

    # Yield a place in the same ordering before a guarded owner takes its next step.
    async def turn(self, priority: CommandPriority = CommandPriority.NORMAL) -> None:
        await self.run(lambda: None, priority=priority)

    # Keep the owner's task handle and its cancellation behavior across the queue wait.
    def start[T](
        self, owner: Callable[[], Awaitable[T]], *, priority: CommandPriority = CommandPriority.NORMAL
    ) -> asyncio.Task[T]:
        # The owner retains its original checks and waits. They do not occupy the processor.
        async def run_owner() -> T:
            await self.turn(priority)
            return await owner()

        return asyncio.create_task(run_owner())
