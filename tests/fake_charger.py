"""A fake charger and a recording listener for engine tests. Real websockets, no mocks of the engine."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

NEO_BOOT = {
    "chargePointVendor": "RAEDIAN",
    "chargePointModel": "NEO_22T_A",
    "chargePointSerialNumber": "TEST0001",
    "firmwareVersion": "s8100000001131&2.0.5_134",
}


class FakeCharger:
    """Connects to the hub like a charger would and exposes calls, answers and hub-initiated requests."""

    # Remember where to connect and who to be; nothing is opened yet.
    def __init__(self, port: int, cp_id: str = "TEST0001", subprotocol: str = "ocpp1.6") -> None:
        self.url = f"ws://127.0.0.1:{port}/{cp_id}"
        self.subprotocol = subprotocol
        self.hub_calls: asyncio.Queue[list[Any]] = asyncio.Queue()
        # Answer GetCompositeSchedule by itself: with an error, or with this limit in amperes when it is set.
        self.answer_composite = True
        self.composite_limit: float | None = None
        # For a request that looks a day ahead: the periods to answer with, as (offset in seconds, amperes).
        self.composite_periods: list[tuple[int, float]] | None = None
        # A charger that cannot answer a request for a whole day, only for the present moment.
        self.refuse_long_composite = False
        self.composite_calls = 0
        self._results: dict[str, asyncio.Future[list[Any]]] = {}
        self._connection: ClientConnection | None = None
        self._reader: asyncio.Task[None] | None = None
        self._counter = 0

    # Open the websocket and start reading hub frames in the background.
    async def connect(self) -> None:
        self._connection = await connect(self.url, subprotocols=[self.subprotocol])  # type: ignore[list-item]
        self._reader = asyncio.create_task(self._read())

    # Close the websocket and stop the reader.
    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
        if self._reader is not None:
            await asyncio.gather(self._reader, return_exceptions=True)

    @property
    def close_code(self) -> int | None:
        """The close code the hub used, once the connection has ended."""
        assert self._connection is not None
        return self._connection.close_code

    # Wait until the hub closed the connection.
    async def wait_closed(self, timeout: float = 3.0) -> None:
        assert self._connection is not None
        await asyncio.wait_for(self._connection.wait_closed(), timeout)

    # Send raw text exactly as given, for malformed-frame tests.
    async def send_raw(self, text: str) -> None:
        assert self._connection is not None
        await self._connection.send(text)

    # Send a CALL and return the answer frame (CALLRESULT or CALLERROR).
    async def call(self, action: str, payload: Any, timeout: float = 3.0) -> list[Any]:
        assert self._connection is not None
        self._counter += 1
        unique_id = f"c{self._counter}"
        future: asyncio.Future[list[Any]] = asyncio.get_running_loop().create_future()
        self._results[unique_id] = future
        await self._connection.send(json.dumps([2, unique_id, action, payload]))
        return await asyncio.wait_for(future, timeout)

    # Wait for the next request the hub sends to this charger.
    async def next_hub_call(self, timeout: float = 3.0) -> list[Any]:
        return await asyncio.wait_for(self.hub_calls.get(), timeout)

    # Answer a hub request with a payload (CALLRESULT).
    async def answer(self, call_frame: list[Any], payload: dict[str, Any] | None = None) -> None:
        assert self._connection is not None
        await self._connection.send(json.dumps([3, call_frame[1], payload or {}]))

    # Answer a hub request with an error (CALLERROR).
    async def answer_error(self, call_frame: list[Any], code: str = "NotSupported") -> None:
        assert self._connection is not None
        await self._connection.send(json.dumps([4, call_frame[1], code, "no", {}]))

    # Send a BootNotification like the NEO does and return the answer.
    async def boot(self, **overrides: Any) -> list[Any]:
        return await self.call("BootNotification", {**NEO_BOOT, **overrides})

    # The answer to a GetCompositeSchedule: the limit that was set for it, or a CALLERROR as a charger without support.
    def _composite_reply(self, frame: list[Any]) -> list[Any]:
        if frame[3].get("duration", 0) > 3600 and self.refuse_long_composite:
            return [4, frame[1], "NotSupported", "", {}]
        if frame[3].get("duration", 0) > 3600 and self.composite_periods is not None:
            periods = [{"startPeriod": offset, "limit": limit} for offset, limit in self.composite_periods]
            schedule = {"chargingRateUnit": "A", "chargingSchedulePeriod": periods}
            body = {
                "status": "Accepted",
                "connectorId": 1,
                "scheduleStart": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "chargingSchedule": schedule,
            }
            return [3, frame[1], body]
        if self.composite_limit is None:
            return [4, frame[1], "NotSupported", "", {}]
        schedule = {
            "chargingRateUnit": "A",
            "chargingSchedulePeriod": [{"startPeriod": 0, "limit": self.composite_limit}],
        }
        return [3, frame[1], {"status": "Accepted", "connectorId": 1, "chargingSchedule": schedule}]

    # Read frames from the hub: results resolve waiting calls, requests go to the queue.
    async def _read(self) -> None:
        assert self._connection is not None
        try:
            async for message in self._connection:
                frame = json.loads(message)
                if frame[0] == 2 and frame[2] == "GetCompositeSchedule" and self.answer_composite:
                    self.composite_calls += 1
                    # The hub reads the limit in effect after every write. Most tests are not about that, so it is
                    # answered at once, as a charger that cannot do it would; a test sets composite_limit to see it.
                    await self._connection.send(json.dumps(self._composite_reply(frame)))
                elif frame[0] == 2:
                    self.hub_calls.put_nowait(frame)
                elif frame[1] in self._results and not self._results[frame[1]].done():
                    self._results[frame[1]].set_result(frame)
        except ConnectionClosed:
            pass


class RecordingListener:
    """Records everything the engine reports, in order."""

    # Start with empty logs.
    def __init__(self) -> None:
        self.snapshots: list[Any] = []
        self.frames: list[tuple[str, str, list[Any]]] = []
        self.connected: list[str] = []
        self.disconnected: list[str] = []
        self.finished: list[Any] = []

    # Record a connection.
    def on_connected(self, session: Any) -> None:
        self.connected.append(session.cp_id)

    # Record a state change.
    def on_snapshot(self, snapshot: Any) -> None:
        self.snapshots.append(snapshot)

    # Record a frame with its sender.
    def on_frame(self, cp_id: str, sender: str, frame: list[Any]) -> None:
        self.frames.append((cp_id, sender, frame))

    # The raw frame and snapshot are recorded separately in these tests.
    def on_start_committed(self, cp_id: str, transaction_id: int, payload: dict[str, Any]) -> None:
        return None

    # Record a finished charge.
    def on_session_finished(self, session: Any) -> None:
        self.finished.append(session)

    # Record a disconnect.
    def on_disconnected(self, cp_id: str) -> None:
        self.disconnected.append(cp_id)

    @property
    def latest(self) -> Any:
        """The most recent snapshot."""
        return self.snapshots[-1]
