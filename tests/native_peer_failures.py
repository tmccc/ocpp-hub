"""Native HTTP peers that withhold their own independent responses."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from tests.handover_helpers import NativeVehiclePeer


class InterruptedNativePeer(NativeVehiclePeer):
    """The independent native endpoint can withhold only its own HTTP availability."""

    # Retain ordinary native physical state and expose the number of real failed HTTP requests.
    def __init__(self, bench: Any) -> None:
        super().__init__(bench)
        self.unavailable = False
        self.failed_reads: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    # Answer complete received requests with a temporary HTTP failure while ordinary peer behavior stays intact.
    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if not self.unavailable:
            await super().handle(reader, writer)
            return
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            fields = {
                line.split(b":", 1)[0].lower(): line.split(b":", 1)[1].strip()
                for line in header.split(b"\r\n")[1:]
                if b":" in line
            }
            request = json.loads(await reader.readexactly(int(fields[b"content-length"])))
            self.failed_reads.put_nowait(request)
            writer.write(b"HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()


# Wait for repeated actual HTTP attempts without invoking or replacing the engine's introduction logic.
async def retries(peer: InterruptedNativePeer) -> None:
    async with asyncio.timeout(3):
        for _ in range(3):
            assert (await peer.failed_reads.get())["action"] == "getConfig"
