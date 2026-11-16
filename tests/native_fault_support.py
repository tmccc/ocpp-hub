"""Independent native HTTP fault gates and read-only coroutine witnesses, without recovery policy."""

import asyncio
import json

from tests.native_schedule_support import SchedulePeer


# Inspect actual suspended coroutines without replacing an engine method, advancing a task or changing a verdict.
def coroutine_locals(task, code):
    item = task.get_coro()
    for _ in range(60):
        if item is None:
            break
        actual_code = getattr(item, "cr_code", None) or getattr(item, "gi_code", None)
        if actual_code is code:
            frame = getattr(item, "cr_frame", None) or getattr(item, "gi_frame", None)
            return dict(frame.f_locals) if frame is not None else {}
        item = getattr(item, "cr_await", None) or getattr(item, "gi_yieldfrom", None)
    return {}


class FinalReleaseHttpPeer(SchedulePeer):
    """The independent endpoint withholds and fails exactly one actual selected native request."""

    # Keep ordinary firmware observations and expose only transport scheduling and plain witness data to the test.
    def __init__(self, bench):
        super().__init__(bench)
        self.select = None
        self.witness = None
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.responded = False

    # Parse a complete real request, fail only the selected one, and delegate all other requests unchanged.
    async def handle(self, reader, writer):
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            fields = {
                line.split(b":", 1)[0].lower(): line.split(b":", 1)[1].strip()
                for line in header.split(b"\r\n")[1:]
                if b":" in line
            }
            body = await reader.readexactly(int(fields[b"content-length"]))
            request = json.loads(body)
            selected = None
            if self.witness is None and self.select is not None and request["action"] == "getConfig":
                selected = self.select()
            if selected is not None:
                self.witness = selected | {"http_action": request["action"]}
                self.entered.set()
                await self.release.wait()
                writer.write(b"HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
                await writer.drain()
                self.responded = True
                return
            replay = asyncio.StreamReader()
            replay.feed_data(header + body)
            replay.feed_eof()
            await super().handle(replay, writer)
        except ConnectionError:
            pass  # A preempted native audit can close its socket before the independent delayed reply.
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass  # The client already retired its cancelled audit.
