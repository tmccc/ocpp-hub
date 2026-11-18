"""Delayed timer lifecycle reports wait for independent physical zero verification."""

import asyncio
import uuid
from datetime import UTC, datetime

from ocpp_hub_core.prototype.gateway import decode

import tests.managed_rig as managed_helpers
from tests.managed_rig import healthy, starts
from tests.managed_scenario_helpers import close_in_order, next_daily, originals, select_current, settle
from tests.native_schedule_support import timer_bench
from tests.prototype_rig import START
from tests.runtime_rig import until
from tests.test_managed_start_overlap import owner_tag
from tests.tls_support import tls as tls


class CompositeReplyPeer:
    """Delay one real composite reply while continuing to receive unrelated TLS replies."""

    # Record actual incoming request tokens so only the intended independent verification can be held.
    def __init__(self, connection, bench):
        self.connection, self.bench = connection, bench
        self.actions = {}
        self.delayed = None

    # Keep every unmodified websocket operation on the actual authenticated socket.
    def __getattr__(self, name):
        return getattr(self.connection, name)

    # Let the ordinary independent device reader retain its normal frame processing.
    def __aiter__(self):
        return self.frames()

    # Observe actual CALL identities without changing their action or payload.
    async def frames(self):
        async for raw in self.connection:
            frame = decode(raw)
            if frame[0] == 2:
                self.actions[frame[1]] = frame[2]
            yield raw

    # Withhold only one actual zero composite response; the reader must still process the Start CALLERROR.
    async def send(self, raw):
        frame = decode(raw)
        if (
            self.bench.hold_zero
            and frame[0] == 3
            and self.actions.get(frame[1]) == "GetCompositeSchedule"
            and self.bench.firmware.effective_limit() == 0
        ):
            self.bench.hold_zero = False
            self.bench.entered.set()
            self.delayed = asyncio.create_task(self.reply_after_release(raw))
            return
        await self.connection.send(raw)

    # Send the original response bytes after the independent test barrier releases the physical proof.
    async def reply_after_release(self, raw):
        await self.bench.release.wait()
        await self.connection.send(raw)


class HeldZeroBench(managed_helpers.OwnerBench):
    """Run the original device behavior behind a single response barrier."""

    # Arm the barrier only after normal commissioning and timer cleanup have completed.
    def __init__(self):
        super().__init__()
        self.hold_zero = False
        self.entered, self.release = asyncio.Event(), asyncio.Event()

    # Own the delayed response until the actual device reader retires, including assertion failures.
    async def read_charger(self, charger):
        peer = CompositeReplyPeer(charger, self)
        try:
            await super().read_charger(peer)
        finally:
            self.release.set()
            if peer.delayed is not None:
                await asyncio.gather(peer.delayed, return_exceptions=True)


# Intent: an unconfirmed real zero yields a retryable wire error without allocating the delayed lifecycle.
# Why: physical zero alone is insufficient; a later identical report must succeed only after actual verification.
async def test_delayed_report_retries_after_zero_join_timeout(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, bench_type=HeldZeroBench) as b:
        await select_current(b, 32)
        saved = originals(b)
        owner_tag(b)
        b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
        await b.c.charging.settle()
        tag = starts(b.rig)[0][3]["idTag"]
        reported = b.clock.now
        g, peer = b.session.gateway, b.rig.bench
        # Give the actual RPC longer than the report's one-second join, so missing proof remains genuinely pending.
        g.charger.timeout = 3
        peer.hold_zero = True
        b.c.command(uuid.uuid4().hex, "pause", b.c.requests.intent().revision)
        try:
            await asyncio.wait_for(peer.entered.wait(), 2)
            assert b.device.effective_limit() == 0 and not g.controls.ready
            before = len(peer.device_calls)
            token = uuid.uuid4().hex
            payload = START | {
                "idTag": tag,
                "timestamp": datetime.fromtimestamp(reported, UTC).isoformat(),
            }
            refused = await peer.call(b.sock, token, "StartTransaction", payload)
            assert refused[0] == 4 and refused[2] == "InternalError"
            assert b.session.journal.active() is None and b.device.active is None
            assert not g.controls.ready and not b.stops() and b.device.effective_limit() == 0
            assert peer.device_calls[before:] == []
            peer.release.set()
            await until(lambda: g.controls.ready and g.controls.desired() == 0)
            accepted = await peer.call(b.sock, token, "StartTransaction", payload)
            assert accepted[0] == 3 and accepted[2]["idTagInfo"]["status"] == "Accepted"
            local = accepted[2]["transactionId"]
            assert b.session.journal.active() == local and b.device.effective_limit() == 0 and not b.stops()
            b.rig.peer.plan_session = (local, 9)
            await settle(b)
            assert local in b.stops()
            await close_in_order(b, local, "native", (local,))
            await healthy(b.c)
            b.c.command(uuid.uuid4().hex, "app_control", b.c.requests.intent().revision)
            await healthy(b.c)
            await next_daily(b, 32, saved, (local,), starts_count=1)
        finally:
            peer.release.set()
