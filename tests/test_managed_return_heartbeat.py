"""A routine in-flight cloud read defers an otherwise verified Return before positive release."""

import asyncio
import json
import uuid

import pytest

from tests.current_rig import adjustable
from tests.managed_rig import healthy, starts
from tests.runtime_rig import until
from tests.test_managed_current_tick_race import OneNativeResponseGate
from tests.tls_support import tls as tls
from tests.trial_peer import Bench


class HeartbeatPeer:
    """The independent backend withholds only one real Heartbeat acknowledgement after normal allocation/setup."""

    # Retain real framing and all ordinary fake-cloud behavior while owning this single transport delay.
    def __init__(self, connection, armed, seen, release):
        self.connection, self.armed, self.seen, self.release = connection, armed, seen, release

    # Forward unmodified correlated responses from the independent backend implementation.
    async def send(self, raw):
        await self.connection.send(raw)

    # Gate only an actually received Heartbeat, independently of the engine's journal or pending-call state.
    def __aiter__(self):
        # Blocking this peer response leaves charger TLS and native HTTP fully functional.
        async def frames():
            async for raw in self.connection:
                frame = json.loads(raw)
                if self.armed.is_set() and frame[0] == 2 and frame[2] == "Heartbeat":
                    self.armed.clear()
                    self.seen.set()
                    await self.release.wait()
                yield raw

        return frames()


# Intent: routine cloud observations before or during native validation retain the same accepted Return.
# Why: a later Pause may still supersede it, and no positive offer can precede verified delivery and zero.
@pytest.mark.parametrize("case", ["fast", "heartbeat", "observation-before", "observation-during", "new_pause"])
async def test_return_waits_for_cloud_rpc_before_positive_dispatch(tmp_path, tls, unused_tcp_port, monkeypatch, case):
    armed, seen, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    actual = Bench.vendor

    # Preserve the backend implementation and delay only its independently received periodic read.
    async def cloud(bench, connection):
        await actual(bench, HeartbeatPeer(connection, armed, seen, release))

    monkeypatch.setattr(Bench, "vendor", cloud)
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch, native_peer=OneNativeResponseGate) as b:
        c, peer, g = b.controller, b.rig.peer, b.session.gateway
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        before = len(b.rig.bench.received_limits)
        request = uuid.uuid4().hex

        # Use a real charger observation while the vendor transport is busy with a Heartbeat.
        async def queue_observation():
            reply = await b.rig.bench.call(
                b.charger,
                "genuine-queued-observation",
                "StatusNotification",
                {"connectorId": 1, "status": "Available", "errorCode": "NoError"},
            )
            assert reply[0] == 3 and b.session.journal.next_delivery() is not None

        if case == "observation-before":
            armed.set()
            await asyncio.wait_for(seen.wait(), 2)
            await queue_observation()
        peer.arm = True
        c.command(request, "app_control", c.requests.intent().revision)
        tick = asyncio.create_task(c.tick())
        try:
            await asyncio.wait_for(peer.entered.wait(), 2)
            if case not in ("fast", "observation-before"):
                armed.set()
                await asyncio.wait_for(seen.wait(), 2)
            if case == "observation-during":
                await queue_observation()
            peer.release.set()
            await tick
            expected = "completed" if case == "fast" else "accepted"
            assert c.requests.receipt(request)["state"] == expected
            if case != "fast":
                assert b.rig.bench.firmware.effective_limit() == 0
                assert all(value == 0 for value in b.rig.bench.received_limits[before:])
            if case == "new_pause":
                c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
                await c.pause_task
                assert c.requests.receipt(request)["state"] == "failed"
            release.set()
            await until(lambda: not g.vendor._pending and b.session.journal.next_delivery() is None)
            if case == "new_pause":
                assert (await healthy(c))["local_hold_verified"] and b.rig.bench.firmware.effective_limit() == 0
            else:
                async with asyncio.timeout(5):
                    while c.requests.receipt(request)["state"] in ("accepted", "running"):
                        await c.tick()
                        await asyncio.sleep(0.01)
                assert c.requests.receipt(request)["state"] == "completed"
                assert b.rig.bench.firmware.effective_limit() == 6
                assert [value for value in b.rig.bench.received_limits[before:] if value > 0] == [6]
            assert not starts(b.rig) and not b.session.journal.get("managed_control_fault")
        finally:
            peer.release.set()
            release.set()
            await asyncio.gather(tick, return_exceptions=True)
