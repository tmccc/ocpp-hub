"""Public controller acceptance during real peer I/O preserves current application and later wire controls."""

import asyncio
import json
import uuid

from ocpp_hub_core import commands
from ocpp_hub_core.frames import build_call
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings
from websockets.asyncio.client import connect

import tests.test_managed_proxy as managed_helpers
from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import NativeVehiclePeer
from tests.test_managed_api import TOKEN, call
from tests.test_managed_current import adjustable
from tests.test_managed_proxy import healthy
from tests.test_prototype_tls import tls as tls


class OneNativeResponseGate(NativeVehiclePeer):
    """The native peer delays one actual HTTP response while every engine method runs unmodified."""

    # The test arms a one-shot response gate only after unrelated native preparation has completed.
    def __init__(self, bench):
        super().__init__(bench)
        self.arm = False
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.gated_request = None
        self.gated_client = None

    # Buffer and delay one complete actual request, then run the original independent peer's normal parser.
    async def handle(self, reader, writer):
        if self.arm:
            self.arm = False
            header = await reader.readuntil(b"\r\n\r\n")
            fields = {
                line.split(b":", 1)[0].lower(): line.split(b":", 1)[1].strip()
                for line in header.split(b"\r\n")[1:]
                if b":" in line
            }
            body = await reader.readexactly(int(fields[b"content-length"]))
            self.gated_request = json.loads(body)
            self.gated_client = writer.get_extra_info("peername")
            self.entered.set()
            await self.release.wait()

            class Buffered:
                # Return the exact already received header to the original fake peer's parser.
                async def readuntil(self, separator):
                    return header

                # Return the exact already received body without modifying its action, credentials or values.
                async def readexactly(self, count):
                    assert count == len(body)
                    return body

                # Delegate any unused reader methods to the original actual socket stream.
                def __getattr__(self, name):
                    return getattr(reader, name)

            reader = Buffered()
        await super().handle(reader, writer)


# Inspect the real coroutine wait chain without replacing or yielding any production engine method.
def chain(task):
    result = []
    item = task.get_coro()
    for _ in range(30):
        if item is None:
            break
        code = getattr(item, "cr_code", None) or getattr(item, "gi_code", None)
        result.append(code.co_name if code else type(item).__name__)
        item = getattr(item, "cr_await", None) or getattr(item, "gi_yieldfrom", None)
    return result


# Inspect one actual coroutine's locals without replacing or yielding any production method.
def waiting_locals(task, function):
    item = task.get_coro()
    for _ in range(30):
        if item is None:
            break
        code = getattr(item, "cr_code", None) or getattr(item, "gi_code", None)
        if code is not None and code.co_name == function:
            frame = getattr(item, "cr_frame", None) or getattr(item, "gi_frame", None)
            return frame.f_locals if frame is not None else {}
        item = getattr(item, "cr_await", None) or getattr(item, "gi_yieldfrom", None)
    return {}


class CorrelatedCloudBench(managed_helpers.OwnerBench):
    # The independent vendor correlates actual TLS replies without consuming the tick's 20 ms wait by polling.
    def __init__(self):
        super().__init__()
        self.pending = {}

    # Observe real response frames while retaining the fake cloud's original independent behavior.
    async def vendor(self, connection):
        pending = self.pending

        class Observed:
            # Delegate the real websocket methods unchanged.
            def __getattr__(self, name):
                return getattr(connection, name)

            # Iterate over the real received frames.
            def __aiter__(self):
                return self.frames()

            # Resolve only this independent peer's request futures before ordinary frame processing continues.
            async def frames(self):
                async for raw in connection:
                    frame = decode(raw)
                    future = pending.pop(frame[1], None) if frame[0] in (3, 4) else None
                    if future is not None and not future.done():
                        future.set_result(frame)
                    yield raw

        await super().vendor(Observed())

    # Send one actual vendor call and await its real socket reply.
    async def actual_call(self, token, action, payload):
        future = asyncio.get_running_loop().create_future()
        self.pending[token] = future
        await self.cloud.send(build_call(token, action, payload))
        return await asyncio.wait_for(future, 2)


# Intent: public current acceptance during tick's real resource wait preserves its worker and later wire Pause/Return.
# Why: management dispatch calls this controller boundary synchronously; the legal ordering needs no wall-time margin.
async def test_controller_current_acceptance_during_tick_resource_wait_completes(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", OneNativeResponseGate)
    monkeypatch.setattr(managed_helpers, "OwnerBench", CorrelatedCloudBench)
    async with adjustable(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        c, j, peer = b.controller, b.session.journal, b.rig.peer
        before = await healthy(c)
        assert before["station_ceiling_a"] == 6 and c.current.selected() == 6
        port = unused_tcp_port_factory()
        published = {**before, "running": True, "certificate_remaining_seconds": 86400}
        api = ManagementServer(ManagementSettings(port, tmp_path / "unused-secret", TOKEN), c, lambda: published)
        await api.start()
        entered, release = asyncio.Event(), asyncio.Event()
        tick = vendor = None
        ordinary_send = b.session.gateway.vendor.sender
        accepted = []
        identifier = uuid.uuid4().hex
        token = "settling-resource"

        # Observe the real reply's transport boundary after durable RPC completion, before the next loop turn.
        # Management dispatch invokes the same synchronous public command; no engine wait or decision is replaced.
        async def accept_on_wire_reply(raw):
            frame = decode(raw)
            if frame[0] == 3 and frame[1] == token:
                waiting = chain(tick)
                assert "settled_native" in waiting and "sleep" in waiting, waiting
                assert not b.session._resource_mutation_inflight() and c.current.available(c.latest_native)
                peer.arm = True
                accepted.append(c.command(identifier, "set_current", c.requests.intent().revision, current_a=16))
                assert accepted[-1]["state"] == "accepted"
            await ordinary_send(raw)

        assert ordinary_send is not None
        monkeypatch.setattr(b.session.gateway.vendor, "sender", accept_on_wire_reply)
        try:
            async with connect(
                f"ws://127.0.0.1:{port}/management",
                additional_headers={"Authorization": "Bearer " + TOKEN},
                subprotocols=["ics-proxy.v1"],
            ) as management:
                # The device applies a real vendor profile and delays only its actual TLS acknowledgement.
                async def hold_ack(action, payload):
                    if action == "SetChargingProfile" and payload["csChargingProfiles"]["chargingProfileId"] != 7001:
                        entered.set()
                        await release.wait()

                b.rig.bench.before_reply = hold_ack
                vendor = asyncio.create_task(
                    b.rig.bench.actual_call(
                        token,
                        "SetChargingProfile",
                        commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 32, 1234, stack_level=0),
                    )
                )
                await asyncio.wait_for(entered.wait(), 2)
                assert b.session._resource_mutation_inflight()
                tick = asyncio.create_task(c.tick())
                await asyncio.sleep(0)
                waiting = chain(tick)
                assert "settled_native" in waiting and "sleep" in waiting, waiting
                release.set()
                assert (await vendor)[0] == 3 and len(accepted) == 1
                await asyncio.wait_for(peer.entered.wait(), 2)
                assert peer.gated_request["action"] == "getConfig"
                worker_stream = waiting_locals(c.current.worker, "read_native").get("writer")
                assert worker_stream is not None and worker_stream.get_extra_info("sockname") == peer.gated_client
                status = await asyncio.wait_for(tick, 2)
                assert status["state"] == "reconciling" and not j.get("managed_control_fault")
                assert j.get("managed_current_change")["state"] == "pending"
                assert not any(
                    f[2] == "SetChargingProfile"
                    and f[3]["csChargingProfiles"]["chargingProfileId"] == 7001
                    and f[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 16
                    for f in b.rig.bench.device_calls
                )
                peer.release.set()
                await asyncio.wait_for(c.current.settle(), 5)
                published = {**await healthy(c), "running": True, "certificate_remaining_seconds": 86400}
                assert c.requests.receipt(identifier)["state"] == "completed"
                assert published["owner_control_enabled"] and published["station_ceiling_a"] == 16
                assert j.get("control")["resume_limit"] == 16 and not j.get("managed_control_fault")
                assert b.rig.bench.firmware.effective_limit() == 16
                assert (await call(management, "pause", revision=c.requests.intent().revision))["ok"]
                published = {**await healthy(c), "running": True, "certificate_remaining_seconds": 86400}
                assert published["station_ceiling_a"] == 0
                assert (await call(management, "app_control", revision=c.requests.intent().revision))["ok"]
                published = {**await healthy(c), "running": True, "certificate_remaining_seconds": 86400}
                assert published["owner_control_enabled"] and b.rig.bench.firmware.effective_limit() == 16
        finally:
            release.set()
            peer.release.set()
            b.session.gateway.vendor.sender = ordinary_send
            b.rig.bench.before_reply = None
            if tick is not None:
                await asyncio.gather(tick, return_exceptions=True)
            if vendor is not None:
                await asyncio.gather(vendor, return_exceptions=True)
            await api.close()


# Intent: unrelated owner uncertainty still latches when no accepted current worker owns its transition.
# Why: avoiding a false pending-current fault must not turn actual unexplained local authority into shared permission.
async def test_owner_uncertainty_without_pending_current_still_latches(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j, g = b.controller, b.session.journal, b.session.gateway
        await healthy(c)
        # The real control boundary persists an unfinished local claim without a current-setting operation.
        g.controls._claim(False)
        assert not c.current.pending()
        await c.tick()
        assert j.get("managed_control_fault")["classification"] == "owner_control_or_delivery_uncertain"
        assert (await c.tick())["state"] == "attention"
        assert b.rig.bench.firmware.effective_limit() == 0
