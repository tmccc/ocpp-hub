"""Actual native HTTP refusals cannot be lost in restrictive callers; selected overdraw still permits genuine Stop."""

import asyncio
import copy
import json
import uuid

import pytest
from ocpp_hub_core.prototype.managed_native_repair import REVOCATIONS, repair_revoked
from ocpp_hub_core.prototype.managed_release_recovery import ACK, recoverable_fault

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench, two_later_sessions
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_release_recovery import explicit_release_pause, release_bench
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_test_charge import positive_flow
from tests.test_ordinary_legacy_transport_recovery import legacy_fault
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


class RepairWirePeer(InterruptedNativePeer):
    """Only the independent native HTTP endpoint changes its actual wire response."""

    # Keep ordinary device facts and count malformed requests independently of the engine.
    def __init__(self, bench):
        super().__init__(bench)
        self.raw_response = None
        self.bad_reads = 0

    # A complete real request reaches the peer before any selected malformed HTTP response is sent.
    async def handle(self, reader, writer):
        if self.raw_response is None:
            await super().handle(reader, writer)
            return
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            fields = {
                line.split(b":", 1)[0].lower(): line.split(b":", 1)[1].strip()
                for line in header.split(b"\r\n")[1:]
                if b":" in line
            }
            json.loads(await reader.readexactly(int(fields[b"content-length"])))
            self.bad_reads += 1
            writer.write(self.raw_response)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()


# Build complete and deliberately distinct wire shapes; none replaces the engine's decoded return values.
def wire_response(mode):
    bodies = {
        "duplicate": b'{"code":200,"code":200,"data":{}}',
        "nonfinite": b'{"code":200,"data":{"value":NaN}}',
        "json-unavailable": b"{",
        "envelope-unavailable": b'{"code":404,"data":null}',
    }
    if mode in bodies:
        body = bodies[mode]
        return b"HTTP/1.0 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body
    if mode == "framing":
        return b"HTTP/1.0 200 OK\r\nContent-Length: 0\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"
    if mode == "http-version":
        return b"HTTP/9.0 200 OK\r\nContent-Length: 0\r\n\r\n"
    if mode == "unauthorized":
        return b"HTTP/1.0 401 Unauthorized\r\nContent-Length: 0\r\n\r\n"
    return b"HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n"


# Intent: strict wire contradictions revoke the exact fault even when the earlier Stop caller swallows its error.
# Why: restoring valid replies cannot let an availability-only Pause acknowledge actually observed strict content.
@pytest.mark.parametrize("caller", ["stop", "tick"])
@pytest.mark.parametrize("mode", ["duplicate", "nonfinite", "framing", "http-version", "unauthorized"])
async def test_strict_native_wire_refusal_revokes_repair(tmp_path, tls, unused_tcp_port, monkeypatch, caller, mode):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", RepairWirePeer)
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j, peer = b.c, b.session.journal, b.rig.peer
        fault = await legacy_fault(b, "NativeTransportUnavailableError")
        pause = await explicit_release_pause(b)
        diagnostic = copy.deepcopy(j.get("managed_native_failure"))
        peer.raw_response = wire_response(mode)
        if caller == "stop":
            await c._stop_requested()
        else:
            assert (await c.tick())["state"] == "attention"
        assert peer.bad_reads >= 1 and repair_revoked(j, fault)
        peer.raw_response = None
        for _ in range(3):
            assert (await c.tick())["state"] == "attention"
        assert j.get("managed_control_fault") == fault and j.get("managed_native_failure") == diagnostic
        assert j.get(ACK) is None and not recoverable_fault(c, fault)
        assert c.requests.receipt(pause)["state"] != "completed"
        assert j.get(REVOCATIONS) and b.rig.bench.firmware.effective_limit() == 0 and not mp.starts(b.rig)


# Intent: incomplete JSON, unavailable envelopes and actual503 still preserve normal same-Pause recovery.
# Why: hard wire contradictions must not broaden to ordinary native unavailability.
@pytest.mark.parametrize("mode", ["json-unavailable", "envelope-unavailable", "http503"])
async def test_native_unavailable_wire_keeps_repair(tmp_path, tls, unused_tcp_port, monkeypatch, mode):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", RepairWirePeer)
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j, peer = b.c, b.session.journal, b.rig.peer
        fault = await legacy_fault(b, "NativeTransportUnavailableError")
        pause = await explicit_release_pause(b)
        ack = copy.deepcopy(j.get(ACK))
        peer.raw_response = wire_response(mode)
        assert (await c.tick())["state"] == "attention"
        assert peer.bad_reads >= 1 and j.get(ACK) == ack and recoverable_fault(c, fault)
        assert not repair_revoked(j, fault) and b.rig.bench.firmware.effective_limit() == 0
        peer.raw_response = None
        assert (await mp.healthy(c))["local_hold_verified"]
        assert c.requests.receipt(pause)["state"] == "completed" and j.get("managed_control_fault") is None


# Intent: valid residual current above selected6 but below absolute32 must not prevent a real known-session Stop.
# Why: selected measurement gates health/positive work, while owner Pause still needs genuine Stop and closure.
async def test_selected_overdraw_cannot_starve_owner_stop(tmp_path, tls, unused_tcp_port, monkeypatch):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, 6, cli_route=True) as b:
        c, j, peer, firmware = b.controller, b.session.journal, b.rig.peer, b.rig.bench.firmware
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        await mp.healthy(c)
        actual = peer.native

        # The real native endpoint alone reports residual draw after the actual station profile reaches zero.
        def residual_native(key):
            value = actual(key)
            if key == "METER" and firmware.active == local:
                value[0]["meterData"].update(cur=[80, 0, 0], power=[1920, 0, 0, 1920])
            return value

        monkeypatch.setattr(peer, "native", residual_native)
        pause = uuid.uuid4().hex
        c.command(pause, "pause", c.requests.intent().revision)
        await asyncio.wait_for(b.rig.bench.remote_stop.wait(), 5)
        stops = [frame for frame in b.rig.bench.device_calls if frame[2] == "RemoteStopTransaction"]
        assert len(stops) == 1 and stops[0][3] == {"transactionId": local}
        assert firmware.effective_limit() == 0
        await b.rig.bench.call(
            b.charger,
            uuid.uuid4().hex,
            "StopTransaction",
            {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
        )
        peer.permission_enabled = False
        monkeypatch.setattr(peer, "native", actual)
        await until(lambda: j.active() is None and j.next_delivery() is None)
        if c.pause_task is not None:
            await c.pause_task
        assert (await mp.healthy(c))["local_hold_verified"]
        assert c.requests.receipt(pause)["state"] == "completed"
        c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
        assert (await mp.healthy(c))["station_ceiling_a"] == 6
        await two_later_sessions(b, selected=6, original_starts=0, first_minute=20)
        assert j.get("managed_control_fault") is None and j.next_delivery() is None
