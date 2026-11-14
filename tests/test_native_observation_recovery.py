"""Repeated undecodable native observations recover without weakening validated authority or HTTP framing."""

import asyncio
import copy
import json
import uuid

import pytest
from ocpp_hub_core.prototype.native_http import NativeTransportUnavailableError, read_native
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench, two_later_sessions
from tests.recovery_barriers import completed_control, returned_to_app
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_prototype_tls import tls as tls


class UndecodablePeer(InterruptedNativePeer):
    """Only this independent HTTP peer chooses a response document; all valid observations remain unchanged."""

    # Arm complete failure responses only after real commissioning and count actual parsed requests.
    def __init__(self, bench):
        super().__init__(bench)
        self.failure = None
        self.corrupt_requests = []

    # Consume each actual request, then emit the chosen complete bounded HTTP response or ordinary native data.
    async def handle(self, reader, writer):
        if self.failure is None:
            await super().handle(reader, writer)
            return
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            length = next(
                int(line.split(b":", 1)[1])
                for line in header.split(b"\r\n")
                if line.lower().startswith(b"content-length:")
            )
            self.corrupt_requests.append(json.loads(await reader.readexactly(length)))
            bodies = {
                "bad-json": b"{",
                "envelope": b'{"code":500,"message":"busy","data":null}',
                "missing-data": b'{"code":200}',
                "encoded-data-json": b'{"code":200,"data":"{"}',
            }
            body = bodies[self.failure]
            writer.write(b"HTTP/1.0 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()


# Complete an explicit new Pause and its own Return without changing the selected cap or fabricating a Start.
async def pause_return(b):
    c = b.controller
    identifier, revision = uuid.uuid4().hex, c.requests.intent().revision
    accepted = c.command(identifier, "pause", revision)
    assert accepted["id"] == identifier
    await completed_control(b, identifier, mode="paused", revision=revision + 1, ceiling=0)
    await returned_to_app(b, c.current.selected())


# Intent: two malformed three-poll outages at every supported cap recover and leave later sessions usable.
# Why: one-shot invalid documents miss the security-latch threshold and zero alone does not prove eventual recovery.
@pytest.mark.parametrize("selected", [6, 16, 32])
@pytest.mark.parametrize("failure", ["bad-json", "envelope", "missing-data", "encoded-data-json"])
async def test_repeated_undecodable_native_observation_recovers(
    tmp_path, tls, unused_tcp_port, monkeypatch, selected, failure
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", UndecodablePeer)
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, selected, cli_route=True) as b:
        c, peer, j, firmware = b.controller, b.rig.peer, b.session.journal, b.rig.bench.firmware
        intent = c.requests.intent()
        epoch, boot = b.session.gateway._charger_epoch, b.session.gateway.boot_observation
        original_plans, original_config = copy.deepcopy(firmware.plans), copy.deepcopy(peer.configuration)
        for _ in range(2):
            before = len(peer.corrupt_requests)
            peer.failure = failure
            try:
                for _ in range(4):
                    status = await c.tick()
                    assert status["state"] == "degraded" and not status["owner_control_enabled"]
                    assert j.get("managed_control_fault") is None
                assert len(peer.corrupt_requests) - before >= 3
                assert firmware.effective_limit() == 0
                assert c.requests.intent() == intent
            finally:
                peer.failure = None
            status = await mp.healthy(c)
            assert status["station_ceiling_a"] == firmware.effective_limit() == selected
            assert c.requests.intent() == intent and j.active() is None and not mp.starts(b.rig)
            assert b.session.gateway._charger_epoch == epoch and b.session.gateway.boot_observation == boot
            assert j.get("managed_control_fault") is None
        await pause_return(b)
        await two_later_sessions(b, selected=selected, original_starts=0, first_minute=10)
        assert firmware.plans == original_plans and peer.configuration == original_config
        assert all(entry["closed"] for entry in b.rig.bench.backend_starts)


# Intent: a complete changed commissioned configuration remains a security fault after three reads and owner Pause.
# Why: unusable envelopes cannot turn validated contradictory installation data into an ordinary retry.
@pytest.mark.parametrize("selected", [6, 32])
async def test_valid_conflicting_native_content_is_still_latched(tmp_path, tls, unused_tcp_port, monkeypatch, selected):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", UndecodablePeer)
    async with commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, selected, cli_route=True) as b:
        c, peer, j = b.controller, b.rig.peer, b.session.journal
        original = peer.configuration["untouched"]
        peer.configuration["untouched"] = "independent-external-configuration-change"
        for _ in range(3):
            await c.tick()
        fault = copy.deepcopy(j.get("managed_control_fault"))
        assert fault["classification"] == "native_authority_or_preservation_failed"
        peer.configuration["untouched"] = original
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        if c.pause_task is not None:
            await asyncio.gather(c.pause_task, return_exceptions=True)
        for _ in range(4):
            assert (await c.tick())["state"] == "attention"
        assert j.get("managed_control_fault") == fault and b.rig.bench.firmware.effective_limit() == 0
        with pytest.raises(RecoveryRequiredError):
            c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
        assert not mp.starts(b.rig)


# Encode complete HTTP frames independently of the implementation's native reader.
def reply(body, *, headers=None, status=b"HTTP/1.0 200 OK"):
    return status + b"\r\n" + (headers or b"Content-Length: " + str(len(body)).encode() + b"\r\n") + b"\r\n" + body


WIRE_CASES = (
    ("invalid-json", reply(b"{"), NativeTransportUnavailableError),
    ("invalid-utf8", reply(b"\xff"), NativeTransportUnavailableError),
    ("empty-body", reply(b""), NativeTransportUnavailableError),
    ("non-object-envelope", reply(b"[]"), NativeTransportUnavailableError),
    ("busy-envelope", reply(b'{"code":500,"data":null}'), NativeTransportUnavailableError),
    ("wrong-code-type", reply(b'{"code":"200","data":{}}'), NativeTransportUnavailableError),
    ("missing-data", reply(b'{"code":200}'), NativeTransportUnavailableError),
    ("encoded-bad-json", reply(b'{"code":200,"data":"{"}'), NativeTransportUnavailableError),
    ("http503-control", b"HTTP/1.0 503 Unavailable\r\nContent-Length: 0\r\n\r\n", NativeTransportUnavailableError),
    ("duplicate-key", reply(b'{"code":200,"code":200,"data":{}}'), SecurityError),
    ("duplicate-data-key", reply(b'{"code":200,"data":{"id":1,"id":2}}'), SecurityError),
    ("nonfinite-data", reply(b'{"code":200,"data":{"value":NaN}}'), SecurityError),
    ("unauthorized", reply(b"{}", status=b"HTTP/1.0 401 Denied"), SecurityError),
    ("invalid-status-version", reply(b'{"code":200,"data":{}}', status=b"HTTP/9.9 200 OK"), SecurityError),
    ("duplicate-content-length", reply(b"{}", headers=b"Content-Length: 2\r\nContent-Length: 2\r\n"), SecurityError),
    ("ambiguous-framing", reply(b"{}", headers=b"Content-Length: 2\r\nTransfer-Encoding: chunked\r\n"), SecurityError),
)


# Intent: framing, duplicate keys and nonfinite content retain their exact security error classification.
# Why: the availability class subclasses SecurityError, so broad raises(SecurityError) checks are insufficient.
@pytest.mark.parametrize("name,raw,expected", [pytest.param(*row, id=row[0]) for row in WIRE_CASES])
async def test_native_response_failure_class_is_exact(name, raw, expected):
    # Serve only received literal-loopback requests and the independent wire case selected above.
    async def peer(reader, writer):
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            length = next(
                int(line.split(b":", 1)[1])
                for line in header.split(b"\r\n")
                if line.lower().startswith(b"content-length:")
            )
            await reader.readexactly(length)
            writer.write(raw)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(peer, "127.0.0.1", 0)
    try:
        with pytest.raises(SecurityError) as error:
            await read_native("127.0.0.1", server.sockets[0].getsockname()[1], "Basic synthetic", "getConfig", None)
        assert type(error.value) is expected
    finally:
        server.close()
        await server.wait_closed()
