"""Assessment-only peer injection keeps the actual RPC and native validators unchanged."""

import asyncio
import json
import uuid

import pytest

from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import NativeVehiclePeer
from tests.test_managed_normal_start import owner_rig
from tests.test_managed_proxy import starts
from tests.test_managed_test_charge import actual_stop, positive_flow
from tests.test_prototype_tls import tls as tls


class OneShot503Peer(NativeVehiclePeer):
    """An independent native HTTP server faults once after the real Start receipt is prepared."""

    # Keep normal peer behavior until one explicitly armed HTTP response boundary.
    # Keep this synthetic peer or lifecycle helper independent of the engine under test.
    def __init__(self, bench):
        super().__init__(bench)
        self.armed = False
        self.journal = None
        self.failures = []

    # Consume the real HTTP request and fail only while the actual journal records a prepared, unsent Start.
    # Keep this synthetic peer or lifecycle helper independent of the engine under test.
    async def handle(self, reader, writer):
        receipt = self.journal.get("local_start_receipt", {}) if self.journal is not None else {}
        if not self.armed or receipt.get("stage") != "prepared":
            await super().handle(reader, writer)
            return
        self.armed = False
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            fields = {
                line.split(b":", 1)[0].lower(): line.split(b":", 1)[1].strip()
                for line in headers.split(b"\r\n")[1:]
                if b":" in line
            }
            request = json.loads(await reader.readexactly(int(fields[b"content-length"])))
            self.failures.append({"request": request, "stage": receipt["stage"]})
            writer.write(b"HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()


# Intent: a genuine fast HTTP503 before dispatch must retire an unsent Start and let a new Pause complete.
# Why: errors injected at validate_send alone do not establish reachability through the real native boundary.
# Intent: preserve a valid lifecycle across this independently injected failure and its control.
# Why: a refused or unsent Start must not strand later supervision, Pause or owner readiness.
@pytest.mark.parametrize("fail", [False, True])
async def test_real_native503_before_remote_start(tmp_path, tls, unused_tcp_port, monkeypatch, fail):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", OneShot503Peer)
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        c, j, g, native = test.controller, test.session.journal, test.session.gateway, test.rig.peer
        assert g.charger.validate_send.__self__ is c.native
        assert g.charger.validate_send.__func__ is type(c.native).validate_send
        native.journal, native.armed = j, fail
        c.command(uuid.uuid4().hex, "start", c.requests.intent().revision)
        await c.tests.settle()
        if fail:
            assert len(native.failures) == 1 and native.failures[0]["stage"] == "prepared"
            assert native.failures[0]["request"]["action"] == "getConfig"
            assert not starts(test.rig)
            assert j.get("managed_test_failure")["type"] == "NativeTransportUnavailableError"
        else:
            assert native.failures == [] and len(starts(test.rig)) == 1
            local = await positive_flow(test)
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            await actual_stop(test, local)
        pause = uuid.uuid4().hex
        c.command(pause, "pause", c.requests.intent().revision)
        for _ in range(30):
            status = await c.tick()
            if c.requests.receipt(pause)["state"] == "completed":
                break
            await asyncio.sleep(0.02)
        summary = {
            "real_http503": fail,
            "native_failures": len(native.failures),
            "start_pending": j.get("control", {}).get("start_pending"),
            "receipt_stage": j.get("local_start_receipt", {}).get("stage"),
            "pause_receipt": c.requests.receipt(pause)["state"],
            "state": status["state"],
            "local_hold_verified": status.get("local_hold_verified"),
            "station_limit_a": test.rig.bench.firmware.effective_limit(),
            "remote_starts": len(starts(test.rig)),
        }
        print(json.dumps(summary, sort_keys=True))
        assert summary["pause_receipt"] == "completed" and summary["local_hold_verified"]
