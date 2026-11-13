"""A failed positive ceiling before RemoteStart must not become an unresolved issued Start."""

import json
import uuid
from contextlib import AsyncExitStack

import pytest
from ocpp_hub_core.frames import build_call_error
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.native_http import NativeTransportUnavailableError
from ocpp_hub_core.prototype.rpc import UnavailableError

from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import crash
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_proxy import healthy, introduced, managed_session, starts
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_test_charge import ChargeRig, positive_flow
from tests.test_ordinary_recovery import close_session
from tests_model.test_schedule_sequences import listener_port, tls_for_case
from tests_model.test_schedule_sequences import model_tls as model_tls


class BeforeCapFailurePeer(InterruptedNativePeer):
    """The real native HTTP peer can fail once after Start admission and before its first positive write."""

    # Keep fault timing separate from the real native validator and transport dispatch implementation.
    def __init__(self, bench):
        super().__init__(bench)
        self.start_journal = None
        self.start_failure = None
        self.failures = []
        self.hold_after_failure = False
        self.start_boundary = "positive"

    # Consume one actual native request and return a genuine transport or payload failure.
    async def handle(self, reader, writer):
        if self.start_failure is None:
            await super().handle(reader, writer)
            return
        state = self.start_journal.get("managed_test_charge", {}) if self.start_journal is not None else {}
        if state.get("start_attempted") is not True:
            await super().handle(reader, writer)
            return
        if self.start_boundary == "remote" and self.bench.firmware.effective_limit() == 0:
            await super().handle(reader, writer)
            return
        failure, self.start_failure = self.start_failure, None
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            fields = {
                line.split(b":", 1)[0].lower(): line.split(b":", 1)[1].strip()
                for line in headers.split(b"\r\n")[1:]
                if b":" in line
            }
            body = json.loads(await reader.readexactly(int(fields[b"content-length"])))
            self.failures.append({"kind": failure, "action": body["action"]})
            if failure == "503":
                writer.write(b"HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
            elif failure == "bad-json":
                writer.write(b"HTTP/1.0 200 OK\r\nContent-Length: 1\r\n\r\n{")
            elif failure != "eof":
                raise AssertionError("unknown independent native failure")
            await writer.drain()
        finally:
            if self.hold_after_failure:
                self.unavailable = True
            writer.close()
            await writer.wait_closed()


# Intent: every positive-cap failure stays safely recoverable without inventing a later Start obligation.
# Why: entering LocalControl is earlier than issuing RemoteStart, including across a subsequent process replacement.
@pytest.mark.parametrize("current", [6, 32])
@pytest.mark.parametrize("failure", ["rejected", "call-error", "503", "eof", "bad-json"])
@pytest.mark.parametrize("replace", [False, True])
async def test_positive_cap_failure_recovers_and_supervises_next_daily(
    tmp_path, model_tls, monkeypatch, current, failure, replace
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", BeforeCapFailurePeer)
    async with commissioned(tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch, current) as b:
        c, j = b.controller, b.session.journal
        actual, send, failed = b.rig.bench.firmware.answer, b.charger.send, []
        b.rig.peer.start_journal = j
        if failure in ("503", "eof", "bad-json"):
            b.rig.peer.start_failure = failure

        # Refuse the first positive physical profile only; every later cleanup profile keeps real peer behavior.
        def answer(name, payload):
            if failure == "rejected" and not failed and name == "SetChargingProfile":
                limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit > 0:
                    failed.append(name)
                    return {"status": "Rejected"}
            return actual(name, payload)

        # A CallError leaves the independently applied profile uncertain until the real cleanup zero succeeds.
        async def reply(raw):
            frame = decode(raw)
            call = next((x for x in b.rig.bench.device_calls if x[1] == frame[1]), None)
            if failure == "call-error" and not failed and frame[0] == 3 and call and call[2] == "SetChargingProfile":
                limit = call[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit > 0:
                    failed.append(call[2])
                    raw = build_call_error(frame[1], "InternalError", "synthetic positive profile refusal")
            await send(raw)

        monkeypatch.setattr(b.rig.bench.firmware, "answer", answer)
        monkeypatch.setattr(b.charger, "send", reply)
        c.command(uuid.uuid4().hex, "start", c.requests.intent().revision)
        await c.tests.settle()
        assert failed or b.rig.peer.failures
        assert not starts(b.rig)
        if c.pause_task is not None:
            await c.pause_task
        s, sock = b.session, b.charger
        if replace:
            await c.tests.close()
            await crash(s)
            s = managed_session(b.rig)
            await s.start()
            c = ManagedController(s)
        async with AsyncExitStack() as stack:
            if replace:
                sock = await stack.enter_async_context(b.rig.charger(s))
                await introduced(b.rig, s, sock, "positive-cap-failure-replacement")
            try:
                c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
                await healthy(c)
                c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
                status = await healthy(c)
                assert status["normal_start_ready"]
                assert c.tests.state()["state"] == "completed"
                fresh = ChargeRig(b.rig, s, c, sock)
                local = await positive_flow(fresh, "daily")
                await healthy(c)
                b.rig.peer.unavailable = True
                for _ in range(5):
                    await c.tick()
                assert b.rig.bench.firmware.effective_limit() == 0
                b.rig.peer.unavailable = False
                await close_session(b.rig, s, sock, local)
            finally:
                b.rig.peer.unavailable = False
                if replace:
                    await c.tests.close()
                    await s.close()


# Intent: unavailable native startup exposes no listener, then actual restoration permits fresh introduction/recovery.
# Why: successful-restart permutation grammars must not credit commands skipped while no real service exists.
@pytest.mark.parametrize("current", [6, 16, 32])
async def test_failed_native_startup_recovers_without_premature_controller(tmp_path, model_tls, monkeypatch, current):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", BeforeCapFailurePeer)
    async with commissioned(tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch, current) as b:
        await b.controller.tests.close()
        await crash(b.session)
        b.rig.peer.unavailable = True
        s = managed_session(b.rig)
        intent = s.journal.get("managed_control_intent")
        try:
            with pytest.raises((NativeTransportUnavailableError, UnavailableError)):
                await s.start()
            assert not s.started and s.runtime._server is None
            assert s.journal.get("managed_control_intent") == intent
            assert s.journal.get("managed_test_charge") is None
            b.rig.peer.unavailable = False
            await s.start()
            c = ManagedController(s)
            async with b.rig.charger(s) as sock:
                await introduced(b.rig, s, sock, "native-startup-restored")
                c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
                assert (await healthy(c))["local_hold_verified"]
                c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
                assert (await healthy(c))["station_ceiling_a"] == current and not starts(b.rig)
                await c.tests.close()
        finally:
            b.rig.peer.unavailable = False
            await s.close()
