"""Current operations resolve actual peer boundaries before or after Pause and Boot, with physical recovery."""

import asyncio
import copy
import json
import os
import uuid

import pytest
from ocpp_hub_core.frames import build_call_error, build_call_result
from ocpp_hub_core.prototype.gateway import decode

from tests import conftest as _engine  # noqa: F401 - initialize the standalone engine before importing it.
from tests import test_native_bounded_session as native_bench
from tests.test_managed_current_tick_race import waiting_locals
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_proxy import healthy
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_prototype import BOOT
from tests_model.catalog import input_digest
from tests_model.control_cases import current_candidates
from tests_model.control_helpers import ordinary_recovery, station_limit
from tests_model.test_schedule_sequences import listener_port, tls_for_case
from tests_model.test_schedule_sequences import model_tls as model_tls


class BoundaryPeer(InterruptedNativePeer):
    """Only one actual HTTP response is held or corrupted; subsequent physical recovery reads stay independent."""

    # Arm only after unrelated commissioning and retain an actual socket-level occurrence of the boundary.
    def __init__(self, bench):
        super().__init__(bench)
        self.arm, self.outcome = False, "valid"
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.request = None
        self.client = None

    # Buffer the actual received request, then delegate its original bytes or send the declared one-shot failure.
    async def handle(self, reader, writer):
        if not self.arm:
            await super().handle(reader, writer)
            return
        self.arm = False
        header = await reader.readuntil(b"\r\n\r\n")
        length = next(
            int(line.split(b":", 1)[1]) for line in header.split(b"\r\n") if line.lower().startswith(b"content-length:")
        )
        body = await reader.readexactly(length)
        self.request = body
        self.client = writer.get_extra_info("peername")
        self.entered.set()
        await self.release.wait()
        if self.outcome != "valid":
            if self.outcome == "503":
                writer.write(b"HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
            elif self.outcome == "bad-json":
                writer.write(b"HTTP/1.0 200 OK\r\nContent-Length: 1\r\n\r\n{")
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return

        class Buffered:
            # Preserve the exact independent request and original native parser after the delayed response.
            async def readuntil(self, separator):
                return header

            # Native parsing reads the already received payload without changing action or credentials.
            async def readexactly(self, count):
                assert count == len(body)
                return body

        await super().handle(Buffered(), writer)


# Keep explicit development sampling out of every final qualification.
def selected():
    values = current_candidates()
    return values[::12] if os.environ.get("MODEL_SMOKE") else values


# Intent: every real proof failure and competing event order retains only a confirmed future current selection.
# Why: an applied profile, stale Boot proof or revoked worker cannot silently become authority to resume charging.
@pytest.mark.parametrize("case", selected(), ids=lambda case: case.identifier)
async def test_current_resolution_sequences(tmp_path, model_tls, monkeypatch, record_property, case):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", BoundaryPeer)
    events, suffix_complete = [], False
    async with commissioned(
        tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch, case.initial
    ) as b:
        c, rig, sock = b.controller, b.rig, b.charger
        peer, firmware = rig.peer, rig.bench.firmware
        plan, config = copy.deepcopy(firmware.plans), copy.deepcopy(peer.configuration)
        if case.boundary == "paused-native":
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            await healthy(c)
        send, actual, held, entered = sock.send, firmware.answer, [], asyncio.Event()
        profile_seen = False
        peer.outcome = case.outcome

        # The real independent firmware refuses only this requested profile or its composite proof.
        def answer(action, payload):
            nonlocal profile_seen
            if (
                action == "SetChargingProfile"
                and payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                == case.target
            ):
                profile_seen = True
                if case.boundary == "profile" and case.outcome == "rejected" and not held:
                    return {"status": "Rejected"}
            if (
                action == "GetCompositeSchedule"
                and profile_seen
                and case.boundary == "composite"
                and case.outcome == "rejected"
                and not held
            ):
                return {"status": "Rejected"}
            return actual(action, payload)

        # Capture a real correlated charger reply, or arm the first native read after its target composite proof.
        async def reply(raw):
            frame = decode(raw)
            call = next((row for row in rig.bench.device_calls if row[1] == frame[1]), None)
            target_profile = call is not None and station_limit(call, case.target)
            target_composite = call is not None and profile_seen and call[2] == "GetCompositeSchedule"
            if frame[0] == 3 and target_composite and case.boundary == "native-after" and not peer.entered.is_set():
                peer.arm = True
            match = case.boundary == "profile" and target_profile or case.boundary == "composite" and target_composite
            if frame[0] == 3 and match and not held:
                if case.outcome == "call-error":
                    raw = build_call_error(frame[1], "InternalError", "synthetic applied current uncertainty")
                elif case.outcome == "bad-schedule":
                    payload = copy.deepcopy(frame[2])
                    payload["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] = 33
                    raw = build_call_result(frame[1], payload)
                held.append(raw)
                entered.set()
            else:
                await send(raw)

        monkeypatch.setattr(firmware, "answer", answer)
        monkeypatch.setattr(sock, "send", reply)
        if case.boundary in ("native-before", "paused-native"):
            peer.arm = True
        identifier = uuid.uuid4().hex
        c.command(identifier, "set_current", c.requests.intent().revision, current_a=case.target)
        try:
            await asyncio.wait_for((peer.entered if "native" in case.boundary else entered).wait(), 3)
            if "native" in case.boundary:
                assert json.loads(peer.request)["action"] == "getConfig"
                stream = waiting_locals(c.current.worker, "read_native")["writer"]
                assert stream.get_extra_info("sockname") == peer.client
            for event in case.events:
                if event == "resolve":
                    if "native" in case.boundary:
                        peer.release.set()
                    else:
                        await send(held[0])
                    await c.current.settle()
                elif event == "pause":
                    c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
                else:
                    assert (await rig.bench.call(sock, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
                events.append(event)
                await asyncio.sleep(0)
                assert 0 <= firmware.effective_limit() <= max(case.initial, case.target)
            confirmed = case.outcome == "valid" and case.events.index("resolve") < case.events.index("pause")
            if "native" in case.boundary and case.events.index("boot") < case.events.index("resolve"):
                confirmed = False
            expected = case.target if confirmed else case.initial
            await ordinary_recovery(b, expected, case.demand, first_close="ledger")
            assert c.current.selected() == expected
            assert firmware.plans == plan and peer.configuration == config
            assert events == list(case.events)
            suffix_complete = True
        finally:
            peer.release.set()
            if held:
                await send(held[0])
            await c.current.settle()
            record_property("model_case", case.identifier)
            record_property("model_family", "current_resolution")
            record_property("model_input_sha256", input_digest("current_resolution", case))
            record_property("completed_events", events)
            record_property("suffix_complete", suffix_complete)
            record_property("boundary_entered", peer.entered.is_set() or bool(held))
