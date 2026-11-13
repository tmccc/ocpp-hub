"""A replaced current worker cannot consume an old native response or promote an unverified resume target."""

import asyncio
import os
import uuid

import pytest
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.managed import ManagedController

from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import crash
from tests.test_managed_proxy import healthy, introduced, managed_session
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_test_charge import ChargeRig
from tests_model.catalog import input_digest
from tests_model.control_cases import current_replacement_candidates
from tests_model.control_helpers import ordinary_recovery
from tests_model.test_current_sequences import BoundaryPeer
from tests_model.test_schedule_sequences import listener_port, tls_for_case
from tests_model.test_schedule_sequences import model_tls as model_tls


# An explicit smoke subset preserves every physical boundary and directed current pair during development.
def cases():
    values = current_replacement_candidates()
    return values[::2] if os.environ.get("MODEL_SMOKE") else values


# Intent: native-boundary process loss before/after a newer Pause retains only the earlier verified selection.
# Why: a canceled worker's delayed response cannot confer authority on a replacement process or transport.
@pytest.mark.parametrize("case", cases(), ids=lambda case: case.identifier)
async def test_current_replacement_sequences(tmp_path, model_tls, monkeypatch, record_property, case):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", BoundaryPeer)
    events, suffix_complete = [], False
    tls = tls_for_case(tmp_path, model_tls)
    async with commissioned(tmp_path, tls, listener_port(), monkeypatch, case.initial) as b:
        c, s, peer = b.controller, b.session, b.rig.peer
        if case.boundary == "paused-native":
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            await healthy(c)
        send = b.charger.send

        # Arm only after the actual target composite reply, so the applied ceiling remains unverified by native HTTP.
        async def reply(raw):
            frame = decode(raw)
            call = next((row for row in b.rig.bench.device_calls if row[1] == frame[1]), None)
            if (
                case.boundary == "native-after"
                and frame[0] == 3
                and call is not None
                and call[2] == "GetCompositeSchedule"
                and not peer.entered.is_set()
            ):
                peer.arm = True
            await send(raw)

        monkeypatch.setattr(b.charger, "send", reply)
        peer.arm = case.boundary != "native-after"
        c.command(uuid.uuid4().hex, "set_current", c.requests.intent().revision, current_a=case.target)
        try:
            await asyncio.wait_for(peer.entered.wait(), 3)
            assert not any(not task.done() for task in s.gateway.controls._operations)
            events.append("held-native")
            if case.pause_before:
                c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
                c.pause_task.cancel()
                await asyncio.gather(c.pause_task, return_exceptions=True)
                events.append("pause")
            c.current.worker.cancel()
            await asyncio.gather(c.current.worker, return_exceptions=True)
            peer.release.set()
            await c.tests.close()
            await crash(s)
            s = managed_session(b.rig)
            await s.start()
            c = ManagedController(s)
            async with b.rig.charger(s) as sock:
                await introduced(b.rig, s, sock, "native-boundary-replacement")
                events.append("replace")
                if not case.pause_before:
                    c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
                    events.append("pause")
                await ordinary_recovery(ChargeRig(b.rig, s, c, sock), case.initial, case.demand)
                assert c.current.selected() == case.initial
                events.append("recover")
                assert events == list(case.events)
                suffix_complete = True
        finally:
            peer.release.set()
            await c.current.settle()
            await c.tests.close()
            await s.close()
            record_property("model_case", case.identifier)
            record_property("model_family", "current_replacement")
            record_property("model_input_sha256", input_digest("current_replacement", case))
            record_property("completed_events", events)
            record_property("suffix_complete", suffix_complete)
            record_property("boundary_entered", peer.entered.is_set())
