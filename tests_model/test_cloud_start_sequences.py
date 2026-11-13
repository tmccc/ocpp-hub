"""Real cloud refusals must drain their genuine lifecycle and recover later owner charging."""

import copy
import uuid
from contextlib import AsyncExitStack

import pytest
from ocpp_hub_core.prototype import managed_start_delivery
from ocpp_hub_core.prototype.managed import ManagedController

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench, two_later_sessions
from tests.handover_helpers import crash
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_stop_reply_recovery import pause
from tests.test_managed_test_charge import ChargeRig
from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import until
from tests_model.catalog import input_digest
from tests_model.review_cases import cloud_start_candidates
from tests_model.test_schedule_sequences import listener_port, tls_for_case
from tests_model.test_schedule_sequences import model_tls as model_tls


# Intent: explicit cloud refusal or denied authorization recovers through every closure/Pause/replacement order.
# Why: an initial zero hold is incomplete coverage unless real Stop delivery and two later sessions also succeed.
@pytest.mark.parametrize("case", cloud_start_candidates(), ids=lambda case: case.identifier)
async def test_cloud_start_recovery_sequences(tmp_path, model_tls, monkeypatch, record_property, case):
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    monkeypatch.setattr(managed_start_delivery, "RETRY_INTERVAL", 0.01)
    events, complete = [], False
    async with commissioned(
        tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch, case.selected, owner_tag="APP_OWNER"
    ) as b:
        stack = AsyncExitStack()
        try:
            rig, s, c, sock = b.rig, b.session, b.controller, b.charger
            original_plans, original_config = (
                copy.deepcopy(rig.bench.firmware.plans),
                copy.deepcopy(rig.peer.configuration),
            )
            rig.bench.start_replies = (
                ["error"] * (1 if case.fault == "error-once" else 2)
                if case.fault.startswith("error-")
                else [case.fault]
            )
            tag = "NATIVE_TIMER"
            if case.source == "local":
                c.command(uuid.uuid4().hex, "start", c.requests.intent().revision)
                await c.tests.settle()
                tag = mp.starts(rig)[-1][3]["idTag"]
            expected_starts = len(mp.starts(rig))
            response = await rig.bench.call(sock, uuid.uuid4().hex, "StartTransaction", START | {"idTag": tag})
            assert response[0] == 3
            local = response[2]["transactionId"]
            rig.peer.plan_session = (local, 9 if case.source == "daily" else 987654)
            rig.peer.permission_enabled = case.demand
            rig.bench.cp, rig.bench.connection = ("C", "Charging") if case.demand else ("B", "Plug")
            rig.bench.firmware.requested_current = case.selected if case.demand else 0
            await until(lambda: s.journal.vendor_id(local) is not None and s.journal.next_delivery() is None)
            starts = [f for f in rig.bench.lifecycle_wires if f[2] == "StartTransaction"]
            expected_attempts = 2 if case.fault == "error-once" else 3 if case.fault == "error-twice" else 1
            assert len(starts) == expected_attempts and len({f[1] for f in starts}) == 1
            if not case.fault.startswith("error-"):
                assert rig.bench.firmware.effective_limit() == 0 and s.journal.get("managed_control_fault")
            for event in case.events:
                if event == "close":
                    rig.peer.permission_enabled = False
                    rig.bench.firmware.requested_current = 0
                    assert (
                        await rig.bench.call(
                            sock,
                            uuid.uuid4().hex,
                            "StopTransaction",
                            {
                                "transactionId": local,
                                "timestamp": "2026-01-01T00:01:00Z",
                                "meterStop": 1100,
                            },
                        )
                    )[0] == 3
                    rig.peer.plan_session = None
                    await until(lambda: s.journal.next_delivery() is None)
                elif event == "pause":
                    await pause(c)
                else:
                    await c.tests.close()
                    await crash(s)
                    await stack.aclose()
                    stack = AsyncExitStack()
                    s = mp.managed_session(rig)
                    await s.start()
                    c = ManagedController(s)
                    sock = await stack.enter_async_context(rig.charger(s))
                    assert (await rig.bench.call(sock, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
                    await until(lambda: s.gateway.controls.ready and s.gateway.vendor_registration == "Accepted")
                events.append(event)
                assert c.current.selected() == case.selected
                assert rig.bench.firmware.plans == original_plans and rig.peer.configuration == original_config
            stops = [f for f in rig.bench.lifecycle_wires if f[2] == "StopTransaction"]
            assert len(stops) == 1 and stops[0][3]["transactionId"] == s.journal.vendor_id(local)
            await pause(c)
            assert (await mp.healthy(c))["local_hold_verified"]
            assert s.journal.get("managed_control_fault") is None
            rig.bench.fixed_vendor = None
            await two_later_sessions(
                ChargeRig(rig, s, c, sock), selected=case.selected, original_starts=expected_starts
            )
            assert events == list(case.events)
            complete = True
        finally:
            await stack.aclose()
            record_property("model_case", case.identifier)
            record_property("model_family", "cloud_start_recovery")
            record_property("model_input_sha256", input_digest("cloud_start_recovery", case))
            record_property("completed_events", events)
            record_property("suffix_complete", complete)
