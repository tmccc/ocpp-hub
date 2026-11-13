"""The declared current recovery cover retains owner control through two later sessions and physical outages."""

import copy
import os
import uuid

import pytest

from tests import test_native_bounded_session as native_bench
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_proxy import healthy
from tests.test_managed_start_refusal_native32 import commissioned
from tests_model.catalog import input_digest
from tests_model.control_helpers import ordinary_recovery
from tests_model.current_obligations import require_completed_current
from tests_model.deep_cases import current_domain_candidates, current_domain_exclusion
from tests_model.test_schedule_sequences import listener_port, tls_for_case
from tests_model.test_schedule_sequences import model_tls as model_tls


# Explicit local development sampling is forbidden by the final qualification validator.
def selected():
    values = tuple(case for case in current_domain_candidates() if current_domain_exclusion(case) is None)
    return values[::243] if os.environ.get("MODEL_SMOKE") else values


# Intent: every declared recovery obligation retains both intents and demands through two later sessions/outages.
# Why: exhaustive literal transport is separate; these131 pairs are a scoped recovery cover, not all-pair equivalence.
@pytest.mark.parametrize("case", selected(), ids=lambda case: case.identifier)
async def test_current_domain_sequences(tmp_path, model_tls, monkeypatch, record_property, case):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    events, complete = [], False
    try:
        async with commissioned(
            tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch, case.initial
        ) as b:
            c, firmware = b.controller, b.rig.bench.firmware
            plans, configuration = copy.deepcopy(firmware.plans), copy.deepcopy(b.rig.peer.configuration)
            if case.intent == "paused":
                c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
                assert (await healthy(c))["local_hold_verified"] and firmware.effective_limit() == 0
            identifier, revision = uuid.uuid4().hex, c.requests.intent().revision
            receipt = c.command(identifier, "set_current", revision, current_a=case.target)
            await c.current.settle()
            state = await healthy(c)
            assert state["requested_current_a"] == case.target
            assert firmware.effective_limit() == (0 if case.intent == "paused" else case.target)
            completed = c.requests.receipt(identifier)
            assert completed["state"] == "completed"
            assert completed["result"] == {"current_a": case.target, "charging_started": False}
            events.append("choose")
            before = list(b.rig.bench.device_calls)
            duplicate = c.command(identifier, "set_current", revision, current_a=case.target)
            await c.current.settle()
            assert b.rig.bench.device_calls == before
            assert duplicate == completed and c.requests.receipt(identifier) == completed
            assert receipt["id"] == identifier
            require_completed_current(b, identifier, revision, case.target, case.intent)
            events.append("replay")
            await ordinary_recovery(b, case.target, case.demand)
            events.extend(case.events[2:])
            assert c.requests.receipt(identifier) == completed
            assert c.current.selected() == case.target
            assert firmware.plans == plans and b.rig.peer.configuration == configuration
            complete = True
    finally:
        record_property("model_case", case.identifier)
        record_property("model_family", "current_domain")
        record_property("model_input_sha256", input_digest("current_domain", case))
        record_property("completed_events", events)
        record_property("suffix_complete", complete)
