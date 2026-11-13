"""Exhaust source revocation, recovery, cutoff and process replacement around a real daily session."""

from __future__ import annotations

import copy
import os
import uuid
from contextlib import AsyncExitStack
from datetime import UTC, datetime

import pytest

from tests import conftest as _engine  # noqa: F401 - load the HA-free engine package.
from tests.managed_scenario_helpers import (
    close_in_order,
    demand_from_vehicle,
    next_daily,
    originals,
    replace_controller,
    select_current,
)
from tests.test_managed_proxy import introduced, starts
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_prototype import START
from tests_model.catalog import input_digest
from tests_model.schedule_cases import source_cases
from tests_model.test_schedule_sequences import Supervisor, listener_port, tls_for_case, turns
from tests_model.test_schedule_sequences import model_tls as model_tls


# Select one ordering per seed only for harness qualification; the default always executes the full family.
def selected_cases():
    candidates = source_cases()
    if os.environ.get("MODEL_SMOKE") == "orders":
        return tuple(case for index, case in enumerate(candidates) if index % 60 == 0 or index < 60)
    return candidates[::60] if os.environ.get("MODEL_SMOKE") == "1" else candidates


# Intent: every admitted ordering must revoke lost source proof and permit a separately verified daily continuation.
# Why: a source label or pre-restart observation cannot permanently exempt an open transaction from the 06:00 hold.
@pytest.mark.parametrize("case", selected_cases(), ids=lambda case: case.identifier)
async def test_source_proof_event_sequences(tmp_path, model_tls, monkeypatch, record_property, case):
    async with timer_bench(tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch) as b:
        await select_current(b, case.selected)
        saved = originals(b)
        await b.arm("00000001" + "0" * 24)
        receipt = copy.deepcopy(b.session.journal.get("managed_temporary_schedule"))
        cutoff = int(
            datetime.fromtimestamp(b.start, ZONE).replace(hour=6, minute=0, second=0, microsecond=0).timestamp()
        )
        b.clock.now = b.start + 601
        await turns(b, 6)
        assert b.device.plans == {9: saved[0]}
        b.clock.now = cutoff - 120
        b.device.active = 987654
        b.rig.peer.plan_session = (987654, 9)
        demand_from_vehicle(b, case.demand)
        await turns(b, 4)
        assert b.device.effective_limit() == 0
        report = START | {"idTag": "NATIVE_TIMER", "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()}
        response = await b.rig.bench.call(b.sock, uuid.uuid4().hex, "StartTransaction", report)
        assert response[0] == 3, response
        local = response[2]["transactionId"]
        b.rig.peer.plan_session = (local, 9)
        await turns(b, 6)
        assert b.device.effective_limit() == case.selected
        assert b.device.current == (case.selected if case.demand else 0)
        current_source = 9
        completed = []
        suffix_complete = False
        async with AsyncExitStack() as stack:
            supervisor = Supervisor()
            await supervisor.start(b)
            stack.push_async_callback(supervisor.close)
            try:
                for name in case.events:
                    if name in ("loss", "restore"):
                        current_source = case.lost_source if name == "loss" else 9
                        b.rig.peer.plan_session = (local, current_source)
                    elif name == "cutoff":
                        b.clock.now = cutoff
                        await turns(b, 6)
                    elif name == "observe":
                        await turns(b, 6)
                    elif name == "restart":
                        await supervisor.close()
                        await replace_controller(b)
                        b.sock = await stack.enter_async_context(b.rig.charger(b.session))
                        await introduced(b.rig, b.session, b.sock, "source-restarted")
                        await supervisor.start(b)
                        await turns(b, 6)
                    completed.append(name)
                    assert not starts(b.rig) and not b.stops()
                    assert b.c.current.selected() == case.selected
                    assert b.device.plans == {9: saved[0]} and b.rig.peer.configuration == saved[1]
                    assert b.session.journal.active() == local
                    current = b.session.journal.get("managed_temporary_schedule")
                    assert all(current[key] == receipt[key] for key in ("id", "start_epoch", "end_epoch", "body"))
                    assert 0 <= b.device.effective_limit() <= case.selected
                    if name in ("observe", "cutoff", "restart") and current_source != 9:
                        assert b.device.effective_limit() == b.device.current == 0
                await turns(b, 8)
                assert current_source == 9
                assert b.device.effective_limit() == case.selected
                assert b.device.current == (case.selected if case.demand else 0)
                await close_in_order(b, local, case.first_close, ())
                await turns(b, 8)
                await next_daily(b, case.selected, saved)
                suffix_complete = True
            finally:
                record_property("model_case", case.identifier)
                record_property("model_family", "source_proof")
                record_property("model_input_sha256", input_digest("source_proof", case))
                record_property("completed_events", completed)
                record_property("suffix_complete", suffix_complete)
