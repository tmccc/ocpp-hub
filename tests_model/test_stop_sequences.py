"""A retained genuine Stop has separate operator proof, actual wire ACK and fresh physical recovery histories."""

import asyncio
import copy
import os
import uuid
from contextlib import AsyncExitStack

import pytest

from tests import conftest as _engine  # noqa: F401 - initialize the standalone engine before importing it.
from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.recovery_barriers import returned_to_app, witnessed_native_loss
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_stop_reply_recovery import ReplyBench, pause, reconcile, running
from tests.test_managed_test_charge import ChargeRig, positive_flow
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests_model.catalog import input_digest
from tests_model.control_cases import stop_candidates
from tests_model.test_schedule_sequences import listener_port, tls_for_case
from tests_model.test_schedule_sequences import model_tls as model_tls
from tests_model.test_start_recovery_sequences import observe


# Development checks take one order per seed; qualification always collects the complete immutable family.
def cases():
    values = stop_candidates()
    return values[::24] if os.environ.get("MODEL_SMOKE") else values


# Intent: all ACK/Pause/replacement/observation orders preserve exact Stop evidence and permit a fresh recovery.
# Why: operator proof alone and an earlier Pause must not become delivery or authorize a later positive release.
@pytest.mark.parametrize("case", cases(), ids=lambda case: case.identifier)
async def test_stop_delivery_sequences(tmp_path, model_tls, monkeypatch, capsys, record_property, case):
    monkeypatch.setattr(mp, "OwnerBench", ReplyBench)
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    events, suffix_complete = [], False
    async with mp.managed_bench(tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch) as rig:
        async with running(rig, "stop-fault-prefix", owner_tag="APP_OWNER") as (s, c, sock):
            await mp.healthy(c)
            if case.origin != "daily":
                c.command(uuid.uuid4().hex, "start", c.requests.intent().revision)
                await c.tests.settle()
            local = await positive_flow(ChargeRig(rig, s, c, sock), "daily" if case.origin == "daily" else "local")
            if case.origin == "local-unobserved":
                assert c.tests.state()["local_transaction"] == local
                rig.peer.permission_enabled, rig.bench.firmware.requested_current = False, 0
            else:
                await mp.healthy(c)
            rig.bench.stop_info = {"status": "invalid"} if case.refusal == "invalid-info" else None
            rig.bench.stop_error = case.refusal == "call-error"
            await rig.bench.call(
                sock,
                uuid.uuid4().hex,
                "StopTransaction",
                {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
            )
            rig.peer.permission_enabled = False
            await until(lambda: s.runtime.status.delivery_blocked)
            await pause(c)
            retained = s.journal.next_delivery()
            token, body = retained.token, copy.deepcopy(retained.payload)
            transaction = dict(s.journal.db.execute("SELECT * FROM transactions WHERE local=?", (local,)).fetchone())
        await reconcile(rig, tmp_path, capsys)
        rig.bench.stop_error, rig.bench.stop_info, rig.bench.stop_gate = False, None, asyncio.Event()
        stack = AsyncExitStack()
        try:
            s, c, sock = await stack.enter_async_context(running(rig, "stop-held-ack", release_on_exit=False))
            await until(lambda: any(row[1] == token for row in rig.bench.stop_wires))
            for event in case.events:
                if event == "ack":
                    rig.bench.stop_gate.set()
                    await until(lambda: s.journal.next_delivery() is None)
                elif event == "pause":
                    await pause(c)
                elif event == "observe":
                    await observe(c)
                else:
                    await stack.aclose()
                    stack = AsyncExitStack()
                    s, c, sock = await stack.enter_async_context(running(rig, "stop-replaced", release_on_exit=False))
                current = s.journal.next_delivery()
                if "ack" not in events and event != "ack":
                    assert current is not None and current.token == token and current.payload == body
                    assert rig.bench.firmware.effective_limit() == 0
                    assert c.requests.intent().mode == "paused"
                assert (
                    dict(s.journal.db.execute("SELECT * FROM transactions WHERE local=?", (local,)).fetchone())
                    == transaction
                )
                events.append(event)
            assert s.journal.next_delivery() is None
            # Fresh post-ACK Pause consumes proof; Return is a distinct owner action and emits no Start.
            await pause(c)
            assert (await mp.healthy(c))["local_hold_verified"]
            historical_starts = len(mp.starts(rig))
            assert historical_starts == int(case.origin != "daily")
            # The shared suffix checks only new ordinary traffic against a preserved historical prefix.
            await stop_suffix(ChargeRig(rig, s, c, sock), case, historical_starts)
            assert events == list(case.events)
            suffix_complete = True
        finally:
            rig.bench.stop_gate.set()
            await stack.aclose()
            record_property("model_case", case.identifier)
            record_property("model_family", "stop_delivery")
            record_property("model_input_sha256", input_digest("stop_delivery", case))
            record_property("completed_events", events)
            record_property("suffix_complete", suffix_complete)


# Run an actual next daily session and outage while retaining every historical owner Start and closed ledger row.
async def stop_suffix(b, case, historical_starts):
    from tests_model.test_start_recovery_sequences import close_daily

    c, rig = b.controller, b.rig
    assert (await returned_to_app(b, 6))["normal_start_ready"]
    local = await positive_flow(
        b, "daily", call_id=uuid.uuid4().hex, meter_start=1100, timestamp="2026-01-01T00:02:00Z"
    )
    if not case.demand:
        rig.peer.permission_enabled, rig.bench.firmware.requested_current = False, 0
    await mp.healthy(c)
    assert await witnessed_native_loss(b, 6, local) >= 3
    await close_daily(b, local, "ledger", timestamp="2026-01-01T00:03:00Z", meter_stop=1200)
    assert len(mp.starts(rig)) == historical_starts
