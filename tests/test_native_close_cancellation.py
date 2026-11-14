"""An actual classified wire refusal must survive cancellation at its real TCP close await."""

import asyncio
import copy
import json
import sys
import uuid

import pytest
from ocpp_hub_core.prototype.managed_native_repair import AUDITS, REVOCATIONS
from ocpp_hub_core.prototype.managed_release_recovery import ACK, PROOF
from ocpp_hub_core.prototype.native_bounded_handover import AuditSupersededError
from ocpp_hub_core.prototype.native_faults import NativeContentError
from ocpp_hub_core.prototype.native_http import NativeTransportUnavailableError

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench, two_later_sessions
from tests.recovery_barriers import completed_control
from tests.test_managed_release_recovery import explicit_release_pause, release_bench
from tests.test_native_repair_sources import RepairWirePeer, wire_response
from tests.test_ordinary_legacy_transport_recovery import legacy_fault
from tests.test_prototype_tls import tls as tls


# Retain complete independently observed journal facts before and after the actual read outcome and later recovery.
def facts(b):
    j = b.session.journal
    return {
        "metadata": {key: json.loads(value) for key, value in j.db.execute("SELECT key,value FROM meta")},
        "requests": [dict(row) for row in j.db.execute("SELECT rowid,* FROM managed_requests ORDER BY rowid")],
        "transactions": [dict(row) for row in j.db.execute("SELECT * FROM transactions ORDER BY rowid")],
        "outbox": [dict(row) for row in j.db.execute("SELECT * FROM outbox ORDER BY rowid")],
        "mutations": [dict(row) for row in j.db.execute("SELECT * FROM mutations ORDER BY rowid")],
        "physical_limit_a": b.rig.bench.firmware.effective_limit(),
        "physical_current_a": b.rig.bench.firmware.current,
    }


# Run the real native reader while observing its actual classified exception and its real client close boundary.
async def interrupted_read(b, monkeypatch, case):
    c, peer = b.c, b.rig.peer
    actual_close, actual_timeout = asyncio.StreamWriter.close, asyncio.timeout
    prior_trace = sys.gettrace()
    target = None
    classifications, closes = [], []
    newer_pause, deadlines, cancellation_results = [], [], []

    # Observe only real exceptions propagated by the HTTP parser in this exact native task, never fabricate one.
    def trace_classification(frame, event, argument):
        if event == "exception" and frame.f_code.co_name == "read_native":
            error = argument[1]
            if isinstance(error, (NativeContentError, NativeTransportUnavailableError)):
                try:
                    task = asyncio.current_task()
                except RuntimeError:
                    task = None
                if task is target:
                    classifications.append({
                        "type": type(error).__name__, "stage": getattr(error, "stage", None),
                        "detail": str(error), "loop_time": asyncio.get_running_loop().time(),
                    })
        return trace_classification

    # Capture the genuine aggregate context; rescheduling its deadline selects an ordering without a wall-time claim.
    def capture_timeout(delay):
        context = actual_timeout(delay)
        if delay == 15 and asyncio.current_task() is target:
            deadlines.append(context)
        return context

    # Keep cancellation cooperative; after removal of the old yield the typed refusal can finish before this callback.
    def cancel_at_close():
        cancellation_results.append(target.cancel())

    # Close the real TCP transport, then select the pending-close ordering without replacing its completion waiter.
    def close_boundary(writer):
        actual_close(writer)
        if asyncio.current_task() is target and classifications and not closes:
            waiter = writer._protocol._get_close_waiter(writer)
            closes.append({
                "transport_closing": writer.is_closing(), "actual_close_waiter_pending": not waiter.done(),
                "peer_port": writer.get_extra_info("peername")[1],
                "audit_before_cleanup": copy.deepcopy(b.session.journal.get(AUDITS)),
                "loop_time": asyncio.get_running_loop().time(),
            })
            if case == "strict-newer-pause":
                identifier = uuid.uuid4().hex
                c.command(identifier, "pause", c.requests.intent().revision)
                newer_pause.append(identifier)
            if case == "strict-observation-deadline":
                assert deadlines
                deadlines[0].reschedule(asyncio.get_running_loop().time())
            elif case != "strict-no-cancel":
                asyncio.get_running_loop().call_soon(cancel_at_close)

    peer.raw_response = wire_response("http503" if case == "availability-cancel" else "duplicate")
    try:
        monkeypatch.setattr(asyncio.StreamWriter, "close", close_boundary)
        monkeypatch.setattr(asyncio, "timeout", capture_timeout)
        sys.settrace(trace_classification)
        target = asyncio.create_task(c.native.read(closed=True))
        result = (await asyncio.gather(target, return_exceptions=True))[0]
    finally:
        sys.settrace(prior_trace)
        monkeypatch.setattr(asyncio.StreamWriter, "close", actual_close)
        monkeypatch.setattr(asyncio, "timeout", actual_timeout)
        peer.raw_response = None
    assert len(closes) == 1 and closes[0]["transport_closing"] and closes[0]["actual_close_waiter_pending"]
    assert closes[0]["peer_port"] == b.rig.prepared.observer.port and closes[0]["audit_before_cleanup"]
    assert classifications and peer.bad_reads >= 1 and isinstance(result, BaseException)
    assert classifications[0]["type"] == (
        "NativeTransportUnavailableError" if case == "availability-cancel" else "NativeContentError"
    )
    if case == "availability-cancel":
        assert type(result) is asyncio.CancelledError and cancellation_results == [True]
    elif case == "strict-newer-pause":
        assert type(result) in (asyncio.CancelledError, AuditSupersededError)
    elif case == "strict-observation-deadline":
        assert type(result) in (NativeTransportUnavailableError, NativeContentError)
        assert deadlines and (deadlines[0].expired() or type(result) is NativeContentError)
    else:
        assert type(result) in (asyncio.CancelledError, NativeContentError)
    return {"classifications": classifications, "actual_close": closes, "result_type": type(result).__name__,
            "task_cancelled": target.cancelled(), "task_cancelling": target.cancelling(),
            "newer_pause": newer_pause[0] if newer_pause else None,
            "cancel_callback_results": cancellation_results,
            "aggregate_deadline_rescheduled": case == "strict-observation-deadline",
            "aggregate_deadline_expired": deadlines[0].expired() if deadlines else False,
            "wall_time_15s_elapsed_claimed": False}


# Complete only real supported owner commands if recovery was actually granted, recording both later lifecycles.
async def observe_later_recovery(b, pause, should_block):
    c, j = b.c, b.session.journal
    if c.pause_task is not None:
        await c.pause_task
    for _ in range(3):
        await c.tick()
    cleared = j.get("managed_control_fault") is None
    outcome = {"fault_cleared": cleared, "pause_receipt": c.requests.receipt(pause), "after_poll": facts(b)}
    if cleared:
        assert c.requests.receipt(pause)["state"] == "completed"
        identifier, revision = uuid.uuid4().hex, c.requests.intent().revision
        c.command(identifier, "app_control", revision)
        await completed_control(b, identifier, mode="app", revision=revision + 1, ceiling=6)
        outcome["return_receipt"] = c.requests.receipt(identifier)
        await two_later_sessions(b, selected=6, original_starts=0, first_minute=20)
        outcome["after_later_sessions"] = facts(b)
    else:
        assert b.rig.bench.firmware.effective_limit() == 0
    outcome["expected_blocked"] = should_block
    return outcome


# Intent: real strict bytes remain disqualifying through close cancellation, with availability and new-Pause controls.
# Why: cancellation during cleanup must not relabel an already classified current strict refusal as availability.
@pytest.mark.parametrize("case", [
    "strict-cancel", "strict-observation-deadline", "availability-cancel", "strict-newer-pause", "strict-no-cancel",
])
async def test_classified_native_refusal_survives_actual_close_cancellation(
    tmp_path, tls, unused_tcp_port, monkeypatch, case
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", RepairWirePeer)
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        b.controller = b.c
        fault = await legacy_fault(b, "NativeTransportUnavailableError")
        pause = await explicit_release_pause(b)
        before = facts(b)
        assert before["metadata"][ACK]["fault"] == fault and not before["outbox"]
        observed = await interrupted_read(b, monkeypatch, case)
        immediately_after = facts(b)
        should_block = case in ("strict-cancel", "strict-observation-deadline", "strict-no-cancel")
        recovery = await observe_later_recovery(b, observed["newer_pause"] or pause, should_block)
        witness = {
            "case": case, "legacy_input_explicit": True, "fault": fault,
            "before": before, "actual_read": observed, "immediately_after": immediately_after,
            "later_recovery": recovery,
        }
        (tmp_path / "native-close-witness.json").write_text(json.dumps(witness, indent=2) + "\n")
        if should_block:
            assert not recovery["fault_cleared"], "classified strict native bytes were erased by close cancellation"
            assert recovery["pause_receipt"]["state"] != "completed"
            assert b.session.journal.get(REVOCATIONS) or b.session.journal.get(AUDITS)
            assert b.session.journal.get(PROOF) is None
        else:
            assert recovery["fault_cleared"] and recovery["return_receipt"]["state"] == "completed"
