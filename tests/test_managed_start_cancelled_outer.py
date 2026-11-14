"""A never-executed outer Start worker cannot acquire Pause or Stop authority over a genuine daily."""

import asyncio
import copy
import json
import uuid
from contextlib import asynccontextmanager

import pytest
from ocpp_hub_core.prototype import managed_test_charge
from ocpp_hub_core.prototype.managed_test_charge import ManagedChargeTests

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench, two_later_sessions
from tests.recovery_barriers import witnessed_native_loss
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_test_charge import positive_flow, ready
from tests.test_ordinary_recovery import close_session
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests_model.test_start_recovery_sequences import close_daily


# Use real USER6 or commissioned USER32 and the ordinary actual current worker, with distinct owner/daily tags.
@asynccontextmanager
async def selected_source(tmp_path, tls, port, monkeypatch, current):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    factory = (
        ready(tmp_path, tls, port, monkeypatch, owner_tag="APP_OWNER")
        if current == 6
        else commissioned(tmp_path, tls, port, monkeypatch, current, owner_tag="APP_OWNER")
    )
    async with factory as b:
        # This declared admission budget is configured before acceptance; no existing lease/deadline is rewritten.
        monkeypatch.setattr(managed_test_charge, "STARTUP_BUDGET", 3)
        yield b


# Cancel only the real outer managed coroutine before its first instruction; LocalControl is never intercepted.
async def cancel_outer_before_execution(b):
    c, j = b.controller, b.session.journal
    loop, original_factory = asyncio.get_running_loop(), asyncio.get_running_loop().get_task_factory()
    victims = []
    identifier, before_intent = uuid.uuid4().hex, c.requests.intent()
    original_calls = copy.deepcopy(b.rig.bench.device_calls)

    # Preserve ordinary scheduling for every other owned native, timer, protocol and cleanup task.
    def cancel_only_outer(loop, coroutine, **kwargs):
        task = asyncio.Task(coroutine, loop=loop, eager_start=False, **kwargs)
        if (
            getattr(coroutine, "cr_code", None) is ManagedChargeTests._start.__code__
            and coroutine.cr_frame.f_locals.get("self") is c.tests
        ):
            task.cancel()
            victims.append(task)
        return task

    try:
        loop.set_task_factory(cancel_only_outer)
        accepted = c.command(identifier, "start", before_intent.revision)
        assert accepted["id"] == identifier and accepted["state"] == "accepted"
        worker = c.tests.worker
        assert worker is not None
        await asyncio.gather(worker, return_exceptions=True)
    finally:
        loop.set_task_factory(original_factory)
    assert victims == [worker] and worker.cancelled()
    assert c.tests.state()["start_attempted"] is False and c.tests.state()["start_rpc_accepted"] is False
    assert j.get("local_start_receipt") is None and b.rig.bench.device_calls == original_calls
    assert not mp.starts(b.rig) and c.requests.intent() == before_intent
    return identifier, before_intent


# Save the actual first-poll outcome before asserting policy, including independent peer and immutable request facts.
def save_first_poll(tmp_path, b, identifier, intent, status, local):
    j, c, rig = b.session.journal, b.controller, b.rig
    value = {
        "cancelled_coroutine": "ManagedChargeTests._start",
        "outer_worker_cancelled": c.tests.worker.cancelled(),
        "request": c.requests.receipt(identifier),
        "state": c.tests.state(),
        "local_start_receipt": j.get("local_start_receipt"),
        "original_intent": {"mode": intent.mode, "revision": intent.revision},
        "after_intent": {"mode": c.requests.intent().mode, "revision": c.requests.intent().revision},
        "actual_daily_local": local,
        "actual_daily_vendor": j.vendor_id(local) if local is not None else None,
        "native_plan_source": rig.peer.plan_session,
        "status": status,
        "physical_limit": rig.bench.firmware.effective_limit(),
        "physical_current": rig.bench.firmware.current,
        "remote_stops": [frame for frame in rig.bench.device_calls if frame[2] == "RemoteStopTransaction"],
        "remote_starts": mp.starts(rig),
    }
    (tmp_path / "cancelled-outer-first-poll.json").write_text(json.dumps(value, indent=2) + "\n")


# Prove a failed unsent request does not impair explicit Pause/Return or two new mapped native-outage lifecycles.
async def finish_recovery(b, identifier, current):
    c, rig, j = b.controller, b.rig, b.session.journal
    pause = uuid.uuid4().hex
    c.command(pause, "pause", c.requests.intent().revision)
    assert (await mp.healthy(c))["local_hold_verified"]
    assert c.requests.receipt(pause)["state"] == "completed"
    assert c.requests.receipt(identifier)["state"] == "failed" and c.tests.state()["state"] == "completed"
    assert j.get("local_start_receipt") is None and not mp.starts(rig)
    c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
    assert (await mp.healthy(c))["station_ceiling_a"] == current
    await two_later_sessions(b, selected=current, original_starts=0)
    assert j.next_delivery() is None and not rig.bench.backend_errors


# Intent: a genuine daily arriving after outer-task cancellation keeps app authority through the first fresh poll.
# Why: no executed owner operation or local Start receipt may manufacture a Pause for an unrelated original plan.
@pytest.mark.parametrize("current", [6, 16, 32])
@pytest.mark.parametrize("demand", [False, True])
async def test_unstarted_cancelled_outer_cannot_pause_independent_daily(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, demand
):
    async with selected_source(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        c, j, rig = b.controller, b.session.journal, b.rig
        plans, configuration = copy.deepcopy(rig.bench.firmware.plans), copy.deepcopy(rig.peer.configuration)
        identifier, intent = await cancel_outer_before_execution(b)
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        if not demand:
            rig.peer.permission_enabled, rig.bench.firmware.requested_current = False, 0
        assert rig.peer.plan_session == (local, 9) and j.vendor_id(local) is not None
        assert c.tests.state().get("independent_seen") is not True
        assert rig.bench.firmware.effective_limit() == current
        assert rig.bench.firmware.current == (current if demand else 0)
        status = await c.tick()
        save_first_poll(tmp_path, b, identifier, intent, status, local)
        assert c.requests.intent() == intent, "cancelled unexecuted owner task manufactured a Pause for a real daily"
        assert not any(frame[2] == "RemoteStopTransaction" for frame in rig.bench.device_calls)
        assert rig.bench.firmware.effective_limit() == current
        assert (await mp.healthy(c))["current_a"] == (current if demand else 0)
        await until(lambda: c.tests.timer.done())
        assert c.requests.intent() == intent and not mp.starts(rig)
        assert await witnessed_native_loss(b, current, local) >= 3
        await close_daily(b, local, "ledger")
        assert c.requests.intent() == intent and rig.bench.firmware.effective_limit() == current
        await finish_recovery(b, identifier, current)
        assert rig.bench.firmware.plans == plans and rig.peer.configuration == configuration
        assert len(rig.bench.backend_starts) == 3 and all(entry["closed"] for entry in rig.bench.backend_starts)


# Intent: idle cancellation recovers, and a genuinely newer explicit Pause keeps its own revision/ticket and Stop.
# Why: preserving an independent daily must not weaken the owner's later restrictive command or leave unsent debt.
@pytest.mark.parametrize("current", [6, 32])
@pytest.mark.parametrize("control", ["idle", "newer-explicit-pause"])
async def test_cancelled_outer_recovery_controls(tmp_path, tls, unused_tcp_port, monkeypatch, current, control):
    async with selected_source(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        c, j, rig = b.controller, b.session.journal, b.rig
        identifier, intent = await cancel_outer_before_execution(b)
        local, newer_pause, newer_ticket = None, None, None
        if control == "newer-explicit-pause":
            local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
            newer_pause = uuid.uuid4().hex
            c.command(newer_pause, "pause", c.requests.intent().revision)
            intent = c.requests.intent()
            await asyncio.wait_for(rig.bench.remote_stop.wait(), 3)
            if c.pause_task is not None:
                await c.pause_task
            # The device actually closes its daily and its real Stop is cloud-acknowledged before the next poll.
            await close_session(rig, b.session, b.charger, local)
            rig.peer.plan_session = None
            # The accepted Pause legitimately claims its own Stop ticket before the obsolete worker is polled.
            assert c.requests.intent() == intent
            newer_ticket = j.get("control")["ticket"]
        status = await c.tick()
        save_first_poll(tmp_path, b, identifier, intent, status, local)
        if local is not None:
            assert c.requests.intent() == intent and j.get("control")["ticket"] == newer_ticket
            stops = [frame for frame in rig.bench.device_calls if frame[2] == "RemoteStopTransaction"]
            assert len(stops) == 1 and stops[0][3] == {"transactionId": local}
            assert rig.bench.firmware.effective_limit() == 0
            assert (await mp.healthy(c))["local_hold_verified"]
            assert c.requests.receipt(newer_pause)["state"] == "completed"
        await finish_recovery(b, identifier, current)
        assert j.get("local_start_receipt") is None and not mp.starts(rig)
        assert len(rig.bench.backend_starts) == (3 if local is not None else 2)
        assert all(entry["closed"] for entry in rig.bench.backend_starts)
