"""The independent timer watchdog retains an existing station zero despite a durable managed-control fault."""

import asyncio
import copy
import time
import uuid
from types import SimpleNamespace

import pytest
from ocpp_hub_core.prototype import managed
from ocpp_hub_core.prototype.managed_schedule import KEY

from tests.test_managed_proxy import healthy, starts
from tests.test_managed_schedule_cutoff import timer_bench
from tests.test_prototype import BOOT
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Establish real bounded positive timer current before faulting; the fault records intent but sends no fake ACK.
async def fault_positive_timer(b, current):
    if current == 32:
        b.c.command(uuid.uuid4().hex, "set_current", b.c.requests.intent().revision, current_a=32)
        await b.c.current.settle()
        await healthy(b.c)
    await b.arm()
    b.clock.now = b.start
    local = await b.fire(b.timer)
    await healthy(b.c)
    assert b.device.current == current and b.device.effective_limit() == current
    b.session.journal.mutation_intent("vendor", "ChangeConfiguration", {})
    b.c.fault("native_authority_or_preservation_failed")
    fault = copy.deepcopy(b.session.journal.get("managed_control_fault"))
    b.clock.now = b.session.journal.get(KEY)["end_epoch"]
    assert b.device.current == current
    return local, fault


# Stop only the independent asyncio observer, preserving the proxy and peer lifetimes owned by the fixture.
async def stop_watcher(watcher):
    watcher.cancel()
    await asyncio.gather(watcher, return_exceptions=True)


# Intent: the watchdog alone reaches physical zero at a due timer despite faulted foreground polling.
# Why: managed fault revokes current authority but must not disable the separately supervised restrictive deadline.
@pytest.mark.parametrize("current", [6, 32])
async def test_faulted_due_watchdog_applies_owned_zero_without_tick_or_stop(
    tmp_path, tls, unused_tcp_port, monkeypatch, current
):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        local, fault = await fault_positive_timer(b, current)
        j = b.session.journal
        intent = b.c.requests.intent()
        debt = [tuple(row) for row in j.db.execute("SELECT * FROM mutations")]
        watcher = asyncio.create_task(b.c.schedule.watch_deadline())
        try:
            await until(lambda: b.device.current == 0 and b.session.gateway.controls.ready)
            assert b.device.effective_limit() == 0 and j.get(KEY)["cutoff_reached"] is True
            assert j.get("managed_control_fault") == fault and b.c.requests.intent() == intent
            assert [tuple(row) for row in j.db.execute("SELECT * FROM mutations")] == debt
            assert j.active() == local and b.device.active == local
            assert not starts(b.rig) and not b.stops()
            assert j.get("trial_phase") == "blocked"
        finally:
            await stop_watcher(watcher)


# Intent: the fault watchdog cannot borrow another Boot, missing station ownership or a positive desired setting.
# Why: the safety path only reuses the commissioned existing zero profile; it does not create general mutation rights.
@pytest.mark.parametrize("blocker", ["changed_boot", "missing_ownership", "positive_desired"])
async def test_fault_deadline_zero_keeps_identity_and_owned_zero_guards(
    tmp_path, tls, unused_tcp_port, monkeypatch, blocker
):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        local, fault = await fault_positive_timer(b, 6)
        j = b.session.journal
        if blocker == "changed_boot":
            reply = await b.rig.bench.call(
                b.sock, uuid.uuid4().hex, "BootNotification", BOOT | {"chargePointModel": "OTHER_SYNTHETIC"}
            )
            assert reply[0] == 3
        elif blocker == "missing_ownership":
            j.db.execute("DELETE FROM trial_resources WHERE key='profile:7001'")
        else:
            control = j.get("control")
            j.put("control", {**control, "owner": "vendor", "inhibit": False})
        before = len(b.rig.bench.device_calls)
        watcher = asyncio.create_task(b.c.schedule.watch_deadline())
        try:
            await until(lambda: j.get(KEY).get("cutoff_reached") is True)
            await asyncio.sleep(0.05)
            assert len(b.rig.bench.device_calls) == before
            assert b.device.current == 6 and j.active() == local
            assert j.get("managed_control_fault") == fault
            assert not starts(b.rig) and not b.stops()
        finally:
            await stop_watcher(watcher)


# Intent: rejected fault-zero writes share one elapsed retry budget between the watcher and foreground ticks.
# Why: independent deadline protection cannot flood the charger or evade its cooldown through health polling.
async def test_fault_zero_retry_is_shared_by_watchdog_and_tick(tmp_path, tls, unused_tcp_port, monkeypatch):
    elapsed = SimpleNamespace(now=0)
    monkeypatch.setattr(managed, "time", SimpleNamespace(time=time.time, monotonic=lambda: elapsed.now))
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        local, fault = await fault_positive_timer(b, 6)
        answer = b.device.answer
        zeros = []

        # Reject only actual station-zero requests at the independent firmware boundary.
        def reject_zero(action, payload):
            if action == "SetChargingProfile":
                limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit == 0:
                    zeros.append(copy.deepcopy(payload))
                    return {"status": "Rejected"}
            return answer(action, payload)

        monkeypatch.setattr(b.device, "answer", reject_zero)
        watcher = asyncio.create_task(b.c.schedule.watch_deadline())
        try:
            await until(lambda: len(zeros) == 1)
            for _ in range(5):
                assert (await b.c.tick())["state"] == "attention"
                b.c.schedule.changed.set()
                await asyncio.sleep(0)
            assert len(zeros) == 1 and b.device.current == 6
            elapsed.now = 16
            await b.c.tick()
            await until(lambda: len(zeros) == 2)
            for _ in range(3):
                await b.c.tick()
            assert len(zeros) == 2
            assert b.session.journal.get("managed_control_fault") == fault
            assert b.session.journal.active() == local and not b.stops() and not starts(b.rig)
        finally:
            await stop_watcher(watcher)


# Intent: a changed ownership receipt during a real zero ACK cannot become a confirmed fault-hold receipt.
# Why: the only allowed row transitions are the captured preimage and this sequence's own exact zero transitions.
async def test_fault_zero_rejects_unrelated_receipt_change_during_real_reply(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        local, fault = await fault_positive_timer(b, 6)
        entered, release = asyncio.Event(), asyncio.Event()

        # The independent physical device has applied zero but deliberately withholds its genuine TLS response.
        async def gate_reply(action, payload):
            if action == "SetChargingProfile":
                limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit == 0:
                    entered.set()
                    await release.wait()

        b.rig.bench.before_reply = gate_reply
        watcher = asyncio.create_task(b.c.schedule.watch_deadline())
        try:
            await asyncio.wait_for(entered.wait(), 1)
            assert b.device.current == 0
            b.session.journal.db.execute(
                "UPDATE trial_resources SET desired='{}',confirmed='{}',state='confirmed' WHERE key='profile:7001'"
            )
            before = len(b.rig.bench.device_calls)
            release.set()
            await asyncio.sleep(0.05)
            assert len(b.rig.bench.device_calls) == before  # The old sequence cannot proceed to composite confirmation.
            assert not b.session.gateway.controls.ready
            assert b.session.journal.get("managed_control_fault") == fault
            assert b.session.journal.active() == local and not b.stops() and not starts(b.rig)
        finally:
            release.set()
            await stop_watcher(watcher)
