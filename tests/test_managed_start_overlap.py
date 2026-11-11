"""Expired normal Starts retain their own lifecycle debt without borrowing daily Stop authority."""

from __future__ import annotations

import asyncio
import copy
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.prototype.managed import ManagedController

from tests.handover_helpers import crash
from tests.test_managed_proxy import healthy, introduced, managed_session, starts
from tests.test_managed_schedule_cutoff import ZONE, TimerBench, timer_bench
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Keep the owner command distinct from the unchanged, independently scheduled native daily identity.
def owner_tag(b: TimerBench) -> None:
    g = b.session.gateway
    g.policy = replace(g.policy, allowed_tags=frozenset({"APP_OWNER", *g.policy.allowed_tags}))
    g.controls.policy = g.policy


# Expire only the monotonic wait after real command dispatch, ACK and independent native observations.
async def expire(b: TimerBench) -> None:
    lease = b.c.tests.lease
    assert lease is not None
    lease.deadline = asyncio.get_running_loop().time() - 1
    b.c.tests.changed.set()
    await until(lambda: b.c.tests.timer is not None and b.c.tests.timer.done())
    if b.c.pause_task is not None:
        await b.c.pause_task


# Submit the delayed real owner lifecycle using the exact tag from its single physical Start command.
async def delayed_owner(b: TimerBench) -> int:
    b.clock.now += 60
    result = await b.rig.bench.call(
        b.sock,
        uuid.uuid4().hex,
        "StartTransaction",
        START
        | {
            "idTag": starts(b.rig)[0][3]["idTag"],
            "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat(),
        },
    )
    assert result[0] == 3, result
    local = result[2]["transactionId"]
    b.rig.peer.plan_session = (local, 987654)
    return int(local)


# Check the two independent reports separately; neither zero power nor one closure report retires the owner.
async def close_in_order(b: TimerBench, owner: int, first: str, returning: str | None = None) -> None:
    if first == "native":
        b.device.active = None
    else:
        await b.close_transaction(owner)
        b.device.active = owner
    for _ in range(2):
        await b.c.tick()
    assert b.device.effective_limit() == 0
    if returning is not None:
        assert b.c.requests.receipt(returning)["state"] == "accepted"
    if first == "native":
        await b.close_transaction(owner)
    else:
        b.device.active = None


# Intent: daily before/after Start ACK survives normal startup expiry, following nights and late owner closure.
# Why: an unrelated command's timeout must not acquire durable Pause or Stop authority over a real native daily.
@pytest.mark.parametrize("before_ack", [False, True])
@pytest.mark.parametrize("demand", [False, True])
@pytest.mark.parametrize("selected", [6, 32])
@pytest.mark.parametrize("completed_timer", [False, True])
async def test_daily_survives_owner_start_timeout_and_following_nights(
    tmp_path: Path,
    tls: Any,
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    before_ack: bool,
    demand: bool,
    selected: int,
    completed_timer: bool,
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        if completed_timer:
            await b.arm()
            b.clock.now = b.start + 601
            await healthy(b.c)
            assert b.c.schedule.summary()["state"] == "complete" and set(b.device.plans) == {9}
        owner_tag(b)
        if selected != 6:
            b.c.command(uuid.uuid4().hex, "set_current", b.c.requests.intent().revision, current_a=selected)
            await b.c.current.settle()
            await healthy(b.c)
        original = copy.deepcopy(b.device.plans)
        daily_job = None

        # Deliver a genuine independent Start while the actual RemoteStart ACK is still withheld.
        async def before_reply(action: str, payload: dict[str, Any]) -> None:
            nonlocal daily_job
            if before_ack and action == "RemoteStartTransaction":
                daily_job = asyncio.create_task(b.start_plan(9, demand=demand))
                await until(lambda: b.session.journal.active() is not None)

        b.rig.bench.before_reply = before_reply
        request = uuid.uuid4().hex
        b.c.command(request, "start", b.c.requests.intent().revision)
        await b.c.tests.settle()
        b.rig.bench.before_reply = None
        daily = await daily_job if daily_job is not None else await b.start_plan(9, demand=demand)
        status = await healthy(b.c)
        assert status["current_a"] == (selected if demand else 0)
        intent = b.c.requests.intent()
        receipt = b.session.journal.get("local_start_receipt")
        assert receipt["local"] is None and receipt["stage"] == "accepted"
        await expire(b)
        assert not b.stops()
        assert b.c.requests.intent() == intent and intent.mode == "app"
        status = await healthy(b.c)
        assert b.c.requests.intent() == intent and intent.mode == "app"
        assert status["station_ceiling_a"] == b.c.current.selected() == selected
        assert b.session.journal.get("control")["start_pending"]
        assert b.session.journal.get("local_start_receipt") == receipt
        assert b.c.requests.receipt(request)["state"] == "uncertain"
        assert not b.stops()

        await b.close_transaction(daily)
        await healthy(b.c)
        following = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
        assert following is not None and following != daily
        assert (await healthy(b.c))["current_a"] == selected
        assert b.session.journal.get("control")["start_pending"] and not b.stops()
        await b.close_transaction(following)
        await healthy(b.c)

        owner = await delayed_owner(b)
        assert b.device.effective_limit() == 0
        assert b.session.journal.get("local_start_receipt")["local"] == owner
        for _ in range(3):
            await b.c.tick()
        assert set(b.stops()) == {owner} and b.device.effective_limit() == 0
        await close_in_order(b, owner, "native" if before_ack else "ledger")
        assert (await healthy(b.c))["station_ceiling_a"] == selected
        assert b.c.requests.intent() == intent
        next_night = await b.advance_idle_to(int(datetime(2026, 11, 13, tzinfo=ZONE).timestamp()))
        assert next_night not in (None, daily, following, owner)
        assert (await healthy(b.c))["current_a"] == selected
        assert set(b.stops()) == {owner} and len(starts(b.rig)) == 1
        assert b.device.plans == original


# Intent: the first Return after automatic timeout stays queued until both genuine owner closure reports agree.
# Why: automatic Pause has no explicit Pause receipt; that absence must not consume an already accepted Return.
@pytest.mark.parametrize("first", ["native", "ledger"])
@pytest.mark.parametrize("native_first", [False, True])
async def test_automatic_pause_return_survives_delayed_owner_and_both_closure_orders(
    tmp_path: Path,
    tls: Any,
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    first: str,
    native_first: bool,
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
        await b.c.tests.settle()
        if native_first:
            provisional = b.session.journal.get("next_transaction") + 1
            b.device.active = provisional
            b.rig.peer.plan_session = (provisional, 987654)
            await b.c.native.read()
        await expire(b)
        assert b.c.requests.intent().mode == "paused" and b.device.effective_limit() == 0
        assert not b.stops()
        assert b.session.journal.active() is None
        returning = uuid.uuid4().hex
        b.c.command(returning, "app_control", b.c.requests.intent().revision)
        await b.c.tick()
        assert b.c.requests.receipt(returning)["state"] == "accepted"
        owner = await delayed_owner(b)
        for _ in range(3):
            await b.c.tick()
        assert set(b.stops()) == {owner} and b.device.effective_limit() == 0
        assert b.c.requests.receipt(returning)["state"] == "accepted"
        await close_in_order(b, owner, first, returning)
        for _ in range(5):
            await b.c.tick()
        status = await healthy(b.c)
        assert b.c.requests.receipt(returning)["state"] == "completed", {
            "receipt": b.c.requests.receipt(returning),
            "owner": b.session.journal.get("local_start_receipt"),
            "control": b.session.journal.get("control"),
            "status": status,
        }
        assert status["station_ceiling_a"] == 6
        assert b.c.requests.receipt(returning)["state"] == "completed"
        assert b.c.requests.intent().mode == "app"
        following = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
        assert following not in (None, owner) and (await healthy(b.c))["current_a"] == 6
        assert set(b.stops()) == {owner} and len(starts(b.rig)) == 1


# Intent: an unidentified native source at expiry holds zero and cannot borrow a transaction's Stop authority.
# Why: a different OCPP tag or zero measured draw alone does not positively identify an independent daily.
@pytest.mark.parametrize("demand", [False, True])
async def test_unidentified_daily_at_timeout_holds_zero_without_guessed_stop(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch, demand: bool
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        owner_tag(b)
        b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
        await b.c.tests.settle()
        local = await b.start_plan(9, demand=demand)
        b.rig.peer.plan_session = (local, 987654)
        await b.c.native.read()
        await expire(b)
        for _ in range(3):
            await b.c.tick()
        assert b.device.effective_limit() == 0 and not b.stops()
        assert b.session.journal.get("control")["start_pending"]
        assert b.session.journal.get("local_start_receipt")["local"] is None


# Intent: earlier positive daily evidence preserves app intent while expiry freshly verifies changed or stale sources.
# Why: a real sixty-second wait can outlive source freshness or the daily itself without authorizing a daily Stop.
@pytest.mark.parametrize("source", ["stale", "closed", "unknown"])
@pytest.mark.parametrize("demand", [False, True])
async def test_timeout_rechecks_previously_identified_daily_without_persistent_pause(
    tmp_path: Path,
    tls: Any,
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    source: str,
    demand: bool,
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        owner_tag(b)
        b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
        await b.c.tests.settle()
        daily = await b.start_plan(9, demand=demand)
        await healthy(b.c)
        intent = b.c.requests.intent()
        if source == "closed":
            await b.close_transaction(daily)
            await healthy(b.c)
        elif source == "unknown":
            b.rig.peer.plan_session = (daily, 987654)
            await b.c.native.read()
        else:
            b.c.native.last_verified_at = 0
        await expire(b)
        assert b.c.requests.intent() == intent and not b.stops()
        assert b.session.journal.get("control")["start_pending"]
        if source == "unknown":
            assert b.device.effective_limit() == 0
            b.rig.peer.plan_session = (daily, 9)
        assert (await healthy(b.c))["station_ceiling_a"] == 6
        if source != "closed":
            await b.close_transaction(daily)
            await healthy(b.c)
        following = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
        assert following not in (None, daily) and (await healthy(b.c))["current_a"] == 6
        assert not b.stops() and len(starts(b.rig)) == 1


# Intent: an explicit Pause still stops the independent daily and a newer Pause supersedes queued Return.
# Why: separating internal expiry authority must preserve the owner's unrestricted, durable Pause choice.
async def test_explicit_pause_still_owns_daily_and_supersedes_return(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        owner_tag(b)
        b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
        await b.c.tests.settle()
        daily = await b.start_plan(9)
        await healthy(b.c)
        await expire(b)
        assert not b.stops() and b.c.requests.intent().mode == "app"
        b.c.command(uuid.uuid4().hex, "pause", b.c.requests.intent().revision)
        assert b.c.pause_task is not None
        await b.c.pause_task
        assert daily in b.stops() and b.device.effective_limit() == 0
        await b.close_transaction(daily)
        returning = uuid.uuid4().hex
        b.c.command(returning, "app_control", b.c.requests.intent().revision)
        await b.c.tick()
        assert b.c.requests.receipt(returning)["state"] == "accepted"
        owner = await delayed_owner(b)
        for _ in range(3):
            await b.c.tick()
        assert owner in b.stops() and b.c.requests.receipt(returning)["state"] == "accepted"
        b.c.command(uuid.uuid4().hex, "pause", b.c.requests.intent().revision)
        await b.close_transaction(owner)
        assert (await healthy(b.c))["local_hold_verified"]
        assert b.c.requests.receipt(returning)["state"] == "failed"
        assert b.c.requests.intent().mode == "paused" and b.device.effective_limit() == 0
        retry = uuid.uuid4().hex
        b.c.command(retry, "app_control", b.c.requests.intent().revision)
        assert (await healthy(b.c))["station_ceiling_a"] == 6
        assert b.c.requests.receipt(retry)["state"] == "completed"
        assert len(starts(b.rig)) == 1


# Intent: explicit Pause during expiry's zero ACK defeats the pending independent daily continuation.
# Why: source preservation must never let an older internal recovery raise current past a newer owner Pause.
async def test_explicit_pause_preempts_expired_daily_restoration(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        owner_tag(b)
        b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
        await b.c.tests.settle()
        daily = await b.start_plan(9)
        await healthy(b.c)
        b.c.native.last_verified_at = 0
        paused_at = None

        # Pause while the physical zero has applied but the expiry task still awaits its actual ACK.
        async def pause_on_zero(action: str, payload: dict[str, Any]) -> None:
            nonlocal paused_at
            if action == "SetChargingProfile" and paused_at is None:
                assert payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
                paused_at = len(b.rig.bench.received_limits)
                b.c.command(uuid.uuid4().hex, "pause", b.c.requests.intent().revision)

        b.rig.bench.before_reply = pause_on_zero
        await expire(b)
        b.rig.bench.before_reply = None
        await b.c.tick()
        assert paused_at is not None and b.c.requests.intent().mode == "paused"
        assert all(limit == 0 for limit in b.rig.bench.received_limits[paused_at:])
        assert daily in b.stops() and b.device.effective_limit() == 0
        assert len(starts(b.rig)) == 1


# Intent: a source change at the actual positive send gate retains zero and later recovers the same daily.
# Why: an earlier positive observation cannot release an unknown owner or turn temporary deferral into a latched fault.
async def test_unknown_source_at_daily_release_gate_retains_zero_and_recovers(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        owner_tag(b)
        b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
        await b.c.tests.settle()
        daily = await b.start_plan(9)
        await healthy(b.c)
        b.c.native.last_verified_at = 0
        validate = b.session.gateway.charger.validate_send
        assert validate is not None
        changed = False

        # Keep the real native validator and alter only the independent physical source before its fresh read.
        async def change_before_positive(action: str, payload: dict[str, Any]) -> None:
            nonlocal changed
            if (
                action == "SetChargingProfile"
                and payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] > 0
            ):
                b.rig.peer.plan_session = (daily, 987654)
                changed = True
            await validate(action, payload)

        monkeypatch.setattr(b.session.gateway.charger, "validate_send", change_before_positive)
        await expire(b)
        assert changed and not b.stops() and b.device.effective_limit() == 0
        assert not b.session.journal.get("managed_control_fault")
        monkeypatch.setattr(b.session.gateway.charger, "validate_send", validate)
        b.rig.peer.plan_session = (daily, 9)
        assert (await healthy(b.c))["station_ceiling_a"] == 6
        assert not b.stops() and len(starts(b.rig)) == 1


# Intent: replacement reidentifies the same independent daily under zero and retains the separate late owner debt.
# Why: persisted expired authority must neither Stop the daily nor replay Start or silently discard owner reporting.
@pytest.mark.parametrize("demand", [False, True])
async def test_replacement_preserves_expired_daily_and_eventual_owner_closure(
    tmp_path: Path, tls: Any, unused_tcp_port: int, monkeypatch: pytest.MonkeyPatch, demand: bool
) -> None:
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        owner_tag(b)
        b.c.command(uuid.uuid4().hex, "start", b.c.requests.intent().revision)
        await b.c.tests.settle()
        daily = await b.start_plan(9, demand=demand)
        await healthy(b.c)
        await expire(b)
        intent = b.c.requests.intent()
        await b.c.tests.close()
        await crash(b.session)
        b.session = managed_session(b.rig)
        await b.session.start()
        b.c = ManagedController(b.session)
        owner_tag(b)
        async with b.rig.charger(b.session) as b.sock:
            await introduced(b.rig, b.session, b.sock, "expired-start-replacement")
            assert b.device.effective_limit() == 0
            assert (await healthy(b.c))["station_ceiling_a"] == 6
            assert b.c.requests.intent() == intent and not b.stops()
            assert b.session.journal.get("control")["start_pending"]
            assert b.session.journal.get("local_start_receipt")["local"] is None
            await b.close_transaction(daily)
            await healthy(b.c)
            owner = await delayed_owner(b)
            for _ in range(3):
                await b.c.tick()
            assert set(b.stops()) == {owner} and b.device.effective_limit() == 0
            await b.close_transaction(owner)
            assert (await healthy(b.c))["station_ceiling_a"] == 6
            following = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
            assert following not in (None, daily, owner) and (await healthy(b.c))["current_a"] == 6
            assert set(b.stops()) == {owner} and len(starts(b.rig)) == 1
