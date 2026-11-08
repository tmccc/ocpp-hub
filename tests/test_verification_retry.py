"""Bounded read-only retries against independent fake-device replies and the real ownership controller."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from ocpp_hub_core.frames import build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.policy import DeniedError, Policy
from ocpp_hub_core.prototype.rpc import UnavailableError

from tests.test_prototype import BOOT, Rig
from tests.test_trial_auth import StrictRig


@dataclass
class Replies:
    """Only the fake device controls schedule timing and reply loss."""

    mode: str
    queries: int = 0
    first_reply: asyncio.Event = field(default_factory=asyncio.Event)


# Keep all real controller calls intact while the synthetic peer supplies its own clock and effective schedule.
def attach(rig: Rig, replies: Replies) -> None:
    original = rig.charger_send

    # Answer each correlated read independently, including a deliberately stalled second read.
    async def peer(raw: str) -> None:
        frame = decode(raw)
        if frame[0] != 2 or frame[2] != "GetCompositeSchedule":
            await original(raw)
            return
        replies.queries += 1
        rig.charger_frames.append(frame)
        answer = rig.firmware.answer(frame[2], frame[3])
        if replies.mode == "stall" and replies.queries == 2:
            return
        future = replies.mode not in ("settle", "stall") or replies.queries == 1
        if future:
            ahead = 1.2 if replies.mode == "large" else 0.154274
            answer["scheduleStart"] = (datetime.now(UTC) + timedelta(seconds=ahead)).isoformat()
        if replies.mode == "expired":
            answer["scheduleStart"] = (datetime.now(UTC) - timedelta(seconds=61)).isoformat()
        elif replies.mode == "high":
            answer["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] = 32
        elif replies.mode == "malformed":
            answer["chargingSchedule"]["chargingSchedulePeriod"][0]["numberPhases"] = True
        await rig.gateway.receive("charger", build_call_result(frame[1], answer))
        replies.first_reply.set()

    rig.gateway.attach_charger(peer)


# Intent: a fresh present-time response can recover a small clock offset without repeating the accepted profile write.
# Why: the observed 154 ms startup offset must not require a new charging mutation or weaker coverage checks.
async def test_small_future_reply_retries_only_read(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "state.db", Policy(max_current_a=6))
    replies = Replies("settle")
    attach(rig, replies)
    try:
        await rig.gateway.controls.ensure_ceiling()
        assert rig.gateway.controls.ready and replies.queries == 2
        assert [frame[2] for frame in rig.charger_frames if frame[0] == 2] == [
            "SetChargingProfile",
            "GetCompositeSchedule",
            "GetCompositeSchedule",
        ]
    finally:
        await rig.close()


# Intent: only bounded small future offsets retry; persistent skew exhausts three reads without another write.
# Why: malformed, unsafe, expired or substantially future evidence is not permission to repeat a physical mutation.
@pytest.mark.parametrize(
    "mode,queries", [("persistent", 3), ("large", 1), ("expired", 1), ("high", 1), ("malformed", 1)]
)
async def test_invalid_or_persistent_schedule_remains_unready(tmp_path: Path, mode: str, queries: int) -> None:
    rig = Rig(tmp_path / "state.db", Policy(max_current_a=6))
    replies = Replies(mode)
    attach(rig, replies)
    try:
        with pytest.raises(UnavailableError):
            await rig.gateway.controls.ensure_ceiling()
        assert not rig.gateway.controls.ready and replies.queries == queries
        assert sum(frame[0] == 2 and frame[2] == "SetChargingProfile" for frame in rig.charger_frames) == 1
    finally:
        await rig.close()


# Intent: a stalled retry is bounded by the shared retry budget, not the peer's ordinary five-second timeout.
# Why: an extra read must not hold the outgoing safety gate for another full normal request timeout.
async def test_retry_deadline_includes_rpc_wait(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "state.db", Policy(max_current_a=6))
    rig.gateway.charger.timeout = 5
    replies = Replies("stall")
    attach(rig, replies)
    try:
        started = asyncio.get_running_loop().time()
        async with asyncio.timeout(1.8):
            with pytest.raises(UnavailableError):
                await rig.gateway.controls.ensure_ceiling()
        assert asyncio.get_running_loop().time() - started < 1.6
        assert replies.queries == 2 and not rig.gateway.controls.ready
        assert not rig.gateway.charger._pending
    finally:
        await rig.close()


# Intent: a Stop, same-value new operation or changed device introduction supersedes an older retry sequence.
# Why: retrying a read cannot retain authority after a new owner ticket, socket or Boot has taken precedence.
@pytest.mark.parametrize("change", ["stop", "same-limit", "desired", "epoch", "boot"])
async def test_authority_change_during_retry_aborts_old_confirmation(tmp_path: Path, change: str) -> None:
    rig = Rig(tmp_path / "state.db", Policy(max_current_a=6))
    replies = Replies("settle")
    attach(rig, replies)
    try:
        await rig.boot()
        old = asyncio.create_task(rig.gateway.controls.ensure_ceiling())
        await replies.first_reply.wait()
        await asyncio.sleep(0.05)
        newer = None
        if change == "stop":
            newer = asyncio.create_task(rig.gateway.stop())
        elif change == "same-limit":
            newer = asyncio.create_task(rig.gateway.set_limit(6))
        elif change == "desired":
            rig.store.put("control", {"inhibit": True})
        elif change == "epoch":
            rig.gateway.attach_charger(rig.charger_send)
        else:
            await rig.charger("changed", "BootNotification", {**BOOT, "firmwareVersion": "changed"})
        with pytest.raises((DeniedError, UnavailableError)):
            if change == "desired":
                # A persisted safety hold is noticed during the short wait, before waiting for the next read.
                await asyncio.wait_for(old, 0.12)
            else:
                await old
        if newer is not None:
            assert (await newer)[2]["status"] == "Accepted"
            assert rig.gateway.controls.ready
            assert rig.firmware.effective_limit() == (0 if change == "stop" else 6)
        else:
            assert not rig.gateway.controls.ready and replies.queries == 1
        assert not any(frame[0] == 2 and frame[2] == "RemoteStartTransaction" for frame in rig.charger_frames)
    finally:
        await rig.close()


# Intent: Stop during a strict Authorize retry returns a denied protocol status rather than an unrelated CALLERROR.
# Why: revoking an older ceiling operation must retain the charger's established authorization response shape.
async def test_stop_during_retry_denies_authorize_with_normal_status(tmp_path: Path) -> None:
    rig = StrictRig(tmp_path / "state.db")
    replies = Replies("settle")
    attach(rig, replies)
    try:
        await rig.boot()
        await rig.cloud()
        pending = asyncio.create_task(rig.charger("auth", "Authorize", {"idTag": "LOCAL_TEST"}))
        await replies.first_reply.wait()
        stop = asyncio.create_task(rig.gateway.stop())
        answer = await pending
        assert answer[0] == 3 and answer[2]["idTagInfo"]["status"] != "Accepted"
        assert (await stop)[2]["status"] == "Accepted"
        assert rig.firmware.effective_limit() == 0
    finally:
        await rig.close()


# Intent: releasing a zero hold may verify a positive ceiling while the original inhibit remains set.
# Why: a universal desired-equals-requested check would break the existing guarded release sequence.
async def test_retry_preserves_release_from_zero_hold(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "state.db", Policy(max_current_a=6))
    try:
        await rig.gateway.stop()
        assert rig.gateway.controls.desired() == 0
        replies = Replies("settle")
        attach(rig, replies)
        await rig.gateway.release()
        assert rig.gateway.controls.ready and rig.gateway.controls.desired() == 6
        assert not rig.store.get("control")["inhibit"] and replies.queries == 2
        assert not any(frame[0] == 2 and frame[2] == "RemoteStartTransaction" for frame in rig.charger_frames)
    finally:
        await rig.close()
