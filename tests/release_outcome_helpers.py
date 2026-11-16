"""Real peer witnesses and explicit owner recovery after a possibly positive automatic release."""

import copy
import json
import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from ocpp_hub_core.prototype.release_attempt import ReleaseOutcome
from ocpp_hub_core.prototype.store import ReleaseDeferredError

from tests.test_managed_proxy import healthy, starts


# Record only real replies sent by the independent TLS peer, always forwarding their original bytes unchanged.
def observe_device_replies(b, monkeypatch):
    send, replies = b.sock.send, []

    # Retain the reply only after the real socket send succeeds; no engine callback or verdict is replaced.
    async def observed(raw):
        await send(raw)
        frame = json.loads(raw)
        if frame[0] in (3, 4):
            replies.append(copy.deepcopy(frame))

    monkeypatch.setattr(b.sock, "send", observed)
    return replies


# Observe the actual owned attempt only after the independent firmware has applied its positive profile.
def applied_release_attempt(b, selected):
    attempt = b.session.gateway.controls.release_attempt
    assert attempt is not None and attempt.purpose == "automatic" and not attempt.finished
    assert attempt.positive_may_have_dispatched and not attempt.positive_confirmed
    assert b.device.effective_limit() == selected
    assert attempt.ticket == b.session.journal.get("control")["ticket"]
    return attempt


# A genuine positive ACK and confirmed later zero cannot reclassify the whole failed operation as unsent.
def assert_positive_rollback(b, attempt, wire, replies, selected):
    assert attempt.finished and not attempt.cancelled and not attempt.known_unsent()
    assert attempt.positive_may_have_dispatched and not attempt.positive_confirmed
    assert attempt.zero_may_have_dispatched and attempt.zero_confirmed
    assert type(attempt.positive_failure) is ReleaseDeferredError and attempt.failure is attempt.positive_failure
    assert attempt.outcome() is ReleaseOutcome.POSSIBLE_POSITIVE
    calls = b.rig.bench.device_calls[wire:]
    profiles = [frame for frame in calls if frame[2] == "SetChargingProfile"]
    limits = [
        frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] for frame in profiles
    ]
    assert len(limits) >= 2 and limits[0] == selected and all(value == 0 for value in limits[1:])
    assert [3, profiles[0][1], {"status": "Accepted"}] in replies
    zero_composites = [
        frame
        for frame in replies
        if frame[0] == 3
        and any(call[1] == frame[1] and call[2] == "GetCompositeSchedule" for call in calls)
        and frame[2].get("chargingSchedule", {}).get("chargingSchedulePeriod") == [{"startPeriod": 0, "limit": 0}]
    ]
    assert zero_composites and b.device.effective_limit() == 0
    assert b.session.gateway.controls.release_attempt is None
    fault = b.session.journal.get("managed_control_fault")
    assert fault["classification"] == "owner_release_unconfirmed"
    assert b.session.journal.get("control")["uncertain"] is True
    assert not b.stops() and not starts(b.rig)


# Recovered HTTP alone retains the fault; new owner Pause, real closure and separate Return prove recovery.
async def explicit_release_recovery(b, local, selected):
    c, j = b.c, b.session.journal
    fault = copy.deepcopy(j.get("managed_control_fault"))
    original, configuration = copy.deepcopy(b.device.plans[9]), copy.deepcopy(b.rig.peer.configuration)
    mark = len(b.rig.bench.device_calls)
    for _ in range(2):
        assert (await c.tick())["state"] == "attention" and j.get("managed_control_fault") == fault
        assert b.device.effective_limit() == 0 and j.active() == local and not b.stops()
    assert all(
        frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
        for frame in b.rig.bench.device_calls[mark:]
        if frame[2] == "SetChargingProfile"
    )
    pause = uuid.uuid4().hex
    c.command(pause, "pause", c.requests.intent().revision)
    if c.pause_task is not None:
        await c.pause_task
    await c.tick()
    assert set(b.stops()) == {local} and b.device.effective_limit() == 0
    assert j.active() == local and j.get("managed_control_fault") == fault
    assert c.requests.receipt(pause)["state"] != "completed"
    await b.close_transaction(local)
    for _ in range(12):
        await c.tick()
        assert b.device.effective_limit() == 0
        if not j.get("managed_control_fault"):
            break
    paused = await healthy(c)
    assert not j.get("managed_control_fault") and c.requests.intent().mode == "paused"
    assert paused["station_ceiling_a"] == 0 and c.current.selected() == selected
    assert c.requests.receipt(pause)["state"] == "completed"
    assert b.device.plans == {9: original} and b.rig.peer.configuration == configuration
    returning = uuid.uuid4().hex
    c.command(returning, "app_control", c.requests.intent().revision)
    assert (await healthy(c))["station_ceiling_a"] == selected
    assert c.requests.intent().mode == "app" and c.requests.receipt(returning)["state"] == "completed"
    day = datetime.fromtimestamp(b.clock.now, ZoneInfo("Europe/London"))
    midnight = int((day + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    following = await b.advance_idle_to(midnight)
    assert following not in (None, local) and (await healthy(c))["current_a"] == selected
    await b.close_transaction(following)
    await healthy(c)
    assert set(b.stops()) == {local} and not starts(b.rig)
    assert b.device.plans == {9: original} and b.rig.peer.configuration == configuration
