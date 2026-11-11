"""An authenticated one-off native timer on an independently observed charger and actual TLS transports."""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import pytest
from ocpp_hub_core.prototype import managed_schedule
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings
from ocpp_hub_core.prototype.managed_current import capability_value
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError
from websockets.asyncio.client import connect

from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import NativeVehiclePeer
from tests.test_managed_api import TOKEN, call
from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session, starts
from tests.test_prototype import BOOT, START
from tests.test_prototype_tls import tls as tls

ORIGINAL_PLAN = {
    "planId": 9,
    "type": "Plan",
    "idTag": "NATIVE_TIMER",
    "eco": "Fast",
    "stopModel": "Duration",
    "stopValue": 360,
    "limitCurrent": 320,
    "ConnectorId": 1,
    "validTime": "2020-01-01T00:00:00Z",
    "startTime": 0,
    "kindType": "Recurring",
    "recurring": 127,
    "maxEnergy": 0,
}


class SchedulePeer(NativeVehiclePeer):
    """The physical getter uses tenths of amps and different connector casing from its OCPP request."""

    # Seed the device with actual mutable wire-format daily settings before the baseline is read.
    def __init__(self, bench: Any) -> None:
        super().__init__(bench)
        wire = dict(ORIGINAL_PLAN)
        wire.update(connectorId=wire.pop("ConnectorId"), limitCurrent=wire["limitCurrent"] / 10)
        del wire["maxEnergy"]
        bench.firmware.plans[9] = wire

    # Simulate the complete native envelope and its independent representation, including fields never sent on OCPP.
    def native(self, key: str) -> Any:
        value = super().native(key)
        if key == "SETPLAN":
            plans = []
            for raw in value["planList"]:
                plan = dict(raw)
                plan.update(
                    ConnectorId=plan.pop("connectorId"),
                    limitCurrent=int(plan["limitCurrent"] * 10),
                    stopValue=int(plan["stopValue"]),
                    maxEnergy=0,
                )
                plans.append(plan)
            return {"planList": plans, "defaultEco": {"mode": "Fast"}}
        return value


# Intent: prove add/readback, actual scheduled Start/Stop, missed starts, superseding Pause, and selective cleanup.
# Why: a timed RemoteStart or an ACK without independent complete native readback cannot establish a native schedule.
@pytest.mark.parametrize(
    "outcome",
    [
        "charge",
        "full",
        "missed",
        "pause",
        "unexpected",
        "replacement",
        "release-deadline",
        "offline-replacement",
        "lost-ack",
        "clear-lost",
        "preempt",
        "unplug",
        "other-plan",
        "untyped-plan",
        "disappeared",
    ],
)
async def test_temporary_native_schedule(
    tmp_path: Path, tls: Any, unused_tcp_port_factory: Any, monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", SchedulePeer)
    monkeypatch.setitem(native_bench.ORIGINAL, "timeZone", "Europe/London")
    logical = datetime(2026, 11, 11, 3, 42, tzinfo=UTC).timestamp()
    monkeypatch.setattr(managed_schedule, "time", SimpleNamespace(time=lambda: logical))
    target = int(logical) + 180
    async with managed_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as rig:
        s = managed_session(rig)
        await s.start()
        c = ManagedController(s)
        rig.route_to_trial()
        async with rig.charger(s) as sock:
            await introduced(rig, s, sock, "timer-boot")
            await healthy(c)
            s.journal.put("managed_current_capability", capability_value(s.journal, rig.spec))
            rig.peer.configuration["AmpLmt"] = "32"
            rig.peer.runtime_user = 320
            s.gateway.policy = replace(s.gateway.policy, max_current_a=32)
            s.gateway.controls.policy = s.gateway.policy
            await healthy(c)
            if outcome == "charge":
                c.current.accept(uuid.uuid4().hex, c.requests.intent().revision, 32)
                await c.current.settle()
                assert (await healthy(c))["station_ceiling_a"] == 32
            identifier = uuid.uuid4().hex
            port = unused_tcp_port_factory()
            api = ManagementServer(ManagementSettings(port, tmp_path / "no-token", TOKEN), c, lambda: {})
            await api.start()
            try:
                async with connect(
                    f"ws://127.0.0.1:{port}/management",
                    additional_headers={"Authorization": "Bearer " + TOKEN},
                    subprotocols=["ics-proxy.v1"],
                ) as client:
                    args = {
                        "id": identifier,
                        "command": "temporary_schedule",
                        "revision": c.requests.intent().revision,
                        "start_epoch": target,
                    }
                    overlap = target + 12 * 3600
                    refused = await call(client, **(args | {"start_epoch": overlap}))
                    assert not refused["ok"] and not c.schedule.summary()
                    reply = await call(client, **args)
                    assert reply["ok"] and reply["result"]["state"] == "accepted"
                    assert await call(client, **args) == reply
                    assert not (await call(client, **(args | {"start_epoch": target + 60})))["ok"]
            finally:
                await api.close()
            old = s.gateway.charger.sender
            assert old is not None
            if outcome == "lost-ack":
                # Apply the outgoing request to the independent firmware, then model a disconnected reply path.
                async def lose(raw: str) -> None:
                    f = json.loads(raw)
                    if f[0] == 2 and f[2] == "DataTransfer" and f[3]["messageId"] == "setPlan":
                        rig.bench.firmware.answer(f[2], f[3])
                        raise OSError("lost response")
                    await old(raw)

                s.gateway.charger.sender = lose
            if outcome == "unplug":
                original_validate = s.gateway.charger.validate_send
                assert original_validate is not None

                # The cable is removed after request acceptance but before the final independent device read.
                async def disconnect_vehicle(action: str, payload: dict[str, Any]) -> None:
                    if action == "DataTransfer" and payload.get("messageId") == "setPlan":
                        rig.bench.cp, rig.bench.connection = "A", "Unplug"
                    await original_validate(action, payload)

                s.gateway.charger.validate_send = disconnect_vehicle
                await healthy(c)
                assert c.schedule.summary()["state"] == "complete"
                rig.bench.cp, rig.bench.connection = "B", "Plug"
                s.gateway.charger.validate_send = original_validate
                await healthy(c)
                assert c.schedule.summary()["result"] == "cancelled_before_install"
                assert all(f[2] != "DataTransfer" or f[3].get("messageId") != "setPlan" for f in rig.bench.device_calls)
                assert set(rig.bench.firmware.plans) == {9}
                return
            if outcome == "preempt":
                c.command(uuid.uuid4().hex, "pause", 0)
                await healthy(c)
                assert c.schedule.summary()["result"] == "cancelled_before_install" and set(
                    rig.bench.firmware.plans
                ) == {9}
                return
            await healthy(c)
            s.gateway.charger.sender = old
            await healthy(c)
            lease = s.journal.get(managed_schedule.KEY)
            assert lease["state"] == "armed" and lease["body"]["planId"] != 9
            timer = lease["body"]["planId"]
            assert lease["body"]["startTime"] == 3 * 60 + 45
            assert lease["body"]["stopValue"] == 135 and lease["end_epoch"] == target + 135 * 60
            assert lease["body"]["limitCurrent"] == (320 if outcome == "charge" else 60)
            assert set(rig.bench.firmware.plans) == {9, timer} and not starts(rig)
            assert s.gateway.policy.extensions.get(("IoCharger", "setPlan")) is None
            before = (await rig.prepared.observer.observe_handover()).saved_schedule
            assert before["planList"] == [ORIGINAL_PLAN, lease["body"]]
            if outcome == "unexpected":
                rig.bench.firmware.plans[timer]["stopValue"] += 1
                with pytest.raises(SecurityError):
                    await c.native.read()
                return
            if outcome == "disappeared":
                del rig.bench.firmware.plans[timer]
                await healthy(c)
                assert c.schedule.summary()["result"] == "disappeared_before_start"
                return
            if outcome in ("charge", "full", "other-plan", "untyped-plan"):
                logical = target
                start = START | {"idTag": "NATIVE_TIMER", "timestamp": datetime.fromtimestamp(logical, UTC).isoformat()}
                answer = await rig.bench.call(sock, "actual-timer", "StartTransaction", start)
                local = answer[2]["transactionId"]
                rig.peer.plan_session = (
                    local,
                    9 if outcome == "other-plan" else float(timer) if outcome == "untyped-plan" else timer,
                )
                if outcome == "charge":
                    rig.peer.paused = False
                    rig.peer.permission_enabled = True
                    rig.bench.cp, rig.bench.connection = "C", "Charging"
                    rig.bench.firmware.fire_plan(timer)
                if outcome == "untyped-plan":
                    for _ in range(3):
                        status = await c.tick()
                    assert rig.bench.firmware.effective_limit() == 0 and status["state"] == "reconciling"
                else:
                    status = await healthy(c)
                assert bool(c.schedule.summary().get("native_plan_confirmed")) == (outcome in ("full", "charge"))
                assert (rig.bench.firmware.current > 0) == (outcome == "charge")
                assert timer in rig.bench.firmware.plans
                rig.peer.permission_enabled = False
                rig.bench.cp, rig.bench.connection = "B", "Plug"
                logical += 120
                await rig.bench.call(
                    sock,
                    "finished",
                    "StopTransaction",
                    {
                        "transactionId": local,
                        "meterStop": start["meterStart"],
                        "timestamp": datetime.fromtimestamp(logical, UTC).isoformat(),
                    },
                )
            elif outcome == "pause":
                c.command(uuid.uuid4().hex, "pause", 0)
                assert c.pause_task is not None
                await c.pause_task
                logical = target + 1
                with pytest.raises(RecoveryRequiredError):
                    c.command(uuid.uuid4().hex, "start", c.requests.intent().revision)
                assert timer in rig.bench.firmware.plans and rig.bench.firmware.effective_limit() == 0
            elif outcome not in ("replacement", "release-deadline", "offline-replacement"):
                logical = target + 601
            if outcome in ("other-plan", "untyped-plan"):
                if outcome == "untyped-plan":
                    await c.tick()
                    assert rig.bench.firmware.effective_limit() == 0
                else:
                    await healthy(c)
                assert c.schedule.summary()["state"] == "armed" and timer in rig.bench.firmware.plans
                logical = target + 601
            if outcome == "clear-lost":
                # The device removes the named plan and loses only its confirmation.
                async def lose_clear(raw: str) -> None:
                    f = json.loads(raw)
                    if f[0] == 2 and f[2] == "DataTransfer" and f[3]["messageId"] == "clearPlans":
                        rig.bench.firmware.answer(f[2], f[3])
                        raise OSError("lost cleanup response")
                    await old(raw)

                s.gateway.charger.sender = lose_clear
            if outcome not in ("replacement", "release-deadline", "offline-replacement"):
                for _ in range(4):
                    await healthy(c)
                last = c.schedule.summary()
                assert last["state"] == "complete" and set(rig.bench.firmware.plans) == {9} and not starts(rig)
                assert last["result"] == {
                    "pause": "missed_while_held",
                    "charge": "session_closed",
                    "full": "session_closed",
                    "other-plan": "session_source_unverified",
                    "untyped-plan": "session_source_unverified",
                }.get(outcome, "no_scheduled_session")
                assert (
                    await rig.prepared.observer.observe_handover()
                ).saved_schedule_digest == rig.spec.saved_schedule_digest
                return
        await s.close()
        replacement = managed_session(rig)
        if outcome == "offline-replacement":
            transport = replacement.runtime.transport
            url = urlsplit(transport.config.vendor_url)
            transport.config = replace(
                transport.config,
                vendor_url=urlunsplit(url._replace(netloc=f"127.0.0.1:{unused_tcp_port_factory()}")),
            )
        await replacement.start()
        logical = target - 2 if outcome == "release-deadline" else target + 601
        c = ManagedController(replacement)
        wire_offset = len(rig.bench.device_calls)
        validate = replacement.gateway.charger.validate_send
        assert validate is not None

        # Move the clock inside the final asynchronous validation of the positive write, after the release claim.
        async def cross_deadline(action: str, body: dict[str, Any]) -> None:
            nonlocal logical
            await validate(action, body)
            if outcome == "release-deadline" and action == "SetChargingProfile":
                if body["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] > 0:
                    logical = target + 1

        replacement.gateway.charger.validate_send = cross_deadline
        async with rig.charger(replacement) as sock:
            if outcome == "offline-replacement":
                assert (await rig.bench.call(sock, "offline-replacement-boot", "BootNotification", BOOT))[0] == 3
                async with asyncio.timeout(6):
                    while True:
                        status = await c.tick()
                        if status["state"] == "local_only" and c.schedule.summary()["state"] == "complete":
                            break
                        await asyncio.sleep(0.02)
                assert status["local_hold_verified"] and status["station_ceiling_a"] == 0
                assert set(rig.bench.firmware.plans) == {9} and not starts(rig)
                return
            await introduced(rig, replacement, sock, "replacement-boot")
            for _ in range(4):
                await healthy(c)
            assert c.schedule.summary()["state"] == "complete" and set(rig.bench.firmware.plans) == {9}
            assert c.schedule.summary()["result"] == "missed_while_held"
            writes = [
                f for f in rig.bench.device_calls if f[2] == "DataTransfer" and f[3].get("messageId") == "setPlan"
            ]
            assert len(writes) == 1 and not starts(rig)
            wire = rig.bench.device_calls[wire_offset:]
            clear = next(i for i, f in enumerate(wire) if f[2] == "DataTransfer" and f[3]["messageId"] == "clearPlans")
            positive = next(
                i
                for i, f in enumerate(wire)
                if f[2] == "SetChargingProfile"
                and f[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] > 0
            )
            assert clear < positive
