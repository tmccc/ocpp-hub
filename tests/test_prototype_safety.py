"""Independent firmware effects and persistent permissions at the offline control boundary."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.frames import build_call_error, build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.policy import DeniedError, Policy
from ocpp_hub_core.prototype.rpc import Peer, UnavailableError
from ocpp_hub_core.prototype.store import ConflictError, RecoveryRequiredError

from tests.test_prototype import START, Rig
from tests.test_prototype_ics_writes import PLAN, POLICY, plan, ready
from tests.test_prototype_runtime import until


# Intent: a native timer cannot charge after a local stop, even without a transaction or a new cloud CALL.
# Why: merely refusing future cloud writes does not inhibit a plan already stored in charger firmware.
async def test_stop_holds_autonomous_plan_across_restart(tmp_path: Path) -> None:
    path = tmp_path / "journal.db"
    rig = Rig(path)
    firmware = rig.firmware
    firmware.plans[1] = {"limitCurrent": 32}
    await rig.gateway.stop()
    firmware.fire_plan(1)
    assert firmware.current == 0
    assert rig.store.get("control")["inhibit"]
    assert not any(f[0] == 2 and f[2] == "RemoteStopTransaction" for f in rig.charger_frames)
    await rig.close()
    recovered = Rig(path)
    recovered.firmware = firmware
    await recovered.gateway.controls.ensure_ceiling()
    firmware.fire_plan(1)
    assert firmware.current == 0 and recovered.gateway.controls.ready
    with pytest.raises(DeniedError):
        await recovered.gateway.set_limit(6)
    await recovered.close()


# Intent: a lower local ceiling survives start, stop and explicit release of local ownership.
# Why: resuming or returning control to the app must not silently raise the user's earlier ceiling.
async def test_lower_local_ceiling_survives_resume_and_release(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")
    await rig.gateway.set_limit(6)
    await rig.gateway.start()
    assert rig.firmware.effective_limit() == 6
    await rig.charger("started", "StartTransaction", START)
    await rig.gateway.stop()
    await rig.charger(
        "stopped",
        "StopTransaction",
        {
            "transactionId": 1001,
            "timestamp": START["timestamp"],
            "meterStop": 1000,
        },
    )
    await rig.gateway.release()
    assert rig.firmware.effective_limit() == 6
    assert rig.store.get("control")["owner"] == "shared"
    await rig.gateway.start()
    assert rig.firmware.effective_limit() == 6
    await rig.close()


# Intent: detect a higher pre-existing station stack which defeats the proxy's requested cap.
# Why: Accepted on SetChargingProfile proves receipt, while an independent composite response exposes precedence.
async def test_composite_verification_detects_higher_station_stack(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")
    rig.firmware.answer(
        "SetChargingProfile",
        commands.set_charging_profile(
            0,
            commands.PURPOSE_CHARGE_POINT_MAX,
            32,
            999,
            stack_level=17,
        ),
    )
    with pytest.raises(UnavailableError, match="exceeds"):
        await rig.gateway.set_limit(6)
    assert not rig.gateway.controls.ready and rig.store.get("control")["uncertain"]
    with pytest.raises(RecoveryRequiredError):
        await rig.gateway.start()
    await rig.close()


# Intent: an actual rejected start restores a physical zero-current hold before reporting failure.
# Why: installing a positive cap before RemoteStart must not leave a timer free to charge after rejection.
async def test_failed_start_reasserts_zero(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")

    # Reject only the start; station-profile and composite results come from independent device state.
    async def refuse_start(raw: str) -> None:
        frame = decode(raw)
        rig.charger_frames.append(frame)
        answer = {"status": "Rejected"} if frame[2] == "RemoteStartTransaction" else rig.firmware.answer(*frame[2:])
        await rig.gateway.receive("charger", build_call_result(frame[1], answer))

    rig.gateway.attach_charger(refuse_start)
    with pytest.raises(UnavailableError):
        await rig.gateway.start()
    assert rig.firmware.effective_limit() == 0 and rig.store.get("control")["inhibit"]
    assert rig.store.get("control")["uncertain"]
    await rig.close()


# Intent: newly generated vendor tags receive one bounded grant, retained through restart and retired at Stop.
# Why: the actual app uses a different tag for a fresh remote start; a permanent catch-all allowlist is unsafe.
async def test_vendor_tag_grant_lifecycle_and_historical_retry(tmp_path: Path) -> None:
    path = tmp_path / "journal.db"
    policy = Policy(trust_vendor_start_tags=True)
    rig = Rig(path, policy)
    await rig.boot()
    await rig.cloud()
    request = {"connectorId": 1, "idTag": "NEW_APP_TAG"}
    assert (await rig.vendor("remote", "RemoteStartTransaction", request))[2]["status"] == "Accepted"
    assert (await rig.vendor("duplicate-new-id", "RemoteStartTransaction", request))[0] == 4
    await rig.close()
    rig = Rig(path, policy)
    assert rig.gateway.authorisation.allows("NEW_APP_TAG")
    assert not rig.gateway.authorisation.allows("OTHER_APP_TAG")
    start = {**START, "idTag": "NEW_APP_TAG"}
    answer = await rig.charger("actual-start", "StartTransaction", start)
    assert answer[2]["transactionId"] == 1001
    rig.gateway.authorisation.retire(9999)
    assert rig.gateway.authorisation.allows("NEW_APP_TAG")
    await rig.charger(
        "stop",
        "StopTransaction",
        {
            "transactionId": 1001,
            "timestamp": START["timestamp"],
            "meterStop": 1000,
        },
    )
    assert not rig.gateway.authorisation.allows("NEW_APP_TAG")
    assert (await rig.charger("late-retry", "StartTransaction", start))[2] == answer[2]
    assert (await rig.charger("different-fact", "StartTransaction", {**start, "meterStart": 1001}))[0] == 4
    await rig.close()


# Intent: expiry closes permission without guessing whether an accepted remote start was executed late.
# Why: an expired unresolved grant still needs reconciliation before another start is forwarded.
async def test_vendor_tag_expiry_and_actual_refusal(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db", Policy(trust_vendor_start_tags=True))
    token = rig.gateway.authorisation.reserve("NEW_APP_TAG")
    grant = rig.store.get("vendor_start_grant")
    rig.store.put("vendor_start_grant", {**grant, "expires": 0})
    assert not rig.gateway.authorisation.allows("NEW_APP_TAG")
    with pytest.raises(ConflictError):
        rig.gateway.authorisation.reserve("OTHER_APP_TAG")
    rig.gateway.authorisation.revoke("retired-token")
    assert rig.store.get("vendor_start_grant")
    rig.gateway.authorisation.revoke(token)
    assert rig.store.get("vendor_start_grant") == {}
    await rig.boot()
    await rig.cloud()
    await rig.gateway.controls.ensure_ceiling()
    rig.charger_result = {"status": "Rejected"}
    assert (await rig.vendor("refusal", "RemoteStartTransaction", {"connectorId": 1, "idTag": "NEW_APP_TAG"}))[2][
        "status"
    ] == "Rejected"
    assert not rig.gateway.authorisation.allows("NEW_APP_TAG")
    await rig.close()


# Intent: SQLite failure before send returns an error without forwarding any uncertain charger mutation.
# Why: the durable intent must exist before an actual effect can be allowed.
async def test_readonly_journal_never_forwards_write(tmp_path: Path) -> None:
    rig = await ready(tmp_path / "journal.db")
    count = len(rig.charger_frames)
    rig.store.db.execute("PRAGMA query_only=ON")
    answer = await rig.vendor("read-only", "DataTransfer", PLAN)
    assert answer[0] == 4 and answer[2] == "InternalError"
    assert len(rig.charger_frames) == count
    rig.store.db.execute("PRAGMA query_only=OFF")
    await rig.close()


# Intent: a failure committing a real peer ACK returns an error and retains a global uncertainty barrier.
# Why: publishing success without durable ownership would allow unsafe retries or clears after restart.
async def test_post_ack_plan_commit_failure_is_quarantined(tmp_path: Path) -> None:
    rig = await ready(tmp_path / "journal.db")
    rig.store.db.execute("CREATE TRIGGER fail_plan_update BEFORE UPDATE ON plans BEGIN SELECT RAISE(ABORT,'test'); END")
    answer = await rig.vendor("commit-failed", "DataTransfer", PLAN)
    assert answer[0] == 4 and answer[2] == "InternalError"
    assert rig.store.db.execute("SELECT state FROM mutations").fetchone()[0] == "uncertain"
    count = len(rig.charger_frames)
    assert (await rig.vendor("changed", "DataTransfer", plan(limitCurrent=6)))[0] == 4
    assert len(rig.charger_frames) == count
    await rig.close()


# Intent: every actual peer CALLERROR remains visible and conservatively quarantines a possible mutation.
# Why: a peer's GenericError does not establish that the requested physical write had no effect.
@pytest.mark.parametrize("code", ["GenericError", "InternalError", "NotSupported"])
async def test_peer_callerrors_keep_actual_fields(tmp_path: Path, code: str) -> None:
    rig = await ready(tmp_path / "journal.db")

    # Fail only the already validated proprietary mutation after its durable intent has been recorded.
    async def fail(raw: str) -> None:
        frame = decode(raw)
        if frame[2] == "DataTransfer":
            answer = build_call_error(frame[1], code, "actual peer text", {"source": "synthetic"})
        else:
            answer = build_call_result(frame[1], rig.firmware.answer(*frame[2:]))
        await rig.gateway.receive("charger", answer)

    rig.gateway.attach_charger(fail)
    answer = await rig.vendor("error", "DataTransfer", PLAN)
    assert answer[2:] == [code, "actual peer text", {"source": "synthetic"}]
    assert rig.store.db.execute("SELECT state FROM mutations").fetchone()[0] == "uncertain"
    await rig.close()


# Intent: plan tombstones remain bounded and a closed transaction retires old profile aliases without reuse.
# Why: long-running app usage must not leak mappings or alias a delayed clear onto a new resource.
async def test_resource_capacity_and_profile_retirement(tmp_path: Path) -> None:
    rig = await ready(tmp_path / "journal.db")
    rig.store.capacity = 2
    book = rig.gateway.plans
    book.adopt(1, 2000001)
    book.adopt(2, 2000002)
    book.accepted((1, 2), True)
    assert book.reserve(1) == 2000001
    with pytest.raises(RecoveryRequiredError):
        book.reserve(3)
    profile = commands.set_charging_profile(1, commands.PURPOSE_TX, 6, 101, transaction_id=6000002)
    assert (await rig.vendor("profile", "SetChargingProfile", profile))[0] == 3
    mapped = rig.store.get("vendor_profiles")["101"]
    await rig.charger(
        "stop",
        "StopTransaction",
        {
            "transactionId": 1001,
            "timestamp": START["timestamp"],
            "meterStop": 1000,
        },
    )
    assert rig.store.get("vendor_profiles") == {}
    assert rig.store.get("next_vendor_profile") == mapped
    assert (await rig.vendor("late-clear", "ClearChargingProfile", {"id": 101}))[0] == 4
    await rig.close()


# Intent: the observed delayed-stop cancellation omits stopTime exactly and only targets an open mapped session.
# Why: null, zero and a historical transaction are different from the captured cancellation command.
async def test_delay_stop_exact_omission_and_active_mapping(tmp_path: Path) -> None:
    policy = Policy(
        max_current_a=32,
        allowed_tags=POLICY.allowed_tags,
        extensions={**POLICY.extensions, ("IoCharger", "delayStop"): "write"},
    )
    rig = Rig(tmp_path / "journal.db", policy)
    rig.charger_result = {"status": "Accepted", "data": ""}
    rig.next_vendor = 6000001
    await rig.boot()
    await rig.charger("start", "StartTransaction", START)
    await rig.cloud()
    payload = {"vendorId": "IoCharger", "messageId": "delayStop", "data": '{"transactionId":6000002}'}
    assert (await rig.vendor("cancel", "DataTransfer", payload))[0] == 3
    assert json.loads(rig.charger_frames[-1][3]["data"]) == {"transactionId": 1001}
    for value in [None, 0, "20300101T000000Z", "2030-01-01T00:00:00+00:00"]:
        bad = {**payload, "data": json.dumps({"transactionId": 6000002, "stopTime": value})}
        assert (await rig.vendor("bad-" + str(value), "DataTransfer", bad))[0] == 4
    await rig.charger(
        "stop",
        "StopTransaction",
        {
            "transactionId": 1001,
            "timestamp": START["timestamp"],
            "meterStop": 1000,
        },
    )
    assert (await rig.vendor("historical", "DataTransfer", payload))[0] == 4
    await rig.close()


# Intent: a cancelled API waiter cannot abandon a claimed stop while it waits behind another outgoing CALL.
# Why: persisted inhibit alone does not stop an autonomous timer until the physical zero profile is installed.
async def test_cancelled_stop_finishes_physical_hold(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")
    rig.firmware.plans[1] = {"limitCurrent": 32}
    async with rig.gateway.charger.gate.enter(10):
        operation = asyncio.create_task(rig.gateway.stop())
        await until(lambda: rig.store.get("control", {}).get("inhibit", False))
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
    await until(lambda: not rig.gateway.controls._operations)
    rig.firmware.fire_plan(1)
    assert rig.firmware.current == 0 and not rig.store.get("control")["uncertain"]
    await rig.close()


# Intent: cancelled local start waits remain owned until a lost ACK causes a confirmed physical rollback.
# Why: otherwise cancellation leaves a positive cap and an abandoned pending start permission.
async def test_cancelled_start_lost_ack_reasserts_zero(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")
    sent = asyncio.Event()

    # Apply the positive cap normally, then simulate a charger which received Start but lost its reply.
    async def lose_start(raw: str) -> None:
        frame = decode(raw)
        if frame[2] == "RemoteStartTransaction":
            sent.set()
        else:
            await rig.gateway.receive("charger", build_call_result(frame[1], rig.firmware.answer(*frame[2:])))

    rig.gateway.attach_charger(lose_start)
    operation = asyncio.create_task(rig.gateway.start())
    await sent.wait()
    operation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await operation
    await until(lambda: not rig.gateway.controls._operations)
    assert rig.firmware.effective_limit() == 0 and rig.store.get("control")["inhibit"]
    await rig.close()


# Intent: an uncertain release from a zero hold physically restores zero before returning failure.
# Why: a positive profile may already be installed even though the subsequent readback was rejected.
async def test_failed_release_restores_physical_hold(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")
    await rig.gateway.stop()
    fail_once = True

    # Reject one positive readback, then allow the compensating zero profile and its independent readback.
    async def reject_readback(raw: str) -> None:
        nonlocal fail_once
        frame = decode(raw)
        if frame[2] == "GetCompositeSchedule" and fail_once:
            fail_once = False
            answer = {"status": "Rejected"}
        else:
            answer = rig.firmware.answer(*frame[2:])
        await rig.gateway.receive("charger", build_call_result(frame[1], answer))

    rig.gateway.attach_charger(reject_readback)
    with pytest.raises(UnavailableError):
        await rig.gateway.release()
    assert rig.store.get("control")["inhibit"] and rig.firmware.effective_limit() == 0
    await rig.close()


# Intent: grant binding, the new transaction, its receipt and outbox all roll back together on storage failure.
# Why: retrying a partially committed Start must not acknowledge a session whose permission was never bound.
async def test_grant_binding_failure_rolls_back_start_fact(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db", Policy(trust_vendor_start_tags=True))
    rig.gateway.authorisation.reserve("NEW_APP_TAG")
    rig.store.db.execute(
        "CREATE TRIGGER fail_bind BEFORE INSERT ON meta "
        "WHEN NEW.key='vendor_start_grant' AND NEW.value LIKE '%local%' "
        "BEGIN SELECT RAISE(ABORT,'test'); END"
    )
    payload = {**START, "idTag": "NEW_APP_TAG"}
    assert (await rig.charger("start", "StartTransaction", payload))[0] == 4
    assert rig.store.active() is None and rig.store.next_delivery() is None
    rig.store.db.execute("DROP TRIGGER fail_bind")
    answer = await rig.charger("retry", "StartTransaction", payload)
    assert answer[2]["transactionId"] == 1001
    grant = rig.store.get("vendor_start_grant")
    rig.store.put("vendor_start_grant", {**grant, "expires": 0})
    assert rig.gateway.authorisation.allows("NEW_APP_TAG")
    await rig.close()


# Intent: new authorisation must fail when the charger cannot establish the station ceiling.
# Why: the inbound permission boundary must be as strict as cloud-originated charging commands.
async def test_inbound_authorize_requires_confirmed_cap(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")
    rig.charger_result = {"status": "Rejected"}
    assert (await rig.charger("authorize", "Authorize", {"idTag": "LOCAL_TEST"}))[0] == 4
    assert not rig.gateway.controls.ready
    assert (await rig.charger("start", "StartTransaction", START))[0] == 4
    assert rig.store.active() is None
    await rig.close()


# Intent: a full ordinary control queue still admits one coalesced priority Stop.
# Why: memory bounds cannot permit pending positive slider operations to reject the owner's stop request.
async def test_stop_has_reserved_admission_when_ordinary_queue_full(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")
    rig.firmware.plans[1] = {"limitCurrent": 32}
    async with rig.gateway.charger.gate.enter(10):
        queued = [asyncio.create_task(rig.gateway.set_limit(16)) for _ in range(32)]
        await until(lambda: len(rig.gateway.controls._operations) == 32)
        stopped = asyncio.create_task(rig.gateway.stop())
        await until(lambda: rig.store.get("control", {}).get("inhibit", False))
        duplicate = asyncio.create_task(rig.gateway.stop())
    assert (await stopped)[2]["status"] == "Accepted"
    assert (await duplicate)[2]["status"] == "Accepted"
    await asyncio.gather(*queued, return_exceptions=True)
    rig.firmware.fire_plan(1)
    assert rig.firmware.current == 0
    await rig.close()


# Intent: losing a socket during send retrieves the exception which detach placed on its pending future.
# Why: the send wrapper can raise before the normal response-await boundary consumes that exception.
async def test_rpc_send_disconnect_retrieves_pending_exception() -> None:
    peer = Peer("charger", 0.1)
    observed: list[dict[str, Any]] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: observed.append(context))

    # Independently emulate a receiver detecting disconnect while a socket send is failing.
    async def disconnect(raw: str) -> None:
        peer.detach()
        raise UnavailableError("send failed after detach")

    peer.sender = disconnect
    try:
        with pytest.raises(UnavailableError):
            await peer.call("Heartbeat", {})
        await asyncio.sleep(0)
        assert not observed and not peer._pending
    finally:
        loop.set_exception_handler(previous)


# Intent: Stop after a newer release must establish a new hold instead of sharing an older superseded stop.
# Why: coalescing is safe only while no intervening operation has changed the ownership ticket.
async def test_coalesced_stop_does_not_lose_newer_stop_to_release(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")
    async with rig.gateway.charger.gate.enter(10):
        first = asyncio.create_task(rig.gateway.stop())
        await until(lambda: rig.store.get("control", {}).get("inhibit", False))
        old_ticket = rig.store.get("control")["ticket"]
        release = asyncio.create_task(rig.gateway.release(acknowledge_uncertainty=True))
        await until(lambda: rig.store.get("control")["ticket"] != old_ticket)
        latest = asyncio.create_task(rig.gateway.stop())
        await until(lambda: rig.store.get("control")["stop_epoch"] == 2)
    assert (await latest)[2]["status"] == "Accepted"
    results = await asyncio.gather(first, release, return_exceptions=True)
    assert all(isinstance(value, DeniedError) for value in results)
    assert rig.store.get("control")["inhibit"] and rig.firmware.effective_limit() == 0
    await rig.close()


# Intent: malformed, historical or future composite schedules cannot confirm a present physical ceiling.
# Why: an Accepted envelope is insufficient if the reported limit does not cover this instant.
@pytest.mark.parametrize(
    "change", ["missing-start", "future", "missing-duration", "zero-duration", "bool-period", "unordered"]
)
async def test_invalid_composite_coverage_fails_closed(tmp_path: Path, change: str) -> None:
    rig = Rig(tmp_path / "journal.db")

    # Return a real fake-device effect with one independently corrupted verification field.
    async def corrupt(raw: str) -> None:
        frame = decode(raw)
        answer = rig.firmware.answer(*frame[2:])
        if frame[2] == "GetCompositeSchedule":
            schedule = answer["chargingSchedule"]
            if change == "missing-start":
                answer.pop("scheduleStart")
            elif change == "future":
                answer["scheduleStart"] = "2099-01-01T00:00:00Z"
            elif change == "missing-duration":
                schedule.pop("duration")
            elif change == "zero-duration":
                schedule["duration"] = 0
            elif change == "bool-period":
                schedule["chargingSchedulePeriod"][0]["startPeriod"] = False
            else:
                schedule["chargingSchedulePeriod"].append({"startPeriod": 0, "limit": 0})
        await rig.gateway.receive("charger", build_call_result(frame[1], answer))

    rig.gateway.attach_charger(corrupt)
    with pytest.raises(UnavailableError):
        await rig.gateway.stop()
    assert not rig.gateway.controls.ready and rig.store.get("control")["inhibit"]
    await rig.close()
