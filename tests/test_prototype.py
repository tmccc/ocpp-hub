"""Fault-injected messages against the actual offline gateway, never a real device."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.frames import build_call, build_call_error, build_call_result
from ocpp_hub_core.prototype.gateway import Gateway, decode
from ocpp_hub_core.prototype.policy import DeniedError, Policy
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.store import ConflictError, Journal, RecoveryRequiredError

from tests.prototype_firmware import Firmware

START = {"connectorId": 1, "idTag": "LOCAL_TEST", "meterStart": 1000, "timestamp": "2026-01-01T00:00:00Z"}
BOOT = {"chargePointVendor": "SYNTHETIC", "chargePointModel": "OFFLINE_TEST"}


class Rig:
    """Synthetic peers observe the real engine and supply configurable results."""

    # Attach in-memory transports without mocking any gateway or journal behavior.
    def __init__(self, path: Path, policy: Policy | None = None, attach: bool = True) -> None:
        self.store = Journal(path, "OFFLINE001")
        self.gateway = Gateway(self.store, policy, timeout=0.08)
        self.charger_frames: list[list[Any]] = []
        self.vendor_frames: list[list[Any]] = []
        self.charger_result: dict[str, Any] = {"status": "Accepted"}
        self.vendor_result: dict[str, Any] = {"status": "Accepted", "data": "synthetic reply"}
        self.drop_cloud_start = False
        self.drop_charger = False
        self.crash_reply = False
        self.next_vendor = 9000
        self.firmware = Firmware()
        if attach:
            self.gateway.attach_charger(self.charger_send)

    # Record charger-bound requests and return synthetic peer results under their actual correlation id.
    async def charger_send(self, raw: str) -> None:
        frame = decode(raw)
        self.charger_frames.append(frame)
        if frame[0] != 2:
            if self.crash_reply:
                raise OSError("simulated crash before local ACK delivery")
            return
        if not self.drop_charger:
            answer = self.charger_result
            if frame[2] == "GetCompositeSchedule":
                answer = self.firmware.answer(frame[2], frame[3])
            elif frame[2] == "SetChargingProfile" and answer.get("status") == "Accepted":
                self.firmware.answer(frame[2], frame[3])
            await self.gateway.receive("charger", build_call_result(frame[1], answer))

    # Record cloud-bound messages; each synthetic Start receives an independently assigned cloud id.
    async def vendor_send(self, raw: str) -> None:
        frame = decode(raw)
        self.vendor_frames.append(frame)
        if frame[0] != 2:
            return
        if frame[2] == "StartTransaction":
            if self.drop_cloud_start:
                return
            self.next_vendor += 1
            answer = {"transactionId": self.next_vendor, "idTagInfo": {"status": "Accepted"}}
        elif frame[2] == "BootNotification":
            answer = {"status": "Accepted", "interval": 60, "currentTime": "2030-01-01T00:00:00Z"}
        else:
            answer = self.vendor_result
        await self.gateway.receive("vendor", build_call_result(frame[1], answer))

    # Introduce the synthetic charger without automatically connecting its cloud mirror.
    async def boot(self) -> None:
        await self.charger("boot", "BootNotification", BOOT)

    # Submit one charger CALL and wait for its actual response task.
    async def charger(self, token: str, action: str, payload: dict[str, Any]) -> list[Any]:
        await self.gateway.receive("charger", build_call(token, action, payload))
        await self.gateway.drain()
        answer = next(frame for frame in reversed(self.charger_frames) if frame[0] != 2 and frame[1] == token)
        if action == "StartTransaction" and answer[0] == 3:
            self.firmware.active = self.store.active()
        elif action == "StopTransaction" and answer[0] == 3 and self.firmware.active == payload["transactionId"]:
            self.firmware.active = None
            self.firmware.requested_current = 0
        return answer

    # Submit one vendor CALL and wait for its real charger reply or explicit refusal.
    async def vendor(self, token: str, action: str, payload: dict[str, Any]) -> list[Any]:
        await self.gateway.receive("vendor", build_call(token, action, payload))
        await self.gateway.drain()
        return next(frame for frame in reversed(self.vendor_frames) if frame[0] != 2 and frame[1] == token)

    # Connect only the synthetic vendor sender and deliver the journal in order.
    async def cloud(self) -> None:
        self.gateway.attach_vendor(self.vendor_send)
        await self.gateway.flush()

    # Stop handlers before closing the private journal.
    async def close(self) -> None:
        await self.gateway.close()
        self.store.close()


# Intent: local start, limit and stop remain usable without a cloud transport.
# Why: forwarding commands through an internet-dependent primary does not satisfy local control.
async def test_prototype_local_control_without_cloud(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")
    await rig.boot()
    assert (await rig.gateway.start())[2]["status"] == "Accepted"
    started = await rig.charger("start", "StartTransaction", START)
    assert (await rig.gateway.set_limit(12))[2]["status"] == "Accepted"
    assert (await rig.gateway.stop())[2]["status"] == "Accepted"
    assert started[2]["transactionId"] == 1001
    assert [f[2] for f in rig.charger_frames if f[0] == 2] == [
        "SetChargingProfile",
        "GetCompositeSchedule",
        "RemoteStartTransaction",
        "SetChargingProfile",
        "GetCompositeSchedule",
        "SetChargingProfile",
        "GetCompositeSchedule",
        "RemoteStopTransaction",
    ]
    assert not rig.vendor_frames
    await rig.close()


# Intent: a crash after commit but before local ACK yields the same local transaction on retry.
# Why: the durable boundary must not be an after-send observer.
async def test_prototype_start_crash_before_ack_and_retry(tmp_path: Path) -> None:
    path = tmp_path / "journal.db"
    rig = Rig(path)
    rig.crash_reply = True
    await rig.charger("start", "StartTransaction", START)
    assert rig.gateway.failures == ["OSError"]
    await rig.close()
    recovered = Rig(path)
    answer = await recovered.charger("start", "StartTransaction", START)
    assert answer[2]["transactionId"] == 1001
    assert recovered.store.db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 1
    assert (await recovered.charger("retry-new-id", "StartTransaction", START))[2] == answer[2]
    changed = await recovered.charger("start", "StartTransaction", {**START, "meterStart": 1200})
    assert changed[0] == 4
    await recovered.close()


# Intent: an entire offline charge survives a complete engine restart and is replayed with cloud ids.
# Why: in-memory mapping alone loses app history across Home Assistant or NAS restarts.
async def test_prototype_completed_offline_charge_survives_restart(tmp_path: Path) -> None:
    path = tmp_path / "journal.db"
    rig = Rig(path)
    await rig.boot()
    await rig.charger("start", "StartTransaction", START)
    meter = {
        "connectorId": 1,
        "transactionId": 1001,
        "meterValue": [{"timestamp": "2026-01-01T00:01:00Z", "sampledValue": [{"value": "1200"}]}],
    }
    stop = {"transactionId": 1001, "meterStop": 2000, "timestamp": "2026-01-01T01:00:00Z"}
    await rig.charger("meter", "MeterValues", meter)
    await rig.charger("stop", "StopTransaction", stop)
    await rig.close()
    recovered = Rig(path)
    await recovered.cloud()
    events = [frame for frame in recovered.vendor_frames if frame[0] == 2]
    assert [frame[2] for frame in events] == ["BootNotification", "StartTransaction", "MeterValues", "StopTransaction"]
    assert [frame[3]["transactionId"] for frame in events[2:]] == [9001, 9001]
    assert recovered.store.next_delivery() is None
    assert recovered.store.active() is None
    await recovered.close()


# Intent: a possible cloud-accepted Start is quarantined rather than automatically duplicated.
# Why: exactly-once cross-connection effects cannot be promised without vendor idempotence.
async def test_prototype_cloud_ack_loss_requires_reconciliation(tmp_path: Path) -> None:
    path = tmp_path / "journal.db"
    rig = Rig(path)
    await rig.boot()
    await rig.charger("start", "StartTransaction", START)
    rig.drop_cloud_start = True
    with pytest.raises(UnavailableError):
        await rig.cloud()
    token = rig.store.next_delivery().token
    await rig.close()
    recovered = Rig(path)
    with pytest.raises(RecoveryRequiredError):
        await recovered.cloud()
    assert not [f for f in recovered.vendor_frames if f[0] == 2 and f[2] == "StartTransaction"]
    assert recovered.store.next_delivery().token == token
    recovered.store.reconcile_start(1001, 9001)
    assert recovered.store.vendor_id(1001) == 9001
    assert recovered.store.next_delivery() is None
    await recovered.close()


# Intent: a hard crash of an in-flight Start yields the same quarantine as a timeout.
# Why: finally blocks do not run during power loss.
async def test_prototype_inflight_start_and_counter_recovery(tmp_path: Path) -> None:
    path = tmp_path / "journal.db"
    journal = Journal(path, "OFFLINE001")
    journal.start("s1", START)
    journal.mark_sent(journal.next_delivery())
    journal.close()
    recovered = Journal(path, "OFFLINE001")
    assert recovered.next_delivery().state == "ambiguous"
    recovered.reconcile_start(1001, 9001)
    recovered.stop("stop", {"transactionId": 1001, "meterStop": 1000, "timestamp": START["timestamp"]})
    answer = recovered.start("s2", {**START, "timestamp": "2026-01-02T00:00:00Z"})
    assert answer["transactionId"] == 1002
    recovered.close()


# Intent: Stop retries under different ids do not duplicate history; conflicting or unknown stops are refused.
# Why: session recovery must not finish the wrong connector or silently alter an accepted energy total.
async def test_prototype_stop_dedupe_and_conflict(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")
    await rig.charger("start", "StartTransaction", START)
    stop = {"transactionId": 1001, "meterStop": 2000, "timestamp": "2026-01-01T01:00:00Z"}
    assert (await rig.charger("stop1", "StopTransaction", stop))[0] == 3
    assert (await rig.charger("stop2", "StopTransaction", stop))[0] == 3
    assert (await rig.charger("bad", "StopTransaction", {**stop, "meterStop": 3000}))[0] == 4
    assert (await rig.charger("unknown", "StopTransaction", {**stop, "transactionId": 333}))[0] == 4
    assert rig.store.db.execute("SELECT COUNT(*) FROM outbox WHERE action='StopTransaction'").fetchone()[0] == 1
    await rig.close()


# Intent: a journal failure prevents successful local acceptance and rolls back the counter, session and outbox.
# Why: swallowing disk failures would reproduce the crash window we are fixing.
async def test_prototype_disk_failure_never_acknowledges_success(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")
    rig.store.db.execute("CREATE TRIGGER no_write BEFORE INSERT ON outbox BEGIN SELECT RAISE(ABORT,'disk fault'); END")
    reply = await rig.charger("start", "StartTransaction", START)
    assert reply[0] == 4 and reply[2] == "InternalError"
    assert rig.store.active() is None
    assert rig.store.get("next_transaction") is None
    assert rig.store.next_delivery() is None
    await rig.close()


# Intent: two processes or charger identities cannot accidentally share one transaction journal.
# Why: silent journal sharing would conflate sessions and cloud mappings.
def test_prototype_journal_exclusivity_and_identity(tmp_path: Path) -> None:
    path = tmp_path / "journal.db"
    first = Journal(path, "OFFLINE001")
    with pytest.raises(RecoveryRequiredError):
        Journal(path, "OFFLINE001")
    first.close()
    with pytest.raises(ConflictError):
        Journal(path, "OTHER")


# Intent: stale replayed meter data does not confirm that a restored session is currently charging.
# Why: receive time is not the event's measurement time.
async def test_prototype_recovered_session_provenance(tmp_path: Path) -> None:
    path = tmp_path / "journal.db"
    rig = Rig(path)
    await rig.charger("start", "StartTransaction", START)
    await rig.close()
    recovered = Rig(path)
    for stamp, expected in [("2026-01-01T00:01:00Z", 0), (datetime.now(UTC).isoformat(), 1)]:
        await recovered.charger(
            stamp,
            "MeterValues",
            {
                "connectorId": 1,
                "transactionId": 1001,
                "meterValue": [{"timestamp": stamp, "sampledValue": [{"value": "1200"}]}],
            },
        )
        assert recovered.store.db.execute("SELECT live FROM transactions WHERE local=1001").fetchone()[0] == expected
    await recovered.close()


# Intent: opaque requests and peer CALLERROR details survive both directions without fake acceptance.
# Why: the vendor app needs actual charger/vendor behavior rather than cached imitations.
async def test_prototype_extensions_and_real_errors(tmp_path: Path) -> None:
    policy = Policy(
        extensions={("SYNTHETIC", "ReadSchedule"): "read"}, configuration_keys=frozenset({"SyntheticSchedule"})
    )
    rig = Rig(tmp_path / "journal.db", policy)
    await rig.boot()
    rig.gateway.attach_vendor(rig.vendor_send)
    payload = {"vendorId": "SYNTHETIC", "messageId": "ReadSchedule", "data": '{"opaque":true}'}
    response = await rig.charger("opaque", "DataTransfer", payload)
    assert response[2] == rig.vendor_result
    cloud_calls = [f for f in rig.vendor_frames if f[0] == 2 and f[2] == "DataTransfer"]
    assert len(cloud_calls) == 1 and cloud_calls[0][3] == payload
    rig.charger_result = {"configurationKey": [{"key": "SyntheticSchedule", "readonly": False, "value": "opaque"}]}
    assert (await rig.vendor("cfg", "GetConfiguration", {"key": ["SyntheticSchedule"]}))[2] == rig.charger_result

    # Return a synthetic peer error, including its original vendor-specific details.
    async def error_sender(raw: str) -> None:
        frame = decode(raw)
        if frame[0] == 2:
            await rig.gateway.receive(
                "charger",
                build_call_error(frame[1], "PropertyConstraintViolation", "synthetic refusal", {"field": "opaque"}),
            )

    rig.gateway.attach_charger(error_sender)
    error = await rig.vendor("real-error", "GetConfiguration", {})
    assert error == [4, "real-error", "PropertyConstraintViolation", "synthetic refusal", {"field": "opaque"}]
    await rig.close()


# Intent: a vendor can issue a nested charger read while answering a charger-originated extension.
# Why: awaiting the extension inline in the receiver would deadlock the reply path.
async def test_prototype_nested_bidirectional_bridge_has_no_deadlock(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db", Policy(extensions={("SYNTHETIC", "Read"): "read"}))
    await rig.boot()
    nested_done = asyncio.Event()

    # Simulate a vendor that needs a configuration response before it answers the proprietary exchange.
    async def nested_vendor(raw: str) -> None:
        frame = decode(raw)
        if frame[0] != 2:
            if frame[1] == "nested":
                nested_done.set()
            return
        if frame[2] == "DataTransfer":
            await rig.gateway.receive("vendor", build_call("nested", "GetConfiguration", {}))
            await asyncio.wait_for(nested_done.wait(), 0.1)
        result = {"status": "Accepted"}
        if frame[2] == "BootNotification":
            result = {"status": "Accepted", "interval": 60, "currentTime": "2030-01-01T00:00:00Z"}
        await rig.gateway.receive("vendor", build_call_result(frame[1], result))

    rig.gateway.attach_vendor(nested_vendor)
    result = await rig.charger("opaque", "DataTransfer", {"vendorId": "SYNTHETIC", "messageId": "Read"})
    assert result[0] == 3 and nested_done.is_set()
    await rig.close()


# Intent: local stop is next after an already-sent read, before queued cloud work, and persists its inhibit.
# Why: cloud schedule/start requests must not silently reverse an explicit local stop.
async def test_prototype_stop_priority_and_restart_inhibit(tmp_path: Path) -> None:
    path = tmp_path / "journal.db"
    rig = Rig(path)
    await rig.boot()
    await rig.charger("start", "StartTransaction", START)
    await rig.cloud()
    sent = asyncio.Event()
    release = asyncio.Event()
    original = rig.charger_send
    delayed: list[asyncio.Task[None]] = []

    # Hold the first in-flight read while leaving both receiver loops free.
    async def hold_first(raw: str) -> None:
        frame = decode(raw)
        if frame[0] == 2 and frame[2] == "GetConfiguration" and not sent.is_set():
            rig.charger_frames.append(frame)
            sent.set()

            # Supply its answer later, as an independent peer receiver would.
            async def respond() -> None:
                await release.wait()
                await rig.gateway.receive("charger", build_call_result(frame[1], {}))

            delayed.append(asyncio.create_task(respond()))
        else:
            await original(raw)

    rig.gateway.attach_charger(hold_first)
    before = len(rig.charger_frames)
    await rig.gateway.receive("vendor", build_call("first", "GetConfiguration", {}))
    await asyncio.wait_for(sent.wait(), 0.5)
    await rig.gateway.receive("vendor", build_call("second", "GetConfiguration", {}))
    await rig.gateway.receive("vendor", build_call("cloud-start", "RemoteStartTransaction", {"idTag": "LOCAL_TEST"}))
    stop_task = asyncio.create_task(rig.gateway.stop())
    await asyncio.sleep(0)
    release.set()
    assert (await asyncio.wait_for(stop_task, 0.5))[2]["status"] == "Accepted"
    await asyncio.wait_for(rig.gateway.drain(), 0.5)
    await asyncio.gather(*delayed)
    calls = [f[2] for f in rig.charger_frames[before:] if f[0] == 2]
    assert calls == [
        "GetConfiguration",
        "SetChargingProfile",
        "GetCompositeSchedule",
        "RemoteStopTransaction",
        "GetConfiguration",
    ]
    assert next(f for f in rig.vendor_frames if f[1] == "cloud-start")[0] == 4
    await rig.charger(
        "stopped", "StopTransaction", {"transactionId": 1001, "meterStop": 1000, "timestamp": START["timestamp"]}
    )
    await rig.close()
    recovered = Rig(path)
    recovered.gateway.attach_vendor(recovered.vendor_send)
    assert (await recovered.vendor("restart", "RemoteStartTransaction", {"idTag": "LOCAL_TEST"}))[0] == 4
    await recovered.gateway.release()
    assert (await recovered.vendor("released", "RemoteStartTransaction", {"idTag": "LOCAL_TEST"}))[0] == 3
    await recovered.close()


# Intent: cloud stop uses the known local id and never stops an unknown session.
# Why: app and local controller assign different ids to the same charge.
async def test_prototype_cloud_stop_translation(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")
    await rig.boot()
    await rig.charger("start", "StartTransaction", START)
    await rig.cloud()
    assert (await rig.vendor("stop", "RemoteStopTransaction", {"transactionId": 9001}))[0] == 3
    assert rig.charger_frames[-1][3] == {"transactionId": 1001}
    assert (await rig.vendor("bad", "RemoteStopTransaction", {"transactionId": 9002}))[0] == 4
    await rig.close()


# Intent: malformed, infinite, boolean and oversized limits never cross the control boundary.
# Why: JSON/Python coercion and single-phase watt conversion must not bypass the ceiling.
@pytest.mark.parametrize("value", [True, -1, 33, float("nan"), float("inf"), "16", 10**500])
async def test_prototype_invalid_local_limits(tmp_path: Path, value: Any) -> None:
    rig = Rig(tmp_path / "journal.db")
    with pytest.raises(DeniedError):
        await rig.gateway.set_limit(value)
    assert not rig.charger_frames
    await rig.close()


# Intent: all profile periods are validated and a watt profile cannot borrow fictitious phases.
# Why: app limits must not exceed the configured single-phase current ceiling.
@pytest.mark.parametrize("change", ["watts", "phase", "boolean-phase", "descending", "late-first", "local-id"])
async def test_prototype_cloud_profile_validation(tmp_path: Path, change: str) -> None:
    rig = Rig(tmp_path / "journal.db")
    rig.gateway.attach_vendor(rig.vendor_send)
    payload = commands.set_charging_profile(0, "ChargePointMaxProfile", 12, 11)
    profile = payload["csChargingProfiles"]
    schedule = profile["chargingSchedule"]
    if change == "watts":
        schedule.update(chargingRateUnit="W", chargingSchedulePeriod=[{"startPeriod": 0, "limit": 22000}])
    elif change in ("phase", "boolean-phase"):
        schedule["chargingSchedulePeriod"][0]["numberPhases"] = 3 if change == "phase" else True
    elif change == "descending":
        schedule["chargingSchedulePeriod"] += [{"startPeriod": 60, "limit": 10}, {"startPeriod": 30, "limit": 9}]
    elif change == "late-first":
        schedule["chargingSchedulePeriod"][0]["startPeriod"] = 1
    else:
        profile["chargingProfileId"] = 7001
    assert (await rig.vendor("invalid", "SetChargingProfile", payload))[0] == 4
    assert not rig.charger_frames
    await rig.close()


# Intent: vendor profile ids occupy their own namespace and blanket clears cannot erase the local ceiling.
# Why: shared control needs to retain each controller's profiles rather than clearing everything.
async def test_prototype_profile_ownership_and_clear(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")
    rig.gateway.attach_vendor(rig.vendor_send)
    payload = commands.set_charging_profile(0, "ChargePointMaxProfile", 12, 11)
    assert (await rig.vendor("limit", "SetChargingProfile", payload))[0] == 3
    assert rig.charger_frames[-1][3]["csChargingProfiles"]["chargingProfileId"] == 100000
    for bad in [{}, {"id": 7001}, {"id": 99}, {"connectorId": 0}]:
        assert (await rig.vendor("bad-" + json.dumps(bad), "ClearChargingProfile", bad))[0] == 4
    assert (await rig.vendor("clear", "ClearChargingProfile", {"id": 11}))[0] == 3
    assert rig.charger_frames[-1][3] == {"id": 100000}
    await rig.close()


# Intent: failed local stop remains uncertain and blocks a cloud restart even after reopening the journal.
# Why: a timeout is not evidence that the stop was rejected or safe to retry automatically.
async def test_prototype_uncertain_stop_remains_inhibited(tmp_path: Path) -> None:
    path = tmp_path / "journal.db"
    rig = Rig(path)
    await rig.charger("start", "StartTransaction", START)
    rig.drop_charger = True
    with pytest.raises(UnavailableError):
        await rig.gateway.stop()
    with pytest.raises(RecoveryRequiredError):
        await rig.gateway.release()
    await rig.close()
    recovered = Rig(path)
    assert recovered.store.get("control")["inhibit"]
    await recovered.gateway.release(acknowledge_uncertainty=True)
    assert not recovered.store.get("control")["inhibit"]
    await recovered.close()


# Intent: failed, unapproved and offline opaque requests return errors rather than a synthetic Accepted.
# Why: app compatibility requires honest outcomes, and opaque writes cannot bypass local ownership.
async def test_prototype_extension_denial_and_no_auto_setup(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db", Policy(extensions={("SYNTHETIC", "Write"): "write"}))
    await rig.boot()
    assert not [f for f in rig.charger_frames if f[0] == 2]
    assert (await rig.charger("tag", "Authorize", {"idTag": "UNKNOWN"}))[2]["idTagInfo"]["status"] == "Invalid"
    assert (await rig.charger("unapproved", "StartTransaction", {**START, "idTag": "UNKNOWN"}))[0] == 4
    body = {"vendorId": "SYNTHETIC", "messageId": "Write", "data": "opaque"}
    assert (await rig.charger("offline", "DataTransfer", body))[0] == 4
    rig.gateway.attach_vendor(rig.vendor_send)
    await rig.gateway.set_limit(12)
    assert (await rig.vendor("held", "DataTransfer", body))[0] == 4
    assert (await rig.vendor("firmware", "UpdateFirmware", {"location": "https://example.invalid"}))[0] == 4
    assert rig.store.next_delivery() is None
    await rig.close()


# Intent: a corrupt journal is not replaced by a blank state database.
# Why: silently starting over loses session identity and can duplicate cloud history.
def test_prototype_corrupt_store_is_not_reset(tmp_path: Path) -> None:
    path = tmp_path / "journal.db"
    path.write_bytes(b"not a database")
    with pytest.raises(sqlite3.DatabaseError):
        Journal(path, "OFFLINE001")
    assert path.read_bytes() == b"not a database"


# Intent: malformed and non-finite protocol frames fail at the input boundary.
# Why: no malformed request should turn into a valid control or an unbounded storage allocation.
@pytest.mark.parametrize(
    "raw",
    [
        '[true,"x",{}]',
        '[2,"x","DataTransfer",{"v":NaN}]',
        '[3,"x",[]]',
        '[4,"x","InternalError","error",[]]',
        "[]",
        "x",
        b"[]",
    ],
)
def test_prototype_invalid_frames(raw: str | bytes) -> None:
    with pytest.raises(DeniedError):
        decode(raw)


# Intent: a legitimate reused CALL counter after reconnect is distinct from a same-connection changed retry.
# Why: retaining global request ids forever would prevent later valid charges.
async def test_prototype_connection_epoch_allows_counter_reuse(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")
    first = await rig.charger("counter-1", "StartTransaction", START)
    stop = {"transactionId": first[2]["transactionId"], "meterStop": 2000, "timestamp": "2026-01-01T01:00:00Z"}
    await rig.charger("counter-2", "StopTransaction", stop)
    retired = rig.gateway._charger_epoch
    rig.gateway.attach_charger(rig.charger_send)
    second = await rig.charger("counter-1", "StartTransaction", {**START, "timestamp": "2026-01-02T00:00:00Z"})
    assert second[0] == 3 and second[2]["transactionId"] == 1002
    with pytest.raises(UnavailableError):
        await rig.gateway.receive("charger", build_call("old", "Heartbeat", {}), epoch=retired)
    await rig.close()


# Intent: malformed observations are rejected before their outbox rows can block later valid history.
# Why: acknowledging a poison event is an irreversible promise to the charger.
@pytest.mark.parametrize(
    "payload",
    [
        {"connectorId": 1, "transactionId": True, "meterValue": []},
        {"connectorId": 1, "transactionId": "bad", "meterValue": []},
        {"connectorId": 1, "transactionId": 999, "meterValue": []},
        {"connectorId": 1, "meterValue": []},
        {"connectorId": 1, "meterValue": [{"timestamp": "bad", "sampledValue": [{"value": "1"}]}]},
        {"connectorId": 1, "meterValue": [{"timestamp": START["timestamp"], "sampledValue": [{"value": "NaN"}]}]},
    ],
)
async def test_prototype_malformed_meter_never_poisoned(tmp_path: Path, payload: dict[str, Any]) -> None:
    rig = Rig(tmp_path / "journal.db")
    assert (await rig.charger("invalid", "MeterValues", payload))[0] == 4
    assert rig.store.next_delivery() is None
    await rig.close()


# Intent: invalid status shape cannot be acknowledged and later rejected only by the cloud.
# Why: valid local history behind it must remain deliverable.
async def test_prototype_invalid_status_rejected_before_ack(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")
    for state in [None, [], "made-up"]:
        result = await rig.charger(
            str(state), "StatusNotification", {"connectorId": 1, "status": state, "errorCode": "NoError"}
        )
        assert result[0] == 4
    assert rig.store.next_delivery() is None
    await rig.close()


# Intent: duplicate incoming commands share one execution and the same actual result.
# Why: an app retry must not trigger two starts or two opaque configuration writes.
async def test_prototype_duplicate_rpc_executes_once(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db", Policy(extensions={("SYNTHETIC", "Read"): "read"}))
    await rig.boot()
    rig.gateway.attach_vendor(rig.vendor_send)
    request = build_call("repeat", "RemoteStartTransaction", {"idTag": "LOCAL_TEST"})
    await rig.gateway.receive("vendor", request)
    await rig.gateway.receive("vendor", request)
    await rig.gateway.drain()
    assert len([f for f in rig.charger_frames if f[0] == 2 and f[2] == "RemoteStartTransaction"]) == 1
    extension = {"vendorId": "SYNTHETIC", "messageId": "Read", "data": "opaque"}
    await rig.charger("extension", "DataTransfer", extension)
    await rig.charger("extension", "DataTransfer", extension)
    assert len([f for f in rig.vendor_frames if f[0] == 2 and f[2] == "DataTransfer"]) == 1
    await rig.close()


# Intent: denied cloud profiles cannot exhaust persistent namespace capacity while a local hold is active.
# Why: validation outside the outgoing gate previously created state for requests that were never allowed.
async def test_prototype_denied_profile_does_not_allocate(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")
    rig.gateway.attach_vendor(rig.vendor_send)
    await rig.gateway.set_limit(12)
    payload = commands.set_charging_profile(0, "ChargePointMaxProfile", 12, 11)
    assert (await rig.vendor("denied", "SetChargingProfile", payload))[0] == 4
    assert rig.store.get("vendor_profiles", {}) == {}
    await rig.close()


# Intent: an opaque write that waited behind another vendor CALL is rechecked after local ownership changes.
# Why: a pre-queue check cannot enforce a stop that happens while a request waits.
async def test_prototype_queued_extension_rechecks_owner(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db", Policy(extensions={("SYNTHETIC", "Write"): "write"}))
    await rig.boot()
    rig.gateway.attach_vendor(rig.vendor_send)
    await rig.gateway._ensure_vendor_boot()
    async with rig.gateway.vendor.gate.enter(0):
        await rig.gateway.receive(
            "charger",
            build_call("write", "DataTransfer", {"vendorId": "SYNTHETIC", "messageId": "Write", "data": "opaque"}),
        )
        await asyncio.sleep(0)
        await rig.gateway.set_limit(12)
    await rig.gateway.drain()
    assert not [f for f in rig.vendor_frames if f[0] == 2 and f[2] == "DataTransfer"]
    assert next(f for f in rig.charger_frames if f[1] == "write")[0] == 4
    await rig.close()


# Intent: an unconfirmed opaque write stays quarantined across restart without saving its secret body.
# Why: a volatile cache cannot determine whether the peer executed a write before a crash.
async def test_prototype_mutating_rpc_uncertainty_survives_restart(tmp_path: Path) -> None:
    path = tmp_path / "journal.db"
    policy = Policy(extensions={("SYNTHETIC", "Write"): "write"})
    rig = Rig(path, policy)
    rig.gateway.attach_vendor(rig.vendor_send)
    await rig.gateway.controls.ensure_ceiling()
    rig.drop_charger = True
    body = {"vendorId": "SYNTHETIC", "messageId": "Write", "data": "pretend-private-setting"}
    assert (await rig.vendor("write", "DataTransfer", body))[0] == 4
    assert rig.store.db.execute("SELECT state FROM mutations").fetchone()[0] == "uncertain"
    await rig.close()
    assert b"pretend-private-setting" not in path.read_bytes()
    recovered = Rig(path, policy)
    recovered.gateway.attach_vendor(recovered.vendor_send)
    assert (await recovered.vendor("retry-new-id", "DataTransfer", body))[0] == 4
    assert not recovered.charger_frames
    await recovered.close()


# Intent: backpressure during send counts toward the same request deadline as waiting for the response.
# Why: a never-finishing send must not hold the CALL gate and starve local stop.
async def test_prototype_blocked_send_releases_call_gate(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")

    # Simulate a socket whose send cannot drain, without opening any socket.
    async def blocked_send(raw: str) -> None:
        await asyncio.Event().wait()

    rig.gateway.attach_charger(blocked_send)
    with pytest.raises(UnavailableError):
        await asyncio.wait_for(rig.gateway.charger.call("GetConfiguration", {}), 0.2)
    rig.gateway.attach_charger(rig.charger_send)
    assert (await rig.gateway.charger.call("GetConfiguration", {}, priority=0))[0] == 3
    await rig.close()


# Intent: a retired origin cannot execute work that was already waiting for an outgoing gate.
# Why: suppressing only the reply does not prevent a stale start after reconnection.
@pytest.mark.parametrize("origin", ["vendor", "charger"])
async def test_prototype_retired_queued_command_never_executes(tmp_path: Path, origin: str) -> None:
    rig = Rig(tmp_path / "journal.db", Policy(extensions={("SYNTHETIC", "Write"): "write"}))
    await rig.boot()
    rig.gateway.attach_vendor(rig.vendor_send)
    await rig.gateway._ensure_vendor_boot()
    peer = rig.gateway.charger if origin == "vendor" else rig.gateway.vendor
    async with peer.gate.enter(0):
        if origin == "vendor":
            request = build_call("old", "RemoteStartTransaction", {"idTag": "LOCAL_TEST"})
        else:
            request = build_call("old", "DataTransfer", {"vendorId": "SYNTHETIC", "messageId": "Write"})
        await rig.gateway.receive(origin, request)
        await asyncio.sleep(0)
        if origin == "vendor":
            rig.gateway.attach_vendor(rig.vendor_send)
        else:
            rig.gateway.attach_charger(rig.charger_send)
    await rig.gateway.drain()
    calls = rig.charger_frames if origin == "vendor" else rig.vendor_frames
    assert not [f for f in calls if f[0] == 2 and f[2] in ("RemoteStartTransaction", "DataTransfer")]
    assert rig.store.db.execute("SELECT COUNT(*) FROM mutations").fetchone()[0] == 0
    await rig.close()


# Intent: an old cloud Boot answer cannot mark a replacement connection ready.
# Why: subsequent replay requires the actual current cloud connection to accept the charger identity.
async def test_prototype_old_boot_reply_does_not_ready_new_connection(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")
    await rig.boot()

    # Resolve the old request then replace its transport before the awaiting task resumes.
    async def replace_during_reply(raw: str) -> None:
        frame = decode(raw)
        await rig.gateway.receive("vendor", build_call_result(frame[1], {"status": "Accepted"}))
        rig.gateway.attach_vendor(rig.vendor_send)

    rig.gateway.attach_vendor(replace_during_reply)
    with pytest.raises(UnavailableError):
        await rig.gateway._ensure_vendor_boot()
    assert not rig.gateway._vendor_ready
    await rig.gateway._ensure_vendor_boot()
    assert rig.gateway._vendor_ready
    assert [f[2] for f in rig.vendor_frames if f[0] == 2] == ["BootNotification"]
    await rig.close()


# Intent: a newer no-active-session stop retains uncertainty from an in-flight local start.
# Why: an older completion must not release the newer stop's durable hold.
async def test_prototype_old_completion_cannot_clear_new_stop_uncertainty(tmp_path: Path) -> None:
    rig = Rig(tmp_path / "journal.db")
    entered = asyncio.Event()
    finish = asyncio.Event()
    original = rig.charger_send

    # Hold the actual Start result, then lose the newer zero-cap ACK without blocking the receive loop.
    async def wait_send(raw: str) -> None:
        frame = decode(raw)
        if frame[0] == 2 and frame[2] == "RemoteStartTransaction":
            entered.set()
            await finish.wait()
        elif frame[0] == 2 and frame[2] == "SetChargingProfile":
            limit = frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
            if limit == 0:
                rig.charger_frames.append(frame)
                return
        await original(raw)

    rig.gateway.attach_charger(wait_send)
    start = asyncio.create_task(rig.gateway.start())
    await entered.wait()
    stop = asyncio.create_task(rig.gateway.stop())
    await asyncio.sleep(0)
    finish.set()
    assert (await start)[2]["status"] == "Accepted"
    with pytest.raises(UnavailableError):
        await stop
    control = rig.store.get("control")
    assert control["inhibit"] and control["uncertain"]
    assert not rig.gateway.controls.ready
    await rig.close()
