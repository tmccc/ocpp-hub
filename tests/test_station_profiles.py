"""Independent clock, capability and inventory checks for the next offline commissioning package."""

from __future__ import annotations

import asyncio
import copy
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.frames import build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.policy import DeniedError
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.station import (
    CAPABILITY_KEYS,
    StationChoice,
    StationDiscovery,
    configuration_value,
)
from ocpp_hub_core.prototype.store import RecoveryRequiredError
from ocpp_hub_core.prototype.trial_evidence import native_profile_projection, profile_projection

from tests.prototype_firmware import Firmware
from tests.test_prototype import BOOT, START, Rig
from tests.trial_helpers import TrialRig

ANCHOR = "2020-01-01T00:00:00Z"
ANCHOR_EPOCH = 1577836800


class StationRig(TrialRig):
    """Only the fake charger controls its capability replies, stored profiles and native inventory."""

    # Use one reserved stack consistently in gateway policy and physical station selection.
    def __init__(self, path: Path, choice: StationChoice, original_profile: bool = False) -> None:
        super().__init__(path, original_profile=original_profile)
        self.gateway.policy = replace(self.gateway.policy, local_stack_level=choice.stack)
        self.gateway.controls.policy = self.gateway.policy
        self.discovery = StationDiscovery(
            self.store,
            choice,
            lambda: (len(self.firmware.profiles), 7001 in self.firmware.profiles),
        )
        self.gateway.controls.prepare_station = self.discovery.prepare
        self.read_started = asyncio.Event()
        self.read_release: asyncio.Event | None = None
        self.drop_set = False

    # Delay only a synthetic capability reply to exercise a concurrent Stop without mocking engine operations.
    async def charger_send(self, raw: str) -> None:
        frame = decode(raw)
        if frame[0] == 2 and frame[2] == "SetChargingProfile" and self.drop_set:
            self.charger_frames.append(frame)
            self.firmware.answer(frame[2], frame[3])
            return
        if frame[0] == 2 and frame[2] == "GetConfiguration" and self.read_release is not None:
            self.read_started.set()
            await self.read_release.wait()
        await super().charger_send(raw)


# Intent: one requested capability is returned exactly once with its protocol types intact.
# Why: missing/duplicate/unknown keys cannot become permission to install an unverified station cap.
@pytest.mark.parametrize("change", ["unknown", "duplicate", "wrong-key", "readonly", "value-type", "extra"])
def test_capability_response_rejects_ambiguous_input(change: str) -> None:
    key = "ChargeProfileMaxStackLevel"
    body: dict[str, Any] = {"configurationKey": [{"key": key, "readonly": True, "value": "1"}], "unknownKey": []}
    if change == "unknown":
        body["unknownKey"] = [key]
    elif change == "duplicate":
        body["configurationKey"] *= 2
    elif change == "wrong-key":
        body["configurationKey"][0]["key"] = "other"
    elif change == "readonly":
        body["configurationKey"][0]["readonly"] = "true"
    elif change == "value-type":
        body["configurationKey"][0]["value"] = 1
    else:
        body["extra"] = True
    with pytest.raises(DeniedError):
        configuration_value([3, "synthetic", body], key)


# Intent: unsupported dates and conflicting kind/anchor choices fail before any device operation.
# Why: an installable schedule must also fit the independently supported native cleanup representation.
@pytest.mark.parametrize(
    "start",
    [
        None,
        "1969-01-01T00:00:00Z",
        "1970-01-01T00:00:00Z",
        "2020-01-01T00:00:00+00:00",
        "2020-01-01T00:00:00.000Z",
        "invalid",
    ],
)
def test_absolute_choice_requires_supported_utc_epoch(start: str | None) -> None:
    with pytest.raises(DeniedError):
        StationChoice("Absolute", 1, start)


# Intent: exact reported bounds authorize only the explicit stack/kind; the station hold still wins over a native timer.
# Why: guessing stack16 or using a transaction profile would repeat the failed trial without station enforcement.
@pytest.mark.parametrize("stack", [0, 1, 4])
@pytest.mark.parametrize("kind", ["Relative", "Absolute"])
async def test_station_discovery_and_zero_hold_use_reported_bounds(tmp_path: Path, stack: int, kind: str) -> None:
    choice = StationChoice(kind, stack, ANCHOR if kind == "Absolute" else None)
    rig = StationRig(tmp_path / "state.db", choice)
    rig.firmware.configuration["ChargeProfileMaxStackLevel"] = str(stack)
    try:
        await rig.boot()
        await rig.gateway.controls.ensure_ceiling()
        frames = [f for f in rig.charger_frames if f[0] == 2]
        assert [f[2] for f in frames[:6]] == ["GetConfiguration"] * 6
        assert [f[3]["key"] for f in frames[:6]] == [[key] for key in CAPABILITY_KEYS]
        assert rig.gateway.controls.ready
        profile = rig.firmware.profiles[7001]
        assert profile["stackLevel"] == stack and profile["chargingProfileKind"] == kind
        assert profile["chargingProfilePurpose"] == "ChargePointMaxProfile"
        await rig.gateway.stop()
        rig.firmware.fire_plan(9)
        assert rig.firmware.current == 0
        count = len(rig.charger_frames)
        rig.gateway.attach_charger(rig.charger_send)
        await rig.charger("reboot", "BootNotification", BOOT)
        await rig.gateway.controls.ensure_ceiling()
        later = [f for f in rig.charger_frames[count:] if f[0] == 2 and f[2] == "SetChargingProfile"]
        assert later and all(
            f[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0 for f in later
        )
        assert rig.discovery.choice == choice and rig.firmware.current == 0
        await rig.rollback.run(rig.quiesce)
        assert not rig.firmware.profiles and rig.firmware.plans[9]["limitCurrent"] == 32
    finally:
        await rig.close()


# Intent: unknown, unsupported or contradictory capabilities result in no physical station write.
# Why: a successful GetConfiguration transport reply is not evidence that a selected profile fits this charger.
@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("NumberOfConnectors", "2"),
        ("SupportedFeatureProfiles", "Core"),
        ("ChargingScheduleAllowedChargingRateUnit", "Power"),
        ("ChargingScheduleAllowedChargingRateUnit", "Unknown"),
        ("ChargingScheduleAllowedChargingRateUnit", "A,A"),
        ("ChargeProfileMaxStackLevel", "0"),
        ("ChargeProfileMaxStackLevel", "256"),
        ("ChargeProfileMaxStackLevel", "1.0"),
        ("ChargingScheduleMaxPeriods", "0"),
        ("MaxChargingProfilesInstalled", "0"),
    ],
)
async def test_station_bad_capabilities_never_write(tmp_path: Path, key: str, value: str) -> None:
    rig = StationRig(tmp_path / "state.db", StationChoice("Absolute", 1, ANCHOR))
    rig.firmware.configuration[key] = value
    try:
        await rig.boot()
        with pytest.raises(DeniedError):
            await rig.gateway.controls.ensure_ceiling()
        assert not any(f[0] == 2 and f[2] == "SetChargingProfile" for f in rig.charger_frames)
        assert not rig.gateway.controls.ready
    finally:
        await rig.close()


# Intent: a newer Stop wins while implicit station discovery awaits the device.
# Why: slow capability discovery must not permit a positive ceiling after a newer local inhibit is stored.
async def test_station_discovery_cannot_outlive_newer_stop(tmp_path: Path) -> None:
    rig = StationRig(tmp_path / "state.db", StationChoice("Absolute", 1, ANCHOR))
    try:
        await rig.boot()
        rig.read_release = asyncio.Event()
        preparation = asyncio.create_task(rig.gateway.controls.ensure_ceiling())
        await asyncio.wait_for(rig.read_started.wait(), 2)
        stopping = asyncio.create_task(rig.gateway.stop())
        await asyncio.sleep(0)
        assert rig.store.get("control")["inhibit"]
        rig.read_release.set()
        with pytest.raises(DeniedError):
            await preparation
        await stopping
        sets = [f for f in rig.charger_frames if f[0] == 2 and f[2] == "SetChargingProfile"]
        assert sets and all(
            f[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0 for f in sets
        )
        assert rig.firmware.current == 0
    finally:
        await rig.close()


# Intent: capability changes block rearming, but never suppress the known-session Stop.
# Why: recovery uncertainty must leave a durable hold and still attempt the available physical stopping operation.
@pytest.mark.parametrize("same_socket", [False, True])
async def test_changed_station_introduction_preserves_known_stop(tmp_path: Path, same_socket: bool) -> None:
    rig = StationRig(tmp_path / "state.db", StationChoice("Absolute", 1, ANCHOR))
    try:
        await rig.boot()
        await rig.charger("start", "StartTransaction", START)
        count = len(rig.charger_frames)
        if same_socket:
            await rig.charger("changed-boot", "BootNotification", {**BOOT, "firmwareVersion": "changed"})
            assert not rig.gateway.controls.ready
        else:
            rig.gateway.attach_charger(rig.charger_send)
            rig.firmware.configuration["ChargeProfileMaxStackLevel"] = "8"
            await rig.charger("changed-boot", "BootNotification", BOOT)
        with pytest.raises(UnavailableError):
            await rig.gateway.stop()
        later = [f for f in rig.charger_frames[count:] if f[0] == 2]
        assert any(f[2] == "RemoteStopTransaction" for f in later)
        assert not any(f[2] == "SetChargingProfile" for f in later)
        assert rig.store.get("control")["inhibit"] and rig.store.get("control")["uncertain"]
        assert not rig.gateway.controls.ready and rig.firmware.requested_current == 0
    finally:
        await rig.close()


# Intent: failed owner Start and release claim a durable hold even when changed selection blocks its reapplication.
# Why: discovery failure must never leave a software resume flag that looks as though the positive operation succeeded.
@pytest.mark.parametrize("operation", ["start", "release"])
async def test_changed_station_selection_keeps_failed_owner_operation_held(tmp_path: Path, operation: str) -> None:
    rig = StationRig(tmp_path / "state.db", StationChoice("Absolute", 1, ANCHOR))
    try:
        await rig.boot()
        await rig.gateway.controls.ensure_ceiling()
        await rig.charger("changed", "BootNotification", {**BOOT, "firmwareVersion": "changed"})
        count = len(rig.charger_frames)
        with pytest.raises(RecoveryRequiredError):
            if operation == "start":
                await rig.gateway.start("LOCAL_TEST")
            else:
                await rig.gateway.release()
        state = rig.store.get("control")
        assert state["inhibit"] and state["uncertain"] and not rig.gateway.controls.ready
        assert not any(
            f[0] == 2 and f[2] in ("SetChargingProfile", "RemoteStartTransaction") for f in rig.charger_frames[count:]
        )
    finally:
        await rig.close()


# Intent: native inventory resolves a lost Absolute ACK while preserving the original plan and refusing anchor drift.
# Why: a missing response must neither prevent safe cleanup nor turn a different profile into owned state.
@pytest.mark.parametrize("drift", [False, True])
async def test_absolute_lost_ack_cleanup_and_anchor_drift(tmp_path: Path, drift: bool) -> None:
    rig = StationRig(tmp_path / "state.db", StationChoice("Absolute", 1, ANCHOR))
    original = copy.deepcopy(rig.firmware.plans[9])
    try:
        await rig.boot()
        rig.drop_set = True
        with pytest.raises(UnavailableError):
            await rig.gateway.controls.ensure_ceiling()
        assert 7001 in rig.firmware.profiles and not rig.gateway.controls.ready
        if drift:
            rig.firmware.profiles[7001]["chargingSchedule"]["startSchedule"] = "2020-01-01T00:00:01Z"
            with pytest.raises(ValueError):
                await rig.rollback.plan()
            assert not rig.clear_order
        else:
            await rig.rollback.run(rig.quiesce)
            assert rig.clear_order == [7001] and not rig.firmware.profiles
        assert rig.firmware.plans[9] == original
    finally:
        await rig.close()


# Intent: capability capacity accounts for the original inventory without counting plans as profile slots.
# Why: discovering support for one profile is insufficient if the device's only slot is already occupied.
async def test_station_capacity_preserves_existing_profile(tmp_path: Path) -> None:
    rig = StationRig(tmp_path / "state.db", StationChoice("Absolute", 1, ANCHOR), original_profile=True)
    rig.firmware.configuration["MaxChargingProfilesInstalled"] = "1"
    try:
        await rig.boot()
        with pytest.raises(DeniedError):
            await rig.gateway.controls.ensure_ceiling()
        assert set(rig.firmware.profiles) == {42}
        assert not any(f[0] == 2 and f[2] == "SetChargingProfile" for f in rig.charger_frames)
    finally:
        await rig.close()


# Intent: native Absolute ownership uses an independently written raw record and the exact recorded UTC anchor.
# Why: generating expected native data through the production OCPP projection would conceal a matching codec error.
def absolute_native() -> dict[str, Any]:
    return {
        "chargingProfileId": 7001,
        "connnectroId": 0,
        "purpose": "ChargePointMaxProfile",
        "periodKind": "Absolute",
        "unit": "A",
        "stackLevel": 1,
        "periods": [{"startPeriod": 0, "endPeriod": 2147483646, "limit": 60, "numberPhases": 1}],
        "transactionId": -1,
        "validFrom": 0,
        "validTo": 2147483646,
        "duration": 2147483646,
        "startSchedule": ANCHOR_EPOCH,
    }


# Intent: future, expired, ambiguous and altered native Absolute shapes cannot establish cleanup ownership.
# Why: a new profile id or matching 6 A number is insufficient evidence of the resource actually installed.
@pytest.mark.parametrize(
    "change",
    [
        "future",
        "milliseconds",
        "fractional",
        "duration",
        "period-end",
        "absolute-end",
        "kind",
        "phases",
        "valid-to",
        "boolean-start",
        "boolean-validity",
    ],
)
def test_native_absolute_rejects_unsafe_timing(change: str) -> None:
    body = absolute_native()
    now = time.time()
    if change == "future":
        body["startSchedule"] = int(now) + 1
    elif change == "milliseconds":
        body["startSchedule"] = ANCHOR_EPOCH * 1000
    elif change == "fractional":
        body["startSchedule"] = "2020-01-01T00:00:00.123Z"
    elif change == "duration":
        body["duration"] = 3600
    elif change == "period-end":
        body["periods"][0]["endPeriod"] = 3600
    elif change == "absolute-end":
        body["periods"][0]["endPeriod"] = int(now) - 1
    elif change == "kind":
        body["periodKind"] = "Relative"
    elif change == "phases":
        body["periods"][0]["numberPhases"] = 2
    elif change == "boolean-start":
        body["periods"][0]["startPeriod"] = False
    elif change == "boolean-validity":
        body["validFrom"] = False
    else:
        body["validTo"] = int(now) + 3599
    assert native_profile_projection(body, 0, "ChargePointMaxProfile", now) == {"unsupported": True}


# Intent: the explicit Absolute request matches independently decoded native integer and UTC-string anchors.
# Why: rollback must recognize one actual resource consistently without silently adjusting its start time.
@pytest.mark.parametrize("wire_start", [ANCHOR_EPOCH, ANCHOR])
def test_absolute_native_and_request_agree(wire_start: int | str) -> None:
    body = absolute_native()
    body["startSchedule"] = wire_start
    expected = {
        "id": 7001,
        "connector": 0,
        "purpose": "ChargePointMaxProfile",
        "stack": 1,
        "kind": "Absolute",
        "unit": "A",
        "limit": 6,
        "phases": 1,
        "transaction": None,
        "start": ANCHOR_EPOCH,
    }
    assert native_profile_projection(body, 0, "ChargePointMaxProfile", time.time()) == expected
    assert profile_projection(StationChoice("Absolute", 1, ANCHOR).payload(6)) == expected


# Intent: an accepted future Absolute profile does not constrain the present current in the independent device model.
# Why: Accepted alone cannot establish an effective station ceiling or a safe pause.
def test_independent_firmware_obeys_absolute_time_and_rejection() -> None:
    now = datetime(2026, 1, 2, tzinfo=UTC)
    firmware = Firmware(lambda: now)
    firmware.requested_current = 32
    payload = commands.set_charging_profile(
        0, "ChargePointMaxProfile", 6, 7001, stack_level=1, kind="Absolute", start_schedule="2026-01-02T00:00:01Z"
    )
    assert firmware.answer("SetChargingProfile", payload)["status"] == "Accepted"
    assert firmware.current == 32
    now += timedelta(seconds=1)
    assert firmware.current == 6
    old = copy.deepcopy(firmware.profiles)
    bad = copy.deepcopy(payload)
    bad["csChargingProfiles"]["stackLevel"] = 17
    assert firmware.answer("SetChargingProfile", bad)["status"] == "Rejected"
    assert firmware.profiles == old
    firmware.profiles[7001]["chargingSchedule"]["duration"] = 1
    now += timedelta(seconds=1)
    assert firmware.current == 32


# Intent: near-future and already-ended composite schedules never confirm a present ceiling.
# Why: the previous two-second tolerance could mark a not-yet-effective or expired schedule as ready.
@pytest.mark.parametrize("duration,offset,valid", [(60, 1, False), (60, -61, False), (1, -2, False), (60, -2, True)])
async def test_composite_covers_response_completion(tmp_path: Path, duration: int, offset: int, valid: bool) -> None:
    rig = Rig(tmp_path / "state.db")
    original = rig.charger_send

    # Only the fake device supplies the bad schedule; all gateway control, storage and verification remain real.
    async def charger(raw: str) -> None:
        frame = decode(raw)
        if frame[0] == 2 and frame[2] == "GetCompositeSchedule":
            rig.charger_frames.append(frame)
            answer = rig.firmware.answer(frame[2], frame[3])
            answer["chargingSchedule"]["duration"] = duration
            answer["scheduleStart"] = (datetime.now(UTC) + timedelta(seconds=offset)).isoformat()
            await rig.gateway.receive("charger", build_call_result(frame[1], answer))
        else:
            await original(raw)

    rig.gateway.attach_charger(charger)
    try:
        if valid:
            await rig.gateway.controls.ensure_ceiling()
        else:
            with pytest.raises(UnavailableError):
                await rig.gateway.controls.ensure_ceiling()
        assert rig.gateway.controls.ready is valid
    finally:
        await rig.close()


# Intent: a changed Boot during profile verification cannot complete the old ceiling confirmation.
# Why: discovery and readiness must refer to the same device introduction, even on one socket.
async def test_changed_boot_invalidates_inflight_station_confirmation(tmp_path: Path) -> None:
    rig = StationRig(tmp_path / "state.db", StationChoice("Absolute", 1, ANCHOR))
    original = rig.charger_send

    # Reintroduce changed firmware before the fake device returns its previously requested composite.
    async def peer(raw: str) -> None:
        frame = decode(raw)
        if frame[0] == 2 and frame[2] == "GetCompositeSchedule":
            await rig.charger("changed-inflight", "BootNotification", {**BOOT, "firmwareVersion": "changed"})
        await original(raw)

    rig.gateway.attach_charger(peer)
    try:
        await rig.boot()
        with pytest.raises(UnavailableError):
            await rig.gateway.controls.ensure_ceiling()
        assert not rig.gateway.controls.ready
    finally:
        await rig.close()


# Intent: future device-side application stays unready even if SetChargingProfile was accepted.
# Why: profile acknowledgement and present effect are independent facts in real firmware.
async def test_accepted_but_future_device_profile_never_confirms(tmp_path: Path) -> None:
    rig = StationRig(tmp_path / "state.db", StationChoice("Absolute", 1, ANCHOR))
    original = rig.firmware.answer

    # Model a faulty device that stores a future anchor rather than the requested past one.
    def answer(action: str, payload: dict[str, Any]) -> dict[str, Any]:
        result = original(action, payload)
        if action == "SetChargingProfile" and result["status"] == "Accepted":
            rig.firmware.profiles[7001]["chargingSchedule"]["startSchedule"] = (
                datetime.now(UTC) + timedelta(seconds=60)
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        return result

    rig.firmware.answer = answer
    try:
        await rig.boot()
        with pytest.raises(UnavailableError):
            await rig.gateway.controls.ensure_ceiling()
        assert not rig.gateway.controls.ready and rig.firmware.effective_limit() == 32
    finally:
        await rig.close()


# Intent: first discovery discards a mixed snapshot if the device reintroduces changed firmware between key reads.
# Why: aborting only the current write is insufficient if a later retry trusts a cached old maximum under the new Boot.
@pytest.mark.parametrize("stack", [1, 16])
async def test_first_discovery_boot_change_discards_cache_before_retry(tmp_path: Path, stack: int) -> None:
    rig = StationRig(tmp_path / "state.db", StationChoice("Absolute", stack, ANCHOR))
    original = rig.charger_send
    changed = False

    # Change the independent device after the maximum-stack reply but before the remaining capabilities arrive.
    async def peer(raw: str) -> None:
        nonlocal changed
        frame = decode(raw)
        if (
            frame[0] == 2
            and frame[2] == "GetConfiguration"
            and frame[3]["key"] == ["ChargingScheduleMaxPeriods"]
            and not changed
        ):
            changed = True
            rig.firmware.configuration["ChargeProfileMaxStackLevel"] = "8"
            await rig.charger("new-introduction", "BootNotification", {**BOOT, "firmwareVersion": "changed"})
        await original(raw)

    rig.gateway.attach_charger(peer)
    try:
        await rig.boot()
        with pytest.raises(RecoveryRequiredError):
            await rig.gateway.controls.ensure_ceiling()
        assert rig.discovery.capabilities is None and rig.store.get("station_selection") is None
        count = len(rig.charger_frames)
        if stack == 16:
            with pytest.raises(DeniedError):
                await rig.gateway.controls.ensure_ceiling()
        else:
            await rig.gateway.controls.ensure_ceiling()
            assert rig.discovery.capabilities.maximum_stack == 8 and rig.gateway.controls.ready
        later = [frame for frame in rig.charger_frames[count:] if frame[0] == 2]
        assert [frame[2] for frame in later[:6]] == ["GetConfiguration"] * 6
        if stack == 16:
            assert not any(frame[2] == "SetChargingProfile" for frame in later)
    finally:
        await rig.close()
