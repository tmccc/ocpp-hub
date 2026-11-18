"""Legacy inventory remains readable while unproven selective clearing cannot damage saved original resources."""

from __future__ import annotations

import copy
from dataclasses import replace
from pathlib import Path

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.prototype.policy import DeniedError
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import Journal
from ocpp_hub_core.prototype.trial_resources import ResourceBook

from tests.prototype_firmware import Firmware
from tests.prototype_rig import START
from tests.tls_support import tls as tls
from tests.trial_helpers import TrialRig


# Load only a historically recorded station profile, never a fresh live write through a weakened admission gate.
def legacy_rig(path: Path, purpose: str = "TxProfile") -> TrialRig:
    rig = TrialRig(path, original_profile=True, original_purpose=purpose)
    rig.seed_legacy_profile(
        commands.set_charging_profile(0, commands.PURPOSE_CHARGE_POINT_MAX, 6, 7001, stack_level=16)
    )
    return rig


# Intent: any unrelated saved profile blocks new profile writes and exact-id clears, including old inactive profiles.
# Why: a closed transaction does not establish that its profile can be deleted without changing the saved baseline.
@pytest.mark.parametrize("purpose", ["TxProfile", "TxDefaultProfile", "unknown"])
async def test_original_profiles_block_writes_and_legacy_clears(tmp_path: Path, purpose: str) -> None:
    rig = legacy_rig(tmp_path / "state.db", purpose)
    original = rig.baseline.value()
    try:
        with pytest.raises(DeniedError):
            await rig.gateway.set_limit(6)
        assert not rig.charger_frames
        assert rig.book.original_profile_count
        with pytest.raises(SecurityError):
            rig.book.require_profile_free_baseline()
        with pytest.raises(DeniedError):
            await rig.gateway.charger.call("ClearChargingProfile", {"id": 7001})
        assert not rig.clear_order and set(rig.firmware.profiles) == {42, 7001}
        assert rig.book.status()["new_profile_writes_blocked"]
        assert rig.baseline.value() == original
        # The refusal did not create a new desired profile or overwrite its previous acknowledgement.
        row = rig.store.db.execute("SELECT * FROM trial_resources").fetchone()
        assert row["state"] == "confirmed" and row["desired"] == row["confirmed"]
    finally:
        await rig.close()


# Intent: replay either possible historical deletion effect without claiming which firmware behaviour caused it.
# Why: both broad clearing and pruning old transaction profiles must preserve the discrepancy and prevent retries.
@pytest.mark.parametrize("mode", ["all_profiles", "prune_inactive"])
@pytest.mark.parametrize("receipt", ["confirmed", "uncertain", "clear_acknowledged"])
async def test_collateral_profile_loss_is_sticky_and_records_owned_absence(
    tmp_path: Path, mode: str, receipt: str
) -> None:
    path = tmp_path / "state.db"
    rig = legacy_rig(path)
    baseline = rig.baseline
    try:
        rig.firmware.clear_mode = mode
        # Reconstruct the old trial's completed device effect, rather than bypassing today's outbound clear gate.
        assert rig.firmware.answer("ClearChargingProfile", {"id": 7001})["status"] == "Accepted"
        rig.store.db.execute("UPDATE trial_resources SET state=?", (receipt,))
        with pytest.raises(SecurityError, match="preservation"):
            rig.book.owned_present(rig.evidence())
        row = rig.store.db.execute("SELECT state FROM trial_resources WHERE key='profile:7001'").fetchone()
        assert row["state"] == "absent_verified"
        fault = rig.store.get("trial_preservation_fault")
        assert fault["missing"] == ["profile:42"] and not fault["changed"]
        assert rig.store.get("trial_phase") == "blocked"
        with pytest.raises(SecurityError):
            rig.book.owned_present(rig.evidence())
        assert rig.store.get("trial_phase") == "blocked" and not rig.clear_order
        assert rig.book.baseline is baseline and 9 in rig.firmware.plans
        assert rig.book.status()["preservation_failed"]
        assert rig.book.status()["missing_original_resources"] == 1
    finally:
        await rig.close()
    reopened = Journal(path, "OFFLINE001")
    try:
        book = ResourceBook(reopened, baseline)
        assert book.status()["preservation_failed"] and book.status()["phase"] == "blocked"
        assert reopened.get("trial_preservation_fault") == fault
        assert reopened.db.execute("SELECT state FROM trial_resources").fetchone()["state"] == "absent_verified"
    finally:
        reopened.close()


# Intent: repairing apparent inventory after a preservation fault cannot complete native restoration.
# Why: identical later readbacks cannot prove that original settings survived an unexplained loss.
async def test_preservation_fault_prevents_completion_after_inventory_returns(tmp_path, tls, unused_tcp_port):
    from tests.test_native_bounded_handover import native_ready, restore_original_route, verify_original_backend

    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, charger):
        await session.command({"command": "native-bounded-handover"})
        await restore_original_route(rig, session, charger)
        await verify_original_backend(rig, session)
        plan = copy.deepcopy(rig.bench.firmware.plans.pop(9))
        with pytest.raises(SecurityError):
            session.book.owned_present((await rig.prepared.observer.observe_handover()).physical)
        rig.bench.firmware.plans[9] = plan
        patches = []

        # A refused final proof must never reach even the independent fake USER writer.
        async def patch(values):
            patches.append(copy.deepcopy(values))
            rig.peer.configuration.update(values)

        with pytest.raises(SecurityError, match="preservation"):
            await session.native_bounded.restore_settings(patch)
        assert patches == [] and rig.peer.configuration["AmpLmt"] == "6"
        assert session.journal.get("trial_phase") == "backend_restore_pending"
        assert session.book.status()["preservation_failed"]


# Intent: a preservation fault cannot prevent a recorded zero hold and the known active transaction's Stop.
# Why: refusing further trial changes must retain the narrowly bounded operations which stop existing charging.
async def test_legacy_preservation_fault_keeps_known_session_stop(tmp_path: Path) -> None:
    rig = legacy_rig(tmp_path / "state.db")
    try:
        # The old process already accepted this lifecycle fact; new positive writes are intentionally forbidden.
        started = rig.store.start("historical-start", START)
        rig.firmware.active = started["transactionId"]
        rig.firmware.requested_current = 6
        rig.firmware.profiles.pop(42)
        with pytest.raises(SecurityError):
            rig.book.owned_present(rig.evidence())
        assert (await rig.gateway.stop())[2]["status"] == "Accepted"
        assert rig.firmware.current == 0 and not rig.gateway.controls.ready
        assert any(frame[0] == 2 and frame[2] == "RemoteStopTransaction" for frame in rig.charger_frames)
        with pytest.raises(DeniedError):
            await rig.gateway.set_limit(6)
    finally:
        await rig.close()


# Intent: Stop after native cleanup cannot reinstall an absent profile, including for a known transaction.
# Why: a frozen cleanup must not be reopened by the restrictive zero-hold exception.
@pytest.mark.parametrize("known_session", [False, True])
async def test_stop_after_clean_rollback_never_recreates_profile(tmp_path, tls, unused_tcp_port, known_session):
    from tests.test_native_bounded_handover import native_ready, restore_original_route, verify_original_backend

    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, charger):
        await session.command({"command": "native-bounded-handover"})
        checkpoint = len(rig.bench.device_calls)
        control = session.journal.get("control")
        if known_session:
            started = session.journal.start("historical-start", START)
            rig.bench.firmware.active = started["transactionId"]
        with pytest.raises(UnavailableError if known_session else DeniedError):
            await session.gateway.stop()
        later = [frame for frame in rig.bench.device_calls[checkpoint:] if frame[0] == 2]
        assert [frame[2] for frame in later] == (["RemoteStopTransaction"] if known_session else [])
        assert not rig.bench.firmware.profiles and not session.gateway.controls.ready
        assert not session.journal.get("trial_preservation_fault")
        assert session.journal.get("trial_phase") == "backend_restore_pending"
        assert (
            session.journal.get("control")["uncertain"] if known_session else session.journal.get("control") == control
        )
        if not known_session:
            await restore_original_route(rig, session, charger)
            await verify_original_backend(rig, session)

            # Change only the fake native USER setting after the real route/backend proofs.
            async def patch(values):
                rig.peer.configuration.update(values)
                rig.peer.runtime_user = int(rig.peer.configuration["AmpLmt"]) * 10

            assert (await session.native_bounded.restore_settings(patch))["complete"]


# Intent: independently observed disappearance invalidates readiness without allowing a second clear in active mode.
# Why: cached confirmation cannot survive a complete native inventory which proves the station profile is gone.
async def test_independent_station_absence_invalidates_readiness_and_clear(tmp_path: Path) -> None:
    rig = TrialRig(tmp_path / "state.db")
    try:
        await rig.gateway.set_limit(6)
        assert rig.gateway.controls.ready
        rig.firmware.answer("ClearChargingProfile", {"id": 7001})
        assert not rig.book.owned_present(rig.evidence())
        assert rig.store.get("trial_phase") == "active" and not rig.gateway.controls.ready
        with pytest.raises(DeniedError):
            await rig.gateway.charger.call("ClearChargingProfile", {"id": 7001})
        assert not rig.clear_order
    finally:
        await rig.close()


# Intent: observed cap drift, raw inventory drift and unknown profiles revoke cached readiness and freeze writes.
# Why: detecting a bad native limit must not leave a confirmed receipt that allows a later charging command through.
@pytest.mark.parametrize("change", ["limit", "native-fields", "unknown-profile"])
async def test_owned_inventory_mismatch_freezes_cached_readiness(tmp_path: Path, change: str) -> None:
    rig = TrialRig(tmp_path / "state.db")
    try:
        await rig.gateway.set_limit(6)
        rig.book.owned_present(rig.evidence())
        if change == "limit":
            rig.firmware.profiles[7001]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] = 32
            evidence = rig.evidence()
        elif change == "unknown-profile":
            rig.firmware.answer(
                "SetChargingProfile", commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 32, 43)
            )
            evidence = rig.evidence()
        else:
            evidence = rig.evidence()
            resources = tuple(
                replace(resource, body={**resource.body, "unexpectedNativeField": 1})
                if resource.identifier == 7001
                else resource
                for resource in evidence.resources
            )
            evidence = replace(evidence, resources=resources)

        with pytest.raises(SecurityError):
            rig.book.owned_present(evidence)
        assert not rig.gateway.controls.ready and rig.store.get("trial_phase") == "blocked"
        assert rig.book.status()["inventory_failed"] and not rig.book.status()["preservation_failed"]
        assert rig.book.status()["uncertain_resources"] == 1
        with pytest.raises(DeniedError):
            await rig.gateway.set_limit(6)
        with pytest.raises(DeniedError):
            await rig.gateway.charger.call("ClearChargingProfile", {"id": 7001})
        assert not rig.clear_order
        # Repairing the apparent body alone cannot silently reconcile the sticky inventory fault.
        rig.firmware.profiles[7001]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] = 6
        rig.firmware.profiles.pop(43, None)
        with pytest.raises(SecurityError):
            rig.book.owned_present(rig.evidence())
    finally:
        await rig.close()


# Intent: an owned-profile inventory fault still permits zero and the known transaction's Stop.
# Why: revoking readiness must prevent new charging while retaining the owner's bounded safety response.
async def test_owned_inventory_fault_keeps_safety_stop(tmp_path: Path) -> None:
    rig = TrialRig(tmp_path / "state.db")
    try:
        await rig.gateway.set_limit(6)
        await rig.charger("start", "StartTransaction", START)
        rig.firmware.requested_current = 32
        rig.firmware.profiles[7001]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] = 32
        with pytest.raises(SecurityError):
            rig.book.owned_present(rig.evidence())
        assert (await rig.gateway.stop())[2]["status"] == "Accepted"
        assert rig.firmware.current == 0 and not rig.gateway.controls.ready
        assert any(frame[0] == 2 and frame[2] == "RemoteStopTransaction" for frame in rig.charger_frames)
        assert rig.book.status()["inventory_failed"]
    finally:
        await rig.close()


# Intent: native cleanup invalidates readiness before any profile is physically cleared.
# Why: cached confirmation cannot authorize charging while its safety profile is being removed.
async def test_rollback_invalidates_readiness_before_clear(tmp_path, tls, unused_tcp_port):
    from tests.test_native_bounded_handover import native_ready

    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, _):
        assert session.gateway.controls.ready
        witnessed = False

        # Observe the real invalidation before vendor retirement and physical mutation.
        async def inspect_freeze():
            nonlocal witnessed
            assert not session.gateway.controls.ready and 7001 in rig.bench.firmware.profiles
            witnessed = True
            await session.runtime.stop_vendor()

        await session.native_bounded.cleanup(inspect_freeze)
        assert witnessed and not rig.bench.firmware.profiles


# Intent: a direct Clear after verified native cleanup cannot target an already absent profile.
# Why: an old ownership receipt is history, not permission for a second destructive deletion.
async def test_resumed_rollback_rejects_direct_duplicate_clear(tmp_path, tls, unused_tcp_port):
    from tests.test_native_bounded_handover import native_ready

    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, _):
        await session.command({"command": "native-bounded-handover"})
        with pytest.raises(SecurityError, match="exact next owned resource"):
            await session.gateway.charger.call("ClearChargingProfile", {"id": 7001})
        with pytest.raises(DeniedError):
            session.book.before_send("ClearChargingProfile", {"id": 7001})
        clears = [frame[3]["id"] for frame in rig.bench.device_calls if frame[2] == "ClearChargingProfile"]
        assert clears == [7001] and not rig.bench.firmware.profiles


# Intent: the two synthetic destructive-clear hypotheses have different effects on non-stale profiles.
# Why: reproducing the observed loss must not turn an unproven explanation into the firmware model's default.
@pytest.mark.parametrize("mode", ["scoped", "all_profiles", "prune_inactive"])
def test_synthetic_clear_hypotheses_remain_distinct(mode: str) -> None:
    firmware = Firmware()
    firmware.active = 77
    for identifier, transaction in ((42, 77), (43, 78)):
        firmware.answer(
            "SetChargingProfile",
            commands.set_charging_profile(1, commands.PURPOSE_TX, 6, identifier, transaction_id=transaction),
        )
    firmware.answer("SetChargingProfile", commands.set_charging_profile(0, commands.PURPOSE_CHARGE_POINT_MAX, 6, 7001))
    firmware.clear_mode = mode
    firmware.answer("ClearChargingProfile", {"id": 7001})
    assert set(firmware.profiles) == {"scoped": {42, 43}, "all_profiles": set(), "prune_inactive": {42}}[mode]


# Intent: an old-confirmed native digest observed during a pending update cannot poison the accepted new profile.
# Why: legitimate6→0 transition bytes differ, while later unrelated byte drift must remain a durable blocker.
async def test_pending_set_old_digest_is_retired_on_real_acceptance(tmp_path: Path) -> None:
    rig = TrialRig(tmp_path / "state.db")
    try:
        await rig.boot()
        await rig.gateway.set_limit(6)
        old = rig.evidence()
        zero = commands.set_charging_profile(0, commands.PURPOSE_CHARGE_POINT_MAX, 0, 7001)
        rig.book.before_send("SetChargingProfile", zero)
        rig.book.owned_present(old)
        row = rig.store.db.execute("SELECT observed_digest FROM trial_resources").fetchone()
        assert row["observed_digest"] is not None
        actual_ack = rig.firmware.answer("SetChargingProfile", zero)
        rig.book.after_result("SetChargingProfile", zero, [3, "real-peer-ack", actual_ack])
        current = rig.evidence()
        rig.book.owned_present(current)
        assert not rig.store.get("trial_inventory_fault")
        resource = next(row for row in current.resources if row.key == "profile:7001")
        changed = replace(resource, body={**resource.body, "unexpected_native_field": True})
        drift = replace(
            current, resources=tuple(changed if row.key == resource.key else row for row in current.resources)
        )
        with pytest.raises(SecurityError):
            rig.book.owned_present(drift)
        assert rig.store.get("trial_inventory_fault")["reason"] == "owned_profile_bytes_changed"
    finally:
        await rig.gateway.close()
        rig.store.close()
