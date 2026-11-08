"""Scoped cleanup, lost acknowledgements and baseline preservation against independent firmware effects."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.prototype.policy import DeniedError
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError
from ocpp_hub_core.prototype.trial_resources import Baseline, ResourceBook
from ocpp_hub_core.prototype.trial_rollback import Rollback

from tests.test_prototype import START
from tests.trial_helpers import TrialRig, no_cloud


# Intent: remove only newly created profiles, keep the original plan byte-equal and clear the station ceiling last.
# Why: restoring a backend URL alone can leave a persistent zero-current hold or erase someone else's plan.
async def test_rollback_preserves_original_resources_and_requires_backend_verification(tmp_path: Path) -> None:
    rig = TrialRig(tmp_path / "state.db")
    originals = rig.baseline.value()
    await rig.gateway.set_limit(6)
    profile = commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 100001, stack_level=2)
    await rig.gateway.charger.call("SetChargingProfile", profile)
    await rig.gateway.stop()
    result = await rig.rollback.run(rig.quiesce)
    assert result == {"profiles_restored": True, "original_backend_restore_required": True, "complete": False}
    assert rig.clear_order == [100001, 7001]
    assert not rig.firmware.profiles and list(rig.firmware.plans) == [9]
    assert rig.book.baseline.value() == originals
    with pytest.raises(UnavailableError):
        await rig.gateway.controls.ensure_ceiling()
    assert 7001 not in rig.firmware.profiles
    rig.configuration = "still on proxy"
    with pytest.raises(SecurityError):
        await rig.rollback.confirm_original_backend()
    rig.configuration = "original"
    rig.online = False
    with pytest.raises(SecurityError):
        await rig.rollback.confirm_original_backend()
    rig.online = True
    with pytest.raises(SecurityError):
        await rig.rollback.confirm_original_backend()
    rig.gateway.detach_charger(rig.gateway._charger_epoch)
    assert (await rig.rollback.confirm_original_backend())["complete"]
    await rig.close()


# Intent: transaction alias retirement does not destroy cleanup ownership of a physically retained TxProfile.
# Why: firmware may keep profiles after Stop even though normal cloud id translation has safely retired them.
async def test_rollback_receipts_survive_transaction_retirement(tmp_path: Path) -> None:
    rig = TrialRig(tmp_path / "state.db", original_profile=False)
    await rig.boot()
    await rig.charger("start", "StartTransaction", START)
    await rig.cloud()
    payload = commands.set_charging_profile(1, commands.PURPOSE_TX, 6, 101, stack_level=2, transaction_id=9001)
    assert (await rig.vendor("profile", "SetChargingProfile", payload))[0] == 3
    local_profile = rig.store.get("vendor_profiles")["101"]
    await rig.charger(
        "stop", "StopTransaction", {"transactionId": 1001, "timestamp": START["timestamp"], "meterStop": 1000}
    )
    await rig.gateway.flush()
    assert rig.store.get("vendor_profiles") == {}
    assert local_profile in rig.firmware.profiles
    await rig.rollback.run(rig.quiesce)
    assert local_profile not in rig.firmware.profiles and 9 in rig.firmware.plans
    await rig.close()


# Intent: a physically successful deletion with a lost reply resumes from independent absence after restart.
# Why: retrying clears blindly can remove a reused resource; ownership and complete inventory must decide instead.
async def test_rollback_lost_ack_is_resumable_without_repeating_clear(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    rig = TrialRig(path)
    await rig.gateway.set_limit(6)
    rig.drop_clear = True
    with pytest.raises(UnavailableError):
        await rig.rollback.run(rig.quiesce)
    assert rig.clear_order == [7001] and 7001 not in rig.firmware.profiles
    assert rig.store.get("trial_phase") == "rollback"
    rig.drop_clear = False
    # Recreate the ownership service just as a restarted process reads its existing durable receipt table.
    book = ResourceBook(rig.store, rig.baseline)
    rollback = Rollback(rig.gateway, book, rig.observe)
    assert not (await rollback.run(no_cloud))["complete"]
    assert rig.clear_order == [7001]
    assert rig.store.get("rollback_pending") == {}
    await rig.close()


# Intent: a peer ACK without physical deletion cannot declare cleanup complete.
# Why: protocol acknowledgement and actual profile persistence are different evidence channels.
async def test_rollback_verifies_physical_deletion(tmp_path: Path) -> None:
    rig = TrialRig(tmp_path / "state.db")
    await rig.gateway.set_limit(6)
    rig.ack_without_clear = True
    with pytest.raises(UnavailableError, match="retained"):
        await rig.rollback.run(rig.quiesce)
    assert rig.store.get("trial_phase") == "rollback" and 7001 in rig.firmware.profiles
    await rig.close()


# Intent: plugged zero-power, stale evidence and native-open sessions never authorize removing a safety profile.
# Why: a paused or fully charged car is still connected and can request energy later.
@pytest.mark.parametrize("change", ["plugged", "stale", "native-open", "journal-open", "pending-start"])
async def test_rollback_physical_and_lifecycle_preconditions(tmp_path: Path, change: str) -> None:
    rig = TrialRig(tmp_path / "state.db")
    await rig.gateway.set_limit(6)
    if change == "plugged":
        rig.cp, rig.connection = "B", "Plug"
    elif change == "stale":
        rig.age = 31
    elif change == "native-open":
        rig.native_open = True
    elif change == "journal-open":
        await rig.charger("start", "StartTransaction", START)
        await rig.gateway.stop()  # An actual RemoteStop ACK is deliberately insufficient without StopTransaction.
    else:
        rig.store.put("control", {"start_pending": True})
    with pytest.raises((SecurityError, RecoveryRequiredError)):
        await rig.rollback.run(rig.quiesce)
    assert not rig.clear_order and 7001 in rig.firmware.profiles
    await rig.close()


# Intent: baseline collisions and edits to original profile scopes fail before the fake device sees a write.
# Why: actual iCS firmware can replace the previous TxProfile at the same stack despite a different transaction id.
async def test_baseline_and_original_scope_collisions(tmp_path: Path) -> None:
    rig = TrialRig(tmp_path / "state.db", original_profile=True)
    rig.seed_legacy_profile(
        commands.set_charging_profile(0, commands.PURPOSE_CHARGE_POINT_MAX, 6, 7001, stack_level=16)
    )
    with pytest.raises(SecurityError):
        Baseline.capture(rig.store.identity, rig.evidence())
    count = len(rig.charger_frames)
    replacement = commands.set_charging_profile(1, commands.PURPOSE_TX, 6, 777, stack_level=1, transaction_id=1001)
    with pytest.raises(DeniedError):
        await rig.gateway.charger.call("SetChargingProfile", replacement)
    assert len(rig.charger_frames) == count and 42 in rig.firmware.profiles
    plan = {"vendorId": "IoCharger", "messageId": "clearPlans", "data": "[9]"}
    with pytest.raises(DeniedError):
        await rig.gateway.charger.call("DataTransfer", plan)
    assert 9 in rig.firmware.plans
    await rig.close()


# Intent: mismatched, newly appeared or out-of-band changed resources prevent automatic cleanup.
# Why: a shared numeric id is not proof that its current contents still belong to this trial.
@pytest.mark.parametrize("change", ["original-changed", "unknown-profile", "owned-changed"])
async def test_rollback_rejects_changed_inventory(tmp_path: Path, change: str) -> None:
    rig = TrialRig(tmp_path / "state.db")
    await rig.gateway.set_limit(6)
    if change == "original-changed":
        rig.firmware.plans[9]["limitCurrent"] = 16
    elif change == "unknown-profile":
        rig.firmware.answer(
            "SetChargingProfile",
            commands.set_charging_profile(1, commands.PURPOSE_TX, 6, 888, stack_level=3, transaction_id=1001),
        )
    else:
        rig.firmware.profiles[7001]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] = 5
    with pytest.raises(SecurityError):
        await rig.rollback.run(rig.quiesce)
    assert not rig.clear_order
    await rig.close()


# Intent: freezing rollback prevents a queued control or reconnect from reinstalling a cleared station profile.
# Why: deleting the physical hold is not sufficient if another task can immediately create it again.
async def test_rollback_freeze_blocks_queued_writes_and_reapplication(tmp_path: Path) -> None:
    rig = TrialRig(tmp_path / "state.db")
    await rig.gateway.set_limit(6)
    await rig.rollback.run(rig.quiesce)
    with pytest.raises(DeniedError):
        await rig.gateway.set_limit(6)
    rig.gateway.attach_charger(rig.charger_send)
    with pytest.raises(UnavailableError):
        await rig.gateway.controls.ensure_ceiling()
    assert 7001 not in rig.firmware.profiles
    await rig.close()


# Intent: an id which reappears after independently verified deletion is not automatically owned again.
# Why: a matching numeric id and profile shape can belong to a later external actor.
async def test_resource_reappearance_after_verified_absence_is_not_owned(tmp_path: Path) -> None:
    rig = TrialRig(tmp_path / "state.db")
    await rig.gateway.set_limit(6)
    await rig.rollback.run(rig.quiesce)
    rig.firmware.answer(
        "SetChargingProfile",
        commands.set_charging_profile(0, commands.PURPOSE_CHARGE_POINT_MAX, 6, 7001, stack_level=16),
    )
    with pytest.raises(SecurityError, match="reappeared"):
        await rig.rollback.run(rig.quiesce)
    assert rig.clear_order == [7001]
    await rig.close()


# Intent: blocked mode still permits a recorded zero-current hold and stopping the known active session.
# Why: a failed native audit must not disable the owner's safety Stop while physical current can still flow.
async def test_blocked_mode_keeps_safety_stop_available(tmp_path: Path) -> None:
    rig = TrialRig(tmp_path / "state.db")
    await rig.gateway.set_limit(6)
    await rig.charger("start", "StartTransaction", START)
    rig.firmware.requested_current = 6
    rig.store.put("trial_phase", "blocked")
    assert (await rig.gateway.stop())[2]["status"] == "Accepted"
    assert rig.firmware.effective_limit() == 0
    assert any(frame[0] == 2 and frame[2] == "RemoteStopTransaction" for frame in rig.charger_frames)
    assert rig.store.active() == 1001
    await rig.close()


# Intent: reconnecting to the proxy during native confirmation cannot prove original-backend restoration.
# Why: configuration and online state can be stale while a different transport takes ownership during the read.
async def test_backend_confirmation_rechecks_connection_after_native_wait(tmp_path: Path) -> None:
    rig = TrialRig(tmp_path / "state.db")
    await rig.gateway.set_limit(6)
    await rig.rollback.run(rig.quiesce)
    rig.gateway.detach_charger(rig.gateway._charger_epoch)

    # An independently observed snapshot races with an actual new admitted gateway transport generation.
    async def reconnect() -> Any:
        evidence = rig.evidence()
        rig.gateway.attach_charger(rig.charger_send)
        return evidence

    rollback = Rollback(rig.gateway, rig.book, reconnect)
    with pytest.raises(SecurityError, match="changed"):
        await rollback.confirm_original_backend()
    assert rig.store.get("trial_phase") == "backend_restore_pending"
    await rig.close()
