"""Scoped cleanup, lost acknowledgements and baseline preservation against independent firmware effects."""

from __future__ import annotations

from pathlib import Path

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.prototype.connection_scope import current_control_scope
from ocpp_hub_core.prototype.policy import DeniedError
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.trial_resources import Baseline

from tests.prototype_rig import START
from tests.tls_support import tls as tls
from tests.trial_helpers import TrialRig


# Intent: transaction alias retirement retains cleanup ownership of the physically present TxProfile.
# Why: Stop may retire cloud mappings before the firmware removes the actual profile.
async def test_rollback_receipts_survive_transaction_retirement(tmp_path, tls, unused_tcp_port):

    from tests.runtime_rig import until
    from tests.test_native_bounded_handover import native_ready

    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, charger):
        await session.command({"command": "release"})
        local = (await rig.bench.call(charger, "start", "StartTransaction", START))[2]["transactionId"]
        await until(lambda: session.journal.vendor_id(local) is not None)
        vendor = session.journal.vendor_id(local)
        payload = commands.set_charging_profile(1, commands.PURPOSE_TX, 6, 101, stack_level=2, transaction_id=vendor)
        result = await session.gateway._vendor_call(
            [2, "profile", "SetChargingProfile", payload],
            session.gateway._vendor_epoch,
            current_control_scope(session.gateway),
        )
        from ocpp_hub_core.prototype.gateway import decode

        assert decode(result)[0] == 3
        local_profile = session.journal.get("vendor_profiles")["101"]
        await rig.bench.call(
            charger,
            "stop",
            "StopTransaction",
            {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1000},
        )
        await until(lambda: session.journal.next_delivery() is None)
        assert session.journal.get("vendor_profiles") == {}
        assert local_profile in rig.bench.firmware.profiles
        await session.command({"command": "native-bounded-handover"})
        assert local_profile not in rig.bench.firmware.profiles and 9 in rig.bench.firmware.plans


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
    await rig.boot()
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
        rig.book.owned_present(rig.evidence())
    assert not rig.clear_order
    await rig.close()


# Intent: frozen native cleanup blocks charging writes and cap reapplication after another transport attaches.
# Why: physical removal is insufficient if a later actor can recreate the deleted hold.
async def test_rollback_freeze_blocks_queued_writes_and_reapplication(tmp_path, tls, unused_tcp_port):
    from tests.test_native_bounded_handover import native_ready

    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, _):
        await session.command({"command": "native-bounded-handover"})
        with pytest.raises(DeniedError):
            await session.gateway.set_limit(6)
        sender = session.gateway.charger.sender
        assert sender is not None
        session.gateway.attach_charger(sender)
        with pytest.raises(UnavailableError):
            await session.gateway.controls.ensure_ceiling()
        assert 7001 not in rig.bench.firmware.profiles


# Intent: a profile reappearing after verified native cleanup cannot become owned again.
# Why: a matching numeric ID and shape may belong to a later external actor.
async def test_resource_reappearance_after_verified_absence_is_not_owned(tmp_path, tls, unused_tcp_port):
    from tests.test_native_bounded_handover import native_ready

    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, _):
        await session.command({"command": "native-bounded-handover"})
        rig.bench.firmware.answer(
            "SetChargingProfile",
            commands.set_charging_profile(0, commands.PURPOSE_CHARGE_POINT_MAX, 6, 7001, stack_level=1),
        )
        with pytest.raises(SecurityError, match="reappeared"):
            await session.command({"command": "native-bounded-handover"})
        assert [frame[3]["id"] for frame in rig.bench.device_calls if frame[2] == "ClearChargingProfile"] == [7001]


# Intent: blocked mode still permits a recorded zero-current hold and stopping the known active session.
# Why: a failed native audit must not disable the owner's safety Stop while physical current can still flow.
async def test_blocked_mode_keeps_safety_stop_available(tmp_path: Path) -> None:
    rig = TrialRig(tmp_path / "state.db")
    await rig.boot()
    await rig.gateway.set_limit(6)
    await rig.charger("start", "StartTransaction", START)
    rig.firmware.requested_current = 6
    rig.store.put("trial_phase", "blocked")
    assert (await rig.gateway.stop())[2]["status"] == "Accepted"
    assert rig.firmware.effective_limit() == 0
    assert any(frame[0] == 2 and frame[2] == "RemoteStopTransaction" for frame in rig.charger_frames)
    assert rig.store.active() == 1001
    await rig.close()
