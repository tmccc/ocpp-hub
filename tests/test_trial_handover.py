"""Fresh independent vehicle evidence gates connected cleanup through the real standalone TLS runtime."""

from __future__ import annotations

import asyncio
import copy
import sqlite3
import ssl
from pathlib import Path

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError
from ocpp_hub_core.prototype.trial_session import TrialSession

from tests.handover_helpers import ORIGINAL, VEHICLE, ready_handover
from tests.test_prototype import START
from tests.test_prototype_runtime import FAST
from tests.test_prototype_tls import tls as tls


# Intent: legacy cleanup still refuses an attached car; the default missing provider cannot freeze or clear it.
# Why: installing the new checks must not silently enable connected cleanup from power readings alone.
async def test_connected_cleanup_is_unavailable_by_default(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
) -> None:
    async with ready_handover(tmp_path, tls, unused_tcp_port, provider=False) as rig:
        with pytest.raises(SecurityError):
            await rig.session.command({"command": "rollback"})
        phase = rig.session.journal.get("trial_phase")
        with pytest.raises(UnavailableError, match="configured live"):
            await rig.session.command({"command": "connected-handover"})
        assert rig.session.journal.get("trial_phase") == phase
        assert rig.session.journal.get("connected_handover") is None
        assert not any(frame[2] == "ClearChargingProfile" for frame in rig.bench.device_calls)
        assert 7001 in rig.bench.firmware.profiles


# Intent: a lost actual Clear response resumes from native absence in a replacement session without any new profile.
# Why: ordinary connected recovery requires a present station hold and must not recreate a deliberately cleared one.
async def test_connected_lost_clear_ack_resumes_without_listener_or_reclear(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
) -> None:
    async with ready_handover(tmp_path, tls, unused_tcp_port) as rig:
        release_reply = asyncio.Event()

        # The independent device has already deleted the exact profile when this transport reply is delayed.
        async def delay_clear(action: str, payload: dict) -> None:
            if action == "ClearChargingProfile":
                await release_reply.wait()

        rig.bench.before_reply = delay_clear
        with pytest.raises(UnavailableError):
            await rig.session.command({"command": "connected-handover"})
        assert 7001 not in rig.bench.firmware.profiles
        release_reply.set()
        await rig.session.close()
        old_writes = list(rig.bench.received_limits)
        second = TrialSession(
            rig.session.prepared, FAST, handover_provider=rig.session.handover.provider, handover_vehicle_digest=VEHICLE
        )
        try:
            with pytest.raises(SecurityError):
                await second.start()
            await second.resume_connected_handover()
            assert second.runtime._server is None
            result = await second.command({"command": "connected-handover"})
            assert result["profiles_restored"]
            assert rig.bench.received_limits == old_writes
            assert len([f for f in rig.bench.device_calls if f[2] == "ClearChargingProfile"]) == 1
        finally:
            await second.close()


# Intent: pause revocation after deletion blocks every later restoration step and retains USER6.
# Why: a prior successful witness cannot authorise backend or vehicle writes after its owner state changes.
async def test_pause_revocation_after_clear_keeps_handover_incomplete(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
) -> None:
    async with ready_handover(tmp_path, tls, unused_tcp_port) as rig:
        # A separate vehicle-side owner changes its permission while the actual charger reply is in flight.
        async def revoke(action: str, payload: dict) -> None:
            if action == "ClearChargingProfile":
                rig.peer.paused, rig.peer.revision = False, "other-owner"

        rig.bench.before_reply = revoke
        with pytest.raises(SecurityError):
            await rig.session.command({"command": "connected-handover"})
        assert rig.peer.runtime_user == 60 and rig.peer.configuration["AmpLmt"] == "6"
        assert rig.session.journal.get("trial_phase") == "rollback"
        assert rig.session.handover.status()["complete"] is False
        assert rig.peer.restore_calls == 0 and 7001 not in rig.bench.firmware.profiles


# Intent: normal charging after a successful car restore is observed as real manufacturer-side activity.
# Why: restoring the previous permission must not fabricate a local transaction or require another vehicle Start.
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_vehicle_restore_reconciles_normal_charging_without_repeating_start(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    lost_reply: bool,
) -> None:
    async with ready_handover(tmp_path, tls, unused_tcp_port) as rig:
        await rig.restore_charger_settings()
        rig.peer.start_normal_on_restore = True
        rig.peer.drop_restore_reply = lost_reply
        if lost_reply:
            with pytest.raises(SecurityError):
                await rig.session.command({"command": "restore-connected-vehicle-permission"})
            assert rig.peer.restore_calls == 1 and rig.bench.firmware.active == 900001
            assert rig.session.journal.get("connected_handover")["stage"] == "vehicle_restore_intent"
        result = await rig.session.command({"command": "restore-connected-vehicle-permission"})
        assert result["complete"] and rig.peer.restore_calls == 1
        assert rig.peer.status_calls == (1 if lost_reply else 0)
        assert rig.session.journal.active() is None
        assert rig.session.journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0
        assert rig.session.journal.get("connected_handover")["normal_native_transaction"] == 900001
        assert rig.peer.native("METER")[0]["meterData"]["power"][0] == 7680


# Intent: remove only the owned profile with fresh car pause, verify original settings, then restore the car last.
# Why: the lead can remain attached without inventing an unplugged reading or clearing an unowned resource.
async def test_connected_handover_full_sequence(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
) -> None:
    async with ready_handover(tmp_path, tls, unused_tcp_port) as rig:
        original_history = copy.deepcopy(rig.bench.history)
        result = await rig.session.command({"command": "connected-handover"})
        assert result["vehicle_pause_retained"] and not result["complete"]
        assert not rig.bench.firmware.profiles and rig.peer.runtime_user == 60 and rig.peer.paused
        assert list(rig.bench.firmware.plans) == [9]
        with pytest.raises(RecoveryRequiredError):
            await rig.session.command({"command": "restore-connected-vehicle-permission"})
        assert rig.peer.restore_calls == 0
        await rig.restore_backend()
        assert (await rig.session.command({"command": "confirm-connected-original-backend"}))[
            "original_backend_verified"
        ]
        rig.restore_user()
        assert (await rig.session.command({"command": "confirm-connected-original-settings"}))[
            "original_configuration_verified"
        ]
        assert rig.peer.paused
        final = await rig.session.command({"command": "restore-connected-vehicle-permission"})
        assert final["complete"] and final["lead_may_remain_connected"]
        assert rig.peer.restore_calls == 1 and not rig.peer.paused
        assert rig.peer.configuration == ORIGINAL and rig.peer.runtime_user == 320
        assert rig.bench.history == original_history and list(rig.bench.firmware.plans) == [9]
        assert rig.session.journal.get("trial_phase") == "complete"


# Intent: two reads of one cached object cannot satisfy the independent stable-zero observation requirement.
# Why: sleeping between reads does not make an old source timestamp a new physical observation.
async def test_cached_native_snapshot_cannot_authorise_connected_cleanup(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
) -> None:
    async with ready_handover(tmp_path, tls, unused_tcp_port) as rig:
        cached = await rig.session.prepared.observer.observe_handover()

        # Return one previously genuine native result without performing another independent observation.
        async def old_view():
            return cached

        rig.session.handover.observe = old_view
        with pytest.raises(UnavailableError):
            await rig.session.command({"command": "connected-handover-plan"})
        assert not any(frame[2] == "ClearChargingProfile" for frame in rig.bench.device_calls)


# Intent: a provider cannot change its own pinned identity while supplying a valid-looking pause result.
# Why: the durable provider binding must survive asynchronous readback rather than trust a mutable attribute.
async def test_provider_identity_change_during_read_is_rejected(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch,
) -> None:
    from ocpp_hub_core.prototype.store import fingerprint

    async with ready_handover(tmp_path, tls, unused_tcp_port) as rig:
        provider = rig.session.handover.provider
        assert provider is not None
        original_read = provider.observe

        # Keep real HTTP readback but substitute the adapter identity before returning to the controller.
        async def switch(request):
            proof = await original_read(request)
            provider.digest = fingerprint("replacement-provider")
            return proof

        monkeypatch.setattr(provider, "observe", switch)
        with pytest.raises(SecurityError, match="provider identity changed"):
            await rig.session.command({"command": "connected-handover"})
        assert 7001 in rig.bench.firmware.profiles


# Intent: an owner permission revision change after planning invalidates the original pause receipt.
# Why: the cleanup must not overwrite newer owner decisions when returning vehicle permission later.
async def test_owner_revision_change_blocks_connected_cleanup(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
) -> None:
    async with ready_handover(tmp_path, tls, unused_tcp_port) as rig:
        await rig.session.command({"command": "connected-handover-plan"})
        rig.peer.revision = "new-owner-decision"
        with pytest.raises(SecurityError, match="permission or capability changed"):
            await rig.session.command({"command": "connected-handover"})
        assert 7001 in rig.bench.firmware.profiles and rig.peer.restore_calls == 0


# Intent: unknown final resources remain unowned and block completion even after the car permission was restored.
# Why: only ordinary original-backend profiles tied to a genuine native session can be accepted as normal activity.
@pytest.mark.parametrize("resource", ["plan", "station", "unrelated-tx", "normal-tx"])
async def test_final_resource_scope_stays_tied_to_actual_normal_session(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    resource: str,
) -> None:
    async with ready_handover(tmp_path, tls, unused_tcp_port) as rig:
        await rig.restore_charger_settings()
        rig.peer.start_normal_on_restore = True
        rig.peer.resource_on_restore = resource
        if resource == "normal-tx":
            assert (await rig.session.command({"command": "restore-connected-vehicle-permission"}))["complete"]
        else:
            with pytest.raises(SecurityError, match="unrelated native resource"):
                await rig.session.command({"command": "restore-connected-vehicle-permission"})
            assert rig.session.handover.status()["complete"] is False
            assert rig.session.handover.status()["vehicle_permission_restore_confirmed"] is True
        assert rig.peer.restore_calls == 1


# Intent: final completion's handover stage and overall trial phase commit together or both roll back.
# Why: process loss between two commits must not strand an already restored car behind contradictory journal stages.
async def test_final_completion_is_atomic_and_reconciles_without_another_start(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch,
) -> None:
    async with ready_handover(tmp_path, tls, unused_tcp_port) as rig:
        await rig.restore_charger_settings()
        journal = rig.session.journal
        original_put = journal.put
        fail = True

        # Fail precisely after the stage write would have happened but before the overall complete phase.
        def fail_complete(key, value):
            nonlocal fail
            if fail and key == "trial_phase" and value == "complete":
                fail = False
                raise sqlite3.OperationalError("synthetic interrupted completion")
            original_put(key, value)

        monkeypatch.setattr(journal, "put", fail_complete)
        with pytest.raises(sqlite3.OperationalError):
            await rig.session.command({"command": "restore-connected-vehicle-permission"})
        assert journal.get("connected_handover")["stage"] == "vehicle_restore_intent"
        assert journal.get("trial_phase") == "backend_restore_pending"
        assert rig.peer.restore_calls == 1
        assert (await rig.session.command({"command": "restore-connected-vehicle-permission"}))["complete"]
        assert rig.peer.restore_calls == 1 and rig.peer.status_calls == 1


# Intent: unplugging after a valid connected plan still permits the original stronger unplugged cleanup path.
# Why: a plan record must not strand the zero profile, and its pending car permission must not be silently discarded.
async def test_unplugged_fallback_preserves_and_reconciles_vehicle_obligation(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
) -> None:
    async with ready_handover(tmp_path, tls, unused_tcp_port) as rig:
        await rig.session.command({"command": "connected-handover-plan"})
        rig.bench.cp, rig.bench.connection = "A", "Unplug"
        assert (await rig.session.command({"command": "rollback"}))["profiles_restored"]
        await rig.restore_backend()
        rig.restore_user()
        result = await rig.session.command({"command": "confirm-original-backend"})
        assert result["vehicle_permission_restore_required"] and not result["complete"]
        assert rig.session.journal.get("trial_phase") == "backend_restore_pending"
        assert rig.session.journal.get("connected_handover")["pause"]
        with pytest.raises(SecurityError):
            await rig.session.command({"command": "confirm-unplugged-vehicle-permission"})
        rig.peer.paused, rig.peer.revision = False, "owner-restored-original"
        result = await rig.session.command({"command": "confirm-unplugged-vehicle-permission"})
        assert result["complete"] and not result["vehicle_command_sent"]
        assert rig.peer.restore_calls == 0


# Intent: a failed initial plan with no accepted vehicle receipt cannot disable ordinary unplugged restoration.
# Why: unverified evidence must not create a fictional owned car-permission obligation.
async def test_failed_plan_record_does_not_block_ordinary_unplugged_cleanup(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
) -> None:
    async with ready_handover(tmp_path, tls, unused_tcp_port) as rig:
        rig.peer.observation_fault = "expired"
        with pytest.raises(SecurityError):
            await rig.session.command({"command": "connected-handover-plan"})
        rig.bench.cp, rig.bench.connection = "A", "Unplug"
        await rig.session.command({"command": "rollback"})
        await rig.restore_backend()
        rig.restore_user()
        result = await rig.session.command({"command": "confirm-original-backend"})
        assert result["complete"] and not result["vehicle_permission_restore_required"]
        assert rig.peer.restore_calls == 0


# Intent: recheck the actual zero station profile after a pending real write releases the charger RPC gate.
# Why: a Release approved earlier may settle after the initial audit; clearing a newly nonzero hold must be refused.
async def test_late_release_cannot_replace_zero_hold_during_clear_gate_wait(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
) -> None:
    async with ready_handover(tmp_path, tls, unused_tcp_port) as rig:
        sent, finish = asyncio.Event(), asyncio.Event()
        zero = copy.deepcopy(rig.bench.firmware.profiles[7001])
        rig.session.gateway.charger.timeout = 15

        # Delay this independent device's real application of a six-ampere command until after quiescence.
        async def delayed_application(action, payload):
            if (
                action == "SetChargingProfile"
                and payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 6
            ):
                positive = copy.deepcopy(rig.bench.firmware.profiles[7001])
                rig.bench.firmware.profiles[7001] = copy.deepcopy(zero)
                sent.set()
                await finish.wait()
                rig.bench.firmware.profiles[7001] = positive

        rig.bench.before_reply = delayed_application
        payload = commands.set_charging_profile(0, commands.PURPOSE_CHARGE_POINT_MAX, 6, 7001, stack_level=16)
        pending = asyncio.create_task(rig.session.gateway.charger.call("SetChargingProfile", payload))
        await asyncio.wait_for(sent.wait(), 2)

        # Use the controller's recorded quiesced observation only to schedule the independent delayed device effect.
        async def settle():
            while rig.session.journal.get("connected_handover", {}).get("last_request", {}).get("stage") != "quiesced":
                await asyncio.sleep(0.01)
            finish.set()

        settling = asyncio.create_task(settle())
        try:
            with pytest.raises(SecurityError, match="owned zero station hold"):
                await rig.session.command({"command": "connected-handover"})
        finally:
            finish.set()
            settling.cancel()
            await asyncio.gather(pending, settling, return_exceptions=True)
        assert not any(frame[2] == "ClearChargingProfile" for frame in rig.bench.device_calls)
        assert rig.bench.firmware.effective_limit() == 6


# Intent: a restarted cleanup with an existing owned zero hold uses real Boot but never reapplies a profile.
# Why: normal recovery must not rearm charging, and a revoked proof must remain visible as a classified failure.
@pytest.mark.parametrize("revoke", [False, True])
@pytest.mark.parametrize("initial_audit_failure", [False, True])
async def test_cleanup_only_reconnection_preserves_hold_without_reapplication(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    revoke: bool,
    initial_audit_failure: bool,
    monkeypatch,
) -> None:
    from tests.test_prototype import BOOT

    async with ready_handover(tmp_path, tls, unused_tcp_port) as rig:
        # Fail after the initial closed-zero proof, leaving a real owned zero profile for the next process.
        async def interrupted_quiesce():
            raise UnavailableError("synthetic process interruption")

        if initial_audit_failure:
            rig.peer.observation_fault = "expired"
            with pytest.raises(SecurityError):
                await rig.session.command({"command": "connected-handover"})
            rig.peer.observation_fault = None
        else:
            monkeypatch.setattr(rig.session.runtime, "stop_vendor", interrupted_quiesce)
            with pytest.raises(UnavailableError):
                await rig.session.command({"command": "connected-handover"})
        await rig.session.close()
        old_limits = list(rig.bench.received_limits)
        second = TrialSession(
            rig.session.prepared, FAST, handover_provider=rig.session.handover.provider, handover_vehicle_digest=VEHICLE
        )
        try:
            await second.resume_connected_handover()
            assert second.connected_cleanup_only
            if revoke:
                rig.peer.paused = False
            async with rig.bench.connection_to(second) as charger:
                reader = asyncio.create_task(rig.bench.read_charger(charger))
                try:
                    await rig.bench.call(charger, "cleanup-boot", "BootNotification", BOOT)
                    async with asyncio.timeout(10):
                        while True:
                            if revoke and second.runtime.status.last_failure == "BootOrCeilingUnavailable":
                                break
                            if (
                                not revoke
                                and second.journal.get("connected_handover")["last_request"]["stage"]
                                == "cleanup_introduction"
                            ):
                                break
                            await asyncio.sleep(0.01)
                    if not revoke:
                        assert (await second.command({"command": "connected-handover"}))["profiles_restored"]
                    else:
                        assert 7001 in rig.bench.firmware.profiles
                    assert rig.bench.received_limits == old_limits
                    assert not second.runtime.status.vendor_connected
                finally:
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            await second.close()


# Intent: current nonce, vehicle, lifetime and physical capability are required even when the charger shows zero.
# Why: historical success or an optimistic provider flag cannot authorise clearing the station's safety hold.
@pytest.mark.parametrize(
    "fault",
    [
        "expired",
        "future",
        "wrong-nonce",
        "wrong-vehicle",
        "short-hold",
        "automatic-release",
        "no-reboot-proof",
        "car-unpaused",
    ],
)
async def test_connected_cleanup_rejects_bad_vehicle_evidence(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    fault: str,
) -> None:
    async with ready_handover(tmp_path, tls, unused_tcp_port) as rig:
        rig.peer.observation_fault = fault
        if fault == "car-unpaused":
            rig.peer.paused = False
        with pytest.raises(SecurityError):
            await rig.session.command({"command": "connected-handover"})
        assert rig.session.journal.get("trial_phase") == "blocked"
        assert 7001 in rig.bench.firmware.profiles and rig.peer.runtime_user == 60
        assert not any(frame[2] == "ClearChargingProfile" for frame in rig.bench.device_calls)
        assert rig.peer.restore_calls == 0


# Intent: independently check saved USER, runtime USER, hardware ceiling and every phase before clearing anything.
# Why: an accepted setting or zero total watts cannot conceal an ineffective limit or a live secondary phase.
@pytest.mark.parametrize("fault", ["saved-user", "runtime-user", "hardware", "secondary-phase"])
async def test_connected_cleanup_rejects_bad_native_guard(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    fault: str,
) -> None:
    async with ready_handover(tmp_path, tls, unused_tcp_port) as rig:
        if fault == "saved-user":
            rig.peer.configuration["AmpLmt"] = "32"
        elif fault == "runtime-user":
            rig.peer.runtime_user = 320
        elif fault == "hardware":
            rig.peer.hardware = 160
        else:
            rig.peer.phase_fault = True
        with pytest.raises(SecurityError):
            await rig.session.command({"command": "connected-handover"})
        assert 7001 in rig.bench.firmware.profiles
        assert not any(frame[2] == "ClearChargingProfile" for frame in rig.bench.device_calls)


# Intent: an initial connected audit failure retains the real shutdown Stop path for an actual charging session.
# Why: entering cleanup too early must not skip the zero hold or strand an active native transaction on close.
async def test_failed_connected_audit_preserves_safety_shutdown(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
) -> None:
    async with ready_handover(tmp_path, tls, unused_tcp_port) as rig:
        await rig.session.command({"command": "release"})
        await rig.bench.call(rig.charger, "actual-start", "StartTransaction", START)
        rig.bench.cp, rig.bench.connection = "C", "Charging"
        rig.bench.firmware.requested_current = 6
        rig.peer.paused, rig.peer.permission_enabled = False, True
        with pytest.raises(SecurityError):
            await rig.session.command({"command": "connected-handover"})
        assert rig.session.journal.get("trial_phase") == "blocked"
        closing = asyncio.create_task(rig.session.close())
        await asyncio.wait_for(rig.bench.remote_stop.wait(), 2)
        assert rig.bench.firmware.effective_limit() == 0
        actual = rig.bench.firmware.active
        await rig.bench.call(
            rig.charger,
            "actual-stop",
            "StopTransaction",
            {"transactionId": actual, "timestamp": START["timestamp"], "meterStop": 2000},
        )
        await closing
        assert not any(frame[2] == "ClearChargingProfile" for frame in rig.bench.device_calls)
