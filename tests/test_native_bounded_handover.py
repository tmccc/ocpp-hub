"""Native six-ampere cleanup and restoration use actual TLS effects and independent native HTTP facts."""

from __future__ import annotations

import asyncio
import copy
import sqlite3
import ssl
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests.test_native_bounded_session import ConnectedBench, connected_bench
from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Introduce the real connected device through both TLS legs, with the native HTTP peer retaining the USER6 guard.
@asynccontextmanager
async def native_ready(
    directory: Path, contexts: tuple[ssl.SSLContext, ssl.SSLContext, Path], port: int
) -> AsyncIterator[tuple[ConnectedBench, Any, Any]]:
    async with connected_bench(directory, contexts, port) as rig:
        session = rig.session()
        await session.start()
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            try:
                assert (await rig.bench.call(charger, "native-cleanup-boot", "BootNotification", BOOT))[0] == 3
                await until(
                    lambda: session.gateway.controls.ready and session.gateway.vendor_registration == "Accepted"
                )
                yield rig, session, charger
            finally:
                await session.close()


# Model only the explicit original-route patch and actual transport retirement; evidence still comes through HTTP.
async def restore_original_route(rig: ConnectedBench, session: Any, charger: Any) -> list[dict[str, Any]]:
    patches: list[dict[str, Any]] = []

    # The injected native writer changes independent device state and never edits the coordinator's journal.
    async def patch(values: dict[str, Any]) -> None:
        patches.append(copy.deepcopy(values))
        rig.peer.configuration.update(values)

    # Retire the physical proxy connection before recording original-backend observations.
    async def retire() -> None:
        await charger.close()
        await until(lambda: session.gateway.charger.sender is None)
        rig.bench.online = False

    await session.native_bounded.restore_route(patch, retire)
    return patches


# Prove a real original-backend offline-to-online transition after the independently verified route change.
async def verify_original_backend(rig: ConnectedBench, session: Any) -> None:
    rig.bench.online = False
    assert not (await session.native_bounded.observe_backend())["original_backend_connected"]
    rig.bench.online = True
    assert (await session.native_bounded.observe_backend())["original_backend_connected"]


# Intent: an admitted trial that never receives Boot can clean up using fresh closed native6 without inventing a hold.
# Why: routing can fail before any socket/profile exists, and recovery must not require a fabricated Stop or unplug.
async def test_aborted_before_boot_cleanup_needs_native6_but_no_profile_or_rpc(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        original_history = copy.deepcopy(rig.bench.history)
        session = rig.session()
        await session.start()
        rig.route_to_trial()
        assert session.gateway.charger.sender is None and session.journal.get("boot") is None
        assert session.journal.get("trial_binding") is None
        assert session.journal.db.execute("SELECT COUNT(*) FROM trial_resources").fetchone()[0] == 0
        rig.peer.runtime_user = 320
        with pytest.raises(SecurityError):
            await session.command({"command": "native-bounded-handover"})
        assert session.journal.get("trial_phase") != "backend_restore_pending"
        assert not rig.bench.device_calls and not rig.bench.calls
        rig.peer.runtime_user = 60
        assert (await session.command({"command": "native-bounded-handover"}))["profiles_restored"]
        assert session.journal.get("trial_phase") == "backend_restore_pending"
        assert session.native_bounded.status()["restoration_pending"]
        assert session.journal.db.execute("SELECT COUNT(*) FROM trial_resources").fetchone()[0] == 0
        assert session.journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0
        assert not rig.bench.device_calls and not rig.bench.calls
        assert rig.bench.history == original_history
        assert rig.peer.configuration["AmpLmt"] == "6" and rig.peer.runtime_user == 60


# Intent: restart after verified final profile absence opens no listener and finishes cleanup without another RPC.
# Why: process loss just before backend-pending persistence must never reapply the cleared station hold.
async def test_rollback_restart_after_actual_absence_never_listens_or_reclears(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int, monkeypatch: Any
) -> None:
    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, _):
        original_put = session.journal.put
        fail = True

        # Leave the last deletion physically applied and independently verified while the pending-stage write fails.
        def fail_pending(key: str, value: Any) -> None:
            nonlocal fail
            if fail and key == "trial_phase" and value == "backend_restore_pending":
                fail = False
                raise sqlite3.OperationalError("synthetic interruption after independently verified absence")
            original_put(key, value)

        monkeypatch.setattr(session.journal, "put", fail_pending)
        with pytest.raises(sqlite3.OperationalError):
            await session.command({"command": "native-bounded-handover"})
        assert session.journal.get("trial_phase") == "rollback"
        assert not rig.bench.firmware.profiles
        assert (
            session.journal.db.execute("SELECT state FROM trial_resources WHERE key='profile:7001'").fetchone()[0]
            == "absent_verified"
        )
        before = copy.deepcopy(rig.bench.device_calls)
        await session.close()
        second = rig.session()
        await second.start()
        assert second.runtime._server is None and not second.started
        assert second.gateway.charger.sender is None
        assert (await second.command({"command": "native-bounded-handover"}))["profiles_restored"]
        assert second.journal.get("trial_phase") == "backend_restore_pending"
        assert rig.bench.device_calls == before
        assert [frame[3] for frame in before if frame[2] == "ClearChargingProfile"] == [{"id": 7001}]
        assert rig.peer.configuration["AmpLmt"] == "6" and rig.peer.runtime_user == 60


# Intent: remove exactly the separately receipted profiles, with the zero station profile last and original data intact.
# Why: connected cleanup must not clear a blanket scope or erase the original saved plan and historical records.
async def test_native_cleanup_preserves_originals_and_clears_station_last(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, _):
        original_history = copy.deepcopy(rig.bench.history)
        original_plan = copy.deepcopy(rig.bench.firmware.plans)
        await session.command({"command": "release"})
        extra = commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 100001, stack_level=2)
        assert (await session.gateway.charger.call("SetChargingProfile", extra))[2]["status"] == "Accepted"
        assert rig.bench.firmware.effective_limit() == 6
        result = await session.command({"command": "native-bounded-handover"})
        assert result == {"profiles_restored": True, "native_limit_a": 6, "complete": False}
        clears = [frame[3] for frame in rig.bench.device_calls if frame[2] == "ClearChargingProfile"]
        assert clears == [{"id": 100001}, {"id": 7001}]
        before_clear = rig.bench.device_calls[
            : next(index for index, frame in enumerate(rig.bench.device_calls) if frame[2] == "ClearChargingProfile")
        ]
        station_sets = [
            frame[3]["csChargingProfiles"]
            for frame in before_clear
            if frame[2] == "SetChargingProfile" and frame[3]["csChargingProfiles"]["chargingProfileId"] == 7001
        ]
        assert station_sets[-1]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
        assert not rig.bench.firmware.profiles
        assert rig.bench.firmware.plans == original_plan and rig.bench.history == original_history
        assert rig.peer.configuration["AmpLmt"] == "6" and rig.peer.runtime_user == 60
        assert session.journal.get("trial_phase") == "backend_restore_pending"
        assert session.native_bounded.status()["restoration_pending"]
        assert not session.gateway.controls.ready
        assert {row[0] for row in session.journal.db.execute("SELECT state FROM trial_resources")} == {
            "absent_verified"
        }


# Intent: a failed current native audit prevents destructive clears even when the protocol ceiling was once ready.
# Why: an old confirmed profile or saved native setting cannot substitute for present independent limit evidence.
@pytest.mark.parametrize("fault", ["runtime-user", "schedule", "secondary-phase"])
async def test_failed_native_guard_blocks_clear(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int, fault: str
) -> None:
    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, _):
        if fault == "runtime-user":
            rig.peer.runtime_user = 320
        elif fault == "schedule":
            rig.bench.firmware.plans[9]["limitCurrent"] = 1
        else:
            rig.peer.phase_fault = True
        with pytest.raises(SecurityError):
            await session.command({"command": "native-bounded-handover"})
        assert not any(frame[2] == "ClearChargingProfile" for frame in rig.bench.device_calls)
        assert 7001 in rig.bench.firmware.profiles
        assert session.journal.get("trial_phase") != "complete"
        assert not session.native_bounded.status()["native_limit_freshly_verified"]


# Intent: an externally open native transaction remains external and blocks cleanup without inventing its Stop.
# Why: a connected zero-current car can retain an active session that was never created by this proxy ledger.
async def test_unknown_native_transaction_is_not_adopted_or_stopped(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, _):
        rig.bench.firmware.active = 9000042
        with pytest.raises(RecoveryRequiredError, match="external"):
            await session.command({"command": "native-bounded-handover"})
        assert rig.bench.firmware.active == 9000042
        assert session.journal.active() is None
        assert session.journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0
        assert not any(
            frame[2] in {"RemoteStopTransaction", "ClearChargingProfile"} for frame in rig.bench.device_calls
        )
        assert not any(frame[2] in {"StartTransaction", "StopTransaction"} for frame in rig.bench.calls)


# Intent: a real known charging session remains open after RemoteStop Accepted and clears only after its actual Stop.
# Why: cleanup must retain actual Start/Stop history and cloud acknowledgements rather than manufacture closure.
async def test_native_cleanup_waits_for_actual_known_stop_and_preserves_its_history(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, charger):
        await session.command({"command": "release"})
        started = await rig.bench.call(charger, "native-real-start", "StartTransaction", START)
        actual = started[2]["transactionId"]
        assert actual == session.journal.active()
        rig.peer.paused, rig.peer.permission_enabled = False, True
        rig.bench.cp, rig.bench.connection = "C", "Charging"
        rig.bench.firmware.requested_current = 6
        await until(lambda: session.journal.next_delivery() is None)
        cleanup = asyncio.create_task(session.command({"command": "native-bounded-handover"}))
        try:
            await asyncio.wait_for(rig.bench.remote_stop.wait(), 2)
            await asyncio.sleep(0.03)
            assert not cleanup.done() and session.journal.active() == actual
            assert not any(frame[2] == "ClearChargingProfile" for frame in rig.bench.device_calls)
            stopped = {"transactionId": actual, "timestamp": START["timestamp"], "meterStop": 2000}
            rig.bench.firmware.active = None
            rig.bench.firmware.requested_current = 0
            rig.bench.cp, rig.bench.connection = "B", "Plug"
            rig.peer.permission_enabled = False
            for row in rig.bench.history:
                if row["transactionId"] == actual:
                    row["stop"] = copy.deepcopy(stopped)
            assert (await rig.bench.call(charger, "native-real-stop", "StopTransaction", stopped))[0] == 3
            assert (await cleanup)["profiles_restored"]
            assert session.journal.active() is None and session.journal.next_delivery() is None
            assert any(row["transactionId"] == actual and row.get("stop") == stopped for row in rig.bench.history)
            assert [frame[3] for frame in rig.bench.device_calls if frame[2] == "RemoteStopTransaction"] == [
                {"transactionId": actual}
            ]
            assert len([frame for frame in rig.bench.calls if frame[2] == "StartTransaction"]) == 1
            assert len([frame for frame in rig.bench.calls if frame[2] == "StopTransaction"]) == 1
        finally:
            cleanup.cancel()
            await asyncio.gather(cleanup, return_exceptions=True)


# Intent: independent physical absence reconciles a lost actual Clear reply without issuing the deletion twice.
# Why: a timeout is not rejection, and repeating an exact ID can delete a subsequently reused resource.
async def test_lost_clear_ack_uses_actual_absence_without_reclear(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, _):
        release = asyncio.Event()
        session.gateway.charger.timeout = 0.1

        # Firmware has already removed the resource before this genuine correlated response is withheld.
        async def hold_reply(action: str, payload: dict[str, Any]) -> None:
            if action == "ClearChargingProfile":
                await release.wait()

        rig.bench.before_reply = hold_reply
        try:
            assert (await session.command({"command": "native-bounded-handover"}))["profiles_restored"]
            assert 7001 not in rig.bench.firmware.profiles
            assert (
                session.journal.db.execute("SELECT state FROM trial_resources WHERE key='profile:7001'").fetchone()[0]
                == "absent_verified"
            )
            assert (await session.command({"command": "native-bounded-handover"}))["profiles_restored"]
            assert len([frame for frame in rig.bench.device_calls if frame[2] == "ClearChargingProfile"]) == 1
        finally:
            release.set()


# Intent: an original route readback and an already-online bit cannot stand in for actual fresh backend reconnection.
# Why: the native OCPP online cache can still describe the old proxy connection during a route transition.
async def test_backend_confirmation_requires_fresh_offline_then_online(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, charger):
        await session.command({"command": "native-bounded-handover"})
        premature: list[dict[str, Any]] = []

        # An old online bit after TLS retirement cannot authorize applying the original route.
        async def patch_before_offline(values: dict[str, Any]) -> None:
            premature.append(values)

        # Retire the actual socket while deliberately retaining the independent native peer's old online view.
        async def retired_with_stale_online() -> None:
            await charger.close()
            await until(lambda: session.gateway.charger.sender is None)

        with pytest.raises(UnavailableError, match="observed offline"):
            await session.native_bounded.restore_route(patch_before_offline, retired_with_stale_online)
        assert premature == []
        patches = await restore_original_route(rig, session, charger)
        assert patches == [{key: rig.spec.original_configuration[key] for key in rig.spec.trial_route}]
        assert not (await session.native_bounded.observe_backend())["original_backend_connected"]
        assert session.journal.get("native_bounded_handover")["stage"] == "awaiting_backend"
        await verify_original_backend(rig, session)
        assert session.journal.get("native_bounded_handover")["stage"] == "backend_verified"
        assert rig.peer.runtime_user == 60 and rig.peer.configuration["AmpLmt"] == "6"


# Intent: fresh native-limit failure or a new manufacturer transaction prevents raising USER6 to the original32.
# Why: original-route restoration alone cannot discharge the remaining current-limit restoration obligation.
@pytest.mark.parametrize("fault", ["runtime-user", "new-native-transaction"])
async def test_fresh_native_guard_blocks_original32_release(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int, fault: str
) -> None:
    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, charger):
        await session.command({"command": "native-bounded-handover"})
        await restore_original_route(rig, session, charger)
        await verify_original_backend(rig, session)
        if fault == "runtime-user":
            rig.peer.runtime_user = 320
        else:
            rig.bench.firmware.active = 9000042
        writes: list[dict[str, Any]] = []

        # The writer must remain unused when actual native admission is no longer satisfied.
        async def patch(values: dict[str, Any]) -> None:
            writes.append(values)

        with pytest.raises(SecurityError):
            await session.native_bounded.restore_settings(patch)
        assert writes == [] and rig.peer.configuration["AmpLmt"] == "6"
        assert session.journal.active() is None and session.journal.get("trial_phase") == "backend_restore_pending"


# Intent: a real route change with a lost response survives process loss and is read back without another patch.
# Why: replaying a connection mutation or reboot can disrupt the original backend that has already been restored.
async def test_lost_original_route_reply_reconciles_after_process_replacement(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, charger):
        await session.command({"command": "native-bounded-handover"})
        writes: list[dict[str, Any]] = []
        retires = 0

        # Apply exactly the original four fields on the independent peer before losing the reply.
        async def lost_reply(values: dict[str, Any]) -> None:
            writes.append(copy.deepcopy(values))
            rig.peer.configuration.update(values)
            raise UnavailableError("synthetic lost original-route response")

        # Actual transport retirement is independently visible through the native online getter.
        async def retire() -> None:
            nonlocal retires
            retires += 1
            await charger.close()
            await until(lambda: session.gateway.charger.sender is None)
            rig.bench.online = False

        with pytest.raises(UnavailableError):
            await session.native_bounded.restore_route(lost_reply, retire)
        assert session.journal.get("native_bounded_handover")["stage"] == "route_intent"
        await session.close()
        second = rig.session()
        await second.native_bounded.restore_route(lost_reply, retire)
        assert len(writes) == retires == 1
        assert second.runtime._server is None and rig.peer.runtime_user == 60
        assert rig.peer.configuration == rig.spec.configuration("original")
        assert second.journal.get("native_bounded_handover")["route_restored"]
        assert second.journal.get("trial_phase") == "backend_restore_pending"
        await verify_original_backend(rig, second)


# Intent: process replacement retains a lost USER32 effect and reconciles it without another physical write or listener.
# Why: restoration intent must survive restart, and original authentication mode cannot be silently discarded.
async def test_lost_original32_reply_reconciles_after_restart_without_rewrite(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, charger):
        await session.command({"command": "native-bounded-handover"})
        await restore_original_route(rig, session, charger)
        await verify_original_backend(rig, session)
        writes: list[dict[str, Any]] = []

        # Independently apply the real device effect before losing the native setter acknowledgement.
        async def lost_reply(values: dict[str, Any]) -> None:
            writes.append(copy.deepcopy(values))
            rig.peer.configuration.update(values)
            rig.peer.runtime_user = 320
            raise UnavailableError("synthetic native reply loss after actual32")

        with pytest.raises(UnavailableError):
            await session.native_bounded.restore_settings(lost_reply)
        assert session.journal.get("native_bounded_handover")["stage"] == "settings_intent"
        await session.close()
        with pytest.raises(SecurityError, match="explicit original mode"):
            rig.session(bounded=False)
        second = rig.session()
        with pytest.raises(RecoveryRequiredError, match="no new listener"):
            await second.start()
        assert second.runtime._server is None
        assert not second.native_bounded.status()["native_limit_freshly_verified"]
        assert (await second.native_bounded.restore_settings(lost_reply))["complete"]
        assert writes == [{"AmpLmt": "32"}]
        assert second.journal.get("trial_phase") == "complete"
        assert second.journal.get("native_bounded_handover")["stage"] == "complete"
        assert second.journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0


# Intent: the final mode receipt and overall phase commit together after the actual original current setting returns.
# Why: a database failure between separate writes must not strand a restored charger behind contradictory stages.
async def test_native_final_completion_is_atomic_and_reconciles_without_rewrite(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int, monkeypatch: Any
) -> None:
    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, charger):
        await session.command({"command": "native-bounded-handover"})
        await restore_original_route(rig, session, charger)
        await verify_original_backend(rig, session)
        writes: list[dict[str, Any]] = []

        # Change only the independently held native USER setting, leaving every other configuration byte unchanged.
        async def patch(values: dict[str, Any]) -> None:
            writes.append(copy.deepcopy(values))
            rig.peer.configuration.update(values)
            rig.peer.runtime_user = 320

        original_put = session.journal.put
        fail = True

        # Interrupt the final phase write exactly after the handover-stage write would otherwise have committed.
        def interrupt(key: str, value: Any) -> None:
            nonlocal fail
            if fail and key == "trial_phase" and value == "complete":
                fail = False
                raise sqlite3.OperationalError("synthetic final commit interruption")
            original_put(key, value)

        monkeypatch.setattr(session.journal, "put", interrupt)
        with pytest.raises(sqlite3.OperationalError):
            await session.native_bounded.restore_settings(patch)
        assert session.journal.get("trial_phase") == "backend_restore_pending"
        assert session.journal.get("native_bounded_handover")["stage"] == "settings_intent"
        assert (await session.native_bounded.restore_settings(patch))["complete"]
        assert writes == [{"AmpLmt": "32"}]


# Intent: actual manufacturer charging may start after the authorized original32 restore without becoming proxy work.
# Why: an enabled original schedule can resume naturally and must not be stopped or falsely adopted to claim closure.
async def test_original_schedule_start_after32_restore_remains_native_activity(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, charger):
        old_history = copy.deepcopy(rig.bench.history)
        old_plan = copy.deepcopy(rig.bench.firmware.plans)
        await session.command({"command": "native-bounded-handover"})
        await restore_original_route(rig, session, charger)
        await verify_original_backend(rig, session)

        # The independent manufacturer's scheduled Start follows restoration, after the proxy transport retired.
        async def patch(values: dict[str, Any]) -> None:
            rig.peer.configuration.update(values)
            rig.peer.runtime_user = 320
            rig.peer.paused, rig.peer.permission_enabled = False, True
            rig.bench.cp, rig.bench.connection = "C", "Charging"
            rig.bench.firmware.active = 9000042
            rig.bench.firmware.requested_current = 32
            rig.bench.firmware.answer(
                "SetChargingProfile",
                commands.set_charging_profile(
                    1, commands.PURPOSE_TX, 32, 2000042, stack_level=1, transaction_id=9000042
                ),
            )
            rig.bench.history.append({"transactionId": 9000042, "source": "actual-original-backend-schedule"})

        assert (await session.native_bounded.restore_settings(patch))["complete"]
        assert rig.bench.firmware.active == 9000042
        assert rig.bench.firmware.current == 32
        assert session.gateway.charger.sender is None and session.journal.active() is None
        assert session.journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0
        assert all(row in rig.bench.history for row in old_history) and rig.bench.firmware.plans == old_plan
        assert set(rig.bench.firmware.profiles) == {2000042}
