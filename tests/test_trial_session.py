"""The complete standalone commissioning lifecycle over both real encrypted loopback connections."""

from __future__ import annotations

import asyncio

import pytest
from ocpp_hub_core.prototype.native_http import NativeTransportUnavailableError
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests.prototype_rig import START
from tests.runtime_rig import until
from tests.tls_support import tls as tls


# Intent: whole-second device timestamps and a delayed real TLS reply still confirm a presently effective ceiling.
# Why: a one-second request can expire in transit even when the charger accepted and applied the requested limit.
async def test_rounded_composite_survives_reply_delay(tmp_path, tls, unused_tcp_port):
    from tests.test_native_bounded_handover import native_ready

    async with native_ready(tmp_path, tls, unused_tcp_port, composite_reply_delay=1.05) as (rig, session, _):
        await session.command({"command": "release"})
        assert rig.bench.firmware.effective_limit() == 6
        assert (await session.command({"command": "native-bounded-handover"}))["profiles_restored"]


# Intent: shutdown after a preservation failure and verified profile absence sends no new profile or clear.
# Why: the zero-hold recovery exception must not resurrect a trial limit after cleanup has removed it.
async def test_fault_shutdown_does_not_recreate_absent_profile(tmp_path, tls, unused_tcp_port):
    from tests.test_native_bounded_handover import native_ready

    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, _):
        rig.bench.firmware.profiles.clear()
        rig.bench.firmware.plans.clear()
        with pytest.raises(SecurityError):
            session.book.owned_present((await rig.prepared.observer.observe_handover()).physical)
        received = list(rig.bench.received_limits)
        await session.close()
        assert rig.bench.received_limits == received and not rig.bench.firmware.profiles
        assert session.final_status is not None and session.final_status["preservation_failed"]
        assert session.final_status["shutdown_uncertain"]


# Intent: a failed native cleanup audit still permits shutdown zero and the exact known transaction's Stop.
# Why: freezing cleanup before validation must not bypass the session's normal safety shutdown path.
@pytest.mark.parametrize("failure", ["native-guard", "observation"])
async def test_failed_rollback_keeps_safety_shutdown(tmp_path, tls, unused_tcp_port, unused_tcp_port_factory, failure):
    from tests.test_native_bounded_handover import native_ready

    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, charger):
        await session.command({"command": "release"})
        started = await rig.bench.call(charger, "start", "StartTransaction", START)
        local = started[2]["transactionId"]
        rig.bench.firmware.requested_current = 6
        native_port = rig.prepared.observer.port
        if failure == "native-guard":
            rig.peer.phase_fault = True
        else:
            rig.prepared.observer.port = unused_tcp_port_factory()
        try:
            with pytest.raises(SecurityError if failure == "native-guard" else NativeTransportUnavailableError):
                await session.command({"command": "native-bounded-handover"})
        finally:
            rig.peer.phase_fault = False
            rig.prepared.observer.port = native_port
        assert session.journal.get("trial_phase") == "blocked"
        closing = asyncio.create_task(session.close())
        await asyncio.wait_for(rig.bench.remote_stop.wait(), 2)
        assert rig.bench.firmware.current == 0
        await rig.bench.call(
            charger,
            "stop",
            "StopTransaction",
            {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1000},
        )
        await closing
        assert session.final_status is not None and not session.final_status["shutdown_uncertain"]


# Intent: native commissioning keeps actual authorization, Start, Stop and cleanup through both TLS legs.
# Why: retiring a session mode must preserve authentication and station-kind coverage of the shared transport.
@pytest.mark.parametrize("incoming_mode", ["basic", "none"])
@pytest.mark.parametrize("profile_kind", ["Relative", "Absolute"])
async def test_trial_session_full_lifecycle_and_verified_rollback(
    tmp_path, tls, unused_tcp_port, incoming_mode, profile_kind
):
    from ocpp_hub_core.prototype.station import StationChoice

    from tests.test_native_bounded_handover import native_ready, restore_original_route, verify_original_backend

    choice = StationChoice(profile_kind, 1, "2020-01-01T00:00:00Z" if profile_kind == "Absolute" else None)
    async with native_ready(tmp_path, tls, unused_tcp_port, station=choice, incoming_mode=incoming_mode) as (
        rig,
        session,
        charger,
    ):
        bench = rig.bench
        status = await session.command({"command": "status"})
        assert status["effective_ceiling_confirmed"] and status["boot_observed"]
        await session.command({"command": "start"})
        assert (await bench.call(charger, "auth", "Authorize", {"idTag": "LOCAL_TEST"}))[2]["idTagInfo"][
            "status"
        ] == "Accepted"
        local = (await bench.call(charger, "start", "StartTransaction", START))[2]["transactionId"]
        await until(lambda: session.journal.vendor_id(local) == 9001)
        bench.cp, bench.connection = "C", "Charging"
        bench.firmware.requested_current = 32
        rig.peer.paused = False
        assert bench.firmware.current == 6
        stopped = await session.command({"command": "stop"})
        assert stopped["transaction_still_open"] and bench.firmware.current == 0
        await bench.call(
            charger,
            "stop",
            "StopTransaction",
            {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1000},
        )
        await until(lambda: session.journal.next_delivery() is None)
        assert not (await session.command({"command": "native-bounded-handover"}))["complete"]
        assert 7001 not in bench.firmware.profiles
        with pytest.raises(RecoveryRequiredError, match="original route"):
            await session.native_bounded.observe_backend()
        await restore_original_route(rig, session, charger)
        await verify_original_backend(rig, session)

        # Apply only the explicit synthetic native patch; verification still comes through real HTTP.
        async def patch(values):
            rig.peer.configuration.update(values)
            rig.peer.runtime_user = int(rig.peer.configuration["AmpLmt"]) * 10

        assert (await session.native_bounded.restore_settings(patch))["complete"]
        assert not session.gateway.failures


# Intent: an older baseline containing an inactive original profile blocks native admission before any listener.
# Why: parsing retained evidence cannot grant permission for unproven selective cleanup beside original profiles.
async def test_original_profile_blocks_fresh_trial_before_binding(tmp_path, tls, unused_tcp_port):
    from dataclasses import replace

    from ocpp_hub_core import commands

    from tests.connected_rig import connected_bench

    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        rig.bench.firmware.answer(
            "SetChargingProfile", commands.set_charging_profile(1, commands.PURPOSE_TX, 32, 42, transaction_id=77)
        )
        native = await rig.prepared.observer.observe_handover()
        # Model an existing saved baseline without weakening the new profile-free capture function.
        rig.prepared.baseline = replace(rig.prepared.baseline, resources=native.physical.resources)
        assert rig.prepared.summary()["new_profile_writes_blocked"]
        session = rig.session()
        with pytest.raises(SecurityError):
            await session.start()
        assert not session.started and not session.runtime.status.charger_connected
        assert not session.runtime.status.vendor_connected and not rig.bench.received_profiles
        assert session.journal.get("trial_phase") == "prepared"
