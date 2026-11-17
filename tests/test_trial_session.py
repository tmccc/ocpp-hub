"""The complete standalone commissioning lifecycle over both real encrypted loopback connections."""

from __future__ import annotations

import asyncio
import copy
import time
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from ocpp_hub_core.frames import build_call, build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.native_http import NativeTransportUnavailableError
from ocpp_hub_core.prototype.security import TRANSPORT_LOG, SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError, fingerprint
from ocpp_hub_core.prototype.trial_allocation import observe_history
from ocpp_hub_core.prototype.trial_evidence import Evidence, Resource, native_profile_projection
from ocpp_hub_core.prototype.trial_session import TrialSession
from websockets.asyncio.client import connect
from websockets.asyncio.server import ServerConnection

from tests.prototype_firmware import Firmware
from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests.test_trial_auth import info
from tests.trial_helpers import native_profile


class Bench:
    """Device and backend own their own state; neither reads the proxy's receipt table to fabricate effects."""

    # Keep one fake physical device alive across proxy process/session replacements.
    def __init__(self) -> None:
        self.firmware = Firmware()
        self.calls: list[list[Any]] = []
        self.sent_vendor_replies: list[list[Any]] = []
        self.configuration = "trial"
        self.cp, self.connection = "A", "Unplug"
        self.online = True
        self.remote_stop = asyncio.Event()
        self.replies: dict[str, asyncio.Future[list[Any]]] = {}
        self.reader: asyncio.Task[None] | None = None
        self.received_profiles: list[int] = []
        self.received_limits: list[float] = []
        self.device_calls: list[list[Any]] = []
        self.composite_reply_delay = 0.0
        self.observation_failure = False
        self.history: list[dict[str, Any]] = []
        self.next_vendor = 9000
        self.boot_on_probe = False
        self.auto_boots = 0
        self.before_reply: Callable[[str, dict[str, Any]], Awaitable[None]] | None = None

    # Use the actual fake firmware's profile list as independent native inventory evidence.
    async def observe(self) -> Evidence:
        if self.observation_failure:
            raise OSError("synthetic native observation failure")
        resources = []
        for identifier, profile in self.firmware.profiles.items():
            connector = 0 if profile["chargingProfilePurpose"] == "ChargePointMaxProfile" else 1
            body = native_profile(profile, connector)
            resources.append(
                Resource(
                    "profile",
                    identifier,
                    body,
                    native_profile_projection(body, connector, body["purpose"], time.time()),
                )
            )
        resources.extend(
            Resource("plan", identifier, copy.deepcopy(body), {"unsupported": True})
            for identifier, body in self.firmware.plans.items()
        )
        return Evidence(
            fingerprint("OFFLINE001"),
            time.time(),
            self.cp,
            self.connection,
            self.firmware.current,
            self.firmware.current * 240,
            self.firmware.active is not None,
            fingerprint(self.configuration),
            tuple(resources),
            self.online,
            self.firmware.active,
            observe_history(self.history, "OFFLINE001", "HARDWARE001", time.time()),
        )

    # The fake cloud answers real correlated frames without sharing transaction mappings with the local gateway.
    async def vendor(self, connection: ServerConnection) -> None:
        async for raw in connection:
            frame = decode(raw)
            self.calls.append(frame)
            if frame[0] != 2:
                continue
            if frame[2] == "BootNotification":
                answer = {"status": "Accepted", "interval": 1, "currentTime": "2030-01-01T00:00:00Z"}
            elif frame[2] == "Authorize":
                answer = {"idTagInfo": info()}
            elif frame[2] == "StartTransaction":
                self.next_vendor += 1
                answer = {"transactionId": self.next_vendor, "idTagInfo": info()}
            elif frame[2] == "Heartbeat":
                answer = {"currentTime": "2030-01-01T00:00:00Z"}
            else:
                answer = {}
            await connection.send(build_call_result(frame[1], answer))
            self.sent_vendor_replies.append([3, frame[1], copy.deepcopy(answer)])

    # Read replies independently from server commands, so local control and lifecycle RPCs cannot deadlock the peer.
    async def read_charger(self, charger: Any) -> None:
        async for raw in charger:
            frame = decode(raw)
            if frame[0] != 2:
                self.replies[frame[1]].set_result(frame)
                continue
            action, payload = frame[2:]
            self.device_calls.append(copy.deepcopy(frame))
            answer = self.firmware.answer(action, payload)
            if action == "GetCompositeSchedule":
                await asyncio.sleep(self.composite_reply_delay)
            if action == "SetChargingProfile":
                self.received_profiles.append(payload["csChargingProfiles"]["chargingProfileId"])
                self.received_limits.append(
                    payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                )
            elif action == "RemoteStopTransaction":
                self.remote_stop.set()
            if self.before_reply is not None:
                await self.before_reply(action, payload)
            await charger.send(build_call_result(frame[1], answer))
            if (
                action == "TriggerMessage"
                and payload.get("requestedMessage") == "BootNotification"
                and self.boot_on_probe
            ):
                self.auto_boots += 1
                token = "device-boot-" + str(self.auto_boots)
                self.replies[token] = asyncio.get_running_loop().create_future()
                await charger.send(build_call(token, "BootNotification", BOOT))

    # Submit an actual device CALL while the separate reader continues answering proxy RPCs.
    async def call(self, charger: Any, token: str, action: str, payload: dict[str, Any]) -> list[Any]:
        future: asyncio.Future[list[Any]] = asyncio.get_running_loop().create_future()
        self.replies[token] = future
        await charger.send(build_call(token, action, payload))
        answer = await asyncio.wait_for(future, 2)
        if answer[0] == 3 and action == "StartTransaction":
            self.firmware.active = answer[2]["transactionId"]
            if not any(row["transactionId"] == self.firmware.active for row in self.history):
                self.history.append({"transactionId": self.firmware.active, "start": copy.deepcopy(payload)})
        elif answer[0] == 3 and action == "StopTransaction":
            self.firmware.active = None
            self.firmware.requested_current = 0
            if self.cp == "C":
                self.cp, self.connection = "B", "Plug"
            for row in self.history:
                if row["transactionId"] == payload["transactionId"]:
                    row["stop"] = copy.deepcopy(payload)
        return answer

    # A new authenticated charger socket represents the same independent physical device after proxy restart.
    def connection_to(self, session: TrialSession) -> Any:
        config = session.prepared.config
        return connect(
            f"wss://127.0.0.1:{config.listener_port}" + config.charger_path,
            ssl=session.prepared.tls.client,
            additional_headers=[]
            if session.prepared.tls.incoming is None
            else [("Authorization", session.prepared.tls.incoming)],
            subprotocols=["ocpp1.6"],
            logger=TRANSPORT_LOG,
            proxy=None,
        )


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

    from tests.test_native_bounded_session import connected_bench

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
