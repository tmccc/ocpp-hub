"""The complete standalone commissioning lifecycle over both real encrypted loopback connections."""

from __future__ import annotations

import asyncio
import ssl
import time
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.frames import build_call, build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.security import TRANSPORT_LOG, SecurityError
from ocpp_hub_core.prototype.store import fingerprint
from ocpp_hub_core.prototype.trial_evidence import Evidence, Resource, profile_projection
from ocpp_hub_core.prototype.trial_session import TrialSession, preflight
from websockets.asyncio.client import connect
from websockets.asyncio.server import ServerConnection, serve

from tests.prototype_firmware import Firmware
from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import FAST, until
from tests.test_prototype_tls import tls as tls
from tests.test_trial_auth import info
from tests.trial_helpers import config_files, native_profile


class Bench:
    """Device and backend own their own state; neither reads the proxy's receipt table to fabricate effects."""

    # Keep one fake physical device alive across proxy process/session replacements.
    def __init__(self) -> None:
        self.firmware = Firmware()
        self.calls: list[list[Any]] = []
        self.configuration = "trial"
        self.cp, self.connection = "A", "Unplug"
        self.online = True
        self.remote_stop = asyncio.Event()
        self.replies: dict[str, asyncio.Future[list[Any]]] = {}
        self.reader: asyncio.Task[None] | None = None
        self.received_profiles: list[int] = []

    # Use the actual fake firmware's profile list as independent native inventory evidence.
    async def observe(self) -> Evidence:
        resources = []
        for identifier, profile in self.firmware.profiles.items():
            connector = 0 if profile["chargingProfilePurpose"] == "ChargePointMaxProfile" else 1
            resources.append(
                Resource(
                    "profile",
                    identifier,
                    native_profile(profile, connector),
                    profile_projection({"connectorId": connector, "csChargingProfiles": profile}),
                )
            )
        return Evidence(
            fingerprint("OFFLINE001"),
            time.time(),
            self.cp,
            self.connection,
            0,
            0,
            self.firmware.active is not None,
            fingerprint(self.configuration),
            tuple(resources),
            self.online,
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
                answer = {"transactionId": 9001, "idTagInfo": info()}
            elif frame[2] == "Heartbeat":
                answer = {"currentTime": "2030-01-01T00:00:00Z"}
            else:
                answer = {}
            await connection.send(build_call_result(frame[1], answer))

    # Read replies independently from server commands, so local control and lifecycle RPCs cannot deadlock the peer.
    async def read_charger(self, charger: Any) -> None:
        async for raw in charger:
            frame = decode(raw)
            if frame[0] != 2:
                self.replies[frame[1]].set_result(frame)
                continue
            action, payload = frame[2:]
            if action == "ClearChargingProfile":
                self.firmware.profiles.pop(payload["id"], None)
                answer = {"status": "Accepted"}
            else:
                answer = self.firmware.answer(action, payload)
            if action == "SetChargingProfile":
                self.received_profiles.append(payload["csChargingProfiles"]["chargingProfileId"])
            elif action == "RemoteStopTransaction":
                self.remote_stop.set()
            await charger.send(build_call_result(frame[1], answer))

    # Submit an actual device CALL while the separate reader continues answering proxy RPCs.
    async def call(self, charger: Any, token: str, action: str, payload: dict[str, Any]) -> list[Any]:
        future: asyncio.Future[list[Any]] = asyncio.get_running_loop().create_future()
        self.replies[token] = future
        await charger.send(build_call(token, action, payload))
        answer = await asyncio.wait_for(future, 2)
        if answer[0] == 3 and action == "StartTransaction":
            self.firmware.active = answer[2]["transactionId"]
        elif answer[0] == 3 and action == "StopTransaction":
            self.firmware.active = None
        return answer

    # A new authenticated charger socket represents the same independent physical device after proxy restart.
    def connection_to(self, session: TrialSession) -> Any:
        config = session.prepared.config
        return connect(
            f"wss://127.0.0.1:{config.listener_port}" + config.charger_path,
            ssl=session.prepared.tls.client,
            additional_headers=[("Authorization", session.prepared.tls.incoming)],
            subprotocols=["ocpp1.6"],
            logger=TRANSPORT_LOG,
            proxy=None,
        )


# Intent: exercise start, authorization, mirroring, stop and rollback through the real transport with fake TLS peers.
# Why: constructor/file tests cannot prove a usable standalone trial package or clean retirement of its owned cap.
async def test_trial_session_full_lifecycle_and_verified_rollback(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    bench = Bench()
    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        config, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        session = TrialSession(preflight(path), FAST, bench.observe)
        try:
            await session.start()
            async with bench.connection_to(session) as charger:
                reader = asyncio.create_task(bench.read_charger(charger))
                try:
                    assert (await bench.call(charger, "boot", "BootNotification", BOOT))[2]["status"] == "Accepted"
                    await until(
                        lambda: session.gateway.controls.ready and session.gateway.vendor_registration == "Accepted"
                    )
                    status = await session.command({"command": "status"})
                    assert status["effective_ceiling_confirmed"] and status["boot_observed"]
                    await session.command({"command": "start"})
                    assert (await bench.call(charger, "auth", "Authorize", {"idTag": "LOCAL_TEST"}))[2]["idTagInfo"][
                        "status"
                    ] == "Accepted"
                    await bench.call(charger, "start", "StartTransaction", START)
                    await until(lambda: session.journal.vendor_id(1001) == 9001)
                    bench.cp, bench.connection = "B", "Plug"
                    bench.firmware.requested_current = 32
                    assert bench.firmware.current == 6
                    stopped = await session.command({"command": "stop"})
                    assert stopped["transaction_still_open"] and bench.firmware.current == 0
                    with pytest.raises((SecurityError, UnavailableError)):
                        await session.command({"command": "rollback-plan"})
                    await bench.call(
                        charger,
                        "stop",
                        "StopTransaction",
                        {"transactionId": 1001, "timestamp": START["timestamp"], "meterStop": 1000},
                    )
                    await until(lambda: session.journal.next_delivery() is None)
                    bench.cp, bench.connection = "A", "Unplug"
                    assert (await session.command({"command": "rollback-plan"}))["owned_profiles_to_clear"] == 1
                    assert not (await session.command({"command": "rollback"}))["complete"]
                    assert 7001 not in bench.firmware.profiles
                    with pytest.raises(SecurityError):
                        await session.command({"command": "confirm-original-backend"})
                finally:
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
            await until(lambda: session.gateway.charger.sender is None)
            bench.configuration = "original"
            assert (await session.command({"command": "confirm-original-backend"}))["complete"]
            assert not session.gateway.failures
        finally:
            await session.close()


# Intent: an unplugged but open session after process loss can reconnect under a zero hold and complete its real Stop.
# Why: requiring the session already closed before binding creates a recovery deadlock on this iCS firmware.
async def test_trial_restart_opens_restricted_recovery_listener(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    bench = Bench()
    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        prepared = preflight(path)
        first = TrialSession(prepared, FAST, bench.observe)
        await first.start()
        async with bench.connection_to(first) as charger:
            reader = asyncio.create_task(bench.read_charger(charger))
            try:
                await bench.call(charger, "boot", "BootNotification", BOOT)
                await until(lambda: first.gateway.controls.ready and first.gateway.vendor_registration == "Accepted")
                await bench.call(charger, "start", "StartTransaction", START)
                await until(lambda: first.journal.vendor_id(1001) == 9001)
                # Simulate process loss rather than graceful shutdown; firmware and its open transaction survive.
                await first.runtime.close()
                first.journal.close()
                first.closed = True
            finally:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
        second = TrialSession(prepared, FAST, bench.observe)
        try:
            await second.start()
            assert second.journal.get("trial_phase") == "recovering"
            async with bench.connection_to(second) as charger:
                reader = asyncio.create_task(bench.read_charger(charger))
                try:
                    bench.remote_stop.clear()
                    await bench.call(charger, "boot2", "BootNotification", BOOT)
                    await asyncio.wait_for(bench.remote_stop.wait(), 2)
                    assert bench.firmware.effective_limit() == 0
                    with pytest.raises(Exception):
                        await second.command({"command": "start"})
                    await bench.call(
                        charger,
                        "stop",
                        "StopTransaction",
                        {"transactionId": 1001, "timestamp": START["timestamp"], "meterStop": 1000},
                    )
                    await until(lambda: second.journal.next_delivery() is None)
                    assert second.journal.active() is None
                    assert len([f for f in bench.calls if f[0] == 2 and f[2] == "StartTransaction"]) == 1
                    await second.command({"command": "rollback"})
                finally:
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            await second.close()


# Intent: shutdown from blocked/recovering mode still establishes zero and requests the actual known-session Stop.
# Why: timer expiry must not simply retire sockets while a previously positive trial ceiling can still be used.
@pytest.mark.parametrize("phase", ["blocked", "recovering"])
async def test_trial_shutdown_stops_known_session_in_restricted_modes(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int, phase: str
) -> None:
    bench = Bench()
    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        session = TrialSession(preflight(path), FAST, bench.observe)
        await session.start()
        async with bench.connection_to(session) as charger:
            reader = asyncio.create_task(bench.read_charger(charger))
            try:
                await bench.call(charger, "boot", "BootNotification", BOOT)
                await until(
                    lambda: session.gateway.controls.ready and session.gateway.vendor_registration == "Accepted"
                )
                await bench.call(charger, "start", "StartTransaction", START)
                await until(lambda: session.journal.vendor_id(1001) == 9001)
                bench.firmware.requested_current = 6
                session.journal.put("trial_phase", phase)
                closing = asyncio.create_task(session.close())
                await asyncio.wait_for(bench.remote_stop.wait(), 2)
                assert bench.firmware.current == 0
                await bench.call(
                    charger,
                    "stop",
                    "StopTransaction",
                    {"transactionId": 1001, "timestamp": START["timestamp"], "meterStop": 1000},
                )
                await closing
                assert session.closed and session.final_status is not None
                assert not session.final_status["shutdown_uncertain"]
                assert session.final_status["local_hold"]
            finally:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
                await session.close()
