"""The complete standalone commissioning lifecycle over both real encrypted loopback connections."""

from __future__ import annotations

import asyncio
import copy
import ssl
import time
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.frames import build_call, build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.security import TRANSPORT_LOG, SecurityError
from ocpp_hub_core.prototype.store import fingerprint
from ocpp_hub_core.prototype.trial_evidence import Evidence, Resource, native_profile_projection
from ocpp_hub_core.prototype.trial_resources import Baseline
from ocpp_hub_core.prototype.trial_session import TrialSession, preflight, save_private
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
        self.received_limits: list[float] = []
        self.composite_reply_delay = 0.0
        self.observation_failure = False

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
            additional_headers=[]
            if session.prepared.tls.incoming is None
            else [("Authorization", session.prepared.tls.incoming)],
            subprotocols=["ocpp1.6"],
            logger=TRANSPORT_LOG,
            proxy=None,
        )


# Intent: whole-second device timestamps and a delayed real TLS reply still confirm a presently effective ceiling.
# Why: a one-second request can expire in transit even when the charger accepted and applied the requested limit.
async def test_rounded_composite_survives_reply_delay(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    bench = Bench()
    bench.composite_reply_delay = 1.05
    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        session = TrialSession(preflight(path), FAST, bench.observe)
        try:
            await session.start()
            async with bench.connection_to(session) as charger:
                reader = asyncio.create_task(bench.read_charger(charger))
                try:
                    await bench.call(charger, "boot", "BootNotification", BOOT)
                    await until(
                        lambda: session.gateway.controls.ready and session.gateway.vendor_registration == "Accepted"
                    )
                    assert bench.firmware.effective_limit() == 6
                    assert (await session.command({"command": "rollback"}))["profiles_restored"]
                finally:
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            await session.close()


# Intent: an existing inactive profile prevents a fresh standalone trial before any listener or cloud is started.
# Why: file validation must remain available, but a known preservation risk must block live admission.
async def test_original_profile_blocks_fresh_trial_before_binding(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    bench = Bench()
    bench.firmware.answer(
        "SetChargingProfile", commands.set_charging_profile(1, commands.PURPOSE_TX, 32, 42, transaction_id=77)
    )
    config, path = config_files(tmp_path, unused_tcp_port, unused_tcp_port)
    save_private(config.baseline, Baseline.capture(config.identity, await bench.observe()).value())
    prepared = preflight(path)
    assert prepared.summary()["new_profile_writes_blocked"]
    session = TrialSession(prepared, FAST, bench.observe)
    try:
        with pytest.raises(SecurityError, match="selective cleanup"):
            await session.start()
        assert not session.started and not session.runtime.status.charger_connected
        assert not session.runtime.status.vendor_connected and not bench.received_profiles
        assert session.journal.get("trial_phase") == "prepared"
    finally:
        await session.close()


# Intent: shutdown after a preservation failure and verified profile absence sends no new profile or clear.
# Why: the zero-hold recovery exception must not resurrect a trial limit after cleanup has removed it.
async def test_fault_shutdown_does_not_recreate_absent_profile(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    bench = Bench()
    bench.firmware.plans[9] = {"planId": 9, "limitCurrent": 32}
    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        config, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        save_private(config.baseline, Baseline.capture(config.identity, await bench.observe()).value())
        session = TrialSession(preflight(path), FAST, bench.observe)
        try:
            await session.start()
            async with bench.connection_to(session) as charger:
                reader = asyncio.create_task(bench.read_charger(charger))
                try:
                    await bench.call(charger, "boot", "BootNotification", BOOT)
                    await until(lambda: session.gateway.controls.ready)
                    bench.firmware.profiles.clear()
                    bench.firmware.plans.clear()
                    with pytest.raises(SecurityError):
                        session.book.owned_present(await bench.observe())
                    received = list(bench.received_limits)
                    await session.close()
                    assert bench.received_limits == received and not bench.firmware.profiles
                    assert session.final_status is not None and session.final_status["preservation_failed"]
                    assert session.final_status["shutdown_uncertain"]
                finally:
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            await session.close()


# Intent: a failed idle audit still permits shutdown to install zero and stop a known plugged-in transaction.
# Why: freezing cleanup before validation must not bypass the session's normal safety shutdown path.
@pytest.mark.parametrize("failure", ["plugged", "observation"])
async def test_failed_rollback_keeps_safety_shutdown(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int, failure: str
) -> None:
    bench = Bench()
    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        session = TrialSession(preflight(path), FAST, bench.observe)
        try:
            await session.start()
            async with bench.connection_to(session) as charger:
                reader = asyncio.create_task(bench.read_charger(charger))
                try:
                    await bench.call(charger, "boot", "BootNotification", BOOT)
                    await until(lambda: session.gateway.controls.ready)
                    await bench.call(charger, "start", "StartTransaction", START)
                    bench.cp, bench.connection = "B", "Plug"
                    bench.firmware.requested_current = 6
                    bench.observation_failure = failure == "observation"
                    with pytest.raises(SecurityError if failure == "plugged" else OSError):
                        await session.command({"command": "rollback"})
                    assert session.journal.get("trial_phase") == "blocked"
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
                    assert session.final_status is not None and not session.final_status["shutdown_uncertain"]
                finally:
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            await session.close()


# Intent: exercise start, authorization, mirroring, stop and rollback through the real transport with fake TLS peers.
# Why: constructor/file tests cannot prove a usable standalone trial package or clean retirement of its owned cap.
@pytest.mark.parametrize("incoming_mode", ["basic", "none"])
@pytest.mark.parametrize("profile_kind", ["Relative", "Absolute"])
async def test_trial_session_full_lifecycle_and_verified_rollback(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    incoming_mode: str,
    profile_kind: str,
) -> None:
    import json

    from ocpp_hub_core.prototype.trial_session import save_private

    bench = Bench()
    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        config, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        value = json.loads(path.read_text())
        if profile_kind == "Absolute":
            value.update(station_profile_kind="Absolute", station_profile_start="2020-01-01T00:00:00Z")
        if incoming_mode == "none":
            value.update(incoming_auth="none", incoming_secret=None)
        save_private(path, value)
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
@pytest.mark.parametrize("profile_kind", ["Relative", "Absolute"])
async def test_trial_restart_opens_restricted_recovery_listener(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int, profile_kind: str
) -> None:
    import json

    from ocpp_hub_core.prototype.trial_session import save_private

    bench = Bench()
    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        if profile_kind == "Absolute":
            value = json.loads(path.read_text())
            value.update(station_profile_kind="Absolute", station_profile_start="2020-01-01T00:00:00Z")
            save_private(path, value)
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
        checkpoint = len(bench.received_limits)
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
                    assert bench.received_limits[checkpoint:] and all(
                        value == 0 for value in bench.received_limits[checkpoint:]
                    )
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
