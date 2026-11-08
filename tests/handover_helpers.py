"""Independent synthetic native/vehicle HTTP peers beside the real TLS proxy engine."""

from __future__ import annotations

import asyncio
import copy
import json
import ssl
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from ocpp_hub_core import commands
from ocpp_hub_core.frames import basic_auth_header
from ocpp_hub_core.prototype.native_http import read_native
from ocpp_hub_core.prototype.security import TRANSPORT_LOG
from ocpp_hub_core.prototype.store import fingerprint
from ocpp_hub_core.prototype.trial_evidence import NativeObserver
from ocpp_hub_core.prototype.trial_handover_evidence import (
    PauseCapability,
    PauseObservation,
    PauseRequest,
    PermissionObservation,
)
from ocpp_hub_core.prototype.trial_resources import Baseline
from ocpp_hub_core.prototype.trial_session import TrialSession, preflight, save_private
from websockets.asyncio.server import serve

from tests.test_prototype import BOOT
from tests.test_prototype_runtime import FAST, until
from tests.test_trial_session import Bench
from tests.trial_helpers import config_files, native_profile

ORIGINAL = {
    "Id": "OFFLINE001",
    "bPSN": "HARDWARE001",
    "AmpLmt": "32",
    "maxOutCurrent0": "32",
    "minOutCurrent0": "6",
    "sDomain": "original.invalid",
    "sPort": "443",
    "sURL": "/ocpp/",
    "allowSelfSign": "0",
    "untouched": "sentinel",
}
VEHICLE = fingerprint("synthetic-vehicle")
PROVIDER = fingerprint("synthetic-pause-provider")
VEHICLE_AUTH = "Basic SYNTHETIC_VEHICLE_ONLY"


class NativeVehiclePeer:
    """Vehicle demand and USER current are independent from gateway readiness and profile receipts."""

    # Keep complete native settings and vehicle permission in this peer, never in the engine under test.
    def __init__(self, bench: Bench) -> None:
        self.bench = bench
        self.configuration = copy.deepcopy(ORIGINAL)
        self.runtime_user = 320
        self.hardware = 320
        self.paused = True
        self.revision = "pause-1"
        self.provider_digest = PROVIDER
        self.observation_fault: str | None = None
        self.restore_calls = 0
        self.status_calls = 0
        self.drop_restore_reply = False
        self.start_normal_on_restore = False
        self.operations: dict[str, str] = {}
        self.requests: list[str] = []
        self.phase_fault = False
        self.permission_enabled = False
        self.resource_on_restore: str | None = None

    # Produce the known device-native representation from the fake firmware's own stored profiles and real effects.
    def native(self, key: str) -> Any:
        bench = self.bench
        current = min(bench.firmware.current, self.runtime_user / 10) if not self.paused else 0
        power = current * 240
        smart: dict[str, Any] = {"conn0": {}, "conn1": {}}
        for connector in smart:
            smart[connector] = {purpose: [] for purpose in ("ChargePointMaxProfile", "TxDefaultProfile", "TxProfile")}
        for profile in bench.firmware.profiles.values():
            connector = 0 if profile["chargingProfilePurpose"] == "ChargePointMaxProfile" else 1
            smart["conn" + str(connector)][profile["chargingProfilePurpose"]].append(native_profile(profile, connector))
        limit = min(self.runtime_user / 10, bench.firmware.effective_limit()) * 10 if self.permission_enabled else 0
        values = {
            "config": self.configuration,
            "SMART": smart,
            "SETPLAN": {"planList": list(bench.firmware.plans.values())},
            "SESSION": {"saveData": {"sessionList": [{"_transactionId": bench.firmware.active or -1}]}},
            "METER": [
                {"meterData": {"cur": [current * 10, 1 if self.phase_fault else 0, 0], "power": [power, 0, 0, power]}}
            ],
            "CONNECTOR": [
                {
                    "info": {"CP": bench.cp},
                    "connStatus": bench.connection,
                    "outLimit": limit,
                    "offerCur": self.runtime_user,
                }
            ],
            "OCPP": {"online": bench.online},
            "TRANSACTIONS": bench.history,
            "STATE": [{"currLimit": {"USER": self.runtime_user, "HWCFG": self.hardware}}],
        }
        return copy.deepcopy(values[key])

    # Generate a live test witness with a new nonce; its format is synthetic and is not a SEAT API.
    def vehicle_observation(self, request: dict[str, Any]) -> dict[str, Any]:
        now = time.time()
        capability = {
            "digest": fingerprint("synthetic-physical-challenge"),
            **{k: request[k] for k in ("identity_digest", "hardware_digest", "vehicle_digest", "boot_digest")},
            "charging_before_a": 6.1,
            "user_limit_a": 6,
            "positive_offer_a": 6,
            "zero_offer_seconds": 60,
            "restart_verified": True,
        }
        result = {
            "request": request,
            "provider_digest": self.provider_digest,
            "observed_at": now,
            "expires_at": now + 20,
            "pause_valid_until": now + 1200,
            "release_only": True,
            "paused": self.paused,
            "revision": self.revision,
            "permission_digest": fingerprint("paused" if self.paused else "accepting"),
            "original_revision": "original-1",
            "original_permission_digest": fingerprint("accepting"),
            "capability": capability,
        }
        fault = self.observation_fault
        if fault == "expired":
            result["expires_at"] = now - 1
        elif fault == "future":
            result["observed_at"] = now + 10
        elif fault == "wrong-nonce":
            result["request"] = request | {"nonce": "historical-proof"}
        elif fault == "wrong-vehicle":
            result["request"] = request | {"vehicle_digest": fingerprint("other-vehicle")}
        elif fault == "short-hold":
            result["pause_valid_until"] = now + 25
        elif fault == "automatic-release":
            result["release_only"] = False
        elif fault == "no-reboot-proof":
            capability["restart_verified"] = False
        return result

    # Restore the exact original vehicle permission only for the owned expected revision and one operation token.
    def vehicle_result(self, data: dict[str, Any], restore: bool) -> dict[str, Any] | None:
        operation = data["operation"]
        if restore:
            self.restore_calls += 1
            assert self.revision == data["expected_revision"]
            assert data["original_permission_digest"] == fingerprint("accepting")
            self.operations[operation] = self.revision
            self.paused, self.revision = False, "restored-1"
            if self.start_normal_on_restore:
                self.bench.firmware.active = 900001
                self.bench.firmware.requested_current = 32
                self.bench.cp, self.bench.connection = "C", "Charging"
                self.permission_enabled = True
                self.bench.history.append({"transactionId": 900001, "origin": "normal-synthetic-backend"})
            if self.resource_on_restore == "plan":
                self.bench.firmware.plans[88] = {"planId": 88, "limitCurrent": 32}
            elif self.resource_on_restore in ("station", "unrelated-tx", "normal-tx"):
                station = self.resource_on_restore == "station"
                self.bench.firmware.answer(
                    "SetChargingProfile",
                    commands.set_charging_profile(
                        0 if station else 1,
                        commands.PURPOSE_CHARGE_POINT_MAX if station else commands.PURPOSE_TX,
                        32,
                        60001,
                        stack_level=1,
                        transaction_id=None
                        if station
                        else 900001
                        if self.resource_on_restore == "normal-tx"
                        else 888888,
                    ),
                )
        else:
            self.status_calls += 1
        if operation not in self.operations:
            return None
        return {
            "request": data["request"],
            "provider_digest": self.provider_digest,
            "operation": operation,
            "observed_at": time.time(),
            "permission_digest": fingerprint("paused" if self.paused else "accepting"),
            "previous_revision": self.operations[operation],
            "revision": self.revision,
        }

    # Serve actual bounded HTTP messages and separate native and vehicle credentials at the test boundary.
    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            fields = {
                line.split(b":", 1)[0].lower(): line.split(b":", 1)[1].strip()
                for line in headers.split(b"\r\n")[1:]
                if b":" in line
            }
            length = int(fields[b"content-length"])
            assert 0 < length <= 65536
            request = json.loads(await reader.readexactly(length))
            action, data = request["action"], request.get("data")
            self.requests.append(action)
            vehicle = action.startswith("vehicle-")
            assert fields[b"authorization"].decode() == (
                VEHICLE_AUTH if vehicle else basic_auth_header("admin", "NATIVE_TEST_SECRET")[1]
            )
            if action == "vehicle-observe":
                value = self.vehicle_observation(data)
            elif action == "vehicle-permission":
                value = {
                    "request": data,
                    "provider_digest": self.provider_digest,
                    "operation": "read-current",
                    "observed_at": time.time(),
                    "permission_digest": fingerprint("paused" if self.paused else "accepting"),
                    "previous_revision": self.revision,
                    "revision": self.revision,
                }
            elif vehicle:
                value = self.vehicle_result(data, action == "vehicle-restore")
                if action == "vehicle-restore" and self.drop_restore_reply:
                    return
            else:
                assert action in ("getConfig", "statistics", "getTransaction")
                value = self.native(
                    "config" if action == "getConfig" else "TRANSACTIONS" if action == "getTransaction" else data
                )
            body = json.dumps({"code": 200, "data": json.dumps(value)}).encode()
            writer.write(b"HTTP/1.0 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()


class SyntheticPauseProvider:
    """Test-only adapter: actual loopback HTTP talks to independently held vehicle permission."""

    # No production app endpoint, account file or charger address can enter this adapter.
    def __init__(self, port: int) -> None:
        self.port = port
        self.digest = PROVIDER

    # Decode the synthetic typed witness returned by the independent peer.
    async def observe(self, request: PauseRequest) -> PauseObservation:
        value = await read_native("127.0.0.1", self.port, VEHICLE_AUTH, "vehicle-observe", asdict(request))
        value["request"] = PauseRequest(**value["request"])
        value["capability"] = PauseCapability(**value["capability"])
        return PauseObservation(**value)

    # Send the compare-and-set once and return actual independent state rather than a command acceptance flag.
    async def restore(
        self, request: PauseRequest, operation: str, expected_revision: str, original_permission_digest: str
    ) -> PermissionObservation:
        value = await read_native(
            "127.0.0.1",
            self.port,
            VEHICLE_AUTH,
            "vehicle-restore",
            {
                "request": asdict(request),
                "operation": operation,
                "expected_revision": expected_revision,
                "original_permission_digest": original_permission_digest,
            },
        )
        value["request"] = PauseRequest(**value["request"])
        return PermissionObservation(**value)

    # A lost response is resolved through a distinct read, never by sending vehicle-restore again.
    async def restoration_status(self, request: PauseRequest, operation: str) -> PermissionObservation | None:
        value = await read_native(
            "127.0.0.1", self.port, VEHICLE_AUTH, "vehicle-status", {"request": asdict(request), "operation": operation}
        )
        if value is None:
            return None
        value["request"] = PauseRequest(**value["request"])
        return PermissionObservation(**value)

    # Observe the current owner permission for an unplugged fallback, without any vehicle mutation.
    async def permission_status(self, request: PauseRequest) -> PermissionObservation:
        value = await read_native("127.0.0.1", self.port, VEHICLE_AUTH, "vehicle-permission", asdict(request))
        value["request"] = PauseRequest(**value["request"])
        return PermissionObservation(**value)


@dataclass
class HandoverBench:
    """The session is real; only the charger, native HTTP and independent vehicle provider are synthetic."""

    session: TrialSession
    bench: Bench
    peer: NativeVehiclePeer
    charger: Any

    # Model the existing explicit operator backend step, keeping USER6 and the vehicle pause intact.
    async def restore_backend(self) -> None:
        self.peer.configuration = copy.deepcopy(ORIGINAL) | {"AmpLmt": "6"}
        await self.charger.close()
        await until(lambda: self.session.gateway.charger.sender is None)

    # Model the final original USER setting only after the original backend has been independently confirmed.
    def restore_user(self) -> None:
        self.peer.configuration["AmpLmt"] = "32"
        self.peer.runtime_user = 320

    # Execute the explicit original-backend and USER restoration checks while the independent car pause stays active.
    async def restore_charger_settings(self) -> None:
        await self.session.command({"command": "connected-handover"})
        await self.restore_backend()
        await self.session.command({"command": "confirm-connected-original-backend"})
        self.restore_user()
        await self.session.command({"command": "confirm-connected-original-settings"})


# Exercise complete commands through real TLS and the actual independent read-only native HTTP adapter.
@asynccontextmanager
async def ready_handover(
    directory: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], listener_port: int, provider: bool = True
) -> AsyncIterator[HandoverBench]:
    bench = Bench()
    bench.firmware.plans[9] = {"planId": 9, "limitCurrent": 32, "original": "preserved"}
    bench.history.append({"transactionId": 77, "original_record": {"preserve": [1, 2, 3]}})
    peer = NativeVehiclePeer(bench)
    native_server = await asyncio.start_server(peer.handle, "127.0.0.1", 0)
    try:
        async with serve(
            bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
        ) as cloud:
            config, path = config_files(directory, listener_port, cloud.sockets[0].getsockname()[1])
            original_observer = NativeObserver(config, native_server.sockets[0].getsockname()[1])
            baseline = Baseline.capture(config.identity, await original_observer.observe())
            save_private(config.baseline, baseline.value())
            save_private(config.baseline.with_suffix(".original-config.json"), ORIGINAL)
            prepared = preflight(path)
            prepared.observer.port = native_server.sockets[0].getsockname()[1]
            witness = SyntheticPauseProvider(prepared.observer.port) if provider else None
            session = TrialSession(
                prepared, FAST, handover_provider=witness, handover_vehicle_digest=VEHICLE if provider else None
            )
            try:
                await session.start()
                peer.configuration.update(
                    sDomain="127.0.0.1", sPort=str(config.listener_port), sURL=config.charger_path, allowSelfSign="1"
                )
                async with bench.connection_to(session) as charger:
                    reader = asyncio.create_task(bench.read_charger(charger))
                    try:
                        await bench.call(charger, "boot", "BootNotification", BOOT)
                        await until(
                            lambda: session.gateway.controls.ready and session.gateway.vendor_registration == "Accepted"
                        )
                        await session.command({"command": "stop"})
                        peer.configuration["AmpLmt"] = "6"
                        peer.runtime_user = 60
                        bench.cp, bench.connection = "B", "Plug"
                        yield HandoverBench(session, bench, peer, charger)
                    finally:
                        reader.cancel()
                        await asyncio.gather(reader, return_exceptions=True)
            finally:
                await session.close()
    finally:
        native_server.close()
        await native_server.wait_closed()
