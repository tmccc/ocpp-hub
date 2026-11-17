"""Independent synthetic native/vehicle HTTP peers beside the real TLS proxy engine."""

from __future__ import annotations

import asyncio
import copy
import json
from typing import Any

from ocpp_hub_core.frames import basic_auth_header

from tests.test_trial_session import Bench
from tests.trial_helpers import native_profile

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


class NativeVehiclePeer:
    """Vehicle demand and USER current are independent from gateway readiness and profile receipts."""

    # Keep complete native settings and vehicle permission in this peer, never in the engine under test.
    def __init__(self, bench: Bench) -> None:
        self.bench = bench
        self.configuration = copy.deepcopy(ORIGINAL)
        self.runtime_user = 320
        self.hardware = 320
        self.paused = True
        self.requests: list[str] = []
        self.phase_fault = False
        self.permission_enabled = False
        self.plan_session: tuple[int, int | float] | None = None

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
        if key == "SESSION" and self.plan_session is not None and bench.firmware.active == self.plan_session[0]:
            values["SESSION"]["saveData"]["sessionList"][0]["_setPlanId"] = self.plan_session[1]
        return copy.deepcopy(values[key])

    # Serve the native observation protocol with the independent device state and its own test credential.
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
            assert fields[b"authorization"].decode() == basic_auth_header("admin", "NATIVE_TEST_SECRET")[1]
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


# Terminate transport and journal ownership without the normal shutdown Stop, preserving real durable facts.
async def crash(session: Any) -> None:
    await session.runtime.close()
    session.journal.close()
    session.closed = True
