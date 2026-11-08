"""Independent native inventory and device effects for the commissioning bench."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from ocpp_hub_core import commands
from ocpp_hub_core.frames import build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.policy import Policy
from ocpp_hub_core.prototype.store import fingerprint
from ocpp_hub_core.prototype.trial_config import TrialConfig
from ocpp_hub_core.prototype.trial_evidence import Evidence, Resource, profile_projection
from ocpp_hub_core.prototype.trial_resources import Baseline, ResourceBook
from ocpp_hub_core.prototype.trial_rollback import Rollback
from ocpp_hub_core.prototype.trial_session import save_private

from tests.test_prototype import Rig


# Model native tenths-of-an-ampere fields independently from the proxy's requested OCPP payload.
def native_profile(profile: dict[str, Any], connector: int) -> dict[str, Any]:
    return {
        "chargingProfileId": profile["chargingProfileId"],
        "connnectroId": connector,
        "stackLevel": profile["stackLevel"],
        "purpose": profile["chargingProfilePurpose"],
        "periodKind": profile["chargingProfileKind"],
        "unit": "A",
        "periods": [
            {
                "startPeriod": 0,
                "endPeriod": 2147483646,
                "limit": profile["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] * 10,
                "numberPhases": 1,
            }
        ],
        "transactionId": profile.get("transactionId", -1),
        "validFrom": 0,
        "validTo": 2147483646,
        "duration": 2147483646,
        "startSchedule": 0,
    }


class TrialRig(Rig):
    """The actual gateway writes to an independent fake device and reads an independent complete inventory."""

    # Seed genuine original resources before taking a baseline; the new journal owns none of them.
    def __init__(self, path: Path, original_profile: bool = True) -> None:
        super().__init__(path, Policy(max_current_a=6))
        original = commands.set_charging_profile(1, commands.PURPOSE_TX, 32, 42, stack_level=1, transaction_id=77)
        if original_profile:
            self.firmware.answer("SetChargingProfile", original)
        self.firmware.plans[9] = {"planId": 9, "limitCurrent": 32}
        self.connectors = {42: 1}
        self.cp, self.connection = "A", "Unplug"
        self.native_open = False
        self.age = 0
        self.configuration = "original"
        self.online = True
        self.drop_clear = False
        self.ack_without_clear = False
        self.clear_order: list[int] = []
        self.baseline = Baseline.capture(self.store.identity, self.evidence())
        self.book = ResourceBook(self.store, self.baseline)
        self.book.activate(self.evidence())
        self.gateway.charger.before_send = self.book.before_send
        self.gateway.charger.after_result = self.book.after_result
        self.rollback = Rollback(self.gateway, self.book, self.observe)

    # Translate only the fake device's own stored profiles, rather than sharing the proxy's receipt table.
    def evidence(self) -> Evidence:
        resources = []
        for identifier, profile in self.firmware.profiles.items():
            connector = self.connectors.get(
                identifier, 0 if profile["chargingProfilePurpose"] == "ChargePointMaxProfile" else 1
            )
            payload = {"connectorId": connector, "csChargingProfiles": profile}
            resources.append(
                Resource("profile", identifier, native_profile(profile, connector), profile_projection(payload))
            )
        resources += [Resource("plan", key, body, {"unsupported": True}) for key, body in self.firmware.plans.items()]
        return Evidence(
            fingerprint(self.store.identity),
            time.time() - self.age,
            self.cp,
            self.connection,
            0,
            0,
            self.native_open,
            fingerprint(self.configuration),
            tuple(resources),
            self.online,
        )

    # A read callback is asynchronous just like the real independent LAN observer.
    async def observe(self) -> Evidence:
        return self.evidence()

    # Freeze the cloud side without closing the charger receive path.
    async def quiesce(self) -> None:
        self.gateway.detach_vendor(self.gateway._vendor_epoch)

    # Execute only peer-side effects and allow a lost cleanup ACK after the physical deletion really happened.
    async def charger_send(self, raw: str) -> None:
        frame = decode(raw)
        self.charger_frames.append(frame)
        if frame[0] != 2:
            return
        action, payload = frame[2:]
        if action == "ClearChargingProfile":
            self.clear_order.append(payload["id"])
            if not self.ack_without_clear:
                self.firmware.profiles.pop(payload["id"], None)
            if self.drop_clear:
                return
            answer = {"status": "Accepted"}
        else:
            if action == "SetChargingProfile":
                self.connectors[payload["csChargingProfiles"]["chargingProfileId"]] = payload["connectorId"]
            answer = self.firmware.answer(action, payload)
        await self.gateway.receive("charger", build_call_result(frame[1], answer))


# Quiescing an already isolated fake cloud requires no real socket or environment operation.
async def no_cloud() -> None:
    return None


# Build one complete private synthetic configuration; its separate Basic credentials never refer to the household.
def config_files(directory: Path, listener_port: int, vendor_port: int) -> tuple[TrialConfig, Path]:
    directory.chmod(0o700)
    (directory / "certificate.pem").chmod(0o644)
    (directory / "key.pem").chmod(0o600)
    for name, username, password in (
        ("incoming.json", "OFFLINE001", "INCOMING_TEST_SECRET"),
        ("upstream.json", "OFFLINE001", "UPSTREAM_TEST_SECRET"),
        ("native.json", "admin", "NATIVE_TEST_SECRET"),
    ):
        save_private(directory / name, {"username": username, "password": password})
    evidence = Evidence(
        fingerprint("OFFLINE001"), time.time(), "A", "Unplug", 0, 0, False, fingerprint("original"), (), True
    )
    save_private(directory / "baseline.json", Baseline.capture("OFFLINE001", evidence).value())
    value = {
        "identity": "OFFLINE001",
        "listener_host": "127.0.0.1",
        "listener_port": listener_port,
        "listener_name": "127.0.0.1",
        "charger_ip": "127.0.0.1",
        "charger_path": "/charger/OFFLINE001",
        "vendor_url": f"wss://127.0.0.1:{vendor_port}/ocpp/OFFLINE001",
        "upstream_auth": "basic",
        "incoming_secret": str(directory / "incoming.json"),
        "upstream_secret": str(directory / "upstream.json"),
        "native_secret": str(directory / "native.json"),
        "certificate": str(directory / "certificate.pem"),
        "private_key": str(directory / "key.pem"),
        "ca_file": str(directory / "certificate.pem"),
        "journal": str(directory / "state.db"),
        "baseline": str(directory / "baseline.json"),
        "evidence": str(directory / "evidence.json"),
        "max_current_a": 6,
        "local_tags": ["LOCAL_TEST"],
    }
    path = directory / "trial.json"
    save_private(path, value)
    return TrialConfig.load(path), path
