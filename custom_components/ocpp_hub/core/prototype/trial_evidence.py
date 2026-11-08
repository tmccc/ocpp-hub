"""Complete physical inventory and read-only observations for a bounded commissioning trial."""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass, field
from typing import Any

from .native_http import read_native
from .policy import DeniedError, integer, number
from .security import SecurityError
from .store import fingerprint
from .trial_config import TrialConfig, basic_secret


@dataclass(frozen=True, repr=False)
class Resource:
    """Complete physical bytes establish equality; a semantic projection checks a newly written constant profile."""

    kind: str
    identifier: int
    body: dict[str, Any] = field(repr=False)
    canonical: dict[str, Any] = field(repr=False)

    # Keep ownership keys stable without persisting private configuration or tag bodies in resource receipts.
    @property
    def key(self) -> str:
        return self.kind + ":" + str(self.identifier)

    # Include every native field so an unexpected out-of-band change blocks rollback.
    @property
    def digest(self) -> str:
        return fingerprint(self.body)


# Project the exact synthetic/OCPP constant-profile subset; unsupported shapes are not cleanup permission.
def profile_projection(payload: dict[str, Any]) -> dict[str, Any]:
    profile = payload["csChargingProfiles"]
    schedule = profile["chargingSchedule"]
    periods = schedule["chargingSchedulePeriod"]
    if (
        profile.get("chargingProfileKind") != "Relative"
        or schedule.get("chargingRateUnit") != "A"
        or len(periods) != 1
        or periods[0].get("startPeriod") != 0
        or set(profile)
        - {
            "chargingProfileId",
            "stackLevel",
            "chargingProfilePurpose",
            "chargingProfileKind",
            "chargingSchedule",
            "transactionId",
        }
        or set(schedule) != {"chargingRateUnit", "chargingSchedulePeriod"}
        or set(periods[0]) - {"startPeriod", "limit", "numberPhases"}
    ):
        raise DeniedError("trial supports constant relative ampere profiles only")
    return {
        "id": integer(profile["chargingProfileId"], 1),
        "connector": integer(payload["connectorId"], 0, 1),
        "purpose": profile["chargingProfilePurpose"],
        "stack": integer(profile["stackLevel"], 0, 255),
        "kind": "Relative",
        "unit": "A",
        "limit": number(periods[0]["limit"], 32),
        "phases": integer(periods[0].get("numberPhases", 1), 1, 1),
        "transaction": profile.get("transactionId"),
    }


@dataclass(frozen=True, repr=False)
class Evidence:
    """Only a fresh native CP A observation can establish physical unplugged state."""

    identity_digest: str
    observed_at: float
    cp: str
    connection: str
    current_a: float
    power_w: float
    transaction_open: bool
    configuration_digest: str
    resources: tuple[Resource, ...]
    upstream_connected: bool = False

    # Validate identity, freshness and inventory uniqueness independently of whether a supervised session is active.
    def require_fresh(self, identity: str, max_age: float = 30) -> None:
        age = time.time() - self.observed_at
        if self.identity_digest != fingerprint(identity) or not math.isfinite(age) or not 0 <= age <= max_age:
            raise SecurityError("trial requires fresh matching physical evidence")
        keys = [resource.key for resource in self.resources]
        if len(keys) != len(set(keys)):
            raise SecurityError("native inventory contains duplicate resource identifiers")

    # Reject incomplete, stale, contradictory and non-finite physical evidence before removing a safety hold.
    def require_unplugged(self, identity: str, max_age: float = 30) -> None:
        self.require_disconnected(identity, max_age)
        if self.transaction_open:
            raise SecurityError("native transaction must actually be closed")

    # Recovery may listen for an existing Stop, but a connected vehicle cannot enter that recovery listener.
    def require_disconnected(self, identity: str, max_age: float = 30) -> None:
        self.require_fresh(identity, max_age)
        if self.cp != "A" or self.connection != "Unplug" or self.current_a != 0 or self.power_w != 0:
            raise SecurityError("trial requires fresh matching unplugged zero-power evidence and a closed session")


class NativeObserver:
    """Only the known read-only iCS LAN getters exist in this adapter."""

    # Constructing a reader loads only its private credential, never opening a socket.
    def __init__(self, config: TrialConfig, native_port: int = 80) -> None:
        self.config = config
        self.port = integer(native_port, 1, 65535)
        self.authorization = basic_secret(config.native_secret, None)
        self.configuration_snapshot: Any = None

    # Reject every action outside the exact observed getter set before contacting the literal charger address.
    async def _read(self, action: str, data: str | None = None) -> Any:
        if action not in ("getConfig", "statistics") or (
            action == "statistics" and data not in ("CONNECTOR", "METER", "SESSION", "SMART", "SETPLAN", "OCPP")
        ):
            raise SecurityError("native action is outside the read-only trial observer")
        return await read_native(self.config.charger_ip, self.port, self.authorization, action, data)

    # The entire observation has one deadline in addition to each bounded request; no worker survives cancellation.
    async def observe(self) -> Evidence:
        async with asyncio.timeout(15):
            started = time.time()
            config = await self._read("getConfig")
            if (
                not isinstance(config, dict)
                or config.get("Id") != self.config.identity
                or config.get("bPSN") != self.config.hardware_identity
            ):
                raise SecurityError("native OCPP and physical identities must both match the configured charger")
            self.configuration_snapshot = config
            values = {}
            for name in ("SMART", "SETPLAN", "SESSION", "METER", "CONNECTOR", "OCPP"):
                values[name] = await self._read("statistics", name)
            return native_evidence(config, values, started)

    # The explicit command-line baseline capture owns its loop; websocket runtime always uses observe directly.
    def snapshot(self) -> Evidence:
        return asyncio.run(self.observe())


# Preserve full native bodies, projecting only profile fields whose scaling was observed in actual iCS captures.
def native_evidence(config: Any, values: dict[str, Any], observed_at: float) -> Evidence:
    try:
        identity = config["Id"]
        if not isinstance(identity, str) or not identity:
            raise ValueError
        resources = []
        smart = values["SMART"]
        if set(smart) != {"conn0", "conn1"}:
            raise ValueError
        for connector in (0, 1):
            purposes = smart["conn" + str(connector)]
            if set(purposes) != {"ChargePointMaxProfile", "TxDefaultProfile", "TxProfile"}:
                raise ValueError
            for purpose, profiles in purposes.items():
                if not isinstance(profiles, list):
                    raise ValueError
                for profile in profiles:
                    identifier = integer(profile["chargingProfileId"], 1)
                    if profile["purpose"] != purpose or profile["connnectroId"] != connector:
                        raise ValueError
                    periods = profile["periods"]
                    canonical: dict[str, Any] = {"unsupported": True}
                    if (
                        profile["purpose"] == purpose
                        and profile["connnectroId"] == connector
                        and profile["periodKind"] == "Relative"
                        and profile["unit"] == "A"
                        and len(periods) == 1
                        and periods[0]["startPeriod"] == 0
                        and profile["validFrom"] == 0
                        and profile["validTo"] >= time.time() + 3600
                        and profile["duration"] >= 3600
                        and profile["startSchedule"] == 0
                    ):
                        canonical = {
                            "id": identifier,
                            "connector": connector,
                            "purpose": purpose,
                            "stack": profile["stackLevel"],
                            "kind": "Relative",
                            "unit": "A",
                            "limit": number(periods[0]["limit"], 320) / 10,
                            "phases": periods[0]["numberPhases"],
                            "transaction": profile["transactionId"] if purpose == "TxProfile" else None,
                        }
                    resources.append(Resource("profile", identifier, profile, canonical))
        plans = values["SETPLAN"]["planList"]
        if not isinstance(plans, list):
            raise ValueError
        for plan in plans:
            resources.append(Resource("plan", integer(plan["planId"], 1), plan, {"unsupported": True}))
        connector = values["CONNECTOR"]
        meter = values["METER"]
        if len(connector) != 1 or len(meter) != 1:
            raise ValueError
        session = values["SESSION"]["saveData"]["sessionList"]
        if len(session) != 1:
            raise ValueError
        current = number(meter[0]["meterData"]["cur"][0], 10000) / 10
        power = number(meter[0]["meterData"]["power"][3], 1000000)
        transaction = integer(session[0]["_transactionId"], -1)
        online = values["OCPP"]["online"]
        if type(online) is not bool:
            raise ValueError
        return Evidence(
            fingerprint(identity),
            observed_at,
            connector[0]["info"]["CP"],
            connector[0]["connStatus"],
            current,
            power,
            transaction > 0,
            fingerprint(config),
            tuple(resources),
            online,
        )
    except ValueError, KeyError, TypeError, IndexError:
        raise SecurityError("complete native trial inventory could not be validated") from None
