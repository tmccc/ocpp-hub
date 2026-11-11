"""Complete physical inventory and read-only observations for a bounded commissioning trial."""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .native_http import read_native
from .policy import DeniedError, integer, number
from .security import SecurityError, StaleEvidenceError
from .station import anchor_epoch
from .store import fingerprint
from .trial_allocation import HistoryObservation, observe_history
from .trial_config import TrialConfig, basic_secret

if TYPE_CHECKING:
    from .trial_handover_evidence import HandoverEvidence


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
    kind = profile.get("chargingProfileKind")
    absolute = kind == "Absolute"
    start = anchor_epoch(schedule.get("startSchedule")) if absolute else None
    if (
        kind not in ("Relative", "Absolute")
        or absolute
        and (
            profile.get("chargingProfileId") != 7001
            or profile.get("chargingProfilePurpose") != "ChargePointMaxProfile"
            or payload.get("connectorId") != 0
            or start is None
            or start >= time.time()
        )
        or schedule.get("chargingRateUnit") != "A"
        or len(periods) != 1
        or type(periods[0].get("startPeriod")) is not int
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
        or set(schedule) != {"chargingRateUnit", "chargingSchedulePeriod"} | ({"startSchedule"} if absolute else set())
        or set(periods[0]) - {"startPeriod", "limit", "numberPhases"}
    ):
        raise DeniedError("trial supports only its explicitly selected constant ampere profiles")
    result = {
        "id": integer(profile["chargingProfileId"], 1),
        "connector": integer(payload["connectorId"], 0, 1),
        "purpose": profile["chargingProfilePurpose"],
        "stack": integer(profile["stackLevel"], 0, 255),
        "kind": kind,
        "unit": "A",
        "limit": number(periods[0]["limit"], 32),
        "phases": integer(periods[0].get("numberPhases", 1), 1, 1),
        "transaction": profile.get("transactionId"),
    }
    if absolute:
        result["start"] = start
    return result


# Only a fully matching native constant schedule can establish ownership of a newly written trial profile.
def native_profile_projection(profile: dict[str, Any], connector: int, purpose: str, now: float) -> dict[str, Any]:
    try:
        periods = profile["periods"]
        kind = profile["periodKind"]
        if (
            kind not in ("Relative", "Absolute")
            or profile["unit"] != "A"
            or not isinstance(periods, list)
            or len(periods) != 1
            or integer(periods[0]["startPeriod"], 0) != 0
            or integer(profile["validFrom"], 0) != 0
            or integer(profile["validTo"], 0) < now + 3600
            or integer(profile["duration"], 0) < 3600
            or integer(periods[0]["endPeriod"], 0) < 3600
        ):
            return {"unsupported": True}
        start: int | None = None
        if kind == "Relative":
            if type(profile["startSchedule"]) is not int or profile["startSchedule"] != 0:
                return {"unsupported": True}
        else:
            raw = profile["startSchedule"]
            start = anchor_epoch(raw) if isinstance(raw, str) else integer(raw, 1)
            if (
                profile["chargingProfileId"] != 7001
                or purpose != "ChargePointMaxProfile"
                or connector != 0
                or start >= now
                or start + profile["duration"] < now + 3600
                or start + periods[0]["endPeriod"] < now + 3600
                or periods[0]["endPeriod"] < now + 3600
            ):
                # Until the native Absolute format is observed, require coverage under both possible end conventions.
                return {"unsupported": True}
        result = {
            "id": integer(profile["chargingProfileId"], 1),
            "connector": connector,
            "purpose": purpose,
            "stack": integer(profile["stackLevel"], 0, 255),
            "kind": kind,
            "unit": "A",
            "limit": number(periods[0]["limit"], 320) / 10,
            "phases": integer(periods[0]["numberPhases"], 1, 1),
            "transaction": profile["transactionId"] if purpose == "TxProfile" else None,
        }
        if start is not None:
            result["start"] = start
        return result
    except KeyError, TypeError, ValueError, OverflowError:
        return {"unsupported": True}


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
    native_transaction_id: int | None = None
    history: HistoryObservation | None = None

    # Validate identity, freshness and inventory uniqueness independently of whether a supervised session is active.
    def require_fresh(self, identity: str, max_age: float = 30) -> None:
        age = time.time() - self.observed_at
        if self.identity_digest != fingerprint(identity) or not math.isfinite(age) or age < 0:
            raise SecurityError("trial requires fresh matching physical evidence")
        if age > max_age:
            raise StaleEvidenceError("native observation exceeded its freshness limit")
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
    async def _read(self, action: str, data: str | dict[str, Any] | None = None) -> Any:
        if (
            action not in ("getConfig", "statistics", "getTransaction")
            or (
                action == "statistics"
                and data not in ("CONNECTOR", "METER", "SESSION", "SMART", "SETPLAN", "OCPP", "STATE")
            )
            or (action == "getConfig" and data is not None)
            or (action == "getTransaction" and data != {})
        ):
            raise SecurityError("native action is outside the read-only trial observer")
        return await read_native(self.config.charger_ip, self.port, self.authorization, action, data)

    # The entire observation has one deadline in addition to each bounded request; no worker survives cancellation.
    async def observe(self) -> Evidence:
        return await self._observe(False)

    # Only fresh admission reads rolling history; recovery never depends on reseeding from it.
    async def observe_with_history(self) -> Evidence:
        return await self._observe(True)

    # The separate connected path needs the actual native USER/HW limits and full history, not desired OCPP state.
    async def observe_handover(self) -> HandoverEvidence:
        from .trial_handover_evidence import handover_evidence

        async with asyncio.timeout(15):
            started = time.time()
            config = await self._read("getConfig")
            if (
                not isinstance(config, dict)
                or config.get("Id") != self.config.identity
                or config.get("bPSN") != self.config.hardware_identity
            ):
                raise SecurityError("connected native identities do not match the configured charger")
            values = {}
            for name in ("SMART", "SETPLAN", "SESSION", "METER", "CONNECTOR", "OCPP", "STATE"):
                values[name] = await self._read("statistics", name)
            values["TRANSACTIONS"] = await self._read("getTransaction", {})
            if await self._read("getConfig") != config:
                raise SecurityError("native configuration changed during the connected audit")
            return handover_evidence(config, values, started)

    # Recheck native identity and configuration around one complete bounded physical snapshot.
    async def _observe(self, include_history: bool) -> Evidence:
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
            if include_history:
                values["TRANSACTIONS"] = await self._read("getTransaction", {})
            if await self._read("getConfig") != config:
                raise SecurityError("native configuration changed during the history audit")
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
                    canonical = native_profile_projection(profile, connector, purpose, time.time())
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
        currents, powers = meter[0]["meterData"]["cur"], meter[0]["meterData"]["power"]
        if not isinstance(currents, list) or len(currents) != 3 or not isinstance(powers, list) or len(powers) != 4:
            raise ValueError
        current = max(number(value, 10000) / 10 for value in currents)
        power = max(number(value, 1000000) for value in powers)
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
            transaction if transaction > 0 else None,
            observe_history(values["TRANSACTIONS"], identity, config["bPSN"], observed_at)
            if "TRANSACTIONS" in values
            else None,
        )
    except ValueError, KeyError, TypeError, IndexError:
        raise SecurityError("complete native trial inventory could not be validated") from None
