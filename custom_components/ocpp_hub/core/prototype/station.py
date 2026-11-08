"""Explicit station-profile choices and single-key capability discovery, without weaker profile fallbacks."""

from __future__ import annotations

import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from .. import commands
from ..control import HUB_PROFILE_ID
from ..frames import Frame
from .policy import DeniedError, integer
from .store import Journal, RecoveryRequiredError, fingerprint

CAPABILITY_KEYS = (
    "NumberOfConnectors",
    "SupportedFeatureProfiles",
    "ChargingScheduleAllowedChargingRateUnit",
    "ChargeProfileMaxStackLevel",
    "ChargingScheduleMaxPeriods",
    "MaxChargingProfilesInstalled",
)
Call = Callable[[str, dict[str, Any]], Awaitable[Frame]]


# A station anchor is an exact whole-second UTC instant, never a local time or a millisecond epoch.
def anchor_epoch(value: Any) -> int:
    if not isinstance(value, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value) is None:
        raise DeniedError("absolute station start requires whole-second UTC Z")
    try:
        return integer(int(datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC).timestamp()), 1)
    except ValueError, OverflowError:
        raise DeniedError("absolute station start is invalid") from None


@dataclass(frozen=True)
class StationChoice:
    """The selected kind, reserved stack and anchor are immutable for one trial."""

    kind: str
    stack: int
    start: str | None = None

    # Reject ambiguous or future schedules before discovery or a physical mutation can begin.
    def __post_init__(self) -> None:
        integer(self.stack, 0, 255)
        if self.kind not in ("Relative", "Absolute"):
            raise DeniedError("station kind must be explicitly Relative or Absolute")
        if self.kind == "Relative" and self.start is not None:
            raise DeniedError("relative station profile cannot carry an absolute anchor")
        if self.kind == "Absolute" and anchor_epoch(self.start) >= time.time():
            raise DeniedError("absolute station anchor must already be in the past")

    # Always retain the station purpose and connector zero, including for a zero-current hold.
    def payload(self, limit: float) -> dict[str, Any]:
        return commands.set_charging_profile(
            0,
            commands.PURPOSE_CHARGE_POINT_MAX,
            limit,
            HUB_PROFILE_ID,
            stack_level=self.stack,
            kind=self.kind,
            start_schedule=self.start,
        )

    # Persist no identity or credential in the selected-profile shape.
    def value(self) -> dict[str, Any]:
        return asdict(self)


# A response must name only the requested key; missing, duplicate and unknown results cannot authorize a write.
def configuration_value(answer: Frame, key: str) -> str:
    body = answer[2] if answer[0] == 3 else {}
    if (
        not isinstance(body, dict)
        or set(body) - {"configurationKey", "unknownKey"}
        or body.get("unknownKey", []) != []
        or not isinstance(body.get("configurationKey"), list)
        or len(body["configurationKey"]) != 1
    ):
        raise DeniedError("station capability response is incomplete or ambiguous")
    row = body["configurationKey"][0]
    if (
        not isinstance(row, dict)
        or set(row) != {"key", "readonly", "value"}
        or row["key"] != key
        or type(row["readonly"]) is not bool
        or not isinstance(row["value"], str)
        or not 1 <= len(row["value"]) <= 256
    ):
        raise DeniedError("station capability key or value is invalid")
    return row["value"]


# Capability integers are bounded decimal strings rather than coercible booleans, floats or arbitrary objects.
def capability_integer(value: str, minimum: int, maximum: int = 2**31 - 1) -> int:
    if re.fullmatch(r"0|[1-9][0-9]{0,9}", value) is None:
        raise DeniedError("station capability integer is malformed")
    return integer(int(value), minimum, maximum)


@dataclass(frozen=True)
class StationCapabilities:
    """Reported capabilities constrain the trial; they do not prove real profile execution."""

    maximum_stack: int
    maximum_periods: int
    maximum_profiles: int

    # Explicitly documented Current/A values permit amperes; Power/W alone never triggers conversion or fallback.
    @classmethod
    def parse(cls, values: dict[str, str], choice: StationChoice) -> StationCapabilities:
        if set(values) != set(CAPABILITY_KEYS) or capability_integer(values["NumberOfConnectors"], 1) != 1:
            raise DeniedError("trial requires complete single-connector capabilities")
        features = {part.strip() for part in values["SupportedFeatureProfiles"].split(",")}
        if not {"Core", "SmartCharging"} <= features or "" in features:
            raise DeniedError("charger has not reported Core and SmartCharging support")
        units = [part.strip() for part in values["ChargingScheduleAllowedChargingRateUnit"].split(",")]
        if not units or len(units) != len(set(units)) or not set(units) <= {"Current", "Power", "A", "W"}:
            raise DeniedError("charger rate-unit capability is unknown or ambiguous")
        if not set(units) & {"Current", "A"}:
            raise DeniedError("commissioning requires explicitly reported ampere support")
        maximum = capability_integer(values["ChargeProfileMaxStackLevel"], 0, 255)
        if choice.stack > maximum:
            raise DeniedError("selected reserved stack exceeds the reported maximum")
        return cls(
            maximum,
            capability_integer(values["ChargingScheduleMaxPeriods"], 1),
            capability_integer(values["MaxChargingProfilesInstalled"], 1),
        )


class StationDiscovery:
    """Fresh generation-scoped reads precede the first station write; selected semantics never change silently."""

    # The caller supplies the independent current inventory; this object never opens a transport or native connection.
    def __init__(self, journal: Journal, choice: StationChoice, inventory: Callable[[], tuple[int, bool]]) -> None:
        self.journal, self.choice, self.inventory = journal, choice, inventory
        self.epoch = -1
        self.capabilities: StationCapabilities | None = None

    # Read one key at a time under the existing control sequence and pin both choice and reported device capabilities.
    async def prepare(self, call: Call, epoch: int) -> StationChoice:
        boot_digest = fingerprint(self.journal.get("boot"))
        if self.epoch == epoch and self.capabilities is not None:
            if self.journal.get("station_selection") != {
                "choice": self.choice.value(),
                "capabilities": asdict(self.capabilities),
                "boot_digest": boot_digest,
            }:
                raise RecoveryRequiredError("station selection or introduction changed on the current connection")
            return self.choice
        values = {}
        for key in CAPABILITY_KEYS:
            values[key] = configuration_value(await call("GetConfiguration", commands.get_configuration([key])), key)
        if fingerprint(self.journal.get("boot")) != boot_digest:
            raise RecoveryRequiredError("device introduction changed during capability discovery; results discarded")
        capabilities = StationCapabilities.parse(values, self.choice)
        count, own_present = self.inventory()
        if capabilities.maximum_profiles < count + int(not own_present):
            raise DeniedError("charger has no reported capacity for a new reserved station profile")
        selected = {
            "choice": self.choice.value(),
            "capabilities": asdict(capabilities),
            "boot_digest": boot_digest,
        }
        previous = self.journal.get("station_selection")
        if previous is None and self.journal.db.execute("SELECT 1 FROM trial_resources LIMIT 1").fetchone():
            raise RecoveryRequiredError("legacy trial profiles require reconciliation before selecting new semantics")
        if previous is not None and previous != selected:
            raise RecoveryRequiredError("station selection or reported capabilities changed; no automatic rearm")
        self.journal.put("station_selection", selected)
        self.capabilities, self.epoch = capabilities, epoch
        return self.choice

    # Report only bounded selection and capability values, excluding the private Boot fingerprint.
    def snapshot(self) -> dict[str, Any]:
        return {"choice": self.choice.value(), "reported": asdict(self.capabilities) if self.capabilities else None}
