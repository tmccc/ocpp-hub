"""Independent native USER6 evidence bounds connected handover without asserting vehicle pause permission."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any, Self

from .policy import integer, number
from .security import SecurityError
from .store import encoded, fingerprint
from .trial_allocation import observe_history
from .trial_handover_evidence import HandoverEvidence, require_digest
from .trial_resources import Baseline

ROUTE_FIELDS = frozenset({"sDomain", "sPort", "sURL", "allowSelfSign"})


# Route changes are explicit native strings; neither credential fields nor another setting can enter this map.
def require_route(value: dict[str, Any]) -> None:
    if (
        not isinstance(value, dict)
        or set(value) != ROUTE_FIELDS
        or any(
            not isinstance(v, str) or not 1 <= len(v) <= 1024 or any(ord(c) < 33 or ord(c) > 126 for c in v)
            for v in value.values()
        )
        or not value["sPort"].isascii()
        or not value["sPort"].isdigit()
        or not 1 <= int(value["sPort"]) <= 65535
        or not value["sURL"].startswith("/")
        or value["allowSelfSign"] not in {"0", "1"}
    ):
        raise SecurityError("native bounded handover requires exactly the four known connection fields")


# Restrict this first native-limit capability to the independently tested single-connector32A hardware configuration.
def require_original(configuration: dict[str, Any], hardware: str) -> None:
    if (
        not isinstance(configuration, dict)
        or not isinstance(hardware, str)
        or not 1 <= len(hardware) <= 64
        or not isinstance(configuration.get("Id"), str)
        or not 1 <= len(configuration["Id"]) <= 64
        or configuration.get("bPSN") != hardware
        or configuration.get("AmpLmt") != "32"
        or configuration.get("maxOutCurrent0") != "32"
        or configuration.get("minOutCurrent0", "6") != "6"
        or configuration.get("connectorNum", "1") != "1"
    ):
        raise SecurityError("native bounded handover requires its original matching32A station configuration")
    require_route({key: configuration.get(key) for key in ROUTE_FIELDS})


# Check direct native values independently of the proxy's desired profile, vendor availability or car permission.
def require_native(
    native: HandoverEvidence,
    identity: str,
    hardware: str,
    user_a: int,
    closed: bool,
    *,
    allow_unplugged: bool = False,
) -> None:
    try:
        physical = native.physical
        if type(user_a) is not int or user_a not in {6, 32} or type(allow_unplugged) is not bool:
            raise ValueError
        unplugged = allow_unplugged and user_a == 6 and physical.cp == "A" and physical.connection == "Unplug"
        current_max, power_max = (6.5, 2000) if user_a == 6 else (32.5, 8500)
        if type(physical.observed_at) not in (int, float) or not math.isfinite(physical.observed_at):
            raise ValueError
        physical.require_fresh(identity, 5)
        require_digest(native.saved_schedule_digest)
        if (
            native.hardware_digest != fingerprint(hardware)
            or native.configuration.get("Id") != identity
            or native.configuration.get("bPSN") != hardware
            or fingerprint(native.configuration) != physical.configuration_digest
            or type(native.saved_user_a) is not int
            or native.saved_user_a != user_a
            or native.configuration.get("AmpLmt") != str(user_a)
            or native.configuration.get("maxOutCurrent0") != "32"
            or number(native.runtime_user_a, 32) != user_a
            or number(native.runtime_hardware_a, 32) != 32
            or len(native.currents_a) != 3
            or len(native.powers_w) != 4
            or physical.cp not in {"B", "C"}
            and not unplugged
            or physical.connection not in {"Plug", "Charging"}
            and not unplugged
            or physical.cp == "C"
            and physical.connection != "Charging"
            or type(physical.transaction_open) is not bool
            or physical.transaction_open != (physical.native_transaction_id is not None)
            or physical.cp == "C"
            and not physical.transaction_open
        ):
            raise ValueError
        if physical.native_transaction_id is not None:
            integer(physical.native_transaction_id, 1)
        for current in native.currents_a:
            number(current, current_max)
        for power in native.powers_w:
            number(power, power_max)
        number(native.output_limit_a, user_a)
        number(native.offered_current_a, 32)
        if (
            number(physical.current_a, current_max) != max(native.currents_a)
            or number(physical.power_w, power_max) != max(native.powers_w)
            or any(value != 0 for value in native.currents_a[1:])
            or any(value != 0 for value in native.powers_w[1:3])
        ):
            raise ValueError
        if (closed or unplugged) and (
            physical.cp != "B"
            and not unplugged
            or physical.transaction_open
            or native.output_limit_a != 0
            or any(value != 0 for value in native.currents_a + native.powers_w)
        ):
            raise SecurityError("native bounded admission or release requires actual closed zero-output evidence")
        if not physical.transaction_open and any(value != 0 for value in native.currents_a + native.powers_w):
            raise ValueError
        if physical.history is None:
            raise ValueError
        physical.history.require_fresh(identity, hardware, 5)
        observed = observe_history(list(native.history), identity, hardware, physical.observed_at)
        if physical.history != observed:
            raise ValueError
    except ValueError, TypeError, KeyError, IndexError, OverflowError:
        raise SecurityError("fresh complete independent native current-limit evidence is required") from None


@dataclass(frozen=True, repr=False, init=False)
class NativeBoundedSpec:
    """Immutable original configuration and tested capability binding; no refreshable car-pause lease exists here."""

    hardware_identity: str
    saved_schedule_digest: str
    capability_digest: str
    boot_digest: str
    _original_json: str
    _trial_route_json: str

    # Canonical private strings prevent caller mutation from silently changing an existing handover's authority.
    def __init__(
        self,
        hardware_identity: str,
        original_configuration: dict[str, Any],
        trial_route: dict[str, Any],
        saved_schedule_digest: str,
        capability_digest: str,
        boot_digest: str,
    ) -> None:
        require_original(original_configuration, hardware_identity)
        require_route(trial_route)
        for digest in (saved_schedule_digest, capability_digest, boot_digest):
            require_digest(digest)
        try:
            original_json, route_json = encoded(original_configuration), encoded(trial_route)
            if len(original_json) > 131072:
                raise ValueError
        except ValueError, TypeError, RecursionError, OverflowError:
            raise SecurityError("native bounded configuration cannot be snapshotted safely") from None
        for key, value in (
            ("hardware_identity", hardware_identity),
            ("saved_schedule_digest", saved_schedule_digest),
            ("capability_digest", capability_digest),
            ("boot_digest", boot_digest),
            ("_original_json", original_json),
            ("_trial_route_json", route_json),
        ):
            object.__setattr__(self, key, value)

    # Return a private copy; consumers cannot mutate the saved original through a shared dictionary.
    @property
    def original_configuration(self) -> dict[str, Any]:
        value: dict[str, Any] = json.loads(self._original_json)
        return value

    # The four connection fields remain separate from USER and installer/hardware settings.
    @property
    def trial_route(self) -> dict[str, Any]:
        value: dict[str, Any] = json.loads(self._trial_route_json)
        return value

    # Bind the complete private strategy without placing charger, endpoint or credential strings in status.
    @property
    def digest(self) -> str:
        return fingerprint(self.value())

    # Only the intended route and USER scalar can differ from the unchanged original configuration.
    def configuration(self, route: str, user_a: int = 6) -> dict[str, Any]:
        if route not in {"original", "trial"} or type(user_a) is not int or user_a not in {6, 32}:
            raise SecurityError("native bounded route or user setting is unsupported")
        if user_a == 32 and route != "original":
            raise SecurityError("original current cannot be released while the charger points at the proxy")
        original = self.original_configuration
        return original | (self.trial_route if route == "trial" else {}) | {"AmpLmt": str(user_a)}

    # A fresh bounded read is not a paused car, a reconciled lifecycle, or proof that the original backend is online.
    def require(
        self,
        native: HandoverEvidence,
        identity: str,
        route: str,
        user_a: int = 6,
        closed: bool = False,
        *,
        allow_unplugged: bool = False,
    ) -> None:
        if type(allow_unplugged) is not bool:
            raise SecurityError("unplugged monitoring requires an explicit boolean option")
        expected = self.configuration(route, user_a)
        if identity != expected["Id"]:
            raise SecurityError("native bounded specification belongs to another charger")
        require_native(
            native,
            identity,
            self.hardware_identity,
            user_a,
            closed or user_a == 32,
            allow_unplugged=allow_unplugged and route == "trial" and user_a == 6,
        )
        if native.configuration != expected or native.saved_schedule_digest != self.saved_schedule_digest:
            raise SecurityError("native bounded configuration or saved schedule changed")

    # Connected admission is stricter than bounded monitoring and cannot start from another open native session.
    def require_admission(self, native: HandoverEvidence, identity: str, route: str = "trial") -> None:
        self.require(native, identity, route, closed=True)
        if any(resource.kind == "profile" for resource in native.physical.resources):
            raise SecurityError("existing native profiles block a fresh connected trial")

    # Only a separately authorized completed release may observe normal32A activity without claiming a paused car.
    def require_normal(self, native: HandoverEvidence, identity: str) -> None:
        expected = self.configuration("original", 32)
        if identity != expected["Id"]:
            raise SecurityError("normal native restoration belongs to another charger")
        require_native(native, identity, self.hardware_identity, 32, False)
        if (
            native.configuration != expected
            or native.saved_schedule_digest != self.saved_schedule_digest
            or native.physical.upstream_connected is not True
        ):
            raise SecurityError("normal native configuration or saved schedule remains unrestored")

    # The caller must also establish a real current Boot on its authenticated transport before using this comparison.
    def require_boot(self, boot: Any) -> None:
        if fingerprint(boot) != self.boot_digest:
            raise SecurityError("native current-limit persistence was not proved for this charger introduction")

    # Keep original data in the private journal for exact restart binding, never in a public status summary.
    def value(self) -> dict[str, Any]:
        return {
            "hardware_identity": self.hardware_identity,
            "original_configuration": self.original_configuration,
            "trial_route": self.trial_route,
            "saved_schedule_digest": self.saved_schedule_digest,
            "capability_digest": self.capability_digest,
            "boot_digest": self.boot_digest,
        }

    # A restart must supply the exact complete saved strategy instead of defaulting missing proof fields.
    @classmethod
    def parse(cls, value: dict[str, Any]) -> Self:
        if not isinstance(value, dict) or set(value) != {
            "hardware_identity",
            "original_configuration",
            "trial_route",
            "saved_schedule_digest",
            "capability_digest",
            "boot_digest",
        }:
            raise SecurityError("native bounded specification is incomplete")
        return cls(**value)


# A fresh connected baseline remains independent from, and never weakens, ordinary unplugged baseline capture.
def capture_native_bounded_baseline(native: HandoverEvidence, identity: str, hardware_identity: str) -> Baseline:
    require_original(native.configuration, hardware_identity)
    require_native(native, identity, hardware_identity, 32, True)
    if any(resource.kind == "profile" for resource in native.physical.resources):
        raise SecurityError("existing native profiles block connected baseline capture")
    return Baseline.parse(
        json.loads(
            encoded(
                {
                    "identity_digest": native.physical.identity_digest,
                    "observed_at": native.physical.observed_at,
                    "configuration_digest": native.physical.configuration_digest,
                    "resources": [
                        {"kind": r.kind, "id": r.identifier, "body": r.body, "canonical": r.canonical}
                        for r in native.physical.resources
                    ],
                }
            )
        )
    )
