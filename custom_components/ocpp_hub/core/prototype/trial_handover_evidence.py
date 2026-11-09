"""Private connected-handover evidence; historical capability is never current vehicle permission."""

from __future__ import annotations

import math
import re
import time
from dataclasses import dataclass
from typing import Any, Protocol

from .policy import integer, number
from .security import SecurityError
from .store import fingerprint
from .trial_evidence import Evidence, native_evidence


# Digests identify private objects without placing vehicle, charger or account identifiers in status output.
def require_digest(value: str) -> None:
    if not isinstance(value, str) or re.fullmatch(r"[a-f0-9]{64}", value) is None:
        raise SecurityError("handover requires a complete identity or evidence digest")


@dataclass(frozen=True, repr=False)
class PauseRequest:
    """A one-use read request binds the provider to this exact trial, native view and local authority."""

    binding: str
    nonce: str
    stage: str
    identity_digest: str
    hardware_digest: str
    vehicle_digest: str
    baseline_digest: str
    native_digest: str
    boot_digest: str
    epoch: int


@dataclass(frozen=True, repr=False)
class PauseCapability:
    """The observed reboot/positive-offer test establishes capability, alongside a separate fresh pause readback."""

    digest: str
    identity_digest: str
    hardware_digest: str
    vehicle_digest: str
    boot_digest: str
    charging_before_a: float
    user_limit_a: float
    positive_offer_a: float
    zero_offer_seconds: float
    restart_verified: bool

    # Reject a different vehicle/firmware or a zero observation without genuine offered current and prior charging.
    def require_match(self, request: PauseRequest) -> None:
        require_digest(self.digest)
        if (
            any(
                getattr(self, key) != getattr(request, key)
                for key in ("identity_digest", "hardware_digest", "vehicle_digest", "boot_digest")
            )
            or not 5.5 <= number(self.charging_before_a, 6.5) <= 6.5
            or type(self.user_limit_a) not in (float, int)
            or self.user_limit_a != 6
            or type(self.positive_offer_a) not in (float, int)
            or self.positive_offer_a != 6
            or number(self.zero_offer_seconds, 1800) < 60
            or self.restart_verified is not True
        ):
            raise SecurityError("vehicle pause has no matching restart and positive-offer capability proof")


@dataclass(frozen=True, repr=False)
class PauseObservation:
    """Only a live provider may assert the current car permission and its explicit bounded validity."""

    request: PauseRequest
    provider_digest: str
    observed_at: float
    expires_at: float
    pause_valid_until: float
    release_only: bool
    paused: bool
    revision: str
    permission_digest: str
    original_revision: str
    original_permission_digest: str
    capability: PauseCapability

    # An old pilot file, unknown expiry, optimistic command ACK or another operation cannot release a charger hold.
    def require_current(self, request: PauseRequest, provider_digest: str) -> None:
        for digest in (self.provider_digest, self.permission_digest, self.original_permission_digest):
            require_digest(digest)
        dates = (self.observed_at, self.expires_at, self.pause_valid_until)
        now = time.time()
        if (
            self.request != request
            or self.provider_digest != provider_digest
            or self.paused is not True
            or self.release_only is not True
            or any(type(value) not in (int, float) or not math.isfinite(value) for value in dates)
            or not 0 <= now - self.observed_at <= 5
            or not now + 5 <= self.expires_at <= self.observed_at + 30
            or not max(self.expires_at, now + 900) <= self.pause_valid_until <= now + 1800
            or any(
                not isinstance(value, str) or not 1 <= len(value) <= 128
                for value in (self.revision, self.original_revision)
            )
        ):
            raise SecurityError("fresh matching vehicle-pause evidence with known validity is required")
        self.capability.require_match(request)


@dataclass(frozen=True, repr=False)
class PermissionObservation:
    """An acknowledged Start is insufficient; the provider must reconcile the exact saved permission state."""

    request: PauseRequest
    provider_digest: str
    operation: str
    observed_at: float
    permission_digest: str
    previous_revision: str
    revision: str


class PauseProvider(Protocol):
    """No live SEAT adapter is inferred from historical UI captures or the read-only Home Assistant integration."""

    digest: str

    # Obtain current vehicle state for this fresh request, independently from charger profiles and power readings.
    async def observe(self, request: PauseRequest) -> PauseObservation: ...

    # Apply an idempotent compare-and-set operation, or reconcile its previous outcome after a lost reply.
    async def restore(
        self, request: PauseRequest, operation: str, expected_revision: str, original_permission_digest: str
    ) -> PermissionObservation: ...

    # Read a prior operation's actual current result; retrying the request itself is never outcome reconciliation.
    async def restoration_status(self, request: PauseRequest, operation: str) -> PermissionObservation | None: ...

    # An unplugged fallback may only observe that the owner already restored the original vehicle permission.
    async def permission_status(self, request: PauseRequest) -> PermissionObservation: ...


@dataclass(frozen=True, repr=False)
class HandoverEvidence:
    """All phases, native USER/HW limits, full configuration and retained history remain independently observable."""

    physical: Evidence
    hardware_digest: str
    configuration: dict[str, Any]
    saved_user_a: int
    runtime_user_a: float
    runtime_hardware_a: float
    output_limit_a: float
    currents_a: tuple[float, ...]
    powers_w: tuple[float, ...]
    history: tuple[dict[str, Any], ...]
    offered_current_a: float = 0
    saved_schedule_digest: str = ""

    # Hash the complete private view, not a public boolean or the proxy's desired profile.
    @property
    def digest(self) -> str:
        return fingerprint({"content": self.content_digest, "at": self.physical.observed_at})

    # Time-independent content equality detects external native changes during provider or transport waits.
    @property
    def content_digest(self) -> str:
        return fingerprint(
            {
                "configuration": self.configuration,
                "hardware": self.hardware_digest,
                "USER": self.runtime_user_a,
                "HWCFG": self.runtime_hardware_a,
                "outLimit": self.output_limit_a,
                "offered": self.offered_current_a,
                "saved_schedule": self.saved_schedule_digest,
                "currents": self.currents_a,
                "powers": self.powers_w,
                "history": self.history,
                "resources": [r.digest for r in self.physical.resources],
                "cp": self.physical.cp,
                "online": self.physical.upstream_connected,
                "native_transaction": self.physical.native_transaction_id,
            }
        )

    # Finished may retain offerCur60; actual output0, every phase0 and a closed session are independently required.
    def require_closed(self, identity: str, hardware: str, user_a: int, hardware_a: int) -> None:
        self.physical.require_fresh(identity, 5)
        if (
            self.hardware_digest != fingerprint(hardware)
            or self.physical.cp != "B"
            or self.physical.connection != "Plug"
            or self.physical.transaction_open
            or self.physical.native_transaction_id is not None
            or self.saved_user_a != user_a
            or self.runtime_user_a != user_a
            or self.runtime_hardware_a != hardware_a
            or self.output_limit_a != 0
            or len(self.currents_a) != 3
            or len(self.powers_w) != 4
            or any(type(v) not in (float, int) or v != 0 for v in self.currents_a + self.powers_w)
            or self.physical.current_a != 0
            or self.physical.power_w != 0
        ):
            raise SecurityError("connected handover requires USER guard, closed session and every phase at zero")


# Parse the known native tenths-of-an-ampere fields without widening the existing unplugged evidence contract.
def handover_evidence(config: Any, values: dict[str, Any], observed_at: float) -> HandoverEvidence:
    try:
        physical = native_evidence(config, values, observed_at)
        limits = values["STATE"][0]["currLimit"]
        meter = values["METER"][0]["meterData"]
        if not isinstance(config["AmpLmt"], str) or re.fullmatch(r"[0-9]{1,2}", config["AmpLmt"]) is None:
            raise ValueError
        history = values["TRANSACTIONS"]
        if physical.history is None or not isinstance(history, list):
            raise ValueError
        return HandoverEvidence(
            physical,
            fingerprint(config["bPSN"]),
            config,
            integer(int(config["AmpLmt"]), 0, 32),
            number(limits["USER"], 320) / 10,
            number(limits["HWCFG"], 320) / 10,
            number(values["CONNECTOR"][0]["outLimit"], 320) / 10,
            tuple(number(v, 10000) / 10 for v in meter["cur"]),
            tuple(number(v, 1000000) for v in meter["power"]),
            tuple(history),
            number(values["CONNECTOR"][0].get("offerCur", 0), 320) / 10,
            fingerprint(values["SETPLAN"]),
        )
    except KeyError, TypeError, ValueError, IndexError:
        raise SecurityError("complete connected-handover native evidence is required") from None
