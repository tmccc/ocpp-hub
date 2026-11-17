"""Complete private native evidence for bounded commissioning and managed control."""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from .native_faults import NativeContentError
from .policy import integer, number
from .security import SecurityError
from .store import fingerprint
from .trial_evidence import Evidence, native_evidence


# Digests identify private objects without placing vehicle, charger or account identifiers in status output.
def require_digest(value: str) -> None:
    if not isinstance(value, str) or re.fullmatch(r"[a-f0-9]{64}", value) is None:
        raise SecurityError("handover requires a complete identity or evidence digest")


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
    saved_schedule: dict[str, Any] | None = None
    session_plan_id: int | None = None

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


ObserveHandover = Callable[[], Awaitable[HandoverEvidence]]


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
        plan_id = values["SESSION"]["saveData"]["sessionList"][0].get("_setPlanId")
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
            values["SETPLAN"],
            plan_id if type(plan_id) is int else None,
        )
    except KeyError, TypeError, ValueError, IndexError:
        raise NativeContentError("content", "complete connected-handover native evidence is required") from None
