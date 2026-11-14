"""Typed native contradictions and measured limits retain the original independently observed sample."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from .security import SecurityError

if TYPE_CHECKING:
    from .trial_handover_evidence import HandoverEvidence

StrictStage = Literal["identity", "content", "configuration", "history", "resources"]
MEASURED_FAULT = "measured_output_limit_exceeded"


class NativeContentError(SecurityError):
    """An explicit validator established a contradiction, rather than an unavailable or unready observation."""

    # Keep the strict source separate from the existing public diagnostic wording.
    def __init__(self, stage: StrictStage, detail: str) -> None:
        super().__init__(detail)
        self.stage = stage


@dataclass(frozen=True)
class MeasuredValues:
    """A pure verdict carries unchanged actual values and unchanged limits; it grants no operation permission."""

    current_a: float
    power_w: float
    current_limit_a: float
    power_limit_w: float

    # Return only the fixed exceeded-field names after all values were independently validated.
    @property
    def exceeded(self) -> tuple[str, ...]:
        return tuple(
            name
            for name, actual, limit in (
                ("current_a", self.current_a, self.current_limit_a),
                ("power_w", self.power_w, self.power_limit_w),
            )
            if actual > limit
        )

    # Existing admission and positive callers always enforce the same absolute and selected bounds.
    def require(self) -> None:
        if self.exceeded:
            raise SecurityError("fresh complete independent native current-limit evidence is required")

    # Apply the selected bound only as another pure verdict, never by changing the observed meter.
    def selected(self, selected: float) -> MeasuredValues:
        return MeasuredValues(
            self.current_a,
            self.power_w,
            min(self.current_limit_a, selected + 0.5),
            min(self.power_limit_w, selected * 270 + 100),
        )

    # Persist exact measured facts without private native configuration or charger identifiers.
    def value(self) -> dict[str, Any]:
        return {
            "current_a": self.current_a,
            "power_w": self.power_w,
            "current_limit_a": self.current_limit_a,
            "power_limit_w": self.power_limit_w,
            "exceeded": list(self.exceeded),
        }


class _NativeMeasurementError(SecurityError):
    """Only the settled controller may finish the session/resource checks and promote this failed read."""

    # Preserve the exact full-audit sample and its final authority instead of performing a replacement read.
    def __init__(self, native: HandoverEvidence, measured: MeasuredValues, authority: str, sequence: int) -> None:
        super().__init__("fresh complete independent native current-limit evidence is required")
        self.native, self.measured, self.authority, self.sequence = native, measured, authority, sequence


class MeasuredOutputLimitError(SecurityError):
    """A fully settled native audit proved only a finite measured-bound excess."""

    # The fixed origin and exact specimen/spec/selection binding distinguish this from generic old faults.
    def __init__(self, native: HandoverEvidence, measured: MeasuredValues, spec_digest: str, selected: float) -> None:
        super().__init__("actual native power exceeds the selected managed current")
        self.cause = {
            "version": 1,
            "origin": "settled_native_measurement",
            "sample_digest": native.digest,
            "spec_digest": spec_digest,
            "selection_a": selected,
            **measured.value(),
        }


# Preserve established operator diagnostic names while typed source causes remain private control flow.
def diagnostic_type(error: Exception) -> str:
    return "SecurityError" if isinstance(error, NativeContentError | _NativeMeasurementError) else type(error).__name__


# A distinct typed diagnostic must remain structurally exact before a fresh Pause can acknowledge its fault.
def measurement_cause(value: Any, spec_digest: str, selected: float, ceiling: int) -> bool:
    if not isinstance(value, dict) or set(value) != {
        "version",
        "origin",
        "sample_digest",
        "spec_digest",
        "selection_a",
        "current_a",
        "power_w",
        "current_limit_a",
        "power_limit_w",
        "exceeded",
    }:
        return False
    if (
        value["version"] != 1
        or type(value["version"]) is not int
        or value["origin"] != "settled_native_measurement"
        or value["spec_digest"] != spec_digest
        or value["selection_a"] != selected
        or not isinstance(value["sample_digest"], str)
        or len(value["sample_digest"]) != 64
        or any(c not in "0123456789abcdef" for c in value["sample_digest"])
    ):
        return False
    fields = ("selection_a", "current_a", "power_w", "current_limit_a", "power_limit_w")
    if any(
        type(value[k]) not in (int, float) or not 0 <= value[k] <= 1000000 or not math.isfinite(value[k])
        for k in fields
    ):
        return False
    measured = MeasuredValues(value["current_a"], value["power_w"], value["current_limit_a"], value["power_limit_w"])
    return (
        6 <= selected <= 32
        and ceiling in (6, 32)
        and (ceiling == 32 or selected == 6)
        and value["current_limit_a"] == (selected + 0.5 if ceiling == 32 else 6.5)
        and value["power_limit_w"] == (min(8500, selected * 270 + 100) if ceiling == 32 else 2000)
        and bool(measured.exceeded)
        and value["exceeded"] == list(measured.exceeded)
    )
