"""Pure control proof and health decisions for supervision and management status."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .control_state import ControlState
from .managed_intent import ControlIntent

if TYPE_CHECKING:
    from .trial_handover_evidence import HandoverEvidence


@dataclass(frozen=True)
class PollStatus:
    """A pass stopped before a complete observation; it publishes no verified authority."""

    state: str
    fault: str | None = None
    intent: ControlIntent | None = None


@dataclass(frozen=True)
class Observation:
    """A complete fresh audit and its independently verified restrictive holds."""

    native: HandoverEvidence
    verified_pause: bool


@dataclass(frozen=True)
class ControlProof:
    """Observed shared control and observed closed zero, scoped to the current charger and intent."""

    shared: bool = False
    hold_verified: bool = False

    # Current ownership can retain an observation. A local claim alone never proves physical zero.
    def retain(self, control: ControlState, *, scope_current: bool = True, released: bool = True) -> ControlProof:
        return ControlProof(
            self.shared and scope_current and released and control.shared,
            self.hold_verified and scope_current and control.local_hold,
        )

    # Either kind of proof can establish local supervision.
    @property
    def available(self) -> bool:
        return self.shared or self.hold_verified

    # This stage is evaluated before the caller reads optional cloud authorization state.
    def locally_ready(self, *, active: bool, control_uncertain: bool, delivery_blocked: bool) -> bool:
        return self.available and active and not control_uncertain and not delivery_blocked


@dataclass(frozen=True)
class HealthVerdict:
    """Health is an assessment, never a request to resume, Start, Stop or erase debt."""

    state: str
    proof: ControlProof
    local_healthy: bool
    healthy: bool
    cloud_ready: bool


# Pending cloud authorization blocks health without changing the physical proof.
def assess(
    proof: ControlProof,
    *,
    stopping: bool,
    locally_ready: bool,
    cloud_pending: bool,
    cloud_ready: bool,
) -> HealthVerdict:
    local = locally_ready and not cloud_pending
    healthy = local and cloud_ready
    state = "stopping" if stopping else "healthy" if healthy else "local_only" if local else "degraded"
    return HealthVerdict(state, proof, local, healthy, cloud_ready)
