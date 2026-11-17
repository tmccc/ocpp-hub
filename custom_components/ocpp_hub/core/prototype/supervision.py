"""Pure assessment of a completed supervision pass; evidence and requested authority remain distinct."""

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
    verified_idle_hold: bool


@dataclass(frozen=True)
class ControlFacts:
    """Current synchronous facts; the caller still rechecks permission at every later effect boundary."""

    control: ControlState
    verified_pause: bool
    verified_idle_hold: bool
    delivery_uncertain: bool
    vendor_uncertain: bool

    # Independent Pause or idle-cutoff proof can establish authority without shared app ownership.
    @property
    def has_authority(self) -> bool:
        return self.control.shared or self.verified_pause or self.verified_idle_hold

    # Faults and retirement revoke labels without discarding the recorded physical observations.
    def authority_valid(self, faulted: bool, stopping: bool) -> bool:
        return self.has_authority and not faulted and not stopping

    # This stage is evaluated before the caller reads optional cloud authorization state.
    def locally_ready(self, *, faulted: bool, stopping: bool, active: bool, delivery_blocked: bool) -> bool:
        return bool(
            self.authority_valid(faulted, stopping)
            and active
            and not self.control.values.get("uncertain")
            and not delivery_blocked
        )

    # A completed audit without matching authority, or with uncertain delivery, needs the existing strict latch.
    @property
    def requires_fault(self) -> bool:
        return not self.has_authority or self.delivery_uncertain or self.vendor_uncertain


@dataclass(frozen=True)
class HealthVerdict:
    """Health is an assessment, never a request to resume, Start, Stop or erase debt."""

    state: str
    shared: bool
    hold_verified: bool
    local_healthy: bool
    healthy: bool
    cloud_ready: bool


# A latched fault or retirement removes every authority label, including a previously proved physical hold.
def assess(
    facts: ControlFacts,
    *,
    faulted: bool,
    stopping: bool,
    active: bool,
    delivery_blocked: bool,
    cloud_pending: bool,
    cloud_ready: bool,
) -> HealthVerdict:
    valid = facts.authority_valid(faulted, stopping)
    shared = facts.control.shared and valid
    hold = (facts.verified_pause or facts.verified_idle_hold) and valid
    local = (
        facts.locally_ready(faulted=faulted, stopping=stopping, active=active, delivery_blocked=delivery_blocked)
        and not cloud_pending
    )
    healthy = local and cloud_ready
    state = "stopping" if stopping else "healthy" if healthy else "local_only" if local else "degraded"
    return HealthVerdict(state, shared, hold, local, healthy, cloud_ready)
