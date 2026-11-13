"""Pure input grammars for complete integer choices and actual process-death boundaries."""

from dataclasses import dataclass
from itertools import permutations, product


@dataclass(frozen=True)
class CurrentDomainCase:
    """Every integer choice is observed from every previous choice under both independent physical demands."""

    initial: int
    target: int
    intent: str
    demand: bool

    # The same-value path is a real request/idempotency control, not an excluded no-op.
    @property
    def identifier(self):
        return f"domain-{self.initial}-{self.target}-{self.intent}-{int(self.demand)}"

    # Fixed actions cover receipt replay and two later real lifecycle/outage/recovery epochs.
    @property
    def events(self):
        return ("choose", "replay", "pause", "return", "lifecycle-1", "outage-1", "lifecycle-2", "outage-2")


# Enumerate every directed pair, including equal values, without relying on production current validation.
def current_domain_candidates():
    return tuple(
        CurrentDomainCase(*values) for values in product(range(6, 33), range(6, 33), ("app", "paused"), (False, True))
    )


@dataclass(frozen=True)
class ProcessStartCase:
    """Real process death precedes dispatch; the parent retains the physical installation across generations."""

    phase: str
    selected: int
    demand: bool

    # These are actual awaited external milestones, with no claim that SIGKILL models a disk power failure.
    @property
    def events(self):
        return (
            "healthy",
            "armed",
            "sql-boundary",
            "durable-read",
            "sigkill",
            "restart",
            "pause",
            "retired",
            "return",
            "lifecycle-1",
            "outage-1",
            "lifecycle-2",
            "outage-2",
        )

    # Stable names preserve the precise persistence boundary in qualification evidence.
    @property
    def identifier(self):
        return f"process-start-{self.phase}-{self.selected}-{int(self.demand)}"


# Include statement, transaction-internal, before-COMMIT and actual after-COMMIT process deaths independently.
def process_start_candidates():
    return tuple(
        ProcessStartCase(*values)
        for values in product(
            (
                "after-request-commit",
                "before-prepared-sql",
                "receipt-before-marker",
                "before-prepared-commit",
                "after-prepared-commit",
                "before-issued-commit",
            ),
            (6, 16, 32),
            (False, True),
        )
    )


@dataclass(frozen=True)
class ProcessCurrentCase:
    """A committed selection is distinguished from an uncommitted physical effect after genuine SIGKILL."""

    initial: int
    target: int
    phase: str
    intent: str

    # Exact old/target direction and intent remain part of the source-pinned qualification denominator.
    @property
    def identifier(self):
        return f"process-current-{self.initial}-{self.target}-{self.phase}-{self.intent}"

    # Every case exercises both later ordinary native-outage recoveries after an explicit new Pause/Return.
    @property
    def events(self):
        return (
            "choose",
            "sql-boundary",
            "durable-read",
            "sigkill",
            "restart",
            "lifecycle-1",
            "outage-1",
            "lifecycle-2",
            "outage-2",
        )


# Keep every directed representative pair under both intents and each side of real success COMMIT.
def process_current_candidates():
    return tuple(
        ProcessCurrentCase(initial, target, phase, intent)
        for initial, target in permutations((6, 16, 32), 2)
        for phase, intent in product(
            ("after-current-request-commit", "before-current-success-commit", "after-current-success-commit"),
            ("app", "paused"),
        )
    )


@dataclass(frozen=True)
class ProcessOverlapCase:
    """An original plan session survives an unconfirmed selection and actual service process replacement."""

    initial: int
    target: int
    boundary: str
    demand: bool

    # Both physical sides of the selection retain independent demand and direction in the denominator.
    @property
    def identifier(self):
        return f"process-overlap-{self.initial}-{self.target}-{self.boundary}-{int(self.demand)}"

    # Automatic retirement and a new selection precede any explicit Pause/Return that could hide an orphan.
    @property
    def events(self):
        return (
            "current-pending",
            "held-native",
            "original-start",
            "cloud-mapped",
            "sigkill",
            "restart",
            "automatic-restore",
            "retired-receipt",
            "original-outage",
            "original-closed",
            "new-selection",
            "replay-old",
            "lifecycle-1",
            "outage-1",
            "lifecycle-2",
            "outage-2",
        )


# Exercise increases and decreases before dispatch and after physical application under both real demand states.
def process_overlap_candidates():
    return tuple(
        ProcessOverlapCase(initial, target, boundary, demand)
        for initial, target in ((6, 32), (32, 6))
        for boundary, demand in product(("native-before", "native-after"), (False, True))
    )
