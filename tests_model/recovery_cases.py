"""Finite recovery histories define external inputs and causal exclusions before any engine runs."""

from dataclasses import dataclass
from itertools import permutations, product

START_EVENTS = ("restore", "pause", "restart-ready", "observe", "replay")
START_FAULTS = (
    "preflight",
    "zero-rejected",
    "positive-rejected",
    "positive-call-error",
    "positive-503",
    "positive-eof",
    "positive-bad-json",
    "remote-503",
    "start-rejected",
)


@dataclass(frozen=True)
class StartRecoveryCase:
    """A known unissued or explicitly refused command followed by a persistent independent native outage."""

    action: str
    selected: int
    fault: str
    demand: bool
    first_close: str
    events: tuple[str, ...]

    # Every seed and every external event order is replayable without reading a production outcome.
    @property
    def identifier(self):
        seed = f"start-recovery-{self.action}-{self.selected}-{self.fault}-{int(self.demand)}-{self.first_close}"
        return seed + "-" + ".".join(self.events)


# Include unsupported timed-test settings in the raw universe, with explicit admission exclusions below.
def start_candidates():
    return (
        StartRecoveryCase(*seed, order)
        for seed in product(("start", "start_test"), (6, 16, 32), START_FAULTS, (False, True), ("native", "ledger"))
        for order in permutations(START_EVENTS)
    )


# Replay needs an existing Pause receipt; the fixed two-minute test is intentionally available only at6A.
def start_exclusion(case):
    if case.action == "start_test" and case.selected != 6:
        return "fixed timed test requires selected6A"
    if case.events.index("replay") < case.events.index("pause"):
        return "receipt replay precedes its original Pause"
    if case.events.index("restart-ready") < case.events.index("restore"):
        return "successful replacement listener requires restored native startup audit"
    return None


# Development smoke is explicitly a subset; full qualification rejects all sampling switches.
def start_cases():
    return tuple(case for case in start_candidates() if start_exclusion(case) is None)
