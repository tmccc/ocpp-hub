"""Enumerate every ordering in the declared two-receipt observation/cancellation universe."""

from dataclasses import dataclass
from itertools import permutations, product

EVENTS = ("native", "report", "observe", "cutoff", "cancel")


@dataclass(frozen=True)
class SequenceCase:
    """One completely specified, replayable environment history; no controller outcome is supplied."""

    inventory: str
    selected: int
    source: str
    demand: bool
    disruption: str
    first_close: str
    events: tuple[str, ...]

    # Keep the case name independent of Python hashes, random request IDs and execution order.
    @property
    def identifier(self) -> str:
        prefix = (
            f"{self.inventory}-{self.selected}-{self.source}-{int(self.demand)}-{self.disruption}-{self.first_close}"
        )
        return prefix + "-" + ".".join(self.events)


# Declare causal exclusions before execution, rather than discarding inconvenient failures afterwards.
def exclusion(case: SequenceCase) -> str | None:
    positions = {event: case.events.index(event) for event in EVENTS}
    first_delivery = min(positions["native"], positions["report"])
    if positions["cutoff"] < first_delivery:
        return "this universe requires the contested session to begin before its cutoff"
    if case.disruption == "unplug" and positions["cancel"] > first_delivery and case.first_close == "ledger":
        return "the physical departure has already established native-first closure"
    return None


# Exhaust the finite product and all permutations; no sampling, random seed or assumed commutativity is used.
def candidates():
    dimensions = product(
        ("requested", "armed", "complete"),
        (6, 32),
        ("daily", "unknown"),
        (False, True),
        ("unplug", "restart"),
        ("native", "ledger"),
    )
    for seed in dimensions:
        for events in permutations(EVENTS):
            yield SequenceCase(*seed, events)


# Retain a deterministic complete list so collection and execution can be reconciled against the same manifest.
def cases():
    return tuple(case for case in candidates() if exclusion(case) is None)


# Validate every seed before spending time on all of its orderings; a prefix slice misses entire device states.
def smoke_cases(*, all_orders=False):
    seen = set()
    result = []
    for case in cases():
        seed = (case.inventory, case.selected, case.source, case.demand, case.disruption, case.first_close)
        order_probe = (
            all_orders
            and case.inventory in ("requested", "complete")
            and case.selected == 6
            and case.demand is False
            and case.first_close == "native"
        )
        if seed not in seen or order_probe:
            seen.add(seed)
            result.append(case)
    return tuple(result)


SOURCE_EVENTS = ("loss", "observe", "cutoff", "restore", "restart")


@dataclass(frozen=True)
class SourceCase:
    """Inputs describe actual source facts; the controller must derive all of its own authority."""

    selected: int
    demand: bool
    lost_source: int | None
    first_close: str
    events: tuple[str, ...]

    # Use a stable complete identifier for collection, sharding and event-prefix evidence.
    @property
    def identifier(self):
        source = "unknown" if self.lost_source is None else "foreign"
        return f"{self.selected}-{int(self.demand)}-{source}-{self.first_close}-" + ".".join(self.events)


# Declare all source histories, including impossible restoration-before-loss inputs for a visible denominator.
def source_candidates():
    return (
        SourceCase(*seed, order)
        for seed in product((6, 32), (False, True), (None, 987655), ("native", "ledger"))
        for order in permutations(SOURCE_EVENTS)
    )


# A restoration belongs to the source-loss episode which precedes it in this finite grammar.
def source_exclusion(case):
    return "source restoration precedes its loss" if case.events.index("restore") < case.events.index("loss") else None


# Enumerate all sixty causally possible permutations for each of the sixteen physical seed configurations.
def source_cases():
    return tuple(case for case in source_candidates() if source_exclusion(case) is None)
