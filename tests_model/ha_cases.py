"""Declared finite HA peer and lifecycle inputs, independent of client implementation outcomes."""

from dataclasses import dataclass
from itertools import permutations, product


@dataclass(frozen=True)
class ReceiptCase:
    """One response position and identity shape following exactly one submitted command."""

    action: str
    boundary: int
    identity: str
    terminal: str

    # Every externally delivered response has its own event in the mandatory evidence sequence.
    @property
    def events(self):
        return ("submit", *[f"poll-{i}" for i in range(1, self.boundary + 1)], "response")

    # Namespace IDs across families so raw/excluded and executed rows can be reconciled exactly.
    @property
    def identifier(self):
        return f"ha-receipt-{self.action}-{self.boundary}-{self.identity}-{self.terminal}"


# Exhaust all four existing mutating controls and every response position through the third receipt poll.
def receipt_candidates():
    return tuple(
        ReceiptCase(*seed)
        for seed in product(
            ("pause", "app_control", "start", "start_test"),
            range(4),
            ("matching", "different", "missing", "null", "malformed"),
            ("completed", "failed"),
        )
    )


# Every declared identity/terminal response combination is a causally deliverable independent peer input.
def receipt_exclusion(case):
    return None


@dataclass(frozen=True)
class OutageCase:
    """A failed entry lifetime receives replacement, deadline and obsolete callbacks in declared orders."""

    origin: str
    lifecycle: str
    stale_okay: bool
    order: tuple[str, ...]

    # Include the obligatory failure/replacement prefix and complete second-episode recovery suffix.
    @property
    def events(self):
        return (
            "setup-failure",
            "owner-replacement",
            *self.order,
            "recover",
            "late-callbacks",
            "second-failure",
            "second-deadline",
            "second-recovery",
        )

    # Keep lifecycle histories distinct from command-response identity histories.
    @property
    def identifier(self):
        return f"ha-outage-{self.origin}-{self.lifecycle}-{int(self.stale_okay)}-" + ".".join(self.order)


# All orders are external inputs; terminal lifetime exclusions are recorded separately from collection.
def outage_candidates():
    return tuple(
        OutageCase(*seed, order)
        for seed in product(("first", "reload"), ("reload", "unload", "remove", "disable"), (False, True))
        for order in permutations(("retry", "deadline", "stale", "lifecycle"))
    )


# An unloaded or removed entry needs explicit reactivation before it can perform another setup retry.
def outage_exclusion(case):
    if case.lifecycle != "reload" and case.order.index("retry") > case.order.index("lifecycle"):
        return "setup retry follows terminal entry cleanup without reactivation"
    return None
