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
    current_a: int | None = None

    # Every externally delivered response has its own event in the mandatory evidence sequence.
    @property
    def events(self):
        return ("submit", *[f"poll-{i}" for i in range(1, self.boundary + 1)], "response")

    # Namespace IDs across families so raw/excluded and executed rows can be reconciled exactly.
    @property
    def identifier(self):
        current = f"-{self.current_a}A" if self.current_a is not None else ""
        return f"ha-receipt-{self.action}{current}-{self.boundary}-{self.identity}-{self.terminal}"


# Exhaust all five controls and every response position through the third receipt poll.
def receipt_candidates():
    return tuple(
        ReceiptCase(action, boundary, identity, terminal, 16 if action == "set_current" else None)
        for action, boundary, identity, terminal in product(
            ("pause", "app_control", "start", "start_test", "set_current"),
            range(4),
            ("matching", "different", "missing", "null", "malformed"),
            ("completed", "failed"),
        )
    )


# A bad identity is rejected before its terminal state is read; completed represents the false-success risk.
def receipt_exclusion(case):
    if case.identity != "matching" and case.terminal == "failed":
        return "identity rejected before terminal read; completed represents this action, response position and shape"
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
