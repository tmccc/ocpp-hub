"""Canonical 192 storage/FIFO histories; independent inputs, with no outcome-based exclusion."""

from dataclasses import dataclass
from itertools import permutations, product


@dataclass(frozen=True)
class StorageFifoCase:
    """Two real storage refusals precede independently proven FIFO recovery and two later physical outages."""

    selected: int
    cut: str
    sqlite_fault: str
    tail_pairs: int
    changed_reply: str
    order: tuple[str, ...]

    # Record mandatory real failures, every competing order and the complete later physical recovery suffix.
    @property
    def events(self):
        return (
            "tails-queued",
            "head-refused",
            "failure-1",
            "failure-2",
            *self.order,
            "head-ack",
            "tails-reconciled",
            "pause",
            "return",
            "lifecycle-1",
            "outage-1",
            "lifecycle-2",
            "outage-2",
        )

    # Keep every declared boundary and order in its stable audit identity.
    @property
    def identifier(self):
        return (
            f"storage-fifo-{self.selected}-{self.cut}-{self.sqlite_fault}-"
            f"{self.tail_pairs}-{self.changed_reply}-" + ".".join(self.order)
        )


# Generate every declared storage/FIFO input independently of product outcomes.
def storage_fifo_candidates():
    return tuple(
        StorageFifoCase(*seed, order)
        for seed in product((6, 32), ("proof", "ack"), ("write", "commit"), (1, 3), ("changed-info", "call-error"))
        for order in permutations(("repair", "pause", "replace"))
    )


# All declared storage inputs represent causally executable peer actions.
def storage_fifo_exclusion(case):
    return None
