"""Journal ownership of managed current selection and fault state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast, overload

if TYPE_CHECKING:
    from .store import Journal

_CURRENT = "managed_current_change"
_FAULT = "managed_control_fault"


@dataclass(frozen=True)
class CurrentChange:
    """Read view of the original JSON, including fields retained for exact recovery proofs."""

    value: dict[str, Any]

    # Keep unrecognized states visible so each caller can apply its own policy to them.
    @property
    def state(self) -> str | None:
        return self.value.get("state")

    # Correlate a selection to the original request and any worker still applying it.
    @property
    def identifier(self) -> str | None:
        return self.value.get("id")

    # Pending applies the requested current; recovery retains the previous selection.
    @property
    def selecting(self) -> bool:
        return self.state == "pending"

    # An open recovery can hold zero and reconcile an independent session.
    @property
    def recovering(self) -> bool:
        return self.state == "recovering"

    # A live selection or automatic recovery blocks a new Start, Return or selection.
    @property
    def awaiting_outcome(self) -> bool:
        return self.state in ("pending", "recovering")

    # Release additionally accounts for interrupted or uncertain terminal outcomes.
    @property
    def unresolved(self) -> bool:
        return self.state in ("pending", "recovering", "interrupted", "uncertain")

    # Debt proof accepts only a missing selection or one already settled.
    @property
    def settled_or_missing(self) -> bool:
        return self.state in (None, "completed", "cancelled")


class CurrentChangeJournal:
    """Write the current record only inside the transaction chosen by the caller."""

    # Share the existing journal connection and keep reads uncached.
    def __init__(self, journal: Journal) -> None:
        self.journal = journal

    # Ordinary decisions use an empty object when no selection has been recorded.
    def read(self) -> CurrentChange:
        return CurrentChange(self.journal.get(_CURRENT, {}))

    # Exact recovery snapshots distinguish a missing record from an empty object.
    def stored(self) -> dict[str, Any] | None:
        return cast("dict[str, Any] | None", self.journal.get(_CURRENT))

    # Preserve the exact pending-record shape written at acceptance.
    def begin(self, identifier: str, target: int, previous: int, mode: str, floor: int) -> None:
        self.replace(
            {
                "id": identifier,
                "current_a": target,
                "previous_a": previous,
                "mode": mode,
                "floor": floor,
                "state": "pending",
            }
        )

    # The verified new selection replaces the entire operation record.
    def complete(self, identifier: str, target: int) -> None:
        self.replace({"id": identifier, "current_a": target, "state": "completed"})

    # Only the caller can establish which exact preimage is still valid.
    def replace(self, value: dict[str, Any]) -> None:
        self.journal.put(_CURRENT, value)

    # Advance the caller's record without losing its original identity or other evidence.
    def cancel(self, value: dict[str, Any], **details: Any) -> None:
        self.replace({**value, "state": "cancelled", **details})


class ManagedFaultJournal:
    """Own the persisted latch. Recovery callers keep responsibility for proof and atomicity."""

    # The latch shares the journal's connection and transaction.
    def __init__(self, journal: Journal) -> None:
        self.journal = journal

    # Proofs may need to distinguish absence from a falsey retained JSON value.
    @overload
    def read(self) -> dict[str, Any] | None: ...

    # A caller expecting a mapping can give its historical missing-row default.
    @overload
    def read(self, default: dict[str, Any]) -> dict[str, Any]: ...

    # Return the original stored representation for equality and fingerprints.
    def read(self, default: dict[str, Any] | None = None) -> dict[str, Any] | None:
        return cast("dict[str, Any] | None", self.journal.get(_FAULT, default))

    # Read the exact classification after the caller proved that the latch is present.
    def classification(self) -> str:
        return cast(str, cast("dict[str, Any]", self.read())["classification"])

    # Most admission checks treat any truthy recorded fault as latched.
    def latched(self) -> bool:
        return bool(self.read())

    # Write the initial fault or a replacement already proved by the caller.
    def replace(self, fault: dict[str, Any]) -> None:
        self.journal.put(_FAULT, fault)

    # Clearing the latch does not itself change phase or grant positive authority.
    def clear(self) -> None:
        self.journal.db.execute("DELETE FROM meta WHERE key=?", (_FAULT,))
