"""The journal's physical control authority: pure state transitions and one durable writer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypedDict, Unpack

if TYPE_CHECKING:
    from .store import Journal


class ControlProjection(TypedDict, total=False):
    """Selected current and unresolved Start are evidence projections, not new ownership claims."""

    resume_limit: float
    start_pending: bool


class ControlCompletion(ControlProjection, total=False):
    """Only an exact current ticket may publish these operation outcomes."""

    owner: str
    inhibit: bool
    uncertain: bool


@dataclass(frozen=True)
class ControlState:
    """Preserve stored fields and defaults exactly; this snapshot grants no permission across an await."""

    values: dict[str, Any]

    # Legacy readers accept falsey flags; they do not require explicit JSON false.
    @property
    def shared(self) -> bool:
        return (
            self.values.get("owner") == "shared" and not self.values.get("inhibit") and not self.values.get("uncertain")
        )

    # Publication-bound readers require both flags to have been explicitly confirmed.
    @property
    def confirmed_shared(self) -> bool:
        return (
            self.values.get("owner") == "shared"
            and self.values.get("inhibit") is False
            and self.values.get("uncertain") is False
        )

    # A local inhibit is a restrictive claim; actual zero and lifecycle proof remain separate.
    @property
    def local_hold(self) -> bool:
        return self.values.get("owner") == "local" and bool(self.values.get("inhibit"))

    # Stronger proof sites require an explicit true inhibit, retaining their original legacy-shape refusal.
    @property
    def explicit_local_hold(self) -> bool:
        return self.values.get("owner") == "local" and self.values.get("inhibit") is True

    # Pure changes retain unknown fields and distinguish missing keys from explicit false.
    def changed(self, **changes: Unpack[ControlCompletion]) -> ControlState:
        return ControlState({**self.values, **changes})

    # New claims fence earlier completions while retaining the selected ceiling and pending Start evidence.
    def claimed(self, ticket: str, inhibit: bool, stopping: bool, ceiling: float) -> ControlState:
        return ControlState(
            {
                **self.values,
                "owner": "local",
                "inhibit": inhibit,
                "uncertain": True,
                "ticket": ticket,
                "stop_epoch": self.values.get("stop_epoch", 0) + int(stopping),
                "resume_limit": self.values.get("resume_limit", ceiling),
            }
        )

    # A late physical reply remains evidence but cannot complete a newer owner's control claim.
    def completed(self, ticket: str, **changes: Unpack[ControlCompletion]) -> ControlState | None:
        return self.changed(**changes) if self.values.get("ticket") == ticket else None


class ControlAuthority:
    """All production writes of control pass here; callers retain transaction and physical-proof ownership."""

    # The owner never caches authority, opens a transaction or performs device I/O.
    def __init__(self, journal: Journal) -> None:
        self.journal = journal

    # Read at the decision boundary; callers retaining a preimage supply it explicitly to the transition.
    def read(self) -> ControlState:
        return ControlState(self.journal.get("control", {}))

    # One serializer keeps every transition visible without adding another stored representation.
    def _write(self, state: ControlState) -> None:
        self.journal.put("control", state.values)

    # Fresh native admission deliberately replaces the previous record, as its original admission did.
    def admit(self) -> None:
        self._write(ControlState({"owner": "local", "inhibit": True, "uncertain": False}))

    # Restrictive recovery keeps the caller's exact preimage, including old tickets and unknown evidence fields.
    def hold(self, base: dict[str, Any], *, uncertain: bool = True, start_pending: bool | None = None) -> None:
        state = ControlState(base)
        changes: ControlCompletion = {"owner": "local", "inhibit": True, "uncertain": uncertain}
        if start_pending is not None:
            changes["start_pending"] = start_pending
        self._write(state.changed(**changes))

    # Phase/session guards and ticket creation remain at the physical executor's original claim boundary.
    def claim(self, base: dict[str, Any], ticket: str, inhibit: bool, stopping: bool, ceiling: float) -> None:
        self._write(ControlState(base).claimed(ticket, inhibit, stopping, ceiling))

    # A stale completion has no write, even when its old device operation really returned Accepted.
    def complete(
        self, ticket: str, *, base: dict[str, Any] | None = None, **changes: Unpack[ControlCompletion]
    ) -> None:
        previous = self.read() if base is None else ControlState(base)
        state = previous.completed(ticket, **changes)
        if state is not None:
            self._write(state)

    # An exact claim that sent no command can restore its predecessor without changing other facts.
    def retire_unsent_claim(self, ticket: str, previous: dict[str, Any]) -> None:
        current = self.read().values
        if current.get("ticket") != ticket:
            return
        state = dict(current)
        for field in ("ticket", "uncertain"):
            if field in previous:
                state[field] = previous[field]
            else:
                state.pop(field, None)
        self._write(ControlState(state))

    # Projection updates retain their original captured base and cannot select a different owner.
    def project(self, base: dict[str, Any], **changes: Unpack[ControlProjection]) -> None:
        self._write(ControlState(base).changed(**changes))

    # App selection first withdraws authority; a paused selection changes only the future resume ceiling.
    def select(self, base: dict[str, Any], value: int, *, app: bool) -> None:
        state = ControlState(base)
        self._write(state.changed(owner="local", uncertain=True) if app else state.changed(resume_limit=value))
