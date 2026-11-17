"""The installation lifecycle's existing stored phases, transitions and shared read predicates.

Callers retain physical admission, request guards and SQL transactions. A phase is
an observation, not permission across an await; these transitions add no validation
or wire work. Unknown stored values and the separate introduction flag stay raw.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .store import Journal


@dataclass(frozen=True)
class InstallationState:
    """One phase observation; tuple membership preserves unknown legacy JSON values."""

    value: object

    # Only the active phase admits the normal positive-control path.
    @property
    def active(self) -> bool:
        return self.value == "active"

    # Existing native admission remains usable while its restrictive recovery is pending.
    @property
    def operating(self) -> bool:
        return self.value in ("active", "recovering")

    # Fault supervision may retain a blocked installation without rearming cleanup.
    @property
    def supervised(self) -> bool:
        return self.value in ("active", "recovering", "blocked")

    # These two phases allow existing restrictive operations under their own hold guards.
    @property
    def held(self) -> bool:
        return self.value in ("blocked", "recovering")

    # Restoration phases and completed cleanup cannot be overwritten by ordinary faults.
    @property
    def cleanup(self) -> bool:
        return self.value in ("rollback", "backend_restore_pending", "complete")

    # The bare gateway rig has no installation phase; falsey non-None values are not that legacy mode.
    @property
    def legacy_active(self) -> bool:
        return self.value in (None, "active")

    # Initial resource admission accepts only prepared or already active installations.
    @property
    def admissible(self) -> bool:
        return self.value in ("prepared", "active")


class InstallationLifecycle:
    """The only phase/starting writer; every operation joins its caller's existing transaction."""

    # No cache or startup normalization may turn an old observation into authority.
    def __init__(self, journal: Journal) -> None:
        self.journal = journal

    # Reading the phase must not also sample the separate introduction flag.
    def read(self) -> InstallationState:
        return InstallationState(self.journal.get("trial_phase"))

    # Keep the flag's original truthiness and its original read boundary.
    @property
    def starting(self) -> Any:
        return self.journal.get("trial_starting")

    # Bootstrap publishes preparation beside its original verified baseline.
    def prepare(self) -> None:
        self.journal.put("trial_phase", "prepared")

    # The caller has already established resource admission or verified recovery.
    def activate(self) -> None:
        self.journal.put("trial_phase", "active")

    # Recovery-only sites must not gain a control hold, flag update or conditional refusal.
    def recover(self) -> None:
        self.journal.put("trial_phase", "recovering")

    # Cleanup's closing step explicitly blocks before its independently verified clear sequence.
    def block(self) -> None:
        self.journal.put("trial_phase", "blocked")

    # Resource and introduction faults retain cleanup, including its completed state.
    def block_unless_cleanup(self) -> None:
        if not self.read().cleanup:
            self.block()

    # A managed fault always holds its supplied preimage, but changes only an existing supervised phase.
    def fault(self, control: dict[str, Any]) -> None:
        self.journal.control.hold(control)
        if self.read().supervised:
            self.block()

    # Connection restriction preserves the original hold-before-phase write order.
    def restrict(self, control: dict[str, Any]) -> None:
        self.journal.control.hold(control)
        self.recover()

    # Local-only recovery retains its certain zero hold before reporting the active phase.
    def resume(self, control: dict[str, Any]) -> None:
        self.journal.control.hold(control, uncertain=False)
        self.activate()

    # Admission can set this flag after other writes without changing the current phase.
    def begin_introduction(self) -> None:
        self.journal.put("trial_starting", True)

    # Audited introduction completes independently of the phase or positive publication.
    def introduced(self) -> None:
        self.journal.put("trial_starting", False)

    # Explicit fault retirement publishes hold, recovering and the cleared flag in that order.
    def recover_introduced(self, control: dict[str, Any]) -> None:
        self.restrict(control)
        self.introduced()

    # Verified closure precedes rollback; no phase change itself permits a native clear.
    def begin_cleanup(self) -> None:
        self.journal.put("trial_phase", "rollback")

    # Physical profile absence is established before original-backend restoration begins.
    def await_backend(self) -> None:
        self.journal.put("trial_phase", "backend_restore_pending")

    # The original route and backend are independently verified before completion is published.
    def complete(self) -> None:
        self.journal.put("trial_phase", "complete")
