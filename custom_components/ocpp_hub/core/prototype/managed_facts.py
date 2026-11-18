"""On-demand facts shared by daily controls. Each operation decides which facts it needs and in what order."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .managed import ManagedController


class ManagedFacts:
    # Hold references only: every check reads the same live state as its caller did.
    def __init__(self, controller: ManagedController) -> None:
        self.session = controller.session
        self.native = controller.native

    # Boot must belong to the charger connection currently owned by the gateway.
    def boot_current(self) -> bool:
        gateway = self.session.gateway
        return gateway.current_boot_seen(gateway._charger_epoch)

    # Introduction binds both the connection and the confirmed control revision.
    def introduction_current(self) -> bool:
        gateway = self.session.gateway
        return (
            self.native.introduced_epoch == gateway._charger_epoch
            and self.native.introduced_revision == gateway.controls._revision
        )

    # Normal Start admits non-lifecycle cloud notifications; other gates require the full queue to be empty.
    def cloud_queue_empty(self, *, lifecycle_only: bool = False) -> bool:
        query = (
            "SELECT 1 FROM outbox WHERE action IN ('StartTransaction','StopTransaction') LIMIT 1"
            if lifecycle_only
            else "SELECT 1 FROM outbox LIMIT 1"
        )
        return not self.session.journal.db.execute(query).fetchone()

    # Unknown effects remain blockers even with confirmed physical zero.
    def mutations_confirmed(self) -> bool:
        return not self.session.journal.db.execute(
            "SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1"
        ).fetchone()
