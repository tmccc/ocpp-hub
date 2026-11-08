"""Persistent plan namespaces and confirmed ownership, without storing proprietary bodies."""

from __future__ import annotations

from .policy import integer
from .store import ConflictError, Journal, RecoveryRequiredError


class PlanBook:
    """A stable mapping survives disable/re-enable, refusal, restart and uncertain delivery."""

    # The journal's exclusive process lock also protects this small resource table.
    def __init__(self, journal: Journal) -> None:
        self.journal = journal
        journal.db.execute(
            "CREATE TABLE IF NOT EXISTS plans("
            "local INTEGER PRIMARY KEY,vendor INTEGER UNIQUE,owner TEXT NOT NULL,state TEXT NOT NULL)"
        )

    # Tombstones consume bounded space too; exhaustion requires explicit review, never silent id recycling.
    def _require_capacity(self) -> None:
        if self.journal.db.execute("SELECT COUNT(*) FROM plans").fetchone()[0] >= self.journal.capacity:
            raise RecoveryRequiredError("plan namespace capacity reached")

    # Reserve an id before sending, without claiming that the charger installed anything.
    def reserve(self, vendor: int) -> int:
        integer(vendor, 1)
        row = self.journal.db.execute("SELECT local FROM plans WHERE vendor=? AND owner='vendor'", (vendor,)).fetchone()
        if row is not None:
            return int(row[0])
        self._require_capacity()
        local = int(self.journal.get("next_plan", 2000000)) + 1
        if local >= 2**31:
            raise RecoveryRequiredError("plan counter exhausted")
        self.journal.put("next_plan", local)
        self.journal.db.execute("INSERT INTO plans VALUES(?,?,'vendor','reserved')", (local, vendor))
        return local

    # An independently verified migration can adopt an existing plan; unknown clears never do this implicitly.
    def adopt(self, vendor: int, local: int) -> None:
        integer(vendor, 1)
        integer(local, 1)
        with self.journal.atomic():
            self._require_capacity()
            if self.journal.db.execute("SELECT 1 FROM plans WHERE local=? OR vendor=?", (local, vendor)).fetchone():
                raise ConflictError("plan adoption conflicts with an existing resource")
            self.journal.db.execute("INSERT INTO plans VALUES(?,?,'vendor','active')", (local, vendor))
            self.journal.put("next_plan", max(local, int(self.journal.get("next_plan", 2000000))))

    # Reserve a local resource so a vendor cannot clear it by naming its local id as a cloud id.
    def protect_local(self, local: int) -> None:
        integer(local, 1)
        with self.journal.atomic():
            self._require_capacity()
            self.journal.db.execute("INSERT INTO plans VALUES(?,NULL,'local','active')", (local,))
            self.journal.put("next_plan", max(local, int(self.journal.get("next_plan", 2000000))))

    # Resolve the entire clear atomically, including known tombstones for harmless repeated clears.
    def clear_targets(self, vendors: list[int]) -> list[int]:
        local = []
        for vendor in vendors:
            row = self.journal.db.execute(
                "SELECT local,state FROM plans WHERE vendor=? AND owner='vendor'", (vendor,)
            ).fetchone()
            if row is None or row[1] not in ("active", "cleared"):
                raise ConflictError("clear names a plan without confirmed vendor ownership")
            local.append(int(row[0]))
        return local

    # Publish installation or clearing only after an actual validated Accepted reply.
    def accepted(self, vendors: tuple[int, ...], clearing: bool) -> None:
        for vendor in vendors:
            self.journal.db.execute(
                "UPDATE plans SET state=? WHERE vendor=? AND owner='vendor'",
                ("cleared" if clearing else "active", vendor),
            )
