"""Recognize a closed, mapped Stop whose actual cloud acknowledgement can safely be awaited at zero."""

from __future__ import annotations

from .events import OBSERVATION_ACTIONS
from .managed_sources import awaiting_owner_start
from .store import Journal


# Routine observations wait for delivery without owning or changing any physical command.
def awaiting_observations(journal: Journal) -> bool:
    rows = journal.db.execute(
        "SELECT q.action,q.state,q.local,t.vendor FROM outbox q LEFT JOIN transactions t ON t.local=q.local"
    ).fetchall()
    return bool(rows) and all(
        action in OBSERVATION_ACTIONS and state in ("queued", "inflight") and (local is None or vendor is not None)
        for action, state, local, vendor in rows
    )


# No inferred lifecycle, unrelated observation, unknown mapping or ambiguous write qualifies for this narrow wait.
def awaiting_known_stops(journal: Journal) -> bool:
    rows = journal.db.execute("SELECT action,payload,local,state FROM outbox ORDER BY seq").fetchall()
    if (
        not rows
        or journal.active() is not None
        or awaiting_owner_start(journal)
        or journal.get("uncertain_vendor_command")
        or journal.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
    ):
        return False
    for item in rows:
        if item["action"] != "StopTransaction" or item["state"] not in ("queued", "inflight"):
            return False
        transaction = journal.db.execute(
            "SELECT stop,vendor FROM transactions WHERE local=?", (item["local"],)
        ).fetchone()
        if transaction is None or transaction["vendor"] is None or transaction["stop"] != item["payload"]:
            return False
    return True
