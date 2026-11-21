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


# Exact closed Stops and routine known observations can wait together for real cloud acknowledgements.
def awaiting_known_stops(journal: Journal) -> bool:
    rows = journal.db.execute(
        "SELECT q.action,q.payload,q.local,q.state,t.stop,t.vendor FROM outbox q "
        "LEFT JOIN transactions t ON t.local=q.local ORDER BY q.seq"
    ).fetchall()
    if (
        not rows
        or journal.active() is not None
        or awaiting_owner_start(journal)
        or journal.outcomes.vendor_evidence().unfinished
        or journal.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
    ):
        return False
    stop_pending = False
    for action, payload, local, state, stop, vendor in rows:
        if state not in ("queued", "inflight"):
            return False
        if action in OBSERVATION_ACTIONS and (local is None or vendor is not None):
            continue
        if action != "StopTransaction" or vendor is None or stop != payload:
            return False
        stop_pending = True
    return stop_pending
