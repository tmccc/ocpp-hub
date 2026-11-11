"""Keep the station's remaining cutoff duties independent of a later unsent request's result."""

from __future__ import annotations

from contextlib import nullcontext
from typing import Any

from .managed_occurrences import ARCHIVE, KEY, activate, occurrences
from .store import Journal

OBSERVABLE = {"installing", "armed", "clearing"}


# A completed inventory cleanup does not end the independent idle cutoff obligation.
def supervised_timer(lease: dict[str, Any]) -> bool:
    return (
        lease.get("state") in OBSERVABLE
        or lease.get("recovery_session") is True
        or lease.get("idle_cutoff_hold") is True
        or lease.get("closure_gap") is True
        or idle_cutoff_pending(lease)
    )


# An installed timer without confirmed closure must still reach its deadline after early cleanup.
# The legacy verified_session label records conditional continuation, rather than permanent cutoff discharge.
def idle_cutoff_pending(lease: dict[str, Any]) -> bool:
    return (
        lease.get("state") in {*OBSERVABLE, "complete"}
        and lease.get("result") not in ("cancelled_before_install", "session_closed")
        and lease.get("retire_reason") != "session_closed"
        and lease.get("idle_cutoff_released") not in ("owner_return", "session_closed", "observed_closure")
        and not lease.get("native_plan_confirmed")
    )


# An unsent cancellation cannot become the station's only working receipt while an earlier duty remains.
def resume_cutoff(journal: Journal, *, reporting: bool = False) -> dict[str, Any]:
    with nullcontext() if journal.db.in_transaction else journal.atomic():
        current = journal.get(KEY, {})
        if (
            current.get("state") == "complete"
            and (
                current.get("result") in ("cancelled_before_install", "session_closed") or not supervised_timer(current)
            )
            or reporting
            and current.get("state") == "requested"
        ):
            pending = [
                lease
                for lease in occurrences(journal).values()
                if lease["id"] != current.get("id")
                and lease.get("result") not in ("cancelled_before_install", "session_closed")
                and supervised_timer(lease)
            ]
            if pending:
                earliest = min(pending, key=lambda lease: (lease.get("end_epoch", lease["start_epoch"]), lease["id"]))
                activate(journal, earliest["id"])
        result: dict[str, Any] = journal.get(KEY, {})
        return result


# A fresh source is conditional; permanent discharge needs Return, a newer timer's closure or elapsed idle closure.
def release_idle_cutoffs(journal: Journal, reason: str, now: float) -> None:
    with nullcontext() if journal.db.in_transaction else journal.atomic():
        current = journal.get(KEY, {})
        identifier = current.get("id")
        saved = occurrences(journal)
        changed = False
        for key, lease in saved.items():
            # Receipt insertion order survives clock corrections; an older closure cannot release a newer window.
            older = (
                reason == "session_closed"
                and journal.db.execute(
                    "SELECT 1 FROM managed_schedule_requests AS older, managed_schedule_requests AS closed "
                    "WHERE older.id=? AND closed.id=? AND older.rowid < closed.rowid",
                    (key, identifier),
                ).fetchone()
                is not None
            )
            prior_closed = lease.get("state") == "complete" and (
                reason == "owner_return"
                or older
                or reason == "observed_closure"
                and lease.get("end_epoch", float("inf")) <= now
            )
            if (key == identifier or prior_closed) and (lease.get("idle_cutoff_hold") or idle_cutoff_pending(lease)):
                saved[key] = {**lease, "idle_cutoff_hold": False, "idle_cutoff_released": reason}
                changed = True
        if changed:
            journal.put(ARCHIVE, saved)
            if identifier in saved:
                journal.put(KEY, saved[identifier])
