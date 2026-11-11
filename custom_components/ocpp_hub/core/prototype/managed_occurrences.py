"""Keep each receipted temporary occurrence intact while independent reports arrive out of order."""

from __future__ import annotations

from typing import Any

from .store import Journal, RecoveryRequiredError

KEY = "managed_temporary_schedule"
ARCHIVE = "managed_temporary_occurrences"
HEAD = "managed_temporary_schedule_head"
IDENTITY = (
    "id",
    "revision",
    "accepted_epoch",
    "start_epoch",
    "end_epoch",
    "floor",
    "body",
    "install",
    "original_digest",
    "augmented_digest",
)


# Overlay the durable working occurrence; its latest lifecycle may be newer than its archived snapshot.
def occurrences(j: Journal) -> dict[str, dict[str, Any]]:
    saved: dict[str, dict[str, Any]] = j.get(ARCHIVE, {})
    current = j.get(KEY, {})
    if current:
        saved[current["id"]] = current
    return saved


# Save the complete lease without allowing any later window or payload to replace its original identity.
def remember(j: Journal) -> None:
    current = j.get(KEY, {})
    if not current:
        return
    saved = j.get(ARCHIVE, {})
    previous = saved.get(current["id"], {})
    if any(previous[k] != current[k] for k in IDENTITY if k in previous and k in current):
        raise RecoveryRequiredError("temporary occurrence identity changed")
    saved[current["id"]] = current
    j.put(ARCHIVE, saved)
    if j.get(HEAD) is None:
        j.put(HEAD, current["id"])


# Move the working slot only as a whole lease; the newer receipt remains the independently archived head.
def activate(j: Journal, identifier: str) -> None:
    current = j.get(KEY, {})
    if current.get("id") == identifier:
        return
    saved = occurrences(j)
    if current.get("state") == "requested":
        j.put(KEY, {**current, "interleaved_with": identifier})
    remember(j)
    j.put(KEY, saved[identifier])


# An exact immutable plan ID selects an older occurrence even after a newer timer has been receipted.
def select_source(j: Journal, plan_id: int | None) -> None:
    matches = [lease for lease in occurrences(j).values() if lease["body"]["planId"] == plan_id]
    if len(matches) == 1 and matches[0].get("result") not in ("cancelled_before_install", "session_closed"):
        activate(j, matches[0]["id"])


# A completed older occurrence gives supervision back to the latest receipt without completing that receipt.
def restore_head(j: Journal) -> None:
    remember(j)
    head = j.get(HEAD)
    if head is not None:
        activate(j, head)
        current = j.get(KEY, {})
        if (
            current.get("inherited_hold")
            and j.active() is None
            and not current.get("bound_local")
            and not current.get("temporary_start_reported")
            and not current.get("retire_reason")
        ):
            j.put(
                KEY,
                {
                    k: v
                    for k, v in current.items()
                    if k
                    not in (
                        "inherited_hold",
                        "hold_since",
                        "next_cap",
                        "next_stop",
                        "unidentified_local",
                        "provisional_boot",
                    )
                },
            )


# Every known plan identifier remains reserved forever, including when its native timer has been removed.
def reserved_plan(j: Journal, plan_id: int) -> bool:
    return any(lease["body"]["planId"] == plan_id for lease in occurrences(j).values())


# Recognize only independently receipted native timetable variants, never a changed window under an old plan ID.
def installed_digest(j: Journal, original: str, actual: str) -> str:
    return next(
        (
            actual
            for lease in occurrences(j).values()
            if lease.get("state") in ("installing", "armed", "clearing")
            and lease.get("original_digest") == original
            and lease.get("augmented_digest") == actual
        ),
        original,
    )


# Match the original absolute occurrence rather than inferring a new date from a shared daily tag.
def report_matches(lease: dict[str, Any], tag: str, occurred: float) -> bool:
    return bool(
        tag.casefold() == lease["body"]["idTag"].casefold()
        and lease.get("accepted_epoch", 0) <= occurred
        and lease["start_epoch"] <= occurred < lease.get("end_epoch", lease["start_epoch"])
    )
