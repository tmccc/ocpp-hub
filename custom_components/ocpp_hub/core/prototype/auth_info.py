"""Validated cloud metadata informs existing permission; it never creates a general offline allowlist."""

from __future__ import annotations

import re
import time
from datetime import UTC, datetime
from typing import Any

from .policy import DeniedError
from .store import Journal, RecoveryRequiredError, encoded, fingerprint

STATUSES = frozenset({"Accepted", "Blocked", "Expired", "Invalid", "ConcurrentTx"})


# Case-insensitive lookup never changes the tag actually sent on the OCPP wire or in a historical fact.
def tag_key(tag: str) -> str:
    return fingerprint(tag.casefold())


# Preserve only actual protocol fields; parent metadata cannot itself authorize another child tag.
def validate_info(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or not {"status"} <= set(value) <= {"status", "expiryDate", "parentIdTag"}:
        raise DeniedError("cloud authorization metadata has unknown or missing fields")
    if not isinstance(value["status"], str) or value["status"] not in STATUSES:
        raise DeniedError("cloud authorization status is invalid")
    if "parentIdTag" in value and (
        not isinstance(value["parentIdTag"], str)
        or not 1 <= len(value["parentIdTag"]) <= 20
        or any(ord(c) < 32 or ord(c) == 127 for c in value["parentIdTag"])
    ):
        raise DeniedError("cloud parent tag is invalid")
    if "expiryDate" in value:
        expiry(value["expiryDate"])
    return dict(value)


# Require an actual timezone-aware RFC3339 instant, avoiding permissive compact/week-date parsing.
def expiry(value: Any) -> float:
    if (
        not isinstance(value, str)
        or re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})", value) is None
    ):
        raise DeniedError("cloud authorization expiry is malformed")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        raise DeniedError("cloud authorization expiry is invalid") from None


class InfoBook:
    """Bounded digest-keyed metadata, separately gated by approved local tags or a single-start grant."""

    # The trial's missing-expiry policy requires a fresh online decision; it does not invent a cloud expiry.
    def __init__(self, journal: Journal, capacity: int = 128, protected_tags: frozenset[str] = frozenset()) -> None:
        self.journal, self.capacity = journal, capacity
        self.protected = frozenset(tag_key(tag) for tag in protected_tags)
        journal.db.execute(
            "CREATE TABLE IF NOT EXISTS auth_info(digest TEXT PRIMARY KEY,info TEXT NOT NULL,observed REAL NOT NULL)"
        )

    # Record actual Accepted and denied decisions alike so a denial cannot be hidden by an older positive entry.
    def put(self, tag: str, value: Any) -> dict[str, Any]:
        info = validate_info(value)
        key = tag_key(tag)
        if self.journal.db.execute("SELECT 1 FROM auth_info WHERE digest=?", (key,)).fetchone() is None:
            if self.journal.db.execute("SELECT COUNT(*) FROM auth_info").fetchone()[0] >= self.capacity:
                current = self.journal.get("vendor_start_grant", {}).get("digest")
                candidates = self.journal.db.execute("SELECT digest FROM auth_info ORDER BY observed").fetchall()
                oldest = next(
                    (row[0] for row in candidates if row[0] not in self.protected and row[0] != current), None
                )
                if oldest is None:
                    raise RecoveryRequiredError(
                        "authorization metadata capacity reached without an evictable historical tag"
                    )
                self.journal.db.execute("DELETE FROM auth_info WHERE digest=?", (oldest,))
        self.journal.db.execute("INSERT OR REPLACE INTO auth_info VALUES(?,?,?)", (key, encoded(info), time.time()))
        return info

    # Only metadata from an actual correlated response can be returned; an expired decision stays expired.
    def stored(self, tag: str) -> dict[str, Any] | None:
        import json

        row = self.journal.db.execute("SELECT info FROM auth_info WHERE digest=?", (tag_key(tag),)).fetchone()
        if row is None:
            return None
        return validate_info(json.loads(row[0]))

    # Apply the actual expiry at the moment of use, without losing the original cloud decision.
    def get(self, tag: str) -> dict[str, Any] | None:
        info = self.stored(tag)
        if info is None:
            return None
        if info["status"] == "Accepted" and "expiryDate" in info and expiry(info["expiryDate"]) <= time.time():
            return {**info, "status": "Expired"}
        return info

    # The narrow dynamic-tag trial requires real future cloud expiry before arming a new remote start.
    def require_dynamic(self, info: dict[str, Any]) -> None:
        if info.get("status") != "Accepted" or "expiryDate" not in info or expiry(info["expiryDate"]) <= time.time():
            raise RecoveryRequiredError("dynamic trial start needs actual Accepted metadata with future expiry")

    # Local owner authorization has its own explicit bounded lifetime and is never described as cloud metadata.
    def local(self, seconds: int = 60) -> dict[str, Any]:
        instant = datetime.fromtimestamp(time.time() + seconds, UTC).isoformat().replace("+00:00", "Z")
        return {"status": "Accepted", "expiryDate": instant}
