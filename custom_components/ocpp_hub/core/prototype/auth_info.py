"""Validated cloud metadata informs existing permission; it never creates a general offline allowlist."""

from __future__ import annotations

import re
import time
from contextlib import nullcontext
from dataclasses import dataclass
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
        or re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})", value) is None
    ):
        raise DeniedError("cloud authorization expiry is malformed")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        raise DeniedError("cloud authorization expiry is invalid") from None


@dataclass(frozen=True)
class AuthorizationObservation:
    """The wire sequence orders decisions, and the timestamp supports expiry checks."""

    sequence: int
    observed: float


class InfoBook:
    """Bounded digest-keyed metadata, separately gated by approved local tags or a single-start grant."""

    # The trial's missing-expiry policy requires a fresh online decision; it does not invent a cloud expiry.
    def __init__(self, journal: Journal, capacity: int = 128, protected_tags: frozenset[str] = frozenset()) -> None:
        self.journal, self.capacity = journal, capacity
        self.protected = frozenset(tag_key(tag) for tag in protected_tags)
        journal.db.execute(
            "CREATE TABLE IF NOT EXISTS auth_info(digest TEXT PRIMARY KEY,info TEXT NOT NULL,observed REAL NOT NULL)"
        )
        journal.db.execute(
            "CREATE TABLE IF NOT EXISTS auth_info_order("
            "digest TEXT PRIMARY KEY REFERENCES auth_info(digest) ON DELETE CASCADE,sequence INTEGER NOT NULL)"
        )
        self._sequence = journal.db.execute(
            "SELECT COALESCE(MAX(sequence), 0) FROM ("
            "SELECT MAX(sequence) AS sequence FROM auth_info_order UNION ALL "
            "SELECT MAX(sequence) FROM delivery_reply_authorization)"
        ).fetchone()[0]

    # Order actual replies inside the wire gate while keeping expiry checks on wall time.
    def observe(self) -> AuthorizationObservation:
        self._sequence += 1
        return AuthorizationObservation(self._sequence, time.time())

    # Record actual Accepted and denied decisions alike so a denial cannot be hidden by an older positive entry.
    def put(self, tag: str, value: Any, observation: AuthorizationObservation | None = None) -> dict[str, Any]:
        info = validate_info(value)
        key = tag_key(tag)
        observation = observation or self.observe()
        with nullcontext() if self.journal.db.in_transaction else self.journal.atomic():
            self._put(key, info, observation)
        return info

    # A saved acknowledgement can retire its event even when newer authorization already exists.
    def put_delivery(
        self, tag: str, info: dict[str, Any], observation: AuthorizationObservation | None, *, restored: bool
    ) -> None:
        key = tag_key(tag)
        cached = self.journal.db.execute(
            "SELECT sequence FROM auth_info LEFT JOIN auth_info_order USING(digest) WHERE digest=?", (key,)
        ).fetchone()
        if cached is not None:
            sequence = cached[0]
            # Old journal rows cannot prove order after replacement, even after a rollback and re-upgrade.
            if observation is None or (sequence is None and restored):
                return
            if sequence is not None and sequence >= observation.sequence:
                return
        self._put(key, info, observation)

    # Persist the metadata and its order in the same transaction, limiting retained historical tags.
    def _put(self, key: str, info: dict[str, Any], observation: AuthorizationObservation | None) -> None:
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
        observed = observation.observed if observation is not None else time.time()
        self.journal.db.execute("INSERT OR REPLACE INTO auth_info VALUES(?,?,?)", (key, encoded(info), observed))
        if observation is not None:
            self.journal.db.execute("INSERT INTO auth_info_order VALUES(?,?)", (key, observation.sequence))

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

    # Return the original decision and expiry for an already acknowledged event that occurred while both were valid.
    def accepted_for_delayed_report(self, tag: str, occurred: float, reported: str) -> dict[str, Any] | None:
        info = self.stored(tag)
        if info is None or info["status"] != "Accepted" or "expiryDate" not in info:
            return None
        end = expiry(info["expiryDate"])
        observed = self.journal.db.execute("SELECT observed FROM auth_info WHERE digest=?", (tag_key(tag),)).fetchone()
        clock = re.search(r"T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:[.,]([0-9]+))?(?:Z|[+-][0-9]{2}:[0-9]{2})$", reported)
        precision = 10.0 ** -min(6, len(clock[1] or "")) if clock else 0.0
        if (
            observed is not None
            and (observed[0] <= occurred or observed[0] - occurred < precision)
            and occurred < end <= time.time()
        ):
            return info
        return None

    # The narrow dynamic-tag trial requires real future cloud expiry before arming a new remote start.
    def require_dynamic(self, info: dict[str, Any]) -> None:
        if info.get("status") != "Accepted" or "expiryDate" not in info or expiry(info["expiryDate"]) <= time.time():
            raise RecoveryRequiredError("dynamic trial start needs actual Accepted metadata with future expiry")

    # Local owner authorization has its own explicit bounded lifetime and is never described as cloud metadata.
    def local(self, seconds: int = 60) -> dict[str, Any]:
        instant = datetime.fromtimestamp(time.time() + seconds, UTC).isoformat().replace("+00:00", "Z")
        return {"status": "Accepted", "expiryDate": instant}
