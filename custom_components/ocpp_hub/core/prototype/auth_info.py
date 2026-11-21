"""Validated cloud metadata informs existing permission; it never creates a general offline allowlist."""

from __future__ import annotations

import json
import re
import sqlite3
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


@dataclass(frozen=True)
class AuthorizationRecord:
    """The actual decision and ordering information retained for one tag."""

    info: str
    observed: float
    sequence: int | None


class InfoBook:
    """Bounded digest-keyed metadata, separately gated by approved local tags or a single-start grant."""

    # The trial's missing-expiry policy requires a fresh online decision; it does not invent a cloud expiry.
    def __init__(self, journal: Journal, capacity: int = 128, protected_tags: frozenset[str] = frozenset()) -> None:
        self.journal, self.capacity = journal, capacity
        self.protected = frozenset(tag_key(tag) for tag in protected_tags)
        self._pending_denials: dict[str, AuthorizationRecord] = {}
        journal.db.execute(
            "CREATE TABLE IF NOT EXISTS auth_info(digest TEXT PRIMARY KEY,info TEXT NOT NULL,observed REAL NOT NULL)"
        )
        journal.db.execute(
            "CREATE TABLE IF NOT EXISTS auth_info_order("
            "digest TEXT PRIMARY KEY REFERENCES auth_info(digest) ON DELETE CASCADE,sequence INTEGER NOT NULL)"
        )
        journal.db.execute(
            "CREATE TABLE IF NOT EXISTS auth_denial_replies("
            "digest TEXT PRIMARY KEY,info TEXT NOT NULL,observed REAL NOT NULL,sequence INTEGER NOT NULL)"
        )
        self._sequence = journal.db.execute(
            "SELECT COALESCE(MAX(sequence), 0) FROM ("
            "SELECT MAX(sequence) AS sequence FROM auth_info_order UNION ALL "
            "SELECT MAX(sequence) FROM delivery_reply_authorization UNION ALL "
            "SELECT MAX(sequence) FROM auth_denial_replies)"
        ).fetchone()[0]

    # Order actual replies inside the wire gate while keeping expiry checks on wall time.
    def observe(self) -> AuthorizationObservation:
        self._sequence += 1
        return AuthorizationObservation(self._sequence, time.time())

    # Publish a checked authorization reply by when it arrived, returning the latest decision for that tag.
    def put(self, tag: str, value: Any, observation: AuthorizationObservation | None = None) -> dict[str, Any]:
        info = validate_info(value)
        observation = observation or self.observe()
        with nullcontext() if self.journal.db.in_transaction else self.journal.atomic():
            self.put_delivery(tag, info, observation, restored=False)
            latest = self.stored(tag)
        assert latest is not None
        return latest

    # A matched denial remains effective even if the older cache row cannot be replaced.
    def deny(self, tag: str, info: dict[str, Any]) -> dict[str, Any]:
        decision = validate_info(info)
        if decision["status"] == "Accepted":
            raise DeniedError("a positive reply requires the current owner check")
        observation = self.observe()
        key = tag_key(tag)
        self._pending_denials[key] = AuthorizationRecord(encoded(decision), observation.observed, observation.sequence)
        self.retry_denials()
        return decision

    # Checkpoint only an actual latest denial. If storage is unavailable, reads still see the retained reply.
    def retry_denials(self) -> None:
        j = self.journal
        for key, reply in tuple(self._pending_denials.items()):
            assert reply.sequence is not None
            try:
                with j.atomic():
                    prior = self._saved(key)
                    if prior is None or prior.sequence is None or prior.sequence < reply.sequence:
                        self._make_room(key)
                        j.db.execute(
                            "INSERT OR REPLACE INTO auth_denial_replies VALUES(?,?,?,?)",
                            (key, reply.info, reply.observed, reply.sequence),
                        )
            except sqlite3.Error, RecoveryRequiredError:
                if j.db.in_transaction:
                    raise
                continue
            self._pending_denials.pop(key, None)
        self._publish_saved_denials()

    # Move a durable refusal into the ordinary cache once that cache is writable again.
    def _publish_saved_denials(self) -> None:
        j = self.journal
        try:
            rows = j.db.execute("SELECT digest,info,observed,sequence FROM auth_denial_replies").fetchall()
        except sqlite3.Error:
            if j.db.in_transaction:
                raise
            return
        for row in rows:
            key, sequence = row["digest"], row["sequence"]
            try:
                with j.atomic():
                    ordinary = j.db.execute("SELECT sequence FROM auth_info_order WHERE digest=?", (key,)).fetchone()
                    if ordinary is None or ordinary["sequence"] < sequence:
                        self._put(
                            key,
                            validate_info(json.loads(row["info"])),
                            AuthorizationObservation(sequence, row["observed"]),
                        )
                    j.db.execute("DELETE FROM auth_denial_replies WHERE digest=? AND sequence=?", (key, sequence))
            except sqlite3.Error:
                if j.db.in_transaction:
                    raise

    # Keep the newest actual decision, whether it came from Authorize or a saved lifecycle acknowledgement.
    def put_delivery(
        self, tag: str, info: dict[str, Any], observation: AuthorizationObservation | None, *, restored: bool
    ) -> None:
        key = tag_key(tag)
        latest = self._latest(key)
        if latest is not None:
            sequence = latest.sequence
            # Old journal rows cannot prove order after replacement, even after a rollback and re-upgrade.
            if observation is None or (sequence is None and restored):
                return
            if sequence is not None and sequence >= observation.sequence:
                return
        self._put(key, info, observation)

    # Keep the same bound across both ordinary decisions and independent refusal receipts.
    def _make_room(self, key: str) -> None:
        j = self.journal
        keys = [
            row[0]
            for row in j.db.execute(
                "SELECT digest FROM (SELECT digest,observed FROM auth_info UNION ALL "
                "SELECT digest,observed FROM auth_denial_replies) GROUP BY digest ORDER BY max(observed)"
            )
        ]
        if key in keys or len(keys) < self.capacity:
            return
        current = j.get("vendor_start_grant", {}).get("digest")
        oldest = next(
            (
                item
                for item in keys
                if item not in self.protected and item != current and item not in self._pending_denials
            ),
            None,
        )
        if oldest is None:
            raise RecoveryRequiredError("authorization metadata capacity reached without an evictable historical tag")
        j.db.execute("DELETE FROM auth_info WHERE digest=?", (oldest,))
        j.db.execute("DELETE FROM auth_denial_replies WHERE digest=?", (oldest,))

    # Persist the metadata and its order in the same transaction.
    def _put(self, key: str, info: dict[str, Any], observation: AuthorizationObservation | None) -> None:
        self._make_room(key)
        observed = observation.observed if observation is not None else time.time()
        self.journal.db.execute("INSERT OR REPLACE INTO auth_info VALUES(?,?,?)", (key, encoded(info), observed))
        if observation is not None:
            self.journal.db.execute("INSERT INTO auth_info_order VALUES(?,?)", (key, observation.sequence))
            self.journal.db.execute(
                "DELETE FROM auth_denial_replies WHERE digest=? AND sequence<?", (key, observation.sequence)
            )

    # Both kinds of durable reply use the same wire order and preserve legacy entries with no sequence.
    def _saved(self, key: str) -> AuthorizationRecord | None:
        row = self.journal.db.execute(
            "SELECT info,observed,sequence FROM auth_info LEFT JOIN auth_info_order USING(digest) WHERE digest=? "
            "UNION ALL SELECT info,observed,sequence FROM auth_denial_replies WHERE digest=? "
            "ORDER BY sequence DESC LIMIT 1",
            (key, key),
        ).fetchone()
        return AuthorizationRecord(row["info"], row["observed"], row["sequence"]) if row else None

    # A known newer refusal supersedes the earlier cache as soon as the wire reply arrives.
    def _latest(self, key: str) -> AuthorizationRecord | None:
        saved, pending = self._saved(key), self._pending_denials.get(key)
        if pending is not None:
            assert pending.sequence is not None
            if saved is None or saved.sequence is None or pending.sequence > saved.sequence:
                return pending
        return saved

    # Only metadata from an actual correlated response can be returned; an expired decision stays expired.
    def stored(self, tag: str) -> dict[str, Any] | None:
        latest = self._latest(tag_key(tag))
        return validate_info(json.loads(latest.info)) if latest is not None else None

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
        record = self._latest(tag_key(tag))
        if record is None:
            return None
        info = validate_info(json.loads(record.info))
        if info["status"] != "Accepted" or "expiryDate" not in info:
            return None
        end = expiry(info["expiryDate"])
        clock = re.search(r"T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:[.,]([0-9]+))?(?:Z|[+-][0-9]{2}:[0-9]{2})$", reported)
        precision = 10.0 ** -min(6, len(clock[1] or "")) if clock else 0.0
        if (record.observed <= occurred or record.observed - occurred < precision) and occurred < end <= time.time():
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
