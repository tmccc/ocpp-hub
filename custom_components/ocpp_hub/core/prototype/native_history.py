"""Durable historical evidence is independent of the charger's rolling record retention."""

from __future__ import annotations

import math
import re
import time
from typing import Any

from .security import SecurityError
from .store import Journal, encoded, fingerprint


# Preserve JSON numeric types and every nested field; Python equality alone conflates integers and floats.
def indexed(records: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    try:
        if not isinstance(records, list):
            raise ValueError
        result: dict[int, dict[str, Any]] = {}
        for record in records:
            identifier, stop = record["transactionId"], record["stopTime"]
            if (
                not isinstance(record, dict)
                or type(identifier) is not int
                or not 0 < identifier <= 2**31 - 1
                or type(stop) is not int
                or stop < 0
                or identifier in result
            ):
                raise ValueError
            encoded(record)
            result[identifier] = record
        return result
    except ValueError, TypeError, KeyError, RecursionError:
        raise SecurityError("native history requires complete unique well-typed records") from None


class NativeHistoryArchive:
    """Original records remain immutable; completed later records retain every observed version."""

    # This private archive shares the journal's identity, durable transaction and exclusive process ownership.
    def __init__(self, journal: Journal) -> None:
        self.journal = journal
        journal.db.execute(
            "CREATE TABLE IF NOT EXISTS native_history_records("
            "transaction_id INTEGER NOT NULL,digest TEXT NOT NULL,body TEXT NOT NULL,"
            "first_archived REAL NOT NULL,last_archived REAL NOT NULL,original INTEGER NOT NULL,"
            "PRIMARY KEY(transaction_id,digest))"
        )

    # A savepoint supports both standalone observations and an atomic operator recovery without partial acceptance.
    def observe(
        self, original: list[dict[str, Any]], current: list[dict[str, Any]], *, allow_missing: bool
    ) -> dict[str, int]:
        if type(allow_missing) is not bool:
            raise SecurityError("native history retention permission must be explicit")
        originals, present = indexed(original), indexed(current)
        binding = {"identity": fingerprint(self.journal.identity), "original": fingerprint(original)}
        saved = self.journal.get("native_history_archive_binding")
        if saved is not None and saved != binding:
            raise SecurityError("native history archive admission binding changed")
        if any(
            identifier in present
            and encoded(record) != encoded(present[identifier])
            or identifier not in present
            and not allow_missing
            for identifier, record in originals.items()
        ):
            raise SecurityError("an original native history body changed or is required by strict policy")
        self.journal.db.execute("SAVEPOINT native_history_observation")
        try:
            self.journal.put("native_history_archive_binding", binding)
            now = time.time()
            records = {**originals, **{key: row for key, row in present.items() if row["stopTime"] > 0}}
            for identifier, record in records.items():
                self.journal.db.execute(
                    "INSERT INTO native_history_records VALUES(?,?,?,?,?,?) "
                    "ON CONFLICT(transaction_id,digest) DO UPDATE SET last_archived=excluded.last_archived",
                    (identifier, fingerprint(record), encoded(record), now, now, int(identifier in originals)),
                )
            identifiers = {
                row[0] for row in self.journal.db.execute("SELECT DISTINCT transaction_id FROM native_history_records")
            }
            summary = {"archived_records": len(identifiers), "missing_records": len(identifiers - present.keys())}
            self.journal.put("native_history_summary", summary)
        except BaseException:
            self.journal.db.execute("ROLLBACK TO native_history_observation")
            self.journal.db.execute("RELEASE native_history_observation")
            raise
        self.journal.db.execute("RELEASE native_history_observation")
        return summary


# Bind the retention permission to this exact admitted managed journal; ordinary commissioning remains strict.
def history_policy(journal: Journal, spec_digest: str) -> dict[str, Any]:
    managed = {"mode": "app_schedule", "spec_digest": spec_digest, "local_start": False}
    original = journal.get("native_bounded_handover", {}).get("original_history")
    if journal.get("managed_mode") != managed or not isinstance(original, list):
        raise SecurityError("missing-history tolerance requires its exact admitted managed policy")
    return {
        "version": 1,
        "mode": "archive_missing",
        "identity_digest": fingerprint(journal.identity),
        "spec_digest": spec_digest,
        "original_history_digest": fingerprint(original),
    }


# An explicit persisted operator decision changes retention handling without changing any electrical authority.
def audit_history(journal: Journal, spec_digest: str, current: list[dict[str, Any]], local_start: bool) -> None:
    original = journal.get("native_bounded_handover", {}).get("original_history", [])
    saved = journal.get("native_history_policy")
    if saved is not None:
        receipt = journal.get("native_history_policy_acknowledgement", {})
        at = receipt.get("at_epoch")
        if (
            local_start
            or saved != history_policy(journal, spec_digest)
            or receipt.get("policy") != saved
            or receipt.get("charging_started") is not False
            or type(at) not in (int, float)
            or not math.isfinite(at)
            or at <= 0
            or not isinstance(receipt.get("native_digest"), str)
            or not re.fullmatch(r"[a-f0-9]{64}", receipt["native_digest"])
        ):
            raise SecurityError("native history retention policy changed or local Start is enabled")
        NativeHistoryArchive(journal).observe(original, current, allow_missing=True)
        return
    present = {row["transactionId"]: row for row in current}
    if any(present.get(row["transactionId"]) != row for row in original):
        raise SecurityError("native bounded handover must preserve every original history record")
