"""Durable historical evidence is independent of the charger's rolling record retention."""

from __future__ import annotations

import json
import math
import re
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from .native_faults import NativeContentError
from .native_start_attempts import split_attempts
from .store import Journal, encoded, fingerprint

ORIGINAL_HISTORY_REFUSAL = "native bounded handover must preserve every original history record"
ORIGINAL_HISTORY_OMISSION = "original_history_omission"
STARTUP_HISTORY_OMISSION = "startup_history_omission"


class MissingOriginalHistoryError(NativeContentError):
    """An admitted history record is absent and the retained originals still agree."""


# Audits run both standalone and inside the operator's recovery transaction.
@contextmanager
def history_savepoint(journal: Journal) -> Iterator[None]:
    if not journal.db.in_transaction:
        with journal.atomic(retry_busy=True):
            yield
        return
    journal.db.execute("SAVEPOINT native_history_observation")
    try:
        yield
        journal.db.execute("RELEASE native_history_observation")
    except BaseException as error:
        # Keep the enclosing recovery transaction; SQLite may already have rolled back the whole transaction.
        if journal.db.in_transaction:
            try:
                journal.db.execute("ROLLBACK TO native_history_observation")
                journal.db.execute("RELEASE native_history_observation")
            except sqlite3.Error as cleanup_error:
                # Keep the original failure primary and expose failed cleanup to the enclosing transaction owner.
                raise error from cleanup_error
        raise


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
        raise NativeContentError("history", "native history requires complete unique well-typed records") from None


# Validate original/archive preservation completely before any archive constructor or publication writes.
def _archive_view(
    journal: Journal, original: list[dict[str, Any]], current: list[dict[str, Any]], allow_missing: bool
) -> tuple[
    dict[int, dict[str, Any]],
    dict[int, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, Any],
]:
    if type(allow_missing) is not bool:
        raise NativeContentError("history", "native history retention permission must be explicit")
    original_rows, admitted_attempts = split_attempts(original)
    current_rows, attempts = split_attempts(current)
    originals, present = indexed(original_rows), indexed(current_rows)
    if any(
        isinstance(row.get("filename"), str) and row["filename"] in admitted_attempts for row in current_rows
    ) or any(isinstance(row.get("filename"), str) and row["filename"] in attempts for row in original_rows):
        raise NativeContentError("history", "an original filename changed between assigned and unassigned history")
    for name, body in admitted_attempts.items():
        if name in attempts and encoded(attempts[name]) != encoded(body) or name not in attempts and not allow_missing:
            raise NativeContentError(
                "history", "an original unassigned start record changed or is required by strict policy"
            )
    binding = {"identity": fingerprint(journal.identity), "original": fingerprint(original)}
    saved = journal.get("native_history_archive_binding")
    if saved is not None and saved != binding:
        raise NativeContentError("history", "native history archive admission binding changed")
    if any(
        identifier in present
        and encoded(record) != encoded(present[identifier])
        or identifier not in present
        and not allow_missing
        for identifier, record in originals.items()
    ):
        raise NativeContentError("history", "an original native history body changed or is required by strict policy")
    return originals, present, admitted_attempts, attempts, binding


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

        journal.db.execute(
            "CREATE TABLE IF NOT EXISTS native_start_attempts(digest TEXT PRIMARY KEY,body TEXT NOT NULL)"
        )

    # Keep previously persisted attempt bodies on repeat observations without touching their rows.
    def _new_attempts(self, attempts: dict[str, dict[str, Any]]) -> list[tuple[str, str]]:
        result = []
        for body in attempts.values():
            digest = fingerprint(body)
            archived = self.journal.db.execute(
                "SELECT 1 FROM native_start_attempts WHERE digest=?", (digest,)
            ).fetchone()
            if archived is None:
                result.append((digest, encoded(body)))
        return result

    # Save only fresh attempts inside the caller's history transaction.
    def retain_attempts(self, attempts: dict[str, dict[str, Any]]) -> None:
        self.journal.db.executemany(
            "INSERT OR IGNORE INTO native_start_attempts VALUES(?,?)", self._new_attempts(attempts)
        )

    # A savepoint supports both standalone observations and an atomic operator recovery without partial acceptance.
    def observe(
        self, original: list[dict[str, Any]], current: list[dict[str, Any]], *, allow_missing: bool
    ) -> dict[str, int]:
        originals, present, admitted_attempts, attempts, binding = _archive_view(
            self.journal, original, current, allow_missing
        )
        new_attempts = self._new_attempts(admitted_attempts | attempts)
        records = {**originals, **{key: row for key, row in present.items() if row["stopTime"] > 0}}
        new_records = []
        for identifier, record in records.items():
            digest = fingerprint(record)
            exists = self.journal.db.execute(
                "SELECT 1 FROM native_history_records WHERE transaction_id=? AND digest=?", (identifier, digest)
            ).fetchone()
            if exists is None:
                new_records.append((identifier, digest, encoded(record), int(identifier in originals)))
        identifiers = {
            row[0] for row in self.journal.db.execute("SELECT DISTINCT transaction_id FROM native_history_records")
        }
        identifiers.update(records)
        summary = {"archived_records": len(identifiers), "missing_records": len(identifiers - present.keys())}
        if missing_attempts := admitted_attempts.keys() - attempts.keys():
            summary["missing_attempts"] = len(missing_attempts)
        bound = self.journal.get("native_history_archive_binding") == binding
        summary_changed = self.journal.get("native_history_summary") != summary
        if bound and not summary_changed and not new_attempts and not new_records:
            return summary
        with history_savepoint(self.journal):
            if not bound:
                self.journal.put("native_history_archive_binding", binding)
            self.journal.db.executemany("INSERT OR IGNORE INTO native_start_attempts VALUES(?,?)", new_attempts)
            now = time.time()
            self.journal.db.executemany(
                "INSERT INTO native_history_records VALUES(?,?,?,?,?,?)",
                ((identifier, digest, body, now, now, original) for identifier, digest, body, original in new_records),
            )
            if summary_changed:
                self.journal.put("native_history_summary", summary)
        return summary


# The latest completed charge is unknown if an absent archive entry could be as recent as the displayed one.
def latest_completion_available(journal: Journal, completed: dict[int, int], closed: int | None = None) -> bool:
    latest = max(completed.values(), default=0)
    try:
        for identifier, body in journal.db.execute("SELECT transaction_id,body FROM native_history_records"):
            if identifier in completed or (closed is not None and identifier < closed):
                continue
            record = json.loads(body)
            if not isinstance(record, dict) or type(stop := record.get("stopTime")) is not int:
                return False
            start = record.get("startTime")
            if stop > 0 and (closed is not None or type(start) is not int or not 0 < start < latest):
                return False
    except sqlite3.Error, ValueError:
        # A failed display check must not interrupt charger supervision or present a plausible older value.
        return False
    return True


# Bind the retention permission to this exact admitted managed journal; ordinary commissioning remains strict.
def history_policy(journal: Journal, spec_digest: str) -> dict[str, Any]:
    managed = {"mode": "app_schedule", "spec_digest": spec_digest, "local_start": False}
    original = journal.get("native_bounded_handover", {}).get("original_history")
    if journal.get("managed_mode") != managed or not isinstance(original, list):
        raise NativeContentError("history", "missing-history tolerance requires its exact admitted managed policy")
    return {
        "version": 1,
        "mode": "archive_missing",
        "identity_digest": fingerprint(journal.identity),
        "spec_digest": spec_digest,
        "original_history_digest": fingerprint(original),
    }


# Finish pure history/policy comparisons before native observation side effects can fail in SQLite.
def inspect_history(journal: Journal, spec_digest: str, current: list[dict[str, Any]], local_start: bool) -> None:
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
            raise NativeContentError("history", "native history retention policy changed or local Start is enabled")
        _archive_view(journal, original, current, True)
        return
    originals, admitted_attempts = split_attempts(original)
    current_rows, attempts = split_attempts(current)
    if any(name in attempts and encoded(attempts[name]) != encoded(row) for name, row in admitted_attempts.items()):
        raise NativeContentError("history", ORIGINAL_HISTORY_REFUSAL)
    present = {row["transactionId"]: row for row in current_rows}
    if any(
        row["transactionId"] in present and encoded(present[row["transactionId"]]) != encoded(row) for row in originals
    ):
        raise NativeContentError("history", ORIGINAL_HISTORY_REFUSAL)
    if admitted_attempts.keys() - attempts.keys() or any(row["transactionId"] not in present for row in originals):
        _archive_view(journal, original, current, True)
        raise MissingOriginalHistoryError("history", ORIGINAL_HISTORY_REFUSAL)


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
            raise NativeContentError("history", "native history retention policy changed or local Start is enabled")
        NativeHistoryArchive(journal).observe(original, current, allow_missing=True)
        return
    original, admitted_attempts = split_attempts(original)
    current, attempts = split_attempts(current)
    if any(name not in attempts or encoded(attempts[name]) != encoded(row) for name, row in admitted_attempts.items()):
        raise NativeContentError("history", "native bounded handover must preserve every original history record")
    present = {row["transactionId"]: row for row in current}
    if any(
        row["transactionId"] not in present or encoded(present[row["transactionId"]]) != encoded(row)
        for row in original
    ):
        raise NativeContentError("history", "native bounded handover must preserve every original history record")
    if attempts:
        with history_savepoint(journal):
            NativeHistoryArchive(journal).retain_attempts(attempts)
