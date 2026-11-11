"""Real SQLite preserves complete native history without making its retention a charging permission."""

from __future__ import annotations

import copy
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.prototype.native_history import NativeHistoryArchive
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import ConflictError, Journal

IDENTITY = "HISTORY_ARCHIVE001"


# Keep opaque nested native fields so tests exercise complete-body preservation, not a chosen projection.
def row(identifier: int, *, closed: bool = True) -> dict[str, Any]:
    return {
        "transactionId": identifier,
        "connectorId": 1,
        "startTime": 1000 + identifier,
        "stopTime": 1060 + identifier if closed else 0,
        "meterStart": 5000,
        "meterStop": 5500 if closed else 0,
        "MeterValues": [{"opaque": {"samples": [1, 2, 3]}}],
        "filename": "synthetic-history-" + str(identifier) + ".json",
    }


# Use the real durable journal and lock, with no engine mock or charger connection.
@pytest.fixture
def history_journal(tmp_path: Path) -> Iterator[Journal]:
    journal = Journal(tmp_path / "history.sqlite3", IDENTITY)
    yield journal
    journal.close()


# Compare the entire application database around a rejected observation, including any newly inserted rows.
def contents(journal: Journal) -> str:
    return "\n".join(journal.db.iterdump())


# Intent: any number of absent original records is archived and reported without rewriting original admission data.
# Why: firmware history retention must not force a charging fault or silently erase the complete original archive.
def test_missing_originals_are_retained_with_unchanged_admission_history(history_journal: Journal) -> None:
    originals = [row(1001), row(1002), row(1003)]
    saved = {"stage": "admitted", "original_history": copy.deepcopy(originals)}
    history_journal.put("native_bounded_handover", saved)
    archive = NativeHistoryArchive(history_journal)
    before = copy.deepcopy(originals)

    result = archive.observe(originals, [], allow_missing=True)

    assert result == {"archived_records": 3, "missing_records": 3}
    assert history_journal.db.execute("SELECT COUNT(*) FROM native_history_records").fetchone()[0] == 3
    assert {r[0] for r in history_journal.db.execute("SELECT original FROM native_history_records")} == {1}
    assert history_journal.get("native_bounded_handover") == saved
    assert originals == before
    assert archive.observe(originals, originals, allow_missing=True) == {
        "archived_records": 3,
        "missing_records": 0,
    }


# Intent: strict callers still reject missing history and commit none of the otherwise valid new records.
# Why: this managed-mode capability must not weaken ordinary commissioning or leave a partial archive on failure.
@pytest.mark.parametrize("all_missing", [False, True])
def test_missing_history_is_still_rejected_without_opt_in(history_journal: Journal, all_missing: bool) -> None:
    archive = NativeHistoryArchive(history_journal)
    before = contents(history_journal)

    with pytest.raises(SecurityError):
        archive.observe([row(1001), row(1002)], [] if all_missing else [row(1001), row(1003)], allow_missing=False)

    assert contents(history_journal) == before


# Intent: a historical record's nested fields remain immutable even when its absence would be tolerated.
# Why: retaining records cannot turn permission to omit an old row into permission to alter its existing full body.
@pytest.mark.parametrize("change", ["meter", "nested", "extra", "numeric_type"])
def test_present_original_body_changes_are_rejected_atomically(history_journal: Journal, change: str) -> None:
    original = row(1001)
    archive = NativeHistoryArchive(history_journal)
    archive.observe([original], [original], allow_missing=True)
    altered = copy.deepcopy(original)
    if change == "meter":
        altered["meterStop"] += 1
    elif change == "nested":
        altered["MeterValues"][0]["opaque"]["samples"][1] = 99
    elif change == "extra":
        altered["unobserved_field"] = "changed"
    else:
        altered["MeterValues"][0]["opaque"]["samples"][0] = 1.0
    before = contents(history_journal)

    with pytest.raises(SecurityError):
        archive.observe([original], [row(1002), altered], allow_missing=True)

    assert contents(history_journal) == before
    assert archive.observe([original], [original], allow_missing=True)["missing_records"] == 0


# Intent: non-original active rows may evolve and disappear; genuinely closed versions are retained without overwrite.
# Why: freezing an active row would reject normal meter updates or invent a completed session from a partial read.
def test_open_history_can_evolve_until_actual_completion(history_journal: Journal) -> None:
    original, opened = row(1001), row(1002, closed=False)
    archive = NativeHistoryArchive(history_journal)
    assert archive.observe([original], [original, opened], allow_missing=True) == {
        "archived_records": 1,
        "missing_records": 0,
    }
    advanced = copy.deepcopy(opened)
    advanced["MeterValues"].append({"opaque": {"samples": [4, 5]}})
    assert archive.observe([original], [original, advanced], allow_missing=True)["archived_records"] == 1
    assert archive.observe([original], [original], allow_missing=True)["missing_records"] == 0

    completed = copy.deepcopy(advanced) | {"stopTime": 2062, "meterStop": 5731}
    assert archive.observe([original], [original, completed], allow_missing=True) == {
        "archived_records": 2,
        "missing_records": 0,
    }
    assert archive.observe([original], [original], allow_missing=True)["missing_records"] == 1
    assert archive.observe([original], [original, completed], allow_missing=True)["missing_records"] == 0

    changed = copy.deepcopy(completed) | {"meterStop": 5732, "syncStop": 2}
    assert archive.observe([original], [original, changed], allow_missing=True) == {
        "archived_records": 2,
        "missing_records": 0,
    }
    versions = history_journal.db.execute(
        "SELECT COUNT(*) FROM native_history_records WHERE transaction_id=1002"
    ).fetchone()[0]
    assert versions == 2
    assert {
        r[0]
        for r in history_journal.db.execute("SELECT original FROM native_history_records WHERE transaction_id=1002")
    } == {0}


# Intent: records first seen after admission remain archived and missing-state reporting survives a real restart.
# Why: an empty firmware list after replacement must not erase old evidence or permit an altered original to reappear.
def test_closed_archive_survives_process_replacement(tmp_path: Path) -> None:
    path = tmp_path / "replacement.sqlite3"
    original, later = row(1001), row(1002)
    journal = Journal(path, IDENTITY)
    try:
        archive = NativeHistoryArchive(journal)
        archive.observe([original], [original, later], allow_missing=True)
        assert archive.observe([original], [], allow_missing=True)["missing_records"] == 2
    finally:
        journal.close()

    replacement = Journal(path, IDENTITY)
    try:
        archive = NativeHistoryArchive(replacement)
        assert archive.observe([original], [], allow_missing=True) == {
            "archived_records": 2,
            "missing_records": 2,
        }
        altered = copy.deepcopy(original)
        altered["MeterValues"][0]["opaque"]["samples"].append(4)
        before = contents(replacement)
        with pytest.raises(SecurityError):
            archive.observe([original], [later, altered], allow_missing=True)
        assert contents(replacement) == before
        assert archive.observe([original], [original, later], allow_missing=True)["missing_records"] == 0
    finally:
        replacement.close()


# Intent: a valid first new record is not committed when a later row fails immutable-history validation.
# Why: an error halfway through observation must not leave a partially accepted native history or missing flags.
def test_late_archive_conflict_rolls_back_new_rows_and_missing_state(history_journal: Journal) -> None:
    archive = NativeHistoryArchive(history_journal)
    originals = [row(1001), row(1002)]
    archive.observe(originals, originals, allow_missing=True)
    archive.observe(originals, [originals[0]], allow_missing=True)
    changed = copy.deepcopy(originals[1]) | {"meterStop": 6000}
    before = contents(history_journal)

    with pytest.raises(SecurityError):
        archive.observe(originals, [row(1003), originals[0], changed], allow_missing=True)

    assert contents(history_journal) == before
    assert archive.observe(originals, [originals[0]], allow_missing=True) == {
        "archived_records": 2,
        "missing_records": 1,
    }


# Intent: the original admission list cannot be shortened, expanded or changed after its first binding.
# Why: relaxing rolling-history retention must not create a way to replace the immutable original archive.
@pytest.mark.parametrize("change", ["remove", "add", "alter"])
def test_original_archive_binding_cannot_be_replaced(history_journal: Journal, change: str) -> None:
    originals = [row(1001), row(1002)]
    archive = NativeHistoryArchive(history_journal)
    archive.observe(originals, originals, allow_missing=True)
    altered = copy.deepcopy(originals)
    if change == "remove":
        altered.pop()
    elif change == "add":
        altered.append(row(1003))
    else:
        altered[0]["MeterValues"][0]["opaque"]["samples"].append(4)
    before = contents(history_journal)

    with pytest.raises(SecurityError):
        archive.observe(altered, altered, allow_missing=True)

    assert contents(history_journal) == before


# Intent: an original open-looking historical body is retained exactly, unlike new active rows seen after admission.
# Why: a rolling list must not silently adopt or revise an unknown pre-existing session when it later disappears.
def test_original_open_body_is_archived_and_remains_immutable(history_journal: Journal) -> None:
    original = row(1001, closed=False)
    archive = NativeHistoryArchive(history_journal)
    assert archive.observe([original], [original], allow_missing=True)["archived_records"] == 1
    assert archive.observe([original], [], allow_missing=True)["missing_records"] == 1
    assert archive.observe([original], [original], allow_missing=True)["missing_records"] == 0
    changed = copy.deepcopy(original) | {"stopTime": 2061, "meterStop": 5500}
    before = contents(history_journal)

    with pytest.raises(SecurityError):
        archive.observe([original], [changed], allow_missing=True)

    assert contents(history_journal) == before


# Intent: duplicate identifiers and malformed scalar types fail before any archive state can be accepted.
# Why: dictionary coalescing, boolean integers and non-finite values could conceal record replacement or absence.
@pytest.mark.parametrize(
    "fault",
    ["duplicate", "boolean_id", "float_id", "zero_id", "boolean_stop", "nan_stop", "negative_stop", "text_stop"],
)
def test_invalid_current_history_does_not_partially_commit(history_journal: Journal, fault: str) -> None:
    archive = NativeHistoryArchive(history_journal)
    original = row(1001)
    archive.observe([original], [original], allow_missing=True)
    bad = row(1003)
    if fault == "boolean_id":
        bad["transactionId"] = True
    elif fault == "float_id":
        bad["transactionId"] = 1003.0
    elif fault == "zero_id":
        bad["transactionId"] = 0
    elif fault == "boolean_stop":
        bad["stopTime"] = True
    elif fault == "nan_stop":
        bad["stopTime"] = float("nan")
    elif fault == "negative_stop":
        bad["stopTime"] = -1
    elif fault == "text_stop":
        bad["stopTime"] = "2063"
    current = [row(1002), bad, bad] if fault == "duplicate" else [row(1002), bad]
    before = contents(history_journal)

    with pytest.raises(SecurityError):
        archive.observe([original], current, allow_missing=True)

    assert contents(history_journal) == before


# Intent: persisted archive data remains bound to the existing journal's charger identity after reopening.
# Why: history retained for one physical charger must not seed another charger's proof or allocation context.
def test_archive_cannot_reopen_under_another_journal_identity(tmp_path: Path) -> None:
    path = tmp_path / "identity.sqlite3"
    original = row(1001)
    journal = Journal(path, IDENTITY)
    try:
        NativeHistoryArchive(journal).observe([original], [original], allow_missing=True)
    finally:
        journal.close()

    with pytest.raises(ConflictError):
        Journal(path, "ANOTHER_SYNTHETIC_CHARGER")

    reopened = Journal(path, IDENTITY)
    try:
        assert NativeHistoryArchive(reopened).observe([original], [], allow_missing=True) == {
            "archived_records": 1,
            "missing_records": 1,
        }
    finally:
        reopened.close()


# Model the single start sample recorded by the iCS before it obtains a transaction number.
def unassigned(name: str = "attempt.json") -> dict[str, Any]:
    return {
        "transactionId": -1,
        "filename": name,
        "connectorId": 1,
        "idTagStart": "NATIVE_TIMER",
        "startTime": 1000,
        "startMs": 5,
        "meterStart": 9000,
        "startQ4": 6,
        "syncStart": 1,
        "syncStop": 0,
        "stopTime": 0,
        "stopMs": 0,
        "meterStop": 0,
        "stopQ4": 0,
        "idTagStop": "",
        "stopTimeStr": "",
        "reasonStr": "",
        "reason": 0,
        "idTypeStop": 0,
        "MeterValues": [
            {"syncMeter": 1, "timestamp": 1001, "context": 5, "A": [0, 0, 0], "energy": 9000, "Q4dot": 6, "Offer": 320}
        ],
    }


# Intent: retain all native attempt bodies across reads, counting only assigned IDs for allocation and HA sessions.
# Why: several failed starts can share -1 without representing either an accepted transaction or delivered energy.
def test_repeated_unassigned_history(history_journal: Journal) -> None:
    import json

    from ocpp_hub_core.prototype.managed_view import charger_history
    from ocpp_hub_core.prototype.trial_allocation import observe_history

    numbered = row(1234)
    attempts = [unassigned("a"), unassigned("b")]
    records = [numbered, *attempts]
    sample = observe_history(records, IDENTITY, "synthetic", 1002)
    assert sample.floor == 1234 and sample.count == 3
    assert charger_history(tuple(records)) == charger_history((numbered,))
    archive = NativeHistoryArchive(history_journal)
    assert archive.observe([numbered], records, allow_missing=True)["archived_records"] == 1
    archive.observe([numbered], [numbered], allow_missing=True)
    assert [json.loads(r[0]) for r in history_journal.db.execute("SELECT body FROM native_start_attempts")] == attempts


# Intent: ambiguous or energised unassigned records still reject the complete observation.
# Why: missing transaction identity cannot hide activity or make a positive operation safe.
@pytest.mark.parametrize(
    "change", ["energy", "current", "missing", "duplicate", "syncStop", "fractional", "reason", "idTypeStop", "power"]
)
def test_inconsistent_unassigned_history(history_journal: Journal, change: str) -> None:
    attempt = unassigned()
    records = [attempt]
    if change == "energy":
        attempt["MeterValues"][0]["energy"] += 1
    elif change == "current":
        attempt["MeterValues"][0]["A"][0] = 1
    elif change in ("reason", "idTypeStop"):
        attempt[change] = 1
    elif change == "power":
        attempt["MeterValues"][0]["power"] = 3000
    elif change == "missing":
        attempt.pop("MeterValues")
    elif change == "duplicate":
        records.append(copy.deepcopy(attempt))
    elif change == "fractional":
        attempt["transactionId"] = -1.0
    else:
        attempt["syncStop"] = 1
    archive = NativeHistoryArchive(history_journal)
    before = contents(history_journal)
    with pytest.raises(SecurityError):
        archive.observe([], records, allow_missing=True)
    assert contents(history_journal) == before


# Intent: a retained original attempt must match its entire body whenever its filename is still present.
# Why: retention tolerance permits disappearance while preserving evidence of a changed or ambiguous record.
def test_admitted_attempt_body_is_immutable(history_journal: Journal) -> None:
    original = unassigned()
    archive = NativeHistoryArchive(history_journal)
    archive.observe([original], [original], allow_missing=True)
    changed = copy.deepcopy(original)
    changed["MeterValues"][0]["Offer"] += 1
    replacement = row(1101) | {"filename": original["filename"]}
    for records in ([changed], [replacement], [original, replacement]):
        with pytest.raises(SecurityError):
            archive.observe([original], records, allow_missing=True)
    assert archive.observe([original], [], allow_missing=True)["archived_records"] == 0
    assert history_journal.db.execute("SELECT COUNT(*) FROM native_start_attempts").fetchone()[0] == 1


# Intent: a numbered original reappearing at its own filename as an unassigned attempt blocks reconciliation.
# Why: retention tolerance cannot turn a changed original into a missing transaction plus a new zero-draw attempt.
def test_numbered_original_cannot_reappear_as_unassigned(history_journal: Journal) -> None:
    original = row(1101)
    archive = NativeHistoryArchive(history_journal)
    archive.observe([original], [original], allow_missing=True)
    before = contents(history_journal)
    with pytest.raises(SecurityError):
        archive.observe([original], [unassigned(original["filename"])], allow_missing=True)
    assert contents(history_journal) == before


# Intent: ordinary strict audits also archive observed start attempts without granting missing-history tolerance.
# Why: a failed start can subsequently roll out of the firmware's history on either retention policy.
def test_strict_audit_retains_attempts(history_journal: Journal) -> None:
    import json

    from ocpp_hub_core.prototype.native_history import audit_history

    original, attempt = row(1101), unassigned()
    history_journal.put("native_bounded_handover", {"original_history": [original]})
    audit_history(history_journal, "synthetic", [original, attempt], False)
    audit_history(history_journal, "synthetic", [original], False)
    with pytest.raises(SecurityError):
        audit_history(history_journal, "synthetic", [attempt], False)
    assert [json.loads(r[0]) for r in history_journal.db.execute("SELECT body FROM native_start_attempts")] == [attempt]
    assert history_journal.get("native_history_policy") is None
