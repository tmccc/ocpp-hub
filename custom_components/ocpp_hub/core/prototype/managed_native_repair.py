"""Exact-fault native repair audits preserve strict refusals and interrupted unknown outcomes durably."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from .managed_denial_faults import denial_source_fault
from .managed_intent import RELEASE_PAUSE_ACK, retire_pause_ack, saved_pause_ack
from .native_faults import NativeContentError
from .native_history import ORIGINAL_HISTORY_OMISSION, ORIGINAL_HISTORY_REFUSAL, MissingOriginalHistoryError
from .store import Journal, RecoveryRequiredError, encoded, fingerprint

AUDITS = "managed_native_repair_audits"
REVOCATIONS = "managed_native_repair_revocations"
ACK = RELEASE_PAUSE_ACK
Outcome = Literal["validated", "availability", "superseded", "readiness", "measurement", "strict"]


# An admitted operator proof retains the exact prior generic fault whose denied lifecycle it still carries.
def repair_sources(journal: Journal, fault: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    prior = denial_source_fault(journal, fault)
    return (fault, prior) if prior is not None and prior != fault else (fault,)


# An operator fault replacement cannot discard a strict refusal belonging to its verified prior fault.
def repair_revoked(journal: Journal, fault: dict[str, Any]) -> bool:
    rows = journal.get(REVOCATIONS, {})
    if not isinstance(rows, dict):
        return True
    return any(fingerprint(source) in rows for source in repair_sources(journal, fault))


# Retire only explicitly typed omissions inside the transaction that independently repairs the history.
def retire_history_omission_revocations(journal: Journal, fault: dict[str, Any]) -> dict[str, Any]:
    if not journal.db.in_transaction:
        raise RecoveryRequiredError("retiring a history refusal requires the archive transaction")
    rows = journal.get(REVOCATIONS, {})
    if not fault or not isinstance(rows, dict):
        return {}
    retired = {}
    for source in repair_sources(journal, fault):
        key = fingerprint(source)
        if key not in rows:
            continue
        row = rows[key]
        if (
            not isinstance(row, dict)
            or row.get("fault") != source
            or row.get("typed_refusal")
            != {"stage": "history", "detail": ORIGINAL_HISTORY_REFUSAL, "source": ORIGINAL_HISTORY_OMISSION}
        ):
            return {}
        retired[key] = row
    if retired:
        journal.put(REVOCATIONS, {key: row for key, row in rows.items() if key not in retired})
    return retired


# An unresolved audit outcome withholds proof even if later peer data is normal after replacement.
def repair_pending(journal: Journal, fault: dict[str, Any]) -> bool:
    rows = journal.get(AUDITS, {})
    if not isinstance(rows, dict):
        return True
    return any(fingerprint(source) in rows for source in repair_sources(journal, fault))


# A new operator fault may observe its own zero while retaining an ancestor's unknown outcome and proof restriction.
def audit_pending(journal: Journal, fault: dict[str, Any]) -> bool:
    rows = journal.get(AUDITS, {})
    return not isinstance(rows, dict) or fingerprint(fault) in rows


@dataclass(frozen=True)
class RepairAttempt:
    """Private immutable preimages prevent another Pause or fault from borrowing an audit outcome."""

    identifier: str
    fault_json: str
    acknowledgement_json: str
    native_failure_json: str
    authority: str
    sequence: int
    epoch: int
    boot: str | None
    revision: int

    # Reconstruct private fault bytes without sharing a mutable object with a caller.
    @property
    def fault(self) -> dict[str, Any]:
        value: dict[str, Any] = json.loads(self.fault_json)
        return value

    # Persist one exact attempt identity before any independent native observation starts.
    def value(self) -> dict[str, Any]:
        return {
            "version": 1,
            "attempt": self.identifier,
            "fault": self.fault,
            "acknowledgement": json.loads(self.acknowledgement_json),
            "original_native_failure": json.loads(self.native_failure_json),
            "authority": self.authority,
            "sequence": self.sequence,
            "epoch": self.epoch,
            "boot": self.boot,
            "revision": self.revision,
        }


class NativeRepairAudits:
    """One live audit may settle its own fence; replacement cannot invent its missing outcome."""

    # Eligible-fault policy stays in the existing release service; this module only owns its durable audit lifecycle.
    def __init__(self, journal: Journal, eligible: Callable[[dict[str, Any]], bool]) -> None:
        self.journal, self.eligible = journal, eligible
        self.active: RepairAttempt | None = None
        self.pending: tuple[RepairAttempt, Outcome, dict[str, Any] | None] | None = None
        self.pending_storage_error: sqlite3.Error | None = None

    # Retry the frozen outcome only, never reread restored peer data to replace an observed refusal.
    def flush(self) -> None:
        if self.pending is not None:
            attempt, outcome, refusal = self.pending
            try:
                self._publish(attempt, outcome, refusal)
            except sqlite3.Error as error:
                self.pending_storage_error = error
                raise
            self.pending = None
            self.pending_storage_error = None
            self.active = None

    # A durable fence precedes the audit; a failed BEGIN or COMMIT therefore prevents the read entirely.
    def begin(self, authority: str, sequence: int, epoch: int, boot: str | None, revision: int) -> RepairAttempt | None:
        self.flush()
        fault = self.journal.fault.read({})
        if not fault or not self.eligible(fault):
            return None
        if audit_pending(self.journal, fault):
            raise RecoveryRequiredError("native repair awaits a durably classified prior audit outcome")
        attempt = RepairAttempt(
            uuid.uuid4().hex,
            encoded(fault),
            encoded(saved_pause_ack(self.journal)),
            encoded(self.journal.get("managed_native_failure")),
            authority,
            sequence,
            epoch,
            boot,
            revision,
        )
        with self.journal.atomic():
            if self.journal.fault.read() != fault or audit_pending(self.journal, fault):
                raise RecoveryRequiredError("native repair fault changed before its independent audit")
            self.journal.put(AUDITS, {**self.journal.get(AUDITS, {}), fingerprint(fault): attempt.value()})
        self.active = attempt
        return attempt

    # Retain a known live outcome without retrying storage while the original SQL error is propagating.
    def defer(
        self,
        attempt: RepairAttempt | None,
        outcome: Outcome,
        error: NativeContentError | None = None,
        *,
        storage_error: sqlite3.Error | None = None,
    ) -> None:
        if attempt is not None:
            refusal = {"stage": error.stage, "detail": str(error)[:1024]} if error is not None else None
            if refusal is not None and isinstance(error, MissingOriginalHistoryError):
                refusal["source"] = ORIGINAL_HISTORY_OMISSION
            self.pending = (attempt, outcome, refusal)
            self.pending_storage_error = storage_error

    # Freeze a classified live outcome before storage so failed publication cannot downgrade strict evidence.
    def finish(self, attempt: RepairAttempt | None, outcome: Outcome, error: NativeContentError | None = None) -> None:
        self.defer(attempt, outcome, error)
        self.flush()

    # Commit refusal plus matching acknowledgement invalidation together, retaining every original cause and proof.
    def _publish(self, attempt: RepairAttempt, outcome: Outcome, refusal: dict[str, Any] | None) -> None:
        journal, fault = self.journal, attempt.fault
        key = fingerprint(fault)
        with journal.atomic():
            audits = journal.get(AUDITS, {})
            if audits.get(key) != attempt.value():
                raise RecoveryRequiredError("native repair audit fence changed before outcome publication")
            if outcome == "strict":
                if refusal is None:
                    raise RecoveryRequiredError("strict native repair outcome requires its typed source")
                journal.put(
                    REVOCATIONS,
                    {
                        **journal.get(REVOCATIONS, {}),
                        key: {
                            **attempt.value(),
                            "typed_refusal": refusal,
                            "at_epoch": time.time(),
                        },
                    },
                )
                if journal.fault.read() == fault:
                    retire_pause_ack(journal, fault)
            del audits[key]
            journal.put(AUDITS, audits)
