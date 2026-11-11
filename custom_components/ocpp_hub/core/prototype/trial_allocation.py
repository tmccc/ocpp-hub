"""Identity-bound native history reserves new local numbers without inventing lifecycle facts."""

from __future__ import annotations

import math
import time
from dataclasses import asdict, dataclass
from typing import Any

from .native_start_attempts import split_attempts
from .policy import integer
from .security import SecurityError
from .store import Journal, RecoveryRequiredError, fingerprint


@dataclass(frozen=True, repr=False)
class HistoryObservation:
    """Only a bounded high-water mark and provenance survive the private complete history read."""

    identity_digest: str
    hardware_digest: str
    observed_at: float
    digest: str
    count: int
    floor: int

    # A history snapshot belongs to both configured identities and the current bounded native audit.
    def require_fresh(self, identity: str, hardware: str, max_age: float = 30) -> None:
        if type(self.observed_at) not in (int, float):
            raise SecurityError("native history observation time is invalid")
        age = time.time() - self.observed_at
        if (
            self.identity_digest != fingerprint(identity)
            or self.hardware_digest != fingerprint(hardware)
            or not math.isfinite(age)
            or not 0 <= age <= max_age
        ):
            raise SecurityError("fresh matching complete native history is required")
        integer(self.count, 0, 10000)
        integer(self.floor, 1000, 2**31 - 1025)


# Treat missing, duplicate, invalid and exhausted native identities as an audit failure rather than an empty history.
def observe_history(value: Any, identity: str, hardware: str, observed_at: float) -> HistoryObservation:
    try:
        if not isinstance(value, list) or len(value) > 10000:
            raise ValueError
        identifiers = []
        numbered, _ = split_attempts(value)
        for row in numbered:
            if not isinstance(row, dict):
                raise ValueError
            identifiers.append(integer(row["transactionId"], 1))
        if len(set(identifiers)) != len(identifiers):
            raise ValueError
        floor = max([1000, *identifiers])
        integer(floor, 1000, 2**31 - 1025)
        return HistoryObservation(
            fingerprint(identity), fingerprint(hardware), observed_at, fingerprint(value), len(value), floor
        )
    except ValueError, KeyError, TypeError, OverflowError:
        raise SecurityError("complete native transaction history is invalid or exhausted") from None


# A fresh trial may reserve numbers once; existing counters and accepted facts are never reset from rolling history.
def initialize_allocation(journal: Journal, history: HistoryObservation, identity: str, hardware: str) -> None:
    if journal.identity != identity:
        raise SecurityError("allocation identity does not match its journal")
    history.require_fresh(identity, hardware)
    with journal.atomic():
        if journal.get("trial_allocation") is not None:
            journal.require_allocation()
            if journal.get("trial_allocation")["hardware_digest"] != fingerprint(hardware):
                raise SecurityError("saved allocation belongs to another physical charger")
            if journal.get("trial_phase") == "prepared" and history.floor > journal.get("next_transaction"):
                raise RecoveryRequiredError("native history advanced beyond the prepared allocation boundary")
            return
        if journal.get("next_transaction") is not None or journal.get("trial_phase") != "prepared":
            raise RecoveryRequiredError("an existing ledger cannot be reseeded from native history")
        if set(row[0] for row in journal.db.execute("SELECT key FROM meta")) != {
            "identity",
            "trial_baseline",
            "trial_phase",
        }:
            raise RecoveryRequiredError("allocation bootstrap found prior ledger authority")
        for table in ("transactions", "outbox", "receipts", "mutations", "trial_resources"):
            if journal.db.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]:
                raise RecoveryRequiredError("allocation bootstrap requires an empty prepared ledger")
        if any(journal.get(key) for key in ("control", "vendor_start_grant", "uncertain_vendor_command")):
            raise RecoveryRequiredError("allocation bootstrap cannot replace prior control authority")
        journal.put("next_transaction", history.floor)
        journal.put("trial_allocation", {"version": 1, **asdict(history)})


# Handoff may take time after initial reservation; no new number can collide with history created during that wait.
def verify_initial_history(journal: Journal, history: HistoryObservation, identity: str, hardware: str) -> None:
    history.require_fresh(identity, hardware)
    journal.require_allocation()
    proof = journal.get("trial_allocation")
    if (
        identity != journal.identity
        or proof["hardware_digest"] != fingerprint(hardware)
        or history.floor > journal.get("next_transaction")
    ):
        raise SecurityError("native history changed beyond the reserved range before first introduction")
