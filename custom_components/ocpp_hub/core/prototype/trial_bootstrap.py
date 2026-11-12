"""Commit first ownership metadata atomically and repair only the exact pristine interrupted shape."""

from __future__ import annotations

from typing import Any

from .security import SecurityError
from .store import Journal

EMPTY_BOOTSTRAP_TABLES = frozenset(
    {
        "transactions",
        "receipts",
        "outbox",
        "mutations",
        "trial_resources",
        "inbound_receipts",
        "transaction_clock_discontinuities",
    }
)


# A baseline-only crash cannot have acquired hardware authority, transactions or another feature's metadata.
def pristine_interrupted_bootstrap(journal: Journal) -> bool:
    keys = {row[0] for row in journal.db.execute("SELECT key FROM meta")}
    if keys != {"identity", "trial_baseline"}:
        return False
    tables = {
        row[0]
        for row in journal.db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    }
    if tables - EMPTY_BOOTSTRAP_TABLES - {"meta"}:
        return False
    return not any(
        journal.db.execute('SELECT 1 FROM "' + table + '" LIMIT 1').fetchone()
        for table in sorted(tables & EMPTY_BOOTSTRAP_TABLES)
    )


# Existing phase-bearing journals retain their authority; only the exact hardware-untouched legacy gap is completed.
def initialize_trial_baseline(journal: Journal, baseline: dict[str, Any]) -> None:
    with journal.atomic():
        saved = journal.get("trial_baseline")
        if saved is not None and saved != baseline:
            raise SecurityError("journal belongs to another trial baseline")
        if saved is None:
            occupied = journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
            occupied += journal.db.execute("SELECT COUNT(*) FROM mutations").fetchone()[0]
            if occupied:
                raise SecurityError("trial requires a new journal or its original verified baseline")
            journal.put("trial_baseline", baseline)
            journal.put("trial_phase", "prepared")
        elif journal.get("trial_phase") is None:
            if not pristine_interrupted_bootstrap(journal):
                raise SecurityError("interrupted trial bootstrap contains unrelated state")
            journal.put("trial_phase", "prepared")
