"""Recognize an expired local Start whose reporting debt survives independent daily charging."""

from __future__ import annotations

from .local_control import local_start_receipt
from .store import Journal


# Callers must separately prove physical and ledger closure; this exception never permits rollback or another Start.
def preserved_start_debt(journal: Journal) -> bool:
    state = journal.get("managed_test_charge", {})
    intent = journal.get("managed_control_intent", {})
    owner = local_start_receipt(journal)
    grant = journal.get("vendor_start_grant", {})
    return bool(
        state.get("kind") == "normal"
        and state.get("source") == "local"
        and state.get("expired_normal") is True
        and state.get("state") == "expired"
        and state.get("independent_seen") is True
        and state.get("start_rpc_accepted") is True
        and intent.get("mode") == "app"
        and intent.get("revision") == state.get("revision")
        and journal.get("managed_owner_pause", {}).get("revision") != intent.get("revision")
        and journal.get("control", {}).get("start_pending") is True
        and journal.active() is None
        and owner.get("stage") == "accepted"
        and owner.get("local") is None
        and isinstance(state.get("tag_digest"), str)
        and len(state["tag_digest"]) == 64
        and owner.get("digest") == state["tag_digest"]
        and (not grant or grant.get("local") is not None)
        and journal.db.execute(
            "SELECT 1 FROM managed_requests WHERE id=? AND action='start' AND state='uncertain'",
            (state.get("id"),),
        ).fetchone()
        is not None
    )
