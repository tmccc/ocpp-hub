"""Pending Stop classification distinguishes genuine closed delivery from unrelated or ambiguous debts."""

import pytest
from ocpp_hub_core.prototype.managed_release_wait import awaiting_known_stops
from ocpp_hub_core.prototype.store import Journal, encoded


# Intent: only exact closed mapped queued/inflight Stops qualify for a pre-effect wait.
# Why: this classifier must not broaden release retry to pending Starts, observations, unknown mappings or writes.
@pytest.mark.parametrize(
    "case,expected",
    [
        ("queued", True),
        ("inflight", True),
        ("empty", False),
        ("open", False),
        ("unknown", False),
        ("missing", False),
        ("changed_stop", False),
        ("wrong_local", False),
        ("rejected", False),
        ("ambiguous", False),
        ("start", False),
        ("observation", False),
        ("owner_pending", False),
        ("vendor_grant", False),
        ("vendor_marker", False),
        ("vendor_mutation", False),
        ("local_mutation", False),
    ],
)
def test_known_pending_stop_classifier(tmp_path, case, expected):
    j = Journal(tmp_path / "journal.db", "SYNTHETIC")
    stop = {"transactionId": 1001, "meterStop": 1100, "timestamp": "2026-01-01T00:01:00Z"}
    try:
        j.db.execute(
            "INSERT INTO transactions(local,connector,start,stop,vendor) VALUES(1001,1,?,?,9001)",
            (encoded({"meterStart": 1000}), encoded(stop)),
        )
        j.db.execute(
            "INSERT INTO outbox(token,action,payload,local,state) VALUES('stop','StopTransaction',?,1001,'queued')",
            (encoded(stop),),
        )
        if case in ("inflight", "rejected", "ambiguous"):
            j.db.execute("UPDATE outbox SET state=?", (case,))
        elif case == "empty":
            j.db.execute("DELETE FROM outbox")
        elif case == "open":
            j.db.execute("UPDATE transactions SET stop=NULL")
        elif case == "unknown":
            j.db.execute("UPDATE transactions SET vendor=NULL")
        elif case == "missing":
            j.db.execute("UPDATE outbox SET local=NULL")
        elif case == "wrong_local":
            j.db.execute("UPDATE outbox SET local=1002")
        elif case == "changed_stop":
            j.db.execute("UPDATE outbox SET payload=?", (encoded(stop | {"meterStop": 1200}),))
        elif case in ("start", "observation"):
            j.db.execute(
                "UPDATE outbox SET action=?", ("StartTransaction" if case == "start" else "StatusNotification",)
            )
        elif case == "owner_pending":
            j.put("control", {"start_pending": True})
        elif case == "vendor_grant":
            j.put("vendor_start_grant", {"token": "unbound", "local": None})
        elif case == "vendor_marker":
            j.put("uncertain_vendor_command", {"action": "RemoteStartTransaction"})
        elif case in ("vendor_mutation", "local_mutation"):
            j.mutation_intent("vendor" if case == "vendor_mutation" else "local", "RemoteStartTransaction", {})
        assert awaiting_known_stops(j) is expected
    finally:
        j.close()
