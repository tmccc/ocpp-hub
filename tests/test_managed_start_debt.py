"""Guard the narrow idle restoration exception while retaining actual outstanding Start debt."""

import pytest
from ocpp_hub_core.prototype.gateway import Gateway
from ocpp_hub_core.prototype.managed_intent import ManagedRequests
from ocpp_hub_core.prototype.managed_start_debt import preserved_start_debt
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError, fingerprint
from ocpp_hub_core.prototype.trial_rollback import require_lifecycle_idle


# Supply one independently witnessed expired command, without fabricating an owner transaction or closure.
@pytest.fixture
def journal(tmp_path):
    j = Journal(tmp_path / "start-debt.db", "START_DEBT_TEST")
    ManagedRequests(j)
    j.put("managed_control_intent", {"mode": "app", "revision": 2})
    j.put("control", {"start_pending": True})
    j.put("local_start_receipt", {"stage": "accepted", "local": None, "digest": "a" * 64})
    j.put(
        "managed_test_charge",
        {
            "id": "owner",
            "source": "local",
            "kind": "normal",
            "expired_normal": True,
            "state": "expired",
            "independent_seen": True,
            "start_rpc_accepted": True,
            "revision": 2,
            "tag_digest": "a" * 64,
        },
    )
    j.db.execute("INSERT INTO managed_requests VALUES('owner','start',1,'uncertain','{}')")
    try:
        yield j
    finally:
        j.close()


# Intent: idle restoration may preserve exactly one expired command, while rollback remains strict by default.
# Why: retaining Start reporting debt must not implicitly authorize profile removal or backend rollback.
def test_preserved_debt_requires_explicit_restoration_scope(journal):
    g = Gateway(journal)
    saved = journal.get("local_start_receipt")
    assert preserved_start_debt(journal)
    with pytest.raises(RecoveryRequiredError, match="unbound start"):
        require_lifecycle_idle(g)
    require_lifecycle_idle(g, preserve_expired_start=True)
    assert journal.get("local_start_receipt") == saved and journal.get("control")["start_pending"]


# Intent: every provenance, scope and authority boundary independently revokes the idle debt exception.
# Why: Pause, a new request, an actual bound owner, or a vendor grant cannot inherit unrelated daily authority.
@pytest.mark.parametrize(
    ("key", "change"),
    [
        ("managed_test_charge", {"source": "cloud"}),
        ("managed_test_charge", {"kind": "test"}),
        ("managed_test_charge", {"expired_normal": False}),
        ("managed_test_charge", {"state": "completed"}),
        ("managed_test_charge", {"independent_seen": False}),
        ("managed_test_charge", {"start_rpc_accepted": False}),
        ("managed_test_charge", {"revision": 1}),
        ("managed_test_charge", {"tag_digest": None}),
        ("managed_test_charge", {"tag_digest": "a"}),
        ("managed_test_charge", {"id": "other"}),
        ("managed_control_intent", {"mode": "paused"}),
        ("managed_control_intent", {"revision": 3}),
        ("managed_owner_pause", {"revision": 2}),
        ("control", {"start_pending": False}),
        ("local_start_receipt", {"stage": "issued"}),
        ("local_start_receipt", {"stage": "rejected"}),
        ("local_start_receipt", {"local": 123}),
        ("local_start_receipt", {"digest": "b" * 64}),
        ("vendor_start_grant", {"token": "another-pending-start", "local": None}),
    ],
)
def test_preserved_debt_revocation(journal, key, change):
    journal.put(key, {**journal.get(key, {}), **change})
    assert not preserved_start_debt(journal)
    if journal.get("control").get("start_pending"):
        with pytest.raises(RecoveryRequiredError):
            require_lifecycle_idle(Gateway(journal), preserve_expired_start=True)


# Intent: terminal or differently typed requests cannot leave a usable idle restoration capability behind.
# Why: provenance must still belong to the specific outstanding ordinary Start, not a historical receipt.
@pytest.mark.parametrize(("action", "state"), [("pause", "uncertain"), ("start", "completed"), ("start", "failed")])
def test_preserved_debt_requires_outstanding_owner_request(journal, action, state):
    journal.db.execute("UPDATE managed_requests SET action=?,state=?", (action, state))
    assert not preserved_start_debt(journal)


# Intent: preserving open-session debt requires the exact ledger scope and remains unavailable to default rollback.
# Why: a caller must not reuse another transaction's continuation permission or infer closure from the shared tag.
def test_continuing_debt_requires_exact_ledger_scope(journal):
    digest = fingerprint("daily")
    journal.put("local_start_receipt", {**journal.get("local_start_receipt"), "digest": digest})
    journal.put("managed_test_charge", {**journal.get("managed_test_charge"), "tag_digest": digest})
    journal.db.execute(
        "INSERT INTO transactions(local,connector,start,live) VALUES(101,1,?,1)",
        ('{"idTag":"DAILY"}',),
    )
    assert not preserved_start_debt(journal)
    assert not preserved_start_debt(journal, continuing_local=102)
    assert preserved_start_debt(journal, continuing_local=101)
    with pytest.raises(RecoveryRequiredError, match="unbound start"):
        require_lifecycle_idle(Gateway(journal), continuing_local=101)
    require_lifecycle_idle(Gateway(journal), continuing_local=101, preserve_expired_start=True)
