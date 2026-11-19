"""The rollback reader recognises a settled migrated journal and its archived evidence."""

import json
import sqlite3
import time
from contextlib import closing

import pytest
from ocpp_hub_core.prototype.store import fingerprint

from tests.process_helpers import process_rig
from tests.tls_support import tls as tls
from tests_installation.migrated_layout import seed_old_layout
from tools.check_proxy_rollback import RollbackRefusedError, check_snapshot, stopped_journal


# Save the same immutable timed-Start outcome and later verified Pause found in the installed journal.
def retain_timed_start(db, now):
    start, pause = "a" * 32, "b" * 32
    proof = {
        "prior_intent": {"mode": "app", "revision": 2},
        "prior_fault": {"classification": "native_authority_or_preservation_failed", "at_epoch": now - 20},
        "at_epoch": now - 10,
        "superseded_test_proofs": [{"request_id": start, "verified_pause_id": pause}],
    }
    db.execute("INSERT INTO meta VALUES('native_start_attempt_recovery',?)", (json.dumps(proof),))
    db.execute(
        "INSERT INTO managed_requests VALUES(?,'start_test',0,'uncertain',?)",
        (start, json.dumps({"classification": "start_not_confirmed"})),
    )
    db.execute(
        "INSERT INTO managed_requests VALUES(?,'pause',1,'completed',?)",
        (pause, json.dumps({"pause_verified": True, "completed_at": now - 30})),
    )
    return pause


# Save a synthetic historical charge and the exact reconciliation proof for an old timed Start.
def retain_history(db, now):
    meta = {key: json.loads(value) for key, value in db.execute("SELECT key,value FROM meta")}
    original = {"transactionId": 123, "startTime": 100, "stopTime": 200}
    handover = meta["native_bounded_handover"] | {"original_history": [original]}
    binding = {"identity": fingerprint(meta["identity"]), "original": fingerprint([original])}
    policy = {
        "version": 1,
        "mode": "archive_missing",
        "identity_digest": binding["identity"],
        "spec_digest": meta["managed_mode"]["spec_digest"],
        "original_history_digest": binding["original"],
    }
    entries = {
        "native_bounded_handover": handover,
        "native_history_archive_binding": binding,
        "native_history_policy": policy,
        "native_history_policy_acknowledgement": {
            "policy": policy,
            "at_epoch": now - 60,
            "native_digest": fingerprint([]),
            "charging_started": False,
        },
    }
    rows = [(key, json.dumps(value)) for key, value in entries.items()]
    db.executemany("INSERT OR REPLACE INTO meta VALUES(?,?)", rows)
    db.execute(
        "INSERT INTO native_history_records VALUES(?,?,?,?,?,1)",
        (original["transactionId"], fingerprint(original), json.dumps(original), now - 60, now - 60),
    )
    return retain_timed_start(db, now)


# Intent: allow a stopped migrated journal only with intact retained history and a proved old timed Start.
# Why: ignoring a genuinely ambiguous receipt would let an older release reopen pending physical work.
async def test_migrated_snapshot_requires_intact_history_and_later_pause(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch
):
    seed_old_layout(tmp_path / "state.db")
    port = unused_tcp_port_factory
    async with process_rig(tmp_path, tls, port(), port(), monkeypatch, timing="fast-poll") as proxy:
        await proxy.start()
        status = await proxy.status()
        paused = await proxy.request("pause", revision=status["control_revision"])
        await proxy.completed_control(paused["result"]["id"])
        status = await proxy.status(lambda state: state["local_hold_verified"])
        await proxy.close()
        config = proxy.rig.prepared.config
        with closing(sqlite3.connect(config.journal)) as db, db:
            old_pause = retain_history(db, time.time())
        snapshot = proxy.durable()
        with stopped_journal(config.journal) as db:
            assert check_snapshot(db, config, migrated_only=True) == status["control_revision"]
        assert proxy.durable() == snapshot

        control = snapshot["metadata"]["control"]
        with closing(sqlite3.connect(config.journal)) as db, db:
            db.execute("UPDATE meta SET value=? WHERE key='control'", (json.dumps(control | {"resume_limit": 6.5}),))
        with (
            stopped_journal(config.journal) as db,
            pytest.raises(RollbackRefusedError, match="^settled_owner_pause_required$"),
        ):
            check_snapshot(db, config)
        with closing(sqlite3.connect(config.journal)) as db, db:
            db.execute("UPDATE meta SET value=? WHERE key='control'", (json.dumps(control),))
            db.execute("UPDATE managed_requests SET result='{}' WHERE id=?", (old_pause,))
        with stopped_journal(config.journal) as db, pytest.raises(RollbackRefusedError, match="^unresolved_request$"):
            check_snapshot(db, config)
        with closing(sqlite3.connect(config.journal)) as db, db:
            old_result = next(row["result"] for row in snapshot["requests"] if row["id"] == old_pause)
            db.execute("UPDATE managed_requests SET result=? WHERE id=?", (old_result, old_pause))
            db.execute("UPDATE native_history_records SET body='{}' WHERE original=1")
        with (
            stopped_journal(config.journal) as db,
            pytest.raises(RollbackRefusedError, match="^native_history_unverified$"),
        ):
            check_snapshot(db, config)
