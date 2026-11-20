"""Retired vehicle-pause state cannot enter either remaining commissioning mode."""

import copy
import sqlite3
from contextlib import closing

import pytest
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError

from tests.connected_rig import connected_bench
from tests.tls_support import tls as tls


# Intent: incomplete or malformed retired state is refused before a listener, observation or command can start.
# Why: removing an old recovery owner must not let its unresolved obligation become ordinary admission.
@pytest.mark.parametrize("bounded", [True])
@pytest.mark.parametrize(
    "record",
    [{}, [], "complete", {"stage": "planned"}, {"stage": "clearing"}, {"stage": "vehicle_restore_intent"}],
)
async def test_incomplete_retired_handover_refuses_startup(tmp_path, tls, unused_tcp_port, bounded, record):
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        with closing(Journal(rig.prepared.config.journal, rig.prepared.config.identity)) as journal:
            journal.put("connected_handover", record)
            before = [tuple(row) for row in journal.db.execute("SELECT * FROM meta ORDER BY key")]
        requests = list(rig.peer.requests)
        session = None
        try:
            with pytest.raises(SecurityError, match="retired vehicle-pause handover"):
                session = rig.session(bounded=bounded)
        finally:
            if session is not None:
                await session.close()
        assert rig.peer.requests == requests and rig.bench.device_calls == [] and rig.bench.calls == []
        with closing(Journal(rig.prepared.config.journal, rig.prepared.config.identity)) as journal:
            assert [tuple(row) for row in journal.db.execute("SELECT * FROM meta ORDER BY key")] == before
            assert journal.get("connected_handover") == record


# Intent: refusing an older installation leaves its interrupted delivery and lifecycle evidence intact.
# Why: merely opening the normal writable journal used to normalize these rows before the refusal.
@pytest.mark.parametrize("retired", ["plain", "vehicle_pause"])
async def test_refused_retired_journal_preserves_wire_state(tmp_path, tls, unused_tcp_port, retired):
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        path = rig.prepared.config.journal
        with closing(Journal(path, rig.prepared.config.identity)) as journal:
            if retired == "plain":
                journal.put("trial_phase", "active")
            else:
                journal.put("connected_handover", {"stage": "clearing"})
            journal.db.execute("INSERT INTO transactions(local,connector,start,live) VALUES(1001,1,'{}',1)")
            journal.db.execute(
                "INSERT INTO outbox(token,action,payload,local,state) "
                "VALUES('saved','StartTransaction','{}',1001,'inflight')"
            )
        error = RecoveryRequiredError if retired == "plain" else SecurityError
        with pytest.raises(error):
            rig.session(bounded=True)
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
            assert db.execute("SELECT action,state FROM outbox").fetchall() == [("StartTransaction", "inflight")]
            assert db.execute("SELECT local,live FROM transactions").fetchall() == [(1001, 1)]
        assert not rig.bench.device_calls


# Intent: a completed historical record remains byte-for-byte evidence in either remaining mode.
# Why: retiring the capability must not migrate completed history or change native-observation fingerprints.
@pytest.mark.parametrize("bounded", [True])
async def test_completed_retired_handover_remains_readable(tmp_path, tls, unused_tcp_port, bounded):
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        record = {"stage": "complete", "vehicle": {"old": [1, 2, 3]}, "sentinel": "preserved"}
        with closing(Journal(rig.prepared.config.journal, rig.prepared.config.identity)) as journal:
            journal.put("connected_handover", copy.deepcopy(record))
        session = rig.session(bounded=bounded)
        assert session.journal.get("connected_handover") == record
        assert session.runtime._server is None and rig.bench.device_calls == []


# Intent: every retired operator command is unsupported in the remaining session modes.
# Why: deleting a controller must leave no fallback that silently grants a different restoration capability.
@pytest.mark.parametrize("bounded", [True])
@pytest.mark.parametrize(
    "command",
    [
        "verify-car-pause",
        "connected-handover-plan",
        "connected-handover",
        "confirm-connected-original-backend",
        "confirm-connected-original-settings",
        "restore-connected-vehicle-permission",
        "confirm-unplugged-vehicle-permission",
    ],
)
async def test_retired_commands_have_no_fallback(tmp_path, tls, unused_tcp_port, bounded, command):
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        session = rig.session(bounded=bounded)
        requests = list(rig.peer.requests)
        with pytest.raises(SecurityError, match="not supported"):
            await session.command({"command": command})
        assert rig.peer.requests == requests and rig.bench.device_calls == [] and rig.bench.calls == []
