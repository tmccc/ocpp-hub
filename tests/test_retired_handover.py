"""Retired vehicle-pause state cannot enter either remaining commissioning mode."""

import copy
from contextlib import closing

import pytest
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import Journal

from tests.test_native_bounded_session import connected_bench
from tests.test_prototype_tls import tls as tls


# Intent: incomplete or malformed retired state is refused before a listener, observation or command can start.
# Why: removing an old recovery owner must not let its unresolved obligation become ordinary admission.
@pytest.mark.parametrize("bounded", [False, True])
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


# Intent: a completed historical record remains byte-for-byte evidence in either remaining mode.
# Why: retiring the capability must not migrate completed history or change native-observation fingerprints.
@pytest.mark.parametrize("bounded", [False, True])
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
@pytest.mark.parametrize("bounded", [False, True])
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
