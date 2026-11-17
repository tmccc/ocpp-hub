"""A required native contract refuses retired journals before resource or network setup."""

from contextlib import closing

import pytest
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError
from ocpp_hub_core.prototype.trial_session import TrialSession

from tests.test_native_bounded_session import connected_bench
from tests.test_prototype_runtime import FAST
from tests.test_prototype_tls import tls as tls


# Intent: a missing or unparsed specification cannot select a default session mode or create a journal.
# Why: retiring plain commissioning must make stale callers fail before native observations or listener startup.
@pytest.mark.parametrize("value", [None, {}, "saved-spec", True])
async def test_explicit_native_specification_is_required(tmp_path, tls, unused_tcp_port, value):
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        requests = list(rig.peer.requests)
        session = None
        try:
            with pytest.raises(SecurityError, match="native bounded specification"):
                session = TrialSession(rig.prepared, FAST, native_bounded=value)
        finally:
            if session is not None:
                await session.close()
        assert not rig.prepared.config.journal.exists()
        assert rig.peer.requests == requests and rig.bench.device_calls == [] and rig.bench.calls == []


# Intent: obsolete positional observation callbacks cannot bind silently to the remaining native observation input.
# Why: changing a constructor signature must not substitute the wrong kind of evidence into an active safety check.
async def test_old_positional_callback_is_rejected(tmp_path, tls, unused_tcp_port):
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        with pytest.raises(TypeError):
            TrialSession(rig.prepared, FAST, rig.prepared.observer.observe, native_bounded=rig.spec)
        assert not rig.prepared.config.journal.exists() and not rig.bench.device_calls


# Intent: every past-admission plain journal refuses the new mode without changing its rows, tables or evidence.
# Why: an explicit specification cannot turn unfinished old recovery or cleanup into a newly admitted installation.
@pytest.mark.parametrize(
    "phase", ["active", "recovering", "blocked", "rollback", "backend_restore_pending", "complete"]
)
@pytest.mark.parametrize("before_boot", [False, True])
async def test_plain_journal_requires_its_original_recovery(tmp_path, tls, unused_tcp_port, phase, before_boot):
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        with closing(Journal(rig.prepared.config.journal, rig.prepared.config.identity)) as journal:
            journal.put("trial_phase", phase)
            journal.put("trial_starting", before_boot)
            journal.put("sentinel", {"original": [1, 2, 3]})
            tables = [tuple(row) for row in journal.db.execute("SELECT * FROM sqlite_master ORDER BY name")]
            rows = [tuple(row) for row in journal.db.execute("SELECT * FROM meta ORDER BY key")]
        requests = list(rig.peer.requests)
        with pytest.raises(RecoveryRequiredError, match="ordinary journal"):
            rig.session()
        assert rig.sessions == [] and rig.peer.requests == requests
        assert not rig.bench.device_calls and not rig.bench.calls
        with closing(Journal(rig.prepared.config.journal, rig.prepared.config.identity)) as journal:
            assert [tuple(row) for row in journal.db.execute("SELECT * FROM sqlite_master ORDER BY name")] == tables
            assert [tuple(row) for row in journal.db.execute("SELECT * FROM meta ORDER BY key")] == rows


# Intent: a merely prepared journal still undergoes full native admission rather than being labelled recovered.
# Why: prepared is common provenance; the native controller must reserve allocation and verify its actual baseline.
async def test_prepared_journal_uses_fresh_native_admission(tmp_path, tls, unused_tcp_port):
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        with closing(Journal(rig.prepared.config.journal, rig.prepared.config.identity)) as journal:
            journal.put("trial_phase", "prepared")
        session = rig.session()
        assert session.journal.get("native_bounded_handover") is None
        await session.start()
        assert session.journal.get("native_bounded_handover")["stage"] == "admitted"
        assert session.journal.get("trial_starting")
        assert session.journal.get("trial_binding") is None
        assert not rig.bench.device_calls


# Intent: a commissioning run without its explicit native specification fails before any file or network access.
# Why: the remaining CLI must never silently select the retired mode.
async def test_cli_run_requires_specification_before_preflight(tmp_path):
    from tools.trial_proxy import main, run

    missing = tmp_path / "does-not-exist.json"
    with pytest.raises(SecurityError, match="native-bounded-spec"):
        await run(missing, 60, False)
    with pytest.raises(SystemExit) as error:
        main(["run", "--config", str(missing)])
    assert error.value.code == 2 and list(tmp_path.iterdir()) == []
