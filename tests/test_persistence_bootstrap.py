"""First bootstrap is atomic; only the exact hardware-untouched legacy baseline gap is repairable."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import ocpp_hub_core
import pytest
from ocpp_hub_core.prototype.rpc import InboundCache
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError
from ocpp_hub_core.prototype.trial_resources import ResourceBook
from ocpp_hub_core.prototype.trial_session import TrialSession

from tests.test_managed_proxy import healthy, introduced, managed_session
from tests.test_native_bounded_session import connected_bench
from tests.test_prototype_runtime import FAST
from tests.test_prototype_tls import tls as tls

CHILD = """
import json, os, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from tools.trial_proxy import load_core
load_core()
from ocpp_hub_core.prototype.store import Journal
from ocpp_hub_core.prototype.trial_resources import Baseline, ResourceBook
j = Journal(Path(sys.argv[2]), 'OFFLINE001')
def crash(sql):
    if sql.startswith('INSERT OR REPLACE INTO meta') and "'trial_phase'" in sql:
        os._exit(77)
j.db.set_trace_callback(crash)
ResourceBook(j, Baseline.parse(json.loads(Path(sys.argv[3]).read_text())))
raise SystemExit('phase boundary not reached')
"""


# Construct the production managed mode with its normal explicit connected/unplugged constraints.
def construct(rig):
    return TrialSession(
        rig.prepared, FAST, native_bounded=rig.spec, allow_local_start=False, allow_unplugged_native=True
    )


# Intent: process death immediately before phase insertion cannot leave a committed baseline without its phase.
# Why: both new initial metadata writes belong to one durable bootstrap transition, before any hardware effect.
async def test_phase_boundary_crash_rolls_back_first_baseline(tmp_path, tls, unused_tcp_port):
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        root = Path(ocpp_hub_core.__file__).parents[3]
        child = subprocess.run(
            [
                sys.executable,
                "-B",
                "-c",
                CHILD,
                str(root),
                str(rig.prepared.config.journal),
                str(rig.prepared.config.baseline),
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert child.returncode == 77, child.stderr
        journal = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        try:
            assert journal.get("trial_baseline") is None and journal.get("trial_phase") is None
            assert not journal.db.execute("SELECT 1 FROM transactions").fetchone()
            assert not journal.db.execute("SELECT 1 FROM mutations").fetchone()
        finally:
            journal.close()
        session = construct(rig)
        assert session.journal.get("trial_phase") == "prepared"
        assert session.journal.get("trial_baseline") == rig.prepared.baseline.value()
        assert not session.started and rig.bench.device_calls == []
        await session.close()


# Intent: the exact baseline-only state left by older code becomes a fresh admission with genuine Boot/native checks.
# Why: repairing metadata must neither fabricate hardware protection nor permanently lock out a pristine installation.
async def test_legacy_pristine_baseline_gap_reintroduces_over_real_tls(tmp_path, tls, unused_tcp_port):
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        journal = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        journal.put("trial_baseline", rig.prepared.baseline.value())
        InboundCache(journal=journal)
        journal.close()
        session = managed_session(rig)
        assert session.journal.get("trial_phase") == "prepared"
        assert session.gateway.charger.sender is None and not session.gateway.controls.ready
        assert rig.bench.device_calls == []
        await session.start()
        from ocpp_hub_core.prototype.managed import ManagedController

        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "repaired-bootstrap")
            assert (await healthy(controller))["owner_control_enabled"]
            assert rig.bench.received_limits[0] == 0 and rig.bench.received_limits[-1] == 6
            assert len(rig.peer.requests) > 0
            await session.close()


# Intent: unrelated metadata, receipt/debt rows or tables cannot turn an ordinary journal into connected admission.
# Why: a strict pristine repair must not grant fresh authority to an already used or ambiguous installation.
@pytest.mark.parametrize("blocker", ["metadata", "receipt", "outbox", "resource", "rpc", "clock", "unknown-table"])
async def test_legacy_baseline_repair_rejects_nonpristine_state(tmp_path, tls, unused_tcp_port, blocker):
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        journal = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        # Create the real current schema, then fixture only the historical missing-phase gap.
        ResourceBook(journal, rig.prepared.baseline)
        journal.db.execute("DELETE FROM meta WHERE key='trial_phase'")
        if blocker == "metadata":
            journal.put("control", {"owner": "shared"})
        elif blocker == "receipt":
            journal.db.execute("INSERT INTO receipts VALUES('old','StartTransaction','digest','{}')")
        elif blocker == "outbox":
            journal.enqueue("Heartbeat", {})
        elif blocker == "resource":
            journal.db.execute("INSERT INTO trial_resources VALUES('profile:1','{}',NULL,'pending',NULL)")
        elif blocker == "rpc":
            InboundCache(journal=journal)
            journal.db.execute("INSERT INTO inbound_receipts VALUES('vendor',1,'old','digest',NULL)")
        elif blocker == "clock":
            journal.db.execute(
                "INSERT INTO transactions(local,connector,start,stop,vendor) VALUES(1001,1,'{}','{}',9001)"
            )
            journal.db.execute("INSERT INTO transaction_clock_discontinuities VALUES(1001,'raw-start','raw-stop',1)")
        else:
            journal.db.execute("CREATE TABLE unrelated_ordinary_state(value TEXT)")
        before = sorted(journal.db.iterdump())
        journal.close()
        with pytest.raises((SecurityError, RecoveryRequiredError)):
            construct(rig)
        journal = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        try:
            assert journal.get("trial_phase") is None
            assert sorted(journal.db.iterdump()) == before
        finally:
            journal.close()
        assert rig.bench.device_calls == []
