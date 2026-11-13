"""A real third refusal remains restrictive when its actual rejection COMMIT fails."""

import asyncio
import json
import time
import uuid
from contextlib import AsyncExitStack

from ocpp_hub_core.prototype import managed_start_delivery
from ocpp_hub_core.prototype.managed_debt_recovery import KEY
from ocpp_hub_core.prototype.managed_start_delivery import RETRIES
from ocpp_hub_core.prototype.store import fingerprint
from ocpp_hub_core.prototype.trial_session import save_private

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import two_later_sessions
from tests.handover_helpers import crash
from tests.test_cloud_ack_storage import HeldReply, StorageBench, assert_retained, close_local, replacement, wire_count
from tests.test_managed_debt_reconciliation import strategy_file
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_stop_reply_recovery import pause
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tools import reconcile_managed_debt as operator


# Intent: a known exhausted wire budget takes zero before its failed rejection commit can be repaired.
# Why: retaining actual reply certainty must not leave unbounded positive authority after the third refusal.
async def test_exhausted_start_refusal_storage_failure_retains_zero_and_supported_recovery(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys
):
    monkeypatch.setattr(mp, "OwnerBench", StorageBench)
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    monkeypatch.setattr(managed_start_delivery, "RETRY_INTERVAL", 0.01)
    async with AsyncExitStack() as stack:
        b = await stack.enter_async_context(
            commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, 16, cli_route=True)
        )
        strategy = strategy_file(b.rig, tmp_path)
        j = b.session.journal
        b.rig.bench.start_replies = ["error", "error", "error", "Accepted"]
        first = HeldReply("StartTransaction", asyncio.Event(), asyncio.Event())
        b.rig.bench.held_reply = first
        answer = await b.rig.bench.call(
            b.charger,
            uuid.uuid4().hex,
            "StartTransaction",
            START
            | {
                "idTag": "NATIVE_TIMER",
                "timestamp": "2026-01-01T00:02:00Z",
            },
        )
        assert answer[0] == 3
        local = answer[2]["transactionId"]
        b.rig.peer.plan_session, b.rig.peer.permission_enabled = (local, 9), True
        b.rig.bench.cp, b.rig.bench.connection = "C", "Charging"
        b.rig.bench.firmware.fire_plan(9)
        assert b.rig.bench.firmware.current == 16
        await asyncio.wait_for(first.arrived.wait(), 3)
        second = HeldReply("StartTransaction", asyncio.Event(), asyncio.Event())
        b.rig.bench.held_reply = second
        first.release.set()
        await asyncio.wait_for(second.arrived.wait(), 3)
        assert j.get(RETRIES)[first.token]["errors"] == 1
        third = HeldReply("StartTransaction", asyncio.Event(), asyncio.Event())
        b.rig.bench.held_reply = third
        second.release.set()
        await asyncio.wait_for(third.arrived.wait(), 3)
        assert first.token == second.token == third.token
        assert j.get(RETRIES)[third.token]["errors"] == 2
        assert wire_count(b.rig, third.token) == 3 and not b.rig.bench.backend_starts
        j.db.execute("CREATE TABLE exhausted_parent(id INTEGER PRIMARY KEY)")
        j.db.execute("CREATE TABLE exhausted_child(id REFERENCES exhausted_parent(id) DEFERRABLE INITIALLY DEFERRED)")
        j.db.execute(
            "CREATE TRIGGER exhausted_refusal AFTER UPDATE ON outbox WHEN NEW.token='"
            + third.token
            + "' AND NEW.state='rejected' "
            "BEGIN INSERT INTO exhausted_child VALUES(1); END"
        )
        try:
            third.release.set()
            await until(lambda: b.session.runtime.status.delivery_blocked)
            assert b.session.runtime.status.delivery_blocked_cause == "storage"
            # No test-owned health turn runs before the independent peer confirms zero application.
            await until(
                lambda: (
                    b.rig.bench.firmware.effective_limit() == 0
                    and b.rig.bench.firmware.current == 0
                    and b.session.gateway.controls.ready
                )
            )
            assert j.get("managed_control_fault") and j.active() == local
            assert j.get(RETRIES)[third.token]["errors"] == 2 and not j.db.in_transaction
            assert_retained(j, third, saved=True)
            assert j.db.execute("SELECT COUNT(*) FROM exhausted_child").fetchone()[0] == 0
            heartbeats = sum(f[0] == 2 and f[2] == "Heartbeat" for f in b.rig.bench.calls)
            await until(
                lambda: sum(f[0] == 2 and f[2] == "Heartbeat" for f in b.rig.bench.calls) >= heartbeats + 1, timeout=3
            )
            assert wire_count(b.rig, third.token) == 3
        finally:
            third.release.set()
            j.db.execute("DROP TRIGGER exhausted_refusal")
            j.db.execute("DROP TABLE exhausted_child")
            j.db.execute("DROP TABLE exhausted_parent")
        await until(lambda: j.next_delivery().state == "rejected")
        assert j.get(RETRIES)[third.token]["errors"] == 3
        assert j.db.execute("SELECT COUNT(*) FROM delivery_replies").fetchone()[0] == 0
        assert wire_count(b.rig, third.token) == 3 and not b.rig.bench.backend_starts
        await close_local(b, local, 3)
        await pause(b.controller)
        assert j.get("managed_control_fault") and b.rig.bench.firmware.effective_limit() == 0
        await b.controller.tests.close()
        await crash(b.session)
        args, port = (tmp_path / "trial.json", strategy), b.rig.prepared.observer.port
        assert await operator.run("inspect", *args, None, port, kind="start-delivery") == 0
        candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
        assert candidate["delivery_token"] == third.token
        assert not any(row["token"] == third.token for row in b.rig.bench.backend_starts)
        evidence = candidate | {
            "cloud_start_not_accepted": True,
            "fault_origin_independently_verified": True,
            "original_state_independently_verified": True,
            "evidence_sha256": fingerprint(b.rig.bench.actual_start_replies),
            "verified_at_epoch": time.time(),
        }
        path = tmp_path / "actual-third-refusal-evidence.json"
        save_private(path, evidence)
        assert await operator.run("reconcile", *args, path, port, kind="start-delivery") == 0
        capsys.readouterr()
        # Only this exact independent nonacceptance permission now authorizes the fourth CALL.
        fresh = await stack.enter_async_context(replacement(b, "approved-after-exhausted-storage"))
        await until(lambda: fresh.session.journal.next_delivery() is None)
        assert wire_count(b.rig, third.token) == 4
        assert fresh.session.journal.vendor_id(local) == b.rig.bench.backend_starts[0]["answer"]["transactionId"]
        assert b.rig.bench.backend_starts[0]["closed"]
        await pause(fresh.controller)
        assert (await mp.healthy(fresh.controller))["local_hold_verified"]
        assert fresh.session.journal.get("managed_control_fault") is None
        assert all(entry["completed"] is not None for entry in fresh.session.journal.get(KEY).values())
        await two_later_sessions(fresh, selected=16, original_starts=0, first_minute=20)
        assert not b.rig.bench.backend_errors and all(row["closed"] for row in b.rig.bench.backend_starts)
