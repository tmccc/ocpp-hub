"""Accepted cloud Start debt needs independent exact-command settlement before a new Pause and Return."""

import json
import sqlite3
import time
import uuid

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_debt_recovery import KEY
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError, fingerprint
from ocpp_hub_core.prototype.trial_session import save_private

from tests.handover_helpers import crash
from tests.test_managed_debt_reconciliation import strategy_file
from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session, starts
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tools import reconcile_managed_debt as operator


# A genuine accepted lifecycle is the normal control; its actual Stop spends the grant without operator evidence.
async def cloud_lifecycle(rig, session, charger, identifier):
    reply = await rig.bench.cloud_call(identifier, "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"})
    assert reply[0] == 3 and reply[2] == {"status": "Accepted"}
    opened = await rig.bench.call(charger, identifier + "-tx", "StartTransaction", START | {"idTag": "CLOUD_OWNER"})
    assert opened[0] == 3
    await rig.bench.call(
        charger,
        identifier + "-stop",
        "StopTransaction",
        {"transactionId": opened[2]["transactionId"], "timestamp": START["timestamp"], "meterStop": 1100},
    )
    await until(lambda: session.journal.active() is None and session.journal.next_delivery() is None)
    assert not session.journal.get("vendor_start_grant")


# Intent: a genuine accepted/closed cloud lifecycle needs no evidence tool and leaves ordinary Pause available.
# Why: repairing an unbound grant must preserve normal one-shot command binding and retirement.
async def test_completed_vendor_start_needs_no_operator(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        c = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "vendor-normal-boot")
            await healthy(c)
            await cloud_lifecycle(rig, session, charger, "ordinary-cloud-start")
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            assert (await healthy(c))["local_hold_verified"]
            assert len(starts(rig)) == 1 and not session.journal.get(KEY)


# Intent: exact independent evidence retires only the accepted unbound cloud command, retaining zero and fresh intent.
# Why: expiry, closed native state and a replayed old Pause cannot prove that a previously accepted Start is gone.
async def test_unbound_vendor_start_requires_evidence_and_new_pause(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys
):
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        strategy = strategy_file(rig, tmp_path)
        config, native_port = tmp_path / "trial.json", rig.prepared.observer.port
        session = managed_session(rig)
        await session.start()
        c = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "vendor-pending-boot")
            await healthy(c)
            reply = await rig.bench.cloud_call(
                "unbound-cloud-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}
            )
            assert reply[0] == 3 and reply[2]["status"] == "Accepted"
            j = session.journal
            grant = j.get("vendor_start_grant")
            mutation = grant["mutation"]
            assert j.db.execute("SELECT state FROM mutations WHERE token=?", (mutation,)).fetchone()[0] == "confirmed"
            j.put("vendor_start_grant", {**grant, "expires": 0})
            pause = uuid.uuid4().hex
            c.command(pause, "pause", c.requests.intent().revision)
            await c.pause_task
            for _ in range(3):
                assert (await c.tick())["state"] == "reconciling"
            assert j.get("vendor_start_grant") and c.requests.receipt(pause)["state"] == "accepted"
            assert rig.bench.firmware.effective_limit() == 0 and len(starts(rig)) == 1
            j.capacity = max(2, j.db.execute("SELECT COUNT(*) FROM mutations").fetchone()[0])
            for value in range(3):
                extra = j.mutation_intent("local", "SetChargingProfile", {"retention_control": value})
                j.mutation_finished(extra, True)
            assert j.db.execute("SELECT 1 FROM mutations WHERE token=?", (mutation,)).fetchone()
            revision = c.requests.intent().revision
            counter = j.get("next_transaction")
            await c.tests.close()
            await crash(session)
        before = list(rig.bench.device_calls)
        assert await operator.run("inspect", config, strategy, None, native_port, kind="vendor-start") == 0
        candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
        assert candidate["mutation_token"] == mutation and candidate["grant_token"] == grant["token"]
        evidence = candidate | {
            "command_no_longer_pending": True,
            "original_state_independently_verified": True,
            "evidence_sha256": fingerprint({"independent_command_cancellation": candidate}),
            "verified_at_epoch": time.time(),
        }
        path = tmp_path / "vendor-start-evidence.json"
        for changed in (
            {"command_no_longer_pending": False},
            {"command_no_longer_pending": 1},
            {"original_state_independently_verified": False},
            {"grant_digest": "0" * 64},
            {"grant_token": "0" * 32},
            {"mutation_token": "0" * 32},
            {"payload_digest": "0" * 64},
            {"candidate_digest": "0" * 64},
            {"verified_at_epoch": time.time() - 7200},
        ):
            save_private(path, evidence | changed)
            with pytest.raises((SecurityError, RecoveryRequiredError)):
                await operator.run("reconcile", config, strategy, path, native_port, kind="vendor-start")
        save_private(path, evidence)
        j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        original = j.get("vendor_start_grant")
        j.db.execute(
            "CREATE TRIGGER fail_vendor_start_proof BEFORE INSERT ON meta "
            "WHEN NEW.key='managed_operator_debt_reconciliations' "
            "BEGIN SELECT RAISE(ABORT,'synthetic proof failure'); END"
        )
        j.close()
        with pytest.raises(sqlite3.IntegrityError, match="synthetic proof failure"):
            await operator.run("reconcile", config, strategy, path, native_port, kind="vendor-start")
        j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        assert j.get("vendor_start_grant") == original and j.get(KEY) is None
        j.db.execute("DROP TRIGGER fail_vendor_start_proof")
        j.close()
        assert await operator.run("reconcile", config, strategy, path, native_port, kind="vendor-start") == 0
        assert await operator.run("reconcile", config, strategy, path, native_port, kind="vendor-start") == 0
        j = Journal(rig.prepared.config.journal, rig.prepared.config.identity, capacity=2)
        for value in range(4):
            extra = j.mutation_intent("local", "SetChargingProfile", {"proof_retention_control": value})
            j.mutation_finished(extra, True)
        assert j.db.execute("SELECT 1 FROM mutations WHERE token=?", (mutation,)).fetchone()
        j.close()
        assert rig.bench.device_calls == before and rig.bench.firmware.effective_limit() == 0
        replacement = managed_session(rig)
        await replacement.start()
        c = ManagedController(replacement)
        async with rig.charger(replacement) as charger:
            await introduced(rig, replacement, charger, "vendor-reconciled-boot")
            assert (await c.tick())["state"] == "attention"
            assert replacement.journal.get("next_transaction") == counter
            c.command(pause, "pause", revision - 1)
            assert (await c.tick())["state"] == "attention"
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            if c.pause_task is not None:
                await c.pause_task
            assert (await healthy(c))["local_hold_verified"]
            assert rig.bench.firmware.effective_limit() == 0 and len(starts(rig)) == 1
            request = uuid.uuid4().hex
            c.command(request, "app_control", c.requests.intent().revision)
            assert (await healthy(c))["station_ceiling_a"] == 6
            assert c.requests.receipt(request)["state"] == "completed"
            await cloud_lifecycle(rig, replacement, charger, "after-operator-start")
            assert len(starts(rig)) == 2
