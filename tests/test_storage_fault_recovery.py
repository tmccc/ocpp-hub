"""Explicit offline storage provenance repairs only a debt-free legacy latch at actual native zero."""

import copy
import json
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager, closing

import pytest
from ocpp_hub_core.prototype.managed_debt_recovery import KEY
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError, fingerprint
from ocpp_hub_core.prototype.trial_session import save_private

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench, two_later_sessions
from tests.test_managed_debt_reconciliation import strategy_file
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_stop_reply_recovery import pause, running
from tests.test_managed_test_charge import ChargeRig, positive_flow
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests_model.test_start_recovery_sequences import close_daily
from tools import reconcile_managed_debt as operator

KIND = "storage-fault"


# Observe every protected durable preimage without opening another journal owner or normalizing its queue.
def durable(path):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        return {
            "metadata": list(db.execute("SELECT key,value FROM meta ORDER BY key")),
            "history": list(db.execute("SELECT * FROM transactions ORDER BY local")),
            "outbox": list(db.execute("SELECT * FROM outbox ORDER BY seq")),
            "mutations": list(db.execute("SELECT * FROM mutations ORDER BY token")),
            "requests": list(db.execute("SELECT * FROM managed_requests ORDER BY id")),
        }


# Real TLS establishes the old receipt/history; only the explicitly described legacy diagnostic is seeded.
@asynccontextmanager
async def stopped_storage_bench(tmp_path, tls, unused_tcp_port, monkeypatch, *, wire_debt=None):
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with mp.managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        async with running(rig, "legacy-storage-input") as (s, c, sock):
            await mp.healthy(c)
            charge = ChargeRig(rig, s, c, sock)
            local = await positive_flow(charge, "daily", call_id=uuid.uuid4().hex)
            await mp.healthy(c)
            await close_daily(charge, local, "native", timestamp="2026-01-01T00:01:00Z")
            report = await rig.bench.call(
                sock,
                uuid.uuid4().hex,
                "StatusNotification",
                {"connectorId": 0, "status": "Available", "errorCode": "NoError"},
            )
            assert report[0] == 3
            await until(lambda: s.journal.next_delivery() is None)
            if wire_debt is not None:
                # Keep a real missing-reply timeout, shortened only for this isolated external fault input.
                if wire_debt == "unknown-start":
                    s.gateway.vendor.timeout = 0.1
                rig.bench.start_replies = ["silent"] if wire_debt == "unknown-start" else ["Accepted"]
                response = await rig.bench.call(
                    sock,
                    uuid.uuid4().hex,
                    "StartTransaction",
                    START | {"idTag": "NATIVE_TIMER", "meterStart": 1200, "timestamp": "2026-01-01T00:02:00Z"},
                )
                assert response[0] == 3 and response[2]["idTagInfo"]["status"] == "Accepted"
                second = response[2]["transactionId"]
                if wire_debt == "refused-stop":
                    await until(lambda: s.journal.vendor_id(second) is not None and s.journal.next_delivery() is None)
                    rig.bench.stop_replies = ["error"]
                else:
                    await until(
                        lambda: s.journal.next_delivery() is not None and s.journal.next_delivery().state == "ambiguous"
                    )
                assert (
                    await rig.bench.call(
                        sock,
                        uuid.uuid4().hex,
                        "StopTransaction",
                        {"transactionId": second, "meterStop": 1300, "timestamp": "2026-01-01T00:03:00Z"},
                    )
                )[0] == 3
                if wire_debt == "refused-stop":
                    await until(
                        lambda: s.journal.next_delivery() is not None and s.journal.next_delivery().state == "rejected"
                    )
                await until(lambda: s.runtime.status.delivery_blocked)
            old_pause, old_revision = await pause(c)
            if s.journal.get("managed_control_fault") is None:
                c.fault("owner_control_or_delivery_uncertain")
            fault = copy.deepcopy(s.journal.get("managed_control_fault"))
            assert set(fault) == {"classification", "at_epoch"}
            assert fault["classification"] == "owner_control_or_delivery_uncertain"
            # This is an explicit old-version input, not a claim that current runtime creates this legacy fault.
            s.journal.put(
                "managed_native_failure",
                {"type": "OperationalError", "detail": "synthetic legacy ACK SQLite failure", "at_epoch": time.time()},
            )
            await c.hold_fault()
            assert rig.bench.firmware.effective_limit() == 0 and s.journal.active() is None
            assert (await c.tick())["state"] == "attention"
        config = tmp_path / "trial.json"
        strategy = strategy_file(rig, tmp_path)
        yield rig, config, strategy, fault, old_pause, old_revision, local


# Inspection yields correlation digests only; the statement adds explicit independently established facts.
async def inspected_statement(rig, config, strategy, capsys):
    assert await operator.run("inspect", config, strategy, None, rig.prepared.observer.port, kind=KIND) == 0
    candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
    assert set(candidate) == {"identity_digest", "spec_digest", "candidate_digest", "fault_digest", "diagnostic_digest"}
    statement = candidate | {
        "command_quiescent": True,
        "fault_origin_independently_verified": True,
        "original_state_independently_verified": True,
        "evidence_sha256": fingerprint({"independent_legacy_storage_origin": candidate}),
        "verified_at_epoch": time.time(),
    }
    return candidate, statement


# Intent: only explicit exact provenance records a proof; a new verified Pause consumes it before separate Return.
# Why: empty outbox and physical zero cannot themselves establish the cause of an old generic fault.
async def test_storage_fault_requires_attestation_then_new_pause_and_two_later_cycles(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys
):
    async with stopped_storage_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as inputs:
        rig, config, strategy, fault, old_pause, old_revision, _ = inputs
        native_port, path = rig.prepared.observer.port, tmp_path / "storage-evidence.json"
        candidate, statement = await inspected_statement(rig, config, strategy, capsys)
        assert candidate["fault_digest"] == fingerprint(fault)
        before, commands = durable(rig.prepared.config.journal), list(rig.bench.device_calls)
        with pytest.raises(SecurityError):
            await operator.run("reconcile", config, strategy, None, native_port, kind=KIND)
        for field in (
            "command_quiescent",
            "fault_origin_independently_verified",
            "original_state_independently_verified",
        ):
            for value in (None, False, 1):
                invalid = dict(statement)
                if value is None:
                    invalid.pop(field)
                else:
                    invalid[field] = value
                save_private(path, invalid)
                with pytest.raises(SecurityError):
                    await operator.run("reconcile", config, strategy, path, native_port, kind=KIND)
                assert durable(rig.prepared.config.journal) == before
        for changed in (
            {"fault_digest": "0" * 64},
            {"diagnostic_digest": "0" * 64},
            {"candidate_digest": "0" * 64},
            {"verified_at_epoch": time.time() - 7200},
        ):
            save_private(path, statement | changed)
            with pytest.raises((SecurityError, RecoveryRequiredError)):
                await operator.run("reconcile", config, strategy, path, native_port, kind=KIND)
            assert durable(rig.prepared.config.journal) == before
        save_private(path, statement)
        assert await operator.run("reconcile", config, strategy, path, native_port, kind=KIND) == 0
        assert await operator.run("reconcile", config, strategy, path, native_port, kind=KIND) == 0
        assert rig.bench.device_calls == commands and rig.bench.firmware.effective_limit() == 0
        j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        try:
            entries = j.get(KEY)
            assert len(entries) == 1
            entry = next(iter(entries.values()))
            assert entry["completed"] is None and entry["proof"]["kind"] == KIND
            assert entry["proof"]["prior"]["fault"] == fault
            assert j.get("managed_control_fault")["classification"] == "operator_debt_reconciled_at_zero"
        finally:
            j.close()
        async with running(rig, "storage-proof-replacement") as (s, c, sock):
            assert (await c.tick())["state"] == "attention"
            c.command(old_pause, "pause", old_revision)
            assert (await c.tick())["state"] == "attention"
            assert next(iter(s.journal.get(KEY).values()))["completed"] is None
            new_pause, _ = await pause(c)
            assert (await mp.healthy(c))["local_hold_verified"]
            assert s.journal.get("managed_control_fault") is None
            assert next(iter(s.journal.get(KEY).values()))["completed"]["pause"] == new_pause
            assert rig.bench.firmware.effective_limit() == 0 and mp.starts(rig) == []
            c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
            assert (await mp.healthy(c))["station_ceiling_a"] == 6
            await two_later_sessions(ChargeRig(rig, s, c, sock), selected=6, original_starts=0)
            assert s.journal.next_delivery() is None and not s.journal.get("managed_control_fault")
            assert len(rig.bench.backend_starts) == 3 and all(row["closed"] for row in rig.bench.backend_starts)


# Intent: a changed diagnostic or actual immutable lifecycle invalidates an already inspected statement.
# Why: a valid operator statement for one old fault cannot authorize a later fault/history preimage.
@pytest.mark.parametrize("changed", ["diagnostic", "history"])
async def test_storage_fault_statement_pins_diagnostic_and_history(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys, changed
):
    async with stopped_storage_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as inputs:
        rig, config, strategy, _, _, _, local = inputs
        _, statement = await inspected_statement(rig, config, strategy, capsys)
        path = tmp_path / "storage-evidence.json"
        save_private(path, statement)
        j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        try:
            if changed == "diagnostic":
                j.put(
                    "managed_native_failure", {**j.get("managed_native_failure"), "detail": "a later distinct failure"}
                )
            else:
                row = j.db.execute("SELECT stop FROM transactions WHERE local=?", (local,)).fetchone()
                stop = json.loads(row["stop"])
                # Deliberately model a changed persisted preimage; the operator must not bless or rewrite it.
                j.db.execute(
                    "UPDATE transactions SET stop=? WHERE local=?", (json.dumps(stop | {"meterStop": 1101}), local)
                )
        finally:
            j.close()
        before, commands = durable(rig.prepared.config.journal), list(rig.bench.device_calls)
        with pytest.raises((SecurityError, RecoveryRequiredError)):
            await operator.run("reconcile", config, strategy, path, rig.prepared.observer.port, kind=KIND)
        assert durable(rig.prepared.config.journal) == before and rig.bench.device_calls == commands


# Intent: real unknown Start and refused Stop retain their exact lifecycle recovery routes despite storage attestation.
# Why: independently attesting storage provenance cannot turn actual unresolved wire outcomes into settled history.
@pytest.mark.parametrize("wire_debt", ["unknown-start", "refused-stop"])
async def test_storage_fault_refuses_actual_wire_debt(tmp_path, tls, unused_tcp_port, monkeypatch, wire_debt):
    async with stopped_storage_bench(tmp_path, tls, unused_tcp_port, monkeypatch, wire_debt=wire_debt) as inputs:
        rig, config, strategy, _, _, _, _ = inputs
        before, commands = durable(rig.prepared.config.journal), list(rig.bench.device_calls)
        with pytest.raises((SecurityError, RecoveryRequiredError)):
            await operator.run("inspect", config, strategy, None, rig.prepared.observer.port, kind=KIND)
        assert durable(rig.prepared.config.journal) == before and rig.bench.device_calls == commands


# Intent: persisted unrelated grants, owner work, current/native faults and matching proof ownership all refuse.
# Why: neither apparent age nor completed-looking metadata can exempt an uncertain management request or foreign fault.
@pytest.mark.parametrize(
    "debt",
    [
        "active-grant",
        "uncertain-owner",
        "historical-uncertain-owner",
        "pending-current",
        "current-native-fault",
        "native-authority-fault",
        "preservation-fault",
        "matching-delivery-proof",
        "matching-denial-proof",
        "matching-stop-proof",
        "pending-local-receipt",
    ],
)
async def test_storage_fault_refuses_other_persisted_authority(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys, debt
):
    async with stopped_storage_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as inputs:
        rig, config, strategy, fault, _, _, local = inputs
        _, statement = await inspected_statement(rig, config, strategy, capsys)
        path = tmp_path / "storage-evidence.json"
        save_private(path, statement)
        j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        try:
            identifier = uuid.uuid4().hex
            if debt == "active-grant":
                j.put("vendor_start_grant", {"mutation": identifier, "local": None, "stage": "accepted"})
            elif debt in ("uncertain-owner", "historical-uncertain-owner"):
                j.db.execute("INSERT INTO managed_requests VALUES(?,?,?,'uncertain','{}')", (identifier, "start", 0))
                if debt == "historical-uncertain-owner":
                    j.put(
                        "managed_test_charge",
                        {"id": identifier, "kind": "normal", "state": "completed", "local_transaction": local},
                    )
                    j.put("local_start_receipt", {"request_id": identifier, "stage": "accepted", "local": local})
            elif debt == "pending-current":
                j.put(
                    "managed_current_change", {"id": identifier, "state": "uncertain", "previous_a": 6, "current_a": 16}
                )
            elif debt == "current-native-fault":
                j.put("managed_current_interrupted_fault", {"id": identifier, "fault": fault})
            elif debt == "native-authority-fault":
                j.put(
                    "managed_control_fault",
                    {"classification": "native_authority_or_preservation_failed", "at_epoch": time.time()},
                )
            elif debt == "preservation-fault":
                j.put("trial_preservation_fault", {"classification": "changed_native_configuration"})
            elif debt == "matching-delivery-proof":
                j.put(
                    "managed_delivery_reconciliations",
                    {identifier: {"proof": {"prior_fault": fault}, "completed": None}},
                )
            elif debt == "matching-denial-proof":
                j.put("managed_start_denials", {str(local): {"fault": fault, "completed": None}})
            elif debt == "matching-stop-proof":
                j.put("managed_stop_wire_acknowledgment", {"fault": fault, "local_transaction_id": local})
            else:
                j.put("local_start_receipt", {"request_id": identifier, "stage": "issued", "local": None})
        finally:
            j.close()
        before, commands = durable(rig.prepared.config.journal), list(rig.bench.device_calls)
        with pytest.raises((SecurityError, RecoveryRequiredError)):
            await operator.run("inspect", config, strategy, None, rig.prepared.observer.port, kind=KIND)
        with pytest.raises((SecurityError, RecoveryRequiredError)):
            await operator.run("reconcile", config, strategy, path, rig.prepared.observer.port, kind=KIND)
        assert durable(rig.prepared.config.journal) == before and rig.bench.device_calls == commands
