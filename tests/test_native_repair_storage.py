"""Real SQLite repair failures retain exact facts; replacement never invents the lost outcome."""

import copy
import sqlite3
import uuid
from contextlib import contextmanager

import pytest
from ocpp_hub_core.prototype.managed_native_repair import (
    ACK,
    AUDITS,
    REVOCATIONS,
    NativeRepairAudits,
    repair_pending,
    repair_revoked,
)
from ocpp_hub_core.prototype.managed_release_recovery import recoverable_fault
from ocpp_hub_core.prototype.native_faults import NativeContentError
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError, fingerprint

from tests.test_managed_proxy import healthy, starts
from tests.test_managed_release_recovery import explicit_release_pause, release_bench
from tests.test_ordinary_legacy_transport_recovery import legacy_fault
from tests.test_prototype_tls import tls as tls


# Refuse an actual BEGIN, statement or deferred COMMIT at one selected publication.
@contextmanager
def refused_write(j, key, failure, *, resource_update=False):
    if failure == "begin":
        # Deny a real BEGIN while retaining all reads and original stored facts.
        def authorize(action, first, second, database, source):
            return (
                sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_TRANSACTION and first == "BEGIN" else sqlite3.SQLITE_OK
            )

        j.db.set_authorizer(authorize)
    else:
        if failure == "commit":
            j.db.execute("CREATE TABLE repair_parent(id INTEGER PRIMARY KEY)")
            j.db.execute("CREATE TABLE repair_child(id REFERENCES repair_parent(id) DEFERRABLE INITIALLY DEFERRED)")
            effect = "INSERT INTO repair_child VALUES(123);"
        else:
            effect = "SELECT RAISE(ABORT,'independent repair storage unavailable');"
        target = (
            "BEFORE UPDATE OF observed_digest ON trial_resources"
            if resource_update
            else ("BEFORE INSERT ON meta WHEN NEW.key='" + key + "'")
        )
        j.db.execute("CREATE TEMP TRIGGER repair_refusal " + target + " BEGIN " + effect + " END")
    try:
        yield
    finally:
        j.db.set_authorizer(None)
        if failure != "begin":
            j.db.execute("DROP TRIGGER repair_refusal")


# These explicit stored-state inputs exercise audit atomicity, not a physical/native recovery claim.
def contract_journal(path):
    j = Journal(path, "synthetic-repair-contract")
    fault = {"classification": "native_authority_or_preservation_failed", "at_epoch": 1.0}
    diagnostic = {"type": "NativeTransportUnavailableError", "detail": "contract input", "at_epoch": 1.0}
    ack = {"fault": fault, "pause_request": "synthetic-pause", "revision": 1, "native_failure": diagnostic}
    for key, value in (("managed_control_fault", fault), ("managed_native_failure", diagnostic), (ACK, ack)):
        j.put(key, value)
    return j, fault, diagnostic, ack


# Intent: BEGIN, statement and COMMIT failure cannot leave half a fence before an independent read.
# Why: an audit that was never durably started cannot claim restart-safe outcome retention.
@pytest.mark.parametrize("failure", ["begin", "statement", "commit"])
def test_repair_fence_creation_is_atomic(tmp_path, failure):
    j, fault, diagnostic, ack = contract_journal(tmp_path / "fence.db")
    try:
        service = NativeRepairAudits(j, lambda value: value == fault)
        with refused_write(j, AUDITS, failure), pytest.raises(sqlite3.Error):
            service.begin("authority", 1, 1, "boot", 1)
        assert j.get(AUDITS) is None and j.get(REVOCATIONS) is None and not j.db.in_transaction
        assert j.get("managed_control_fault") == fault and j.get("managed_native_failure") == diagnostic
        assert j.get(ACK) == ack
        attempt = service.begin("authority", 2, 1, "boot", 1)
        service.finish(attempt, "availability")
        assert not repair_pending(j, fault) and not repair_revoked(j, fault) and j.get(ACK) == ack
    finally:
        j.close()


# Intent: live outcomes retry only their frozen facts; replacement keeps a genuinely unknown durable fence.
# Why: fresh normal data and another Pause cannot reconstruct a lost strict-versus-availability outcome.
@pytest.mark.parametrize("failure", ["begin", "statement", "commit"])
@pytest.mark.parametrize("outcome", ["strict", "availability"])
@pytest.mark.parametrize("replacement", [False, True])
def test_repair_failed_outcome_retry_or_replacement(tmp_path, failure, outcome, replacement):
    path = tmp_path / "outcome.db"
    j, fault, diagnostic, ack = contract_journal(path)
    try:
        service = NativeRepairAudits(j, lambda value: value == fault)
        attempt = service.begin("authority", 1, 1, "boot", 1)
        error = NativeContentError("configuration", "explicit contract strict input") if outcome == "strict" else None
        with refused_write(j, REVOCATIONS if outcome == "strict" else AUDITS, failure), pytest.raises(sqlite3.Error):
            service.finish(attempt, outcome, error)
        assert repair_pending(j, fault) and not repair_revoked(j, fault) and service.pending is not None
        assert j.get(ACK) == ack and not j.db.in_transaction
        if replacement:
            j.close()
            j = Journal(path, "synthetic-repair-contract")
            service = NativeRepairAudits(j, lambda value: value == fault)
            service.flush()
            assert repair_pending(j, fault) and not repair_revoked(j, fault) and j.get(ACK) == ack
            with pytest.raises(RecoveryRequiredError, match="prior audit outcome"):
                service.begin("new-authority", 2, 2, "new-boot", 9)
        else:
            service.flush()
            assert not repair_pending(j, fault) and repair_revoked(j, fault) is (outcome == "strict")
            assert j.get(ACK) == (None if outcome == "strict" else ack)
        assert j.get("managed_control_fault") == fault and j.get("managed_native_failure") == diagnostic
    finally:
        j.close()


# Intent: a strict outcome for an old fault cannot invalidate a different installed fault and acknowledgement.
# Why: classification and timestamps alone never transfer audit or Pause authority.
def test_repair_strict_publication_keeps_foreign_fault(tmp_path):
    j, fault, diagnostic, ack = contract_journal(tmp_path / "foreign.db")
    try:
        service = NativeRepairAudits(j, lambda value: value == fault)
        attempt = service.begin("authority", 1, 1, "boot", 1)
        other = {"classification": "owner_release_unconfirmed", "at_epoch": 2.0}
        other_ack = {**ack, "fault": other, "pause_request": "new-pause", "revision": 9}
        j.put("managed_control_fault", other)
        j.put(ACK, other_ack)
        service.finish(attempt, "strict", NativeContentError("history", "explicit contract strict input"))
        assert repair_revoked(j, fault) and not repair_revoked(j, other)
        assert j.get("managed_control_fault") == other and j.get(ACK) == other_ack
        assert j.get("managed_native_failure") == diagnostic
    finally:
        j.close()


# Intent: actual strict native content survives statement/COMMIT failure publishing its revocation.
# Why: restoring the peer and healing SQL must retry the frozen strict result rather than reread it away.
@pytest.mark.parametrize("failure", ["statement", "commit"])
@pytest.mark.parametrize("conflict", ["configuration", "history"])
async def test_actual_strict_result_survives_publication_refusal(
    tmp_path, tls, unused_tcp_port, monkeypatch, failure, conflict
):
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j, peer = b.c, b.session.journal, b.rig.peer
        fault = await legacy_fault(b, "NativeTransportUnavailableError")
        await explicit_release_pause(b)
        diagnostic, ack = copy.deepcopy(j.get("managed_native_failure")), copy.deepcopy(j.get(ACK))
        configuration, history = copy.deepcopy(peer.configuration), copy.deepcopy(b.rig.bench.history)
        if conflict == "configuration":
            peer.configuration["untouched"] = "independent native contradiction"
        else:
            b.rig.bench.history[0]["original_record"]["preserve"].append(4)
        with refused_write(j, REVOCATIONS, failure), pytest.raises(sqlite3.Error):
            await c.tick()
        assert repair_pending(j, fault) and not repair_revoked(j, fault) and j.get(ACK) == ack
        assert c.native.repair_audits.pending[1] == "strict"
        peer.configuration, b.rig.bench.history = configuration, history
        assert (await c.tick())["state"] == "attention"
        assert repair_revoked(j, fault) and not repair_pending(j, fault) and j.get(ACK) is None
        assert j.get("managed_control_fault") == fault and j.get("managed_native_failure") == diagnostic
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        for _ in range(3):
            assert (await c.tick())["state"] == "attention"
        assert not recoverable_fault(c, fault) and b.rig.bench.firmware.effective_limit() == 0 and not starts(b.rig)
        assert j.get(REVOCATIONS)[fingerprint(fault)]["typed_refusal"]["stage"] == conflict


# Intent: ordinary valid-content SQL refusal can retry the known live readiness outcome and exact Pause.
# Why: an observed storage wait is not a permanent native-content fault or an unknown replacement outcome.
@pytest.mark.parametrize("failure", ["statement", "commit"])
async def test_valid_native_publication_sql_wait_recovers_same_process(
    tmp_path, tls, unused_tcp_port, monkeypatch, failure
):
    async with release_bench(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j = b.c, b.session.journal
        fault = await legacy_fault(b, "NativeTransportUnavailableError")
        pause = await explicit_release_pause(b)
        diagnostic, ack = copy.deepcopy(j.get("managed_native_failure")), copy.deepcopy(j.get(ACK))
        with refused_write(j, "unused", failure, resource_update=True), pytest.raises(sqlite3.Error):
            await c.tick()
        assert repair_pending(j, fault) and not repair_revoked(j, fault) and j.get(ACK) == ack
        assert c.native.repair_audits.pending[1] == "readiness"
        assert j.get("managed_control_fault") == fault and j.get("managed_native_failure") == diagnostic
        assert (await healthy(c))["local_hold_verified"]
        assert c.requests.receipt(pause)["state"] == "completed" and j.get("managed_control_fault") is None
        c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
        assert (await healthy(c))["owner_control_enabled"] and b.rig.bench.firmware.effective_limit() == 6
        assert not starts(b.rig)
