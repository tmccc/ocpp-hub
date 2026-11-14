"""Diagnostic projections inspect real retained state without acquiring control or manufacturing confirmation."""

import copy
import json
import sqlite3
import uuid

import pytest
from ocpp_hub_core.prototype.managed_diagnostics import snapshot
from ocpp_hub_core.prototype.managed_native_repair import AUDITS, REVOCATIONS, NativeRepairAudits
from ocpp_hub_core.prototype.managed_view import health_snapshot
from ocpp_hub_core.prototype.native_faults import NativeContentError
from ocpp_hub_core.prototype.store import fingerprint

from tests.test_managed_current import adjustable
from tests.test_managed_proxy import healthy
from tests.test_managed_service_cli import healthy_status
from tests.test_prototype_tls import tls as tls
from tools import managed_proxy
from tools.managed_reporting import Reporting


# Intent: a cloud outage and a simultaneous local verification failure both remain visible without changing health.
# Why: the connection symptom cannot hide an independent recovery/preservation/native-limit prerequisite.
@pytest.mark.parametrize("change", [{"phase": "recovering"}, {"preservation_failed": True}, {"native_limit_a": 32}])
def test_health_explanation_retains_local_failure_during_cloud_outage(change):
    result = health_snapshot(healthy_status(10000) | {"cloud_connected": False} | change, 10000)
    assert result["healthy"] is False and result["local_healthy"] is False
    assert set(result["health_blockers"]) == {"cloud_disconnected", "local_health_unverified"}


# Intent: strict/unknown audit markers affect only their exact fault, without authorizing any recovery action.
# Why: diagnostic text must preserve the distinction between a measured fault and a permanently stricter refusal.
@pytest.mark.parametrize("kind", ["healthy", "measured", "strict", "unknown", "unrelated"])
async def test_fault_diagnostics_are_read_only_and_bound_to_current_fault(
    tmp_path, tls, unused_tcp_port, monkeypatch, kind
):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        status = await healthy(c)
        if kind != "healthy":
            c.fault("measured_output_limit_exceeded")
            fault = j.get("managed_control_fault")
            if kind in ("strict", "unknown", "unrelated"):
                service = NativeRepairAudits(j, lambda value: value == fault)
                attempt = service.begin("diagnostic-test", 1, 1, "test-boot", c.requests.intent().revision)
                if kind != "unknown":
                    service.finish(attempt, "strict", NativeContentError("configuration", "synthetic strict input"))
                    if kind == "unrelated":
                        j.put(
                            REVOCATIONS,
                            {fingerprint({"classification": "different"}): j.get(REVOCATIONS)[fingerprint(fault)]},
                        )
        before = j.db.total_changes, j.get("control"), c.requests.intent(), list(b.rig.bench.device_calls)
        result = snapshot(c, status, now=10000)
        assert result["available"] and result["pending_commands"] == []
        assert (j.db.total_changes, j.get("control"), c.requests.intent(), b.rig.bench.device_calls) == before
        assert "pause" not in result and "can_return" not in result
        if kind == "healthy":
            assert result["original_fault"] is None and result["recovery"] == "not_assessed"
        else:
            assert result["original_fault"]["code"] == "measured_output_limit_exceeded"
            assert result["original_fault"]["first_at"] == j.get("managed_control_fault")["at_epoch"]
            assert result["recovery"] == {
                "strict": "strict_refusal",
                "unknown": "audit_outcome_unknown",
            }.get(kind, "not_assessed")


# Intent: real read refusal produces unavailable detail and later recovers without changing any receipt or command.
# Why: a diagnostic SQL error cannot escape into supervision or appear as an empty successful diagnostic result.
async def test_optional_diagnostic_sql_read_failure_is_honest_and_recovers(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        status = await healthy(c)
        before = j.db.total_changes, list(b.rig.bench.device_calls)
        attempts = []

        # SQLite itself rejects the optional receipt read, including already prepared statements.
        def deny(action, first, second, database, trigger):
            if action == sqlite3.SQLITE_READ and first == "managed_requests":
                attempts.append(first)
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        j.db.set_authorizer(deny)
        try:
            result = snapshot(c, status)
            assert attempts and result["available"] is False and "blockers" not in result
        finally:
            j.db.set_authorizer(None)
        assert snapshot(c, status)["available"]
        assert (j.db.total_changes, b.rig.bench.device_calls) == before


# Intent: pending receipt time is known only when retained and the list remains bounded without copying private results.
# Why: old running/replacement results have no reliable acceptance timestamp and never establish physical success.
async def test_pending_diagnostics_keep_uncertainty_missing_times_and_bounds(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        status = await healthy(c)
        for index in range(7):
            j.db.execute(
                "INSERT INTO managed_requests VALUES(?,?,?,?,?)",
                (
                    uuid.uuid4().hex,
                    "pause",
                    index,
                    "uncertain" if index % 2 else "running",
                    json.dumps({"accepted_at": 9990} if index == 6 else {"private": "DO_NOT_COPY"}),
                ),
            )
        before = j.db.total_changes
        value = snapshot(c, status, 10000)
        assert value["pending_command_count"] == 7 and value["commands_truncated"]
        assert len(value["pending_commands"]) == 5
        assert value["pending_commands"][0]["age_seconds"] == 10
        assert value["pending_commands"][1]["age_seconds"] is None
        assert value["pending_commands"][1]["confirmation"] == "unknown"
        assert "DO_NOT_COPY" not in json.dumps(value) and j.db.total_changes == before


# Intent: corrupt optional metadata and deeply nested receipt JSON report unavailable without escaping supervision.
# Why: a conservative control refusal is not evidence of a specific observed fault, and diagnostic parsing is optional.
@pytest.mark.parametrize(
    "corrupt", ["audit-map", "refusal-entry", "receipt-json", "version-bool", "version-float", "attempt-text"]
)
async def test_malformed_optional_evidence_cannot_invent_a_cause_or_escape(
    tmp_path, tls, unused_tcp_port, monkeypatch, corrupt
):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        status = await healthy(c)
        c.fault("native_authority_or_preservation_failed")
        if corrupt == "audit-map":
            j.put(AUDITS, [])
        elif corrupt == "refusal-entry":
            j.put(REVOCATIONS, {fingerprint(j.get("managed_control_fault")): {"retained": True}})
        elif corrupt in ("version-bool", "version-float", "attempt-text"):
            fault = j.get("managed_control_fault")
            service = NativeRepairAudits(j, lambda value: value == fault)
            attempt = service.begin("diagnostic-test", 1, 1, "test-boot", c.requests.intent().revision)
            service.finish(attempt, "strict", NativeContentError("configuration", "synthetic strict input"))
            rows = j.get(REVOCATIONS)
            rows[fingerprint(fault)].update(
                {"attempt": "x" * 32}
                if corrupt == "attempt-text"
                else {"version": True if corrupt == "version-bool" else 1.0}
            )
            j.put(REVOCATIONS, rows)
        else:
            j.db.execute(
                "INSERT INTO managed_requests VALUES(?,?,?,'running',?)",
                (
                    uuid.uuid4().hex,
                    "pause",
                    0,
                    "[" * 2000 + "0" + "]" * 2000,
                ),
            )
        assert snapshot(c, status)["available"] is False


# Intent: unchanged polls do not grow the timeline; phase/recovery changes retain order and the original fault.
# Why: a bounded incident history must add evidence without generating one record per poll.
def test_reporting_timeline_is_bounded_and_keeps_phase_changes(tmp_path):
    reporter = Reporting(tmp_path)
    original = {"code": "measured_output_limit_exceeded", "first_at": 9000}
    status = {
        "state": "attention",
        "phase": "blocked",
        "checked_at": 10000,
        "diagnostics": {"available": True, "original_fault": original, "blockers": ["fault_latched"]},
    }
    first = reporter.update(copy.deepcopy(status))
    reporter.update(copy.deepcopy(status) | {"checked_at": 10005})
    assert len(reporter.timeline) == 1
    changed = reporter.update(copy.deepcopy(status) | {"phase": "recovering", "checked_at": 10010})
    assert [row["phase"] for row in changed["diagnostics"]["timeline"]] == ["blocked", "recovering"]
    assert first["diagnostics"]["timeline"][0]["phase"] == "blocked"
    for revision in range(30):
        changed = reporter.update(copy.deepcopy(status) | {"control_revision": revision})
    assert len(changed["diagnostics"]["timeline"]) == 20
    assert changed["diagnostics"]["original_fault"] == original
    assert changed["diagnostics"]["timeline_scope"] == "current_process"
    assert not Reporting(tmp_path).timeline


# Intent: actual file replacement/open failures remain optional, and a later write repairs both diagnostic outputs.
# Why: a directory or unavailable disk at a reporting path must not terminate the controller which called reporting.
@pytest.mark.parametrize("sink", ["status", "events"])
def test_reporting_io_failure_is_counted_without_escaping_and_can_recover(tmp_path, sink):
    reporter = Reporting(tmp_path)
    status_path, event_path = tmp_path / "status.json", tmp_path / "events.jsonl"
    blocked = status_path if sink == "status" else event_path
    blocked.mkdir()
    value = reporter.update({"state": "healthy", "checked_at": 10000, "diagnostics": {"available": True}})
    reporter.write(value, status_path, event_path, managed_proxy.save_private, managed_proxy.event)
    assert value["diagnostics"]["reporting_errors"] == 1
    blocked.rmdir()
    reporter.write(value, status_path, event_path, managed_proxy.save_private, managed_proxy.event)
    assert json.loads(status_path.read_text())["diagnostics"]["reporting_errors"] == 1
    assert json.loads(event_path.read_text())["state"] == "healthy"
