"""Offline historical-loss acknowledgement keeps actual native and encrypted OCPP recovery authority intact."""

from __future__ import annotations

import copy
import json
import sqlite3
import ssl
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_current import capability_value
from ocpp_hub_core.prototype.native_history import audit_history, history_policy
from ocpp_hub_core.prototype.native_history_recovery import acknowledge_history_policy
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError
from ocpp_hub_core.prototype.trial_resources import ResourceBook

from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session, starts
from tests.test_native_bounded_handover import restore_original_route, verify_original_backend
from tests.test_native_bounded_session import ConnectedBench
from tests.test_prototype import START
from tests.test_prototype_tls import tls as tls


@dataclass
class HistoryFault:
    """A stopped service has its real fault, retained zero and independently read native HTTP evidence."""

    rig: ConnectedBench
    journal: Journal
    book: ResourceBook
    original: list[dict[str, Any]]
    before: dict[str, Any]


# Read stable private database contents for an exact rollback comparison, without accepting new state.
def database(journal: Journal) -> str:
    return "\n".join(journal.db.iterdump())


# Preserve full historical bodies, including nested data, before commissioning the real managed fixture.
def closed_records() -> list[dict[str, Any]]:
    end = int(time.time()) - 3600
    return [
        {
            "transactionId": 9000000 + index,
            "startTime": end - 3600 + index * 120,
            "stopTime": end - 3540 + index * 120,
            "meterStart": 1000 + index * 100,
            "meterStop": 1010 + index * 100,
            "original_record": {"preserve": [index, {"number": 1, "text": "complete original body"}]},
        }
        for index in range(3)
    ]


# Latch a genuine loss through native HTTP audits and actual TLS peers, then relinquish ownership before ack.
@asynccontextmanager
async def missing_history(
    directory: Path,
    contexts: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    port: int,
    monkeypatch: pytest.MonkeyPatch,
    *,
    paused: bool = False,
) -> AsyncIterator[HistoryFault]:
    async with managed_bench(directory, contexts, port, monkeypatch) as rig:
        rig.bench.history = closed_records()
        first = managed_session(rig)
        await first.start()
        controller = ManagedController(first)
        rig.route_to_trial()
        async with rig.charger(first) as charger:
            await introduced(rig, first, charger, "history-original-boot")
            await healthy(controller)
            if paused:
                controller.command(uuid.uuid4().hex, "pause", 0)
                await healthy(controller)
            original = copy.deepcopy(first.journal.get("native_bounded_handover")["original_history"])
            before = {
                key: copy.deepcopy(first.journal.get(key))
                for key in ("trial_allocation", "next_transaction", "managed_mode", "managed_control_intent")
            }
            rig.bench.history = copy.deepcopy(original[2:])
            for _ in range(3):
                assert (await controller.tick())["state"] == "degraded"
            assert (await controller.tick())["state"] == "attention"
            assert first.journal.get("managed_control_fault")["classification"] == (
                "native_authority_or_preservation_failed"
            )
            assert first.journal.get("managed_native_failure")["detail"] == (
                "native bounded handover must preserve every original history record"
            )
            assert rig.bench.firmware.effective_limit() == 0
            with pytest.raises(RecoveryRequiredError, match="active owner"):
                Journal(first.journal.path, first.journal.identity)
            path, identity = first.journal.path, first.journal.identity
            await first.close()
        journal = Journal(path, identity)
        fault = HistoryFault(rig, journal, ResourceBook(journal, rig.prepared.baseline), original, before)
        try:
            yield fault
        finally:
            fault.journal.close()


# Intent: an offline operator acknowledges precisely the missing bodies while keeping allocations, intent and zero.
# Why: acknowledging rolling history is not permission to erase evidence, allocate ids, or initiate charging.
@pytest.mark.parametrize("paused", [False, True], ids=["app-intent", "paused-intent"])
async def test_acknowledgement_archives_exact_bodies_and_preserves_authority(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    paused: bool,
) -> None:
    async with missing_history(tmp_path, tls, unused_tcp_port, monkeypatch, paused=paused) as fault:
        rig, journal = fault.rig, fault.journal
        native = await rig.prepared.observer.observe_handover()
        writes = copy.deepcopy(rig.bench.device_calls)
        native_requests = list(rig.peer.requests)
        old_fault, old_failure = journal.get("managed_control_fault"), journal.get("managed_native_failure")
        result = acknowledge_history_policy(journal, fault.book, rig.spec, native)
        assert result == {"archived_records": 3, "missing_records": 2, "fault_cleared": True, "charging_started": False}
        archive = [json.loads(row[0]) for row in journal.db.execute("SELECT body FROM native_history_records")]
        assert sorted(archive, key=lambda r: r["transactionId"]) == fault.original
        assert journal.get("native_bounded_handover")["original_history"] == fault.original
        assert all(journal.get(key) == value for key, value in fault.before.items())
        assert journal.get("managed_control_fault") is None
        assert journal.get("managed_native_failure") == old_failure
        assert journal.get("trial_phase") == "recovering"
        control = journal.get("control")
        assert control["owner"] == "local" and control["inhibit"] and control["uncertain"]
        receipt = journal.get("native_history_policy_acknowledgement")
        assert receipt["policy"] == history_policy(journal, rig.spec.digest)
        assert journal.get("native_history_policy") == receipt["policy"]
        assert receipt["prior_fault"] == old_fault and receipt["prior_failure"] == old_failure
        assert receipt["charging_started"] is False and receipt["summary"] == journal.get("native_history_summary")
        assert rig.bench.device_calls == writes and rig.peer.requests == native_requests
        assert rig.bench.firmware.effective_limit() == 0 and not starts(rig)


# Intent: the persisted decision permits fresh real Boot/reconciliation and later retention loss without Start.
# Why: offline acknowledgement cannot bypass introduction, promote a stale physical zero, or clear future drift.
async def test_real_tls_recovery_warns_on_later_absence_but_latches_changed_original(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with missing_history(tmp_path, tls, unused_tcp_port, monkeypatch) as fault:
        rig, journal = fault.rig, fault.journal
        native = await rig.prepared.observer.observe_handover()
        acknowledge_history_policy(journal, fault.book, rig.spec, native)
        saved = copy.deepcopy(journal.get("native_history_policy_acknowledgement"))
        journal.close()
        # Reopen this same handle for context cleanup after the independently owned replacement retires.
        try:
            replacement = managed_session(rig)
            await replacement.start()
            controller = ManagedController(replacement)
            assert replacement.journal.get("native_history_policy_acknowledgement") == saved
            assert replacement.journal.get("trial_phase") == "recovering"
            assert rig.bench.firmware.effective_limit() == 0
            async with rig.charger(replacement) as charger:
                assert (await controller.tick())["state"] == "starting"
                assert rig.bench.firmware.effective_limit() == 0
                await introduced(rig, replacement, charger, "history-replacement-boot")
                status = await healthy(controller)
                assert status["owner_control_enabled"] and rig.bench.firmware.effective_limit() == 6
                assert replacement.journal.get("native_history_summary") == {
                    "archived_records": 3,
                    "missing_records": 2,
                }
                # The last remaining original can later leave native retention, while its full archive remains.
                rig.bench.history = []
                assert (await healthy(controller))["state"] == "healthy"
                assert replacement.journal.get("native_history_summary")["missing_records"] == 3
                # Reappearing original ids must still carry their exact original bodies, including numeric types.
                changed = copy.deepcopy(fault.original[-1])
                changed["original_record"]["preserve"][1]["number"] = 1.0
                rig.bench.history = [changed]
                for _ in range(3):
                    assert (await controller.tick())["state"] == "degraded"
                assert (await controller.tick())["state"] == "attention"
                assert rig.bench.firmware.effective_limit() == 0
                assert not starts(rig) and replacement.journal.active() is None
                assert replacement.journal.get("native_bounded_handover")["original_history"] == fault.original
                await replacement.close()
        finally:
            replacement = next((s for s in reversed(rig.sessions) if not s.closed), None)
            if replacement is not None:
                await replacement.close()
            fault.journal = Journal(journal.path, journal.identity)


# Alter only explicitly named persistent preconditions; the independent physical peer stays at confirmed zero.
def corrupt_guard(journal: Journal, guard: str) -> None:
    if guard == "managed-mode":
        journal.put("managed_mode", journal.get("managed_mode") | {"mode": "other"})
    elif guard == "local-start":
        journal.put("managed_mode", journal.get("managed_mode") | {"local_start": True})
    elif guard == "other-fault":
        journal.put("managed_control_fault", journal.get("managed_control_fault") | {"classification": "other"})
    elif guard == "failure-kind":
        journal.put("managed_native_failure", journal.get("managed_native_failure") | {"type": "OtherError"})
    elif guard == "failure-time":
        journal.put("managed_native_failure", journal.get("managed_native_failure") | {"at_epoch": 1})
    elif guard in ("trial_preservation_fault", "trial_inventory_fault", "uncertain_vendor_command"):
        journal.put(guard, {"reason": "independently unresolved"})
    elif guard == "outbox":
        journal.enqueue("Heartbeat", {})
    elif guard == "mutation":
        journal.mutation_intent("vendor", "DataTransfer", {"unresolved": True})
    elif guard in ("request-accepted", "request-running", "request-uncertain"):
        journal.db.execute(
            "INSERT INTO managed_requests VALUES(?,?,?,?,?)",
            (uuid.uuid4().hex, "app_control", 0, guard.removeprefix("request-"), "{}"),
        )
    elif guard == "active-transaction":
        journal.start("offline-unresolved-start", START)
        journal.db.execute("DELETE FROM outbox")
    elif guard == "unbound-start-grant":
        journal.put("vendor_start_grant", {"local": None, "tag": "CLOUD_OWNER"})
    elif guard == "not-inhibited":
        journal.put("control", journal.get("control") | {"inhibit": False})
    else:
        raise AssertionError(guard)


# Intent: every unrelated uncertain state or fault refuses recovery without a partial acknowledgement.
# Why: narrowly acknowledging missing historical bodies cannot launder live sessions or unresolved operations.
@pytest.mark.parametrize(
    "guard",
    [
        "managed-mode",
        "local-start",
        "other-fault",
        "failure-kind",
        "failure-time",
        "trial_preservation_fault",
        "trial_inventory_fault",
        "uncertain_vendor_command",
        "outbox",
        "mutation",
        "request-accepted",
        "request-running",
        "request-uncertain",
        "active-transaction",
        "unbound-start-grant",
        "not-inhibited",
    ],
)
async def test_offline_acknowledgement_refuses_unrelated_or_unresolved_state(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    guard: str,
) -> None:
    async with missing_history(tmp_path, tls, unused_tcp_port, monkeypatch) as fault:
        native = await fault.rig.prepared.observer.observe_handover()
        corrupt_guard(fault.journal, guard)
        before = database(fault.journal)
        with pytest.raises((SecurityError, RecoveryRequiredError)):
            acknowledge_history_policy(fault.journal, fault.book, fault.rig.spec, native)
        assert database(fault.journal) == before


# Mutate the independent charger peer rather than substituting the recovery function's native checks.
def change_native(rig: ConnectedBench, guard: str) -> None:
    if guard == "user-limit":
        rig.peer.configuration["AmpLmt"], rig.peer.runtime_user = "7", 70
    elif guard == "hardware-limit":
        rig.peer.hardware = 310
    elif guard == "configuration":
        rig.peer.configuration["untouched"] = "changed"
    elif guard == "saved-plan":
        rig.bench.firmware.plans[9]["original"] = "changed"
    elif guard == "owned-profile":
        rig.bench.firmware.profiles[7001]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] = 1
    elif guard == "other-phase-current":
        rig.peer.phase_fault = True
    elif guard == "original-body":
        rig.bench.history[0]["original_record"]["preserve"][0] = "changed"
    elif guard == "no-missing":
        raise AssertionError("handled with retained complete original bodies")
    else:
        raise AssertionError(guard)


# Intent: current native6/HW32/configuration/plans/owned0/all-phase closure remain mandatory for acknowledgement.
# Why: an old historical absence never excuses a concurrent physical or preservation failure.
@pytest.mark.parametrize(
    "guard",
    [
        "user-limit",
        "hardware-limit",
        "configuration",
        "saved-plan",
        "owned-profile",
        "other-phase-current",
        "original-body",
        "stale-observation",
        "no-missing",
    ],
)
async def test_independent_native_guards_still_refuse_recovery(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    guard: str,
) -> None:
    async with missing_history(tmp_path, tls, unused_tcp_port, monkeypatch) as fault:
        if guard == "no-missing":
            fault.rig.bench.history = copy.deepcopy(fault.original)
        elif guard != "stale-observation":
            change_native(fault.rig, guard)
        native = await fault.rig.prepared.observer.observe_handover()
        if guard == "stale-observation":
            native = replace(native, physical=replace(native.physical, observed_at=time.time() - 30))
        before = database(fault.journal)
        with pytest.raises((SecurityError, RecoveryRequiredError)):
            acknowledge_history_policy(fault.journal, fault.book, fault.rig.spec, native)
        assert database(fault.journal) == before


# Intent: failure writing the final operator receipt rolls archive, policy, resource observations and fault back.
# Why: a crash or SQLite error cannot publish missing-history tolerance without its complete auditable decision.
async def test_receipt_write_failure_rolls_back_all_recovery_changes(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with missing_history(tmp_path, tls, unused_tcp_port, monkeypatch) as fault:
        native = await fault.rig.prepared.observer.observe_handover()
        fault.journal.db.execute(
            "CREATE TRIGGER reject_history_receipt BEFORE INSERT ON meta "
            "WHEN NEW.key='native_history_policy_acknowledgement' "
            "BEGIN SELECT RAISE(ABORT,'synthetic recovery receipt failure'); END"
        )
        before = database(fault.journal)
        with pytest.raises(sqlite3.IntegrityError, match="synthetic recovery receipt failure"):
            acknowledge_history_policy(fault.journal, fault.book, fault.rig.spec, native)
        assert database(fault.journal) == before
        assert fault.journal.get("managed_control_fault") is not None
        assert fault.journal.get("native_history_policy") is None


# Intent: a matching policy row alone cannot activate retention tolerance without the atomic operator receipt.
# Why: the exceptional permission must be evidenced by the complete accepted recovery, rather than a lone flag.
async def test_policy_without_operator_receipt_cannot_enable_missing_history(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with missing_history(tmp_path, tls, unused_tcp_port, monkeypatch) as fault:
        fault.journal.put("native_history_policy", history_policy(fault.journal, fault.rig.spec.digest))
        before = database(fault.journal)
        with pytest.raises(SecurityError):
            audit_history(fault.journal, fault.rig.spec.digest, list(fault.rig.bench.history), False)
        assert database(fault.journal) == before


# Intent: approved archived absence survives real cleanup and original32 restoration, with current HW still checked.
# Why: a recovery exception must permit the ordinary exit path without weakening its independent native safeguards.
async def test_managed_history_policy_allows_complete_original32_restoration(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with missing_history(tmp_path, tls, unused_tcp_port, monkeypatch) as fault:
        rig, journal = fault.rig, fault.journal
        native = await rig.prepared.observer.observe_handover()
        acknowledge_history_policy(journal, fault.book, rig.spec, native)
        original_plans = copy.deepcopy(rig.bench.firmware.plans)
        remaining_history = copy.deepcopy(rig.bench.history)
        journal.close()
        replacement = None
        try:
            replacement = managed_session(rig)
            await replacement.start()
            controller = ManagedController(replacement)
            async with rig.charger(replacement) as charger:
                await introduced(rig, replacement, charger, "history-original32-boot")
                assert (await healthy(controller))["owner_control_enabled"]
                assert rig.bench.firmware.effective_limit() == 6
                assert (await replacement.command({"command": "native-bounded-handover"}))["profiles_restored"]
                assert not rig.bench.firmware.profiles
                await restore_original_route(rig, replacement, charger)
                await verify_original_backend(rig, replacement)
                writes = []

                # Only the independent fake native peer receives the authorized original USER scalar.
                async def patch(values: dict[str, Any]) -> None:
                    writes.append(copy.deepcopy(values))
                    rig.peer.configuration.update(values)
                    rig.peer.runtime_user = 320

                rig.peer.hardware = 310
                assert replacement.native_bounded is not None
                with pytest.raises(SecurityError):
                    await replacement.native_bounded.restore_settings(patch)
                assert not writes and rig.peer.configuration["AmpLmt"] == "6"
                assert replacement.journal.get("trial_phase") == "backend_restore_pending"
                rig.peer.hardware = 320
                assert (await replacement.native_bounded.restore_settings(patch))["complete"]
                assert writes == [{"AmpLmt": "32"}]
                assert rig.peer.configuration == rig.spec.original_configuration
                assert replacement.journal.get("trial_phase") == "complete"
                assert replacement.journal.get("native_bounded_handover")["stage"] == "complete"
                assert replacement.journal.get("native_bounded_handover")["original_history"] == fault.original
                archive = [
                    json.loads(row[0])
                    for row in replacement.journal.db.execute("SELECT body FROM native_history_records")
                ]
                assert sorted(archive, key=lambda r: r["transactionId"]) == fault.original
                assert rig.bench.history == remaining_history and rig.bench.firmware.plans == original_plans
                assert replacement.journal.active() is None and not starts(rig)
                assert all(replacement.journal.get(key) == value for key, value in fault.before.items())
        finally:
            if replacement is not None:
                await replacement.close()
            fault.journal = Journal(journal.path, journal.identity)


# Intent: recover only the expected unassigned attempts under proven zero and retain paused intent across restart.
# Why: repaired parsing cannot itself clear an unrelated fault or release power at service replacement.
@pytest.mark.parametrize("native_limit", [6, 32])
async def test_recover_unassigned_start_to_verified_pause(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    native_limit: int,
) -> None:
    from ocpp_hub_core.prototype.native_history_recovery import acknowledge_start_attempts

    from tests.test_native_history_archive import unassigned

    async with missing_history(tmp_path, tls, unused_tcp_port, monkeypatch) as fault:
        rig, journal = fault.rig, fault.journal
        native = await rig.prepared.observer.observe_handover()
        acknowledge_history_policy(journal, fault.book, rig.spec, native)
        if native_limit == 32:
            journal.put("managed_current_capability", capability_value(journal, rig.spec))
            rig.peer.configuration["AmpLmt"] = "32"
            rig.peer.runtime_user = 320
        now = time.time()
        fault_record = {"at_epoch": now, "classification": "native_authority_or_preservation_failed"}
        failure = {
            "at_epoch": now,
            "type": "SecurityError",
            "detail": "complete connected-handover native evidence is required",
        }
        journal.put("managed_control_fault", fault_record)
        journal.put("managed_native_failure", failure)
        journal.put("trial_phase", "blocked")
        attempt = unassigned()
        attempt["startTime"] = int(now) - 6
        attempt["MeterValues"][0]["timestamp"] = attempt["startTime"]
        rig.bench.history.append(attempt)
        native = await rig.prepared.observer.observe_handover()
        journal.put("uncertain_vendor_command", True)
        before = database(journal)
        with pytest.raises(RecoveryRequiredError):
            acknowledge_start_attempts(journal, fault.book, rig.spec, native)
        assert database(journal) == before
        journal.db.execute("DELETE FROM meta WHERE key='uncertain_vendor_command'")
        initial = {"mode": "app", "revision": 5}
        journal.put("managed_control_intent", {**initial, "unexpected": True})
        before = database(journal)
        with pytest.raises(RecoveryRequiredError):
            acknowledge_start_attempts(journal, fault.book, rig.spec, native)
        assert database(journal) == before
        journal.put("managed_control_intent", initial)
        audit_history(journal, rig.spec.digest, list(native.history), False)
        before = database(journal)
        with pytest.raises(RecoveryRequiredError):
            acknowledge_start_attempts(journal, fault.book, rig.spec, native)
        assert database(journal) == before
        attempt["MeterValues"][0]["Offer"] += 1
        changed_attempt = await rig.prepared.observer.observe_handover()
        with pytest.raises(RecoveryRequiredError):
            acknowledge_start_attempts(journal, fault.book, rig.spec, changed_attempt)
        attempt["MeterValues"][0]["Offer"] -= 1
        journal.db.execute("DELETE FROM native_start_attempts")
        journal.put("native_start_attempt_recovery", {"prior_fault": {"retained": True}})
        before = database(journal)
        with pytest.raises(RecoveryRequiredError):
            acknowledge_start_attempts(journal, fault.book, rig.spec, native)
        assert database(journal) == before
        journal.db.execute("DELETE FROM meta WHERE key='native_start_attempt_recovery'")
        uncertain_id, pause_id = uuid.uuid4().hex, uuid.uuid4().hex
        uncertain_result = json.dumps({"classification": "start_not_confirmed"})
        journal.db.execute(
            "INSERT INTO managed_requests VALUES(?, 'start_test', 2, 'uncertain', ?)", (uncertain_id, uncertain_result)
        )
        insert_pause = "INSERT INTO managed_requests VALUES(?, 'pause', ?, 'completed', ?)"
        for pause_revision, when, verified in ((1, now - 1, True), (3, now + 1, True), (3, now - 1, False)):
            journal.db.execute(
                insert_pause, (pause_id, pause_revision, json.dumps({"completed_at": when, "pause_verified": verified}))
            )
            before = database(journal)
            with pytest.raises(RecoveryRequiredError):
                acknowledge_start_attempts(journal, fault.book, rig.spec, native)
            assert database(journal) == before
            journal.db.execute("DELETE FROM managed_requests WHERE id=?", (pause_id,))
        journal.db.execute(insert_pause, (pause_id, 3, json.dumps({"completed_at": now - 1, "pause_verified": True})))
        for action, state in (("start", "uncertain"), ("start_test", "accepted"), ("start_test", "running")):
            journal.db.execute("UPDATE managed_requests SET action=?,state=? WHERE id=?", (action, state, uncertain_id))
            with pytest.raises(RecoveryRequiredError):
                acknowledge_start_attempts(journal, fault.book, rig.spec, native)
        journal.db.execute(
            "UPDATE managed_requests SET action='start_test',state='uncertain' WHERE id=?", (uncertain_id,)
        )
        journal.put("managed_native_failure", {**failure, "at_epoch": now - 60})
        with pytest.raises(RecoveryRequiredError):
            acknowledge_start_attempts(journal, fault.book, rig.spec, native)
        journal.put("managed_native_failure", failure)
        attempt["startTime"] -= 60
        old_attempt = await rig.prepared.observer.observe_handover()
        with pytest.raises(RecoveryRequiredError):
            acknowledge_start_attempts(journal, fault.book, rig.spec, old_attempt)
        attempt["startTime"] += 60
        journal.db.execute(
            "CREATE TEMP TRIGGER reject_recovery BEFORE INSERT ON meta WHEN NEW.key='native_start_attempt_recovery' "
            "BEGIN SELECT RAISE(ABORT,'failed receipt write'); END"
        )
        before = database(journal)
        with pytest.raises(sqlite3.IntegrityError):
            acknowledge_start_attempts(journal, fault.book, rig.spec, native)
        assert database(journal) == before
        journal.db.execute("DROP TRIGGER reject_recovery")
        result = acknowledge_start_attempts(journal, fault.book, rig.spec, native)
        assert result["unassigned_attempts"] == 1
        assert journal.get("managed_control_intent") == {"mode": "paused", "revision": initial["revision"] + 1}
        assert journal.get("native_start_attempt_recovery")["prior_failure"] == failure
        assert journal.get("native_start_attempt_recovery")["superseded_test_proofs"] == [
            {"request_id": uncertain_id, "verified_pause_id": pause_id}
        ]
        assert tuple(
            journal.db.execute("SELECT state,result FROM managed_requests WHERE id=?", (uncertain_id,)).fetchone()
        ) == ("uncertain", uncertain_result)
        assert journal.get("control")["inhibit"] and journal.active() is None
        assert not starts(rig)
        journal.close()
        replacement = managed_session(rig)
        try:
            await replacement.start()
            async with rig.charger(replacement) as charger:
                await introduced(rig, replacement, charger, "attempt-recovery-boot")
                state = await healthy(ManagedController(replacement))
                assert state["local_hold_verified"] and state["station_ceiling_a"] == 0 and not starts(rig)
                assert state["native_limit_a"] == native_limit
        finally:
            await replacement.close()
            fault.journal = Journal(journal.path, journal.identity)
