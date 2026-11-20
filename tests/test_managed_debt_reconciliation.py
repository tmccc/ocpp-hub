"""Real TLS failures and explicit offline operator evidence recover only their exact debt at physical zero."""

import asyncio
import json
import sqlite3
import time
import uuid
from contextlib import closing

import pytest
from ocpp_hub_core.frames import build_call_error
from ocpp_hub_core.prototype import managed_charging, managed_start_delivery
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_debt_recovery import KEY
from ocpp_hub_core.prototype.managed_native_repair import ACK, REVOCATIONS
from ocpp_hub_core.prototype.native_history import ORIGINAL_HISTORY_OMISSION
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError, fingerprint
from ocpp_hub_core.prototype.trial_session import save_private

from tests.charge_rig import ChargeRig, positive_flow
from tests.cloud_start_helpers import StartReplyBench
from tests.connected_rig import strategy_file
from tests.handover_helpers import crash
from tests.managed_rig import healthy, introduced, managed_bench, managed_session, starts
from tests.prototype_rig import START
from tests.recovery_barriers import completed_control, require_zero_held
from tests.runtime_rig import until
from tests.test_managed_start_delivery_recovery import approve_retry, approve_stop_after_start, map_unknown_retry
from tests.test_managed_stop_reply_recovery import pause, running
from tests.test_native_history_recovery import closed_records
from tests.test_ordinary_recovery import close_session
from tests.tls_support import tls as tls
from tests.trial_peer import Bench
from tools import acknowledge_native_history as history_tool
from tools import reconcile_managed_debt as operator
from tools import reconcile_managed_delivery as mapping


class ErrorReplyPeer:
    """An independent charger returns CALLERROR after its ordinary fake implementation received the command."""

    # Keep fault injection on the peer's transport, with no replacement of an engine method.
    def __init__(self, connection, trace, command="RemoteStartTransaction"):
        self.connection, self.trace = connection, trace
        self.command = command
        self.targets = set()

    # Observe genuine forwarded vendor commands on the encrypted charger socket.
    def __aiter__(self):
        # All unrelated station-profile, configuration and composite calls keep their ordinary implementation.
        async def frames():
            async for raw in self.connection:
                frame = json.loads(raw)
                if frame[0] == 2 and frame[2] == self.command:
                    self.targets.add(frame[1])
                    self.trace.append(frame)
                yield raw

        return frames()

    # A generic error is deliberately ambiguous even if the independent device has already acted on the command.
    async def send(self, raw):
        frame = json.loads(raw)
        if frame[0] == 3 and frame[1] in self.targets:
            raw = build_call_error(frame[1], "InternalError", "synthetic uncertain outcome")
        await self.connection.send(raw)


# Record the exact vendor and owner state that the separate history repair must leave with its original owners.
def vendor_recovery_preimage(journal):
    keys = ("control", "managed_control_intent", "uncertain_vendor_command", "vendor_start_grant", KEY)
    return (
        journal.fault.read(),
        [journal.get(key) for key in keys],
        [dict(row) for row in journal.db.execute("SELECT * FROM mutations ORDER BY rowid")],
        [dict(row) for row in journal.db.execute("SELECT * FROM managed_requests ORDER BY rowid")],
    )


# Prove the offline tool can archive an omission without pretending to settle any vendor outcome.
async def repair_overlapping_history(rig, config, strategy, monkeypatch, capsys, original, history_loss, kind):
    native_port = rig.prepared.observer.port
    path, identity = rig.prepared.config.journal, rig.prepared.config.identity
    wire_mark = len(rig.bench.device_calls)
    rig.bench.history = original[1:]
    refused = managed_session(rig)
    try:
        before = vendor_recovery_preimage(refused.journal)
        revocations = refused.journal.get(REVOCATIONS, {})
        with pytest.raises(SecurityError, match="preserve every original history record"):
            await refused.start()
        assert not refused.started and vendor_recovery_preimage(refused.journal) == before
    finally:
        await refused.close()

    with pytest.raises(SecurityError, match="preserve every original history record"):
        await operator.run("inspect", config, strategy, None, native_port, kind=kind)
    preflight = history_tool.preflight_native

    # The real offline entry point reads the independent native HTTP peer on its assigned loopback port.
    def native_peer(config_path):
        prepared = preflight(config_path)
        prepared.observer.port = native_port
        return prepared

    monkeypatch.setattr(history_tool, "preflight_native", native_peer)
    if history_loss == "before-writes":
        rig.bench.history = original
        with pytest.raises(RecoveryRequiredError, match="no missing original historical records"):
            await history_tool.run("acknowledge", config, strategy)
        rig.bench.history = [original[1] | {"meterStop": 99999}, original[2]]
        with pytest.raises(SecurityError, match="preserve every original history record"):
            await history_tool.run("acknowledge", config, strategy)
        rig.bench.history = original[1:]
    if history_loss in ("between-writes", "pause-audit"):
        with closing(Journal(path, identity)) as journal:
            journal.db.execute(
                "CREATE TRIGGER refuse_history_receipt BEFORE INSERT ON meta "
                "WHEN NEW.key='native_history_policy_acknowledgement' "
                "BEGIN SELECT RAISE(ABORT,'synthetic history receipt failure'); END"
            )
        with pytest.raises(sqlite3.IntegrityError, match="history receipt failure"):
            await history_tool.run("acknowledge", config, strategy)
        with closing(Journal(path, identity)) as journal:
            assert vendor_recovery_preimage(journal) == before
            assert journal.get(REVOCATIONS, {}) == revocations
            assert journal.get("native_history_policy") is None
            assert journal.get("native_history_summary") is None
            journal.db.execute("DROP TRIGGER refuse_history_receipt")

    if history_loss in ("changed-audit", "resource-audit", "mixed-online-fault"):
        refusal = (
            "cannot waive another unsettled operation"
            if history_loss == "mixed-online-fault"
            else "settled native repair evidence"
        )
        with pytest.raises(RecoveryRequiredError, match=refusal):
            await history_tool.run("acknowledge", config, strategy)
        with closing(Journal(path, identity)) as journal:
            assert vendor_recovery_preimage(journal) == before
            assert journal.get(REVOCATIONS, {}) == revocations
            assert journal.get("native_history_policy") is None
        assert len(rig.bench.device_calls) == wire_mark
        require_zero_held(rig, wire_mark)
        return

    assert await history_tool.run("acknowledge", config, strategy) == 0
    result = json.loads(capsys.readouterr().out.splitlines()[-1])
    retired_original_fault = history_loss == "online-fault"
    assert result["missing_records"] == 1 and result["fault_cleared"] is retired_original_fault
    expected = (None, *before[1:]) if retired_original_fault else before
    with closing(Journal(path, identity)) as journal:
        assert vendor_recovery_preimage(journal) == expected
        receipt = journal.get("native_history_policy_acknowledgement")
        assert receipt["prior_fault"] == (before[0] or {}) and receipt["vendor_recovery"]["kind"] == kind
        assert receipt["policy"] == journal.get("native_history_policy")
        if history_loss == "pause-audit":
            assert receipt["vendor_recovery"]["history_revocations"] == revocations
            assert not journal.get(REVOCATIONS) and journal.get(ACK) is None
        archived = [
            json.loads(row[0])
            for row in journal.db.execute(
                "SELECT body FROM native_history_records WHERE original=1 ORDER BY transaction_id"
            )
        ]
        assert archived == original

    assert await history_tool.run("acknowledge", config, strategy) == 0
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["already_recorded"] is True
    with closing(Journal(path, identity)) as journal:
        assert vendor_recovery_preimage(journal) == expected
        assert journal.get("native_history_policy_acknowledgement") == receipt
    assert len(rig.bench.device_calls) == wire_mark
    require_zero_held(rig, wire_mark)


# Intent: settle uncertain app commands in order even if a separate loss of original history prevents restart.
# Why: independent evidence must let both recovery owners finish while the charger remains at zero.
@pytest.mark.parametrize(
    "legacy_marker,start_tag,history_loss",
    [
        (False, None, None),
        (True, None, None),
        (False, "CLOUD_OWNER", None),
        (False, "NATIVE_TIMER", None),
        (False, None, "before-writes"),
        (False, None, "between-writes"),
        (False, None, "after-writes"),
        (False, "NATIVE_TIMER", "after-writes"),
        (False, "NATIVE_TIMER", "after-grant"),
        (False, None, "online-fault"),
        (False, None, "mixed-online-fault"),
        (False, None, "pause-audit"),
        (False, None, "changed-audit"),
        (False, None, "resource-audit"),
    ],
    ids=[
        "exact-marker",
        "legacy-marker",
        "separate-accepted-start",
        "same-tag-scheduled-start",
        "history-before-vendor",
        "history-between-commands",
        "history-after-vendor",
        "history-before-grant",
        "history-after-grant",
        "live-history-latch",
        "live-mixed-failures",
        "live-history-pause-audit",
        "live-contradiction-then-omission",
        "live-omission-and-resource-conflict",
    ],
)
async def test_sequential_vendor_recovery_retains_zero_until_both_commands_settle(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys, legacy_marker, start_tag, history_loss
):
    original = closed_records() if history_loss is not None else None
    async with managed_bench(
        tmp_path, tls, unused_tcp_port, bench_type=StartReplyBench, initial_history=original
    ) as rig:
        strategy = strategy_file(rig, tmp_path)
        config, port = tmp_path / "trial.json", rig.prepared.observer.port
        path, identity = rig.prepared.config.journal, rig.prepared.config.identity
        async with running(rig, "two-unconfirmed-commands") as (session, controller, charger):
            await healthy(controller)
            if start_tag:
                grant = await rig.bench.cloud_call(
                    "accepted-unbound-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": start_tag}
                )
                assert grant[0] == 3 and grant[2] == {"status": "Accepted"}
            local = await positive_flow(ChargeRig(rig, session, controller, charger), "daily")
            journal = session.journal
            vendor = journal.vendor_id(local)
            assert vendor is not None
            transmit, faulted = charger.send, []

            # The real device executes each request. Replace only those two actual correlated replies.
            async def send_error(raw):
                answer = json.loads(raw)
                frame = next((f for f in rig.bench.device_calls if f[1] == answer[1]), None)
                if answer[0] == 3 and frame is not None and frame[2] in ("DataTransfer", "RemoteStopTransaction"):
                    faulted.append(frame[2])
                    raw = build_call_error(answer[1], "InternalError", "synthetic uncertain command")
                await transmit(raw)

            monkeypatch.setattr(charger, "send", send_error)
            update = {
                "vendorId": "IoCharger",
                "messageId": "setTransaction",
                "data": json.dumps({"transactionId": vendor, "eco": "Fast"}),
            }
            assert (await rig.bench.cloud_call("unconfirmed-fast", "DataTransfer", update))[0] == 4
            assert (await rig.bench.cloud_call("unconfirmed-stop", "RemoteStopTransaction", {"transactionId": vendor}))[
                0
            ] == 4
            assert faulted == ["DataTransfer", "RemoteStopTransaction"]
            await close_session(rig, session, charger, local)
            old_pause, old_revision = await pause(controller)
            assert journal.active() is None and journal.next_delivery() is None
            assert rig.bench.firmware.effective_limit() == 0
            rows = [
                dict(row)
                for row in journal.db.execute("SELECT * FROM mutations WHERE state='uncertain' ORDER BY rowid")
            ]
            assert [row["action"] for row in rows] == faulted
            unbound = journal.get("vendor_start_grant")
            if start_tag:
                assert unbound and unbound.get("local") is None
            if start_tag == "NATIVE_TIMER":
                assert journal.get("managed_deferred_start") == {
                    "local": local,
                    "owner": False,
                    "ticket": None,
                    "vendor": unbound["token"],
                }
            marker = journal.get("uncertain_vendor_command")
            assert marker["mutation"] == rows[1]["token"]
            if legacy_marker:
                marker.pop("mutation")
                journal.put("uncertain_vendor_command", marker)
            if history_loss in ("online-fault", "mixed-online-fault"):
                if history_loss == "mixed-online-fault":
                    rig.bench.history = [original[0] | {"meterStop": 99999}, *original[1:]]
                    assert (await controller.tick())["state"] == "degraded"
                rig.bench.history = original[1:]
                for _ in range(4):
                    await controller.tick()
                assert journal.fault.classification() == "native_authority_or_preservation_failed"
                expected_source = ORIGINAL_HISTORY_OMISSION if history_loss == "online-fault" else None
                assert journal.get("managed_native_failure").get("source") == expected_source

        wire_mark = len(rig.bench.device_calls)
        evidence_files = []
        for index, row in enumerate(rows):
            if (index == 0 and history_loss in ("before-writes", "online-fault", "mixed-online-fault")) or (
                index == 1 and history_loss == "between-writes"
            ):
                await repair_overlapping_history(
                    rig, config, strategy, monkeypatch, capsys, original, history_loss, "vendor-write"
                )
                if history_loss == "mixed-online-fault":
                    return
            before = list(rig.bench.device_calls)
            assert await operator.run("inspect", config, strategy, None, port, kind="vendor-write") == 0
            candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
            assert candidate["mutation_token"] == row["token"]
            evidence = candidate | {
                "command_quiescent": True,
                "original_state_independently_verified": True,
                "evidence_sha256": fingerprint(faulted[: index + 1]),
                "verified_at_epoch": time.time(),
            }
            evidence_path = tmp_path / f"command-{index}-evidence.json"
            if index == 0:
                save_private(evidence_path, evidence | {"mutation_token": rows[1]["token"]})
                with pytest.raises(RecoveryRequiredError, match="exact current debt"):
                    await operator.run("reconcile", config, strategy, evidence_path, port, kind="vendor-write")
            save_private(evidence_path, evidence)
            evidence_files.append(evidence_path)
            if index == 1:
                with closing(Journal(path, identity)) as journal:
                    journal.db.execute(
                        "CREATE TRIGGER refuse_second_proof BEFORE INSERT ON meta "
                        "WHEN NEW.key='managed_operator_debt_reconciliations' "
                        "BEGIN SELECT RAISE(ABORT,'synthetic second proof failure'); END"
                    )
                with pytest.raises(sqlite3.IntegrityError, match="second proof failure"):
                    await operator.run("reconcile", config, strategy, evidence_path, port, kind="vendor-write")
                with closing(Journal(path, identity)) as journal:
                    remaining = journal.db.execute("SELECT state FROM mutations WHERE token=?", (row["token"],))
                    assert remaining.fetchone()[0] == "uncertain"
                    assert journal.get("uncertain_vendor_command") == marker and len(journal.get(KEY)) == 1
                    journal.db.execute("DROP TRIGGER refuse_second_proof")
            assert await operator.run("reconcile", config, strategy, evidence_path, port, kind="vendor-write") == 0
            assert await operator.run("reconcile", config, strategy, evidence_path, port, kind="vendor-write") == 0
            assert rig.bench.device_calls == before and rig.bench.firmware.effective_limit() == 0

            if index == 0:
                async with running(rig, "only-first-command-settled") as (session, controller, _):
                    assert session.journal.get("uncertain_vendor_command") == marker
                    assert (await controller.tick())["state"] == "attention"
                    await pause(controller)
                    with pytest.raises(RecoveryRequiredError, match="latched fault"):
                        controller.command(uuid.uuid4().hex, "app_control", controller.requests.intent().revision)
                    assert rig.bench.firmware.effective_limit() == 0

        if history_loss == "after-writes":
            kind = "vendor-start" if start_tag else "vendor-write"
            await repair_overlapping_history(rig, config, strategy, monkeypatch, capsys, original, history_loss, kind)

        if start_tag:
            with closing(Journal(path, identity)) as journal:
                assert journal.get("vendor_start_grant") == unbound
            async with running(rig, "commands-settled-start-grant-remains") as (_, controller, _):
                await pause(controller)
                with pytest.raises(RecoveryRequiredError, match="latched fault"):
                    controller.command(uuid.uuid4().hex, "app_control", controller.requests.intent().revision)
            assert await operator.run("inspect", config, strategy, None, port, kind="vendor-start") == 0
            candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
            assert candidate["mutation_token"] == unbound["mutation"]
            grant_path = tmp_path / "accepted-start-evidence.json"
            save_private(
                grant_path,
                candidate
                | {
                    "command_no_longer_pending": True,
                    "original_state_independently_verified": True,
                    "evidence_sha256": fingerprint(unbound),
                    "verified_at_epoch": time.time(),
                },
            )
            assert await operator.run("reconcile", config, strategy, grant_path, port, kind="vendor-start") == 0
            if start_tag == "NATIVE_TIMER":
                with closing(Journal(path, identity)) as journal:
                    assert journal.get("managed_deferred_start") == {}

        if history_loss == "after-grant":
            await repair_overlapping_history(
                rig, config, strategy, monkeypatch, capsys, original, history_loss, "vendor-start"
            )
        if history_loss in ("pause-audit", "changed-audit", "resource-audit"):
            async with running(rig, "history-lost-during-pause") as (session, controller, _):
                journal = session.journal
                fault = journal.fault.read()
                assert (await controller.tick())["state"] == "attention"
                rig.bench.history = (
                    [original[0] | {"meterStop": 99999}, *original[1:]]
                    if history_loss == "changed-audit"
                    else original[1:]
                )
                if history_loss == "resource-audit":
                    profiles = rig.bench.firmware.profiles
                    profiles[7002] = profiles[7001] | {"chargingProfileId": 7002}
                old_pause, old_revision = await pause(controller)
                failure = journal.get(REVOCATIONS, {})[fingerprint(fault)]["typed_refusal"]
                expected_source = ORIGINAL_HISTORY_OMISSION if history_loss == "pause-audit" else None
                assert failure.get("source") == expected_source and journal.get(ACK) is None
                if history_loss == "resource-audit":
                    assert failure["stage"] == "resources"
                    profiles.pop(7002)
            await repair_overlapping_history(
                rig, config, strategy, monkeypatch, capsys, original, history_loss, "vendor-write"
            )
            if history_loss != "pause-audit":
                return
        assert await operator.run("reconcile", config, strategy, evidence_files[0], port, kind="vendor-write") == 0
        async with running(rig, "both-commands-attested") as (session, controller, charger):
            assert session.journal.get("uncertain_vendor_command") is None
            assert (await controller.tick())["state"] == "attention"
            controller.command(old_pause, "pause", old_revision)
            assert (await controller.tick())["state"] == "attention"
            pause_id, revision = await pause(controller)
            b = ChargeRig(rig, session, controller, charger)
            await completed_control(b, pause_id, mode="paused", revision=revision + 1, ceiling=0)
            proofs = session.journal.get(KEY)
            assert len(proofs) == (3 if start_tag else 2)
            assert all(entry["completed"]["pause"] == pause_id for entry in proofs.values())
            require_zero_held(rig, wire_mark)
            request, revision = uuid.uuid4().hex, controller.requests.intent().revision
            controller.command(request, "app_control", revision)
            await completed_control(b, request, mode="app", revision=revision + 1, ceiling=6)
            assert faulted == ["DataTransfer", "RemoteStopTransaction"]
            assert len(starts(rig)) == int(start_tag is not None)


# Intent: a real correlated vendor error stays quarantined until exact independent evidence and a fresh Pause.
# Why: a generic CALLERROR cannot justify retry, positive release, or successful-command reporting.
@pytest.mark.parametrize("legacy_marker", [False, True])
async def test_vendor_error_operator_reconciliation(tmp_path, tls, unused_tcp_port, monkeypatch, capsys, legacy_marker):
    trace = []
    actual = Bench.read_charger

    # Change only the fake charger's response transport; the real gateway, journal and RPC still execute.
    async def charger_peer(bench, connection):
        await actual(bench, ErrorReplyPeer(connection, trace))

    monkeypatch.setattr(Bench, "read_charger", charger_peer)
    async with managed_bench(tmp_path, tls, unused_tcp_port) as rig:
        strategy = strategy_file(rig, tmp_path)
        config, native_port = tmp_path / "trial.json", rig.prepared.observer.port
        assert await operator.run("check", config, strategy, None, native_port, kind="vendor-write") == 0
        s = managed_session(rig)
        await s.start()
        c = ManagedController(s)
        rig.route_to_trial()
        async with rig.charger(s) as charger:
            await introduced(rig, s, charger, "vendor-debt-boot")
            await healthy(c)
            error = await rig.bench.cloud_call(
                "uncertain-app-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}
            )
            assert error[0] == 4 and error[2] == "InternalError" and len(trace) == 1
            row = s.journal.db.execute("SELECT * FROM mutations WHERE origin='vendor'").fetchone()
            token = row["token"]
            assert row["state"] == "uncertain"
            assert s.journal.get("uncertain_vendor_command")["mutation"] == token
            refused = await rig.bench.cloud_call(
                "changed-app-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": "OTHER_OWNER"}
            )
            assert refused[0] == 4 and len(trace) == 1
            pause = uuid.uuid4().hex
            c.command(pause, "pause", c.requests.intent().revision)
            await c.pause_task
            status = await c.tick()
            assert status["vendor_write_uncertain"] == 1 and status["operator_reconciliation_required"]
            if legacy_marker:
                s.journal.db.execute("DELETE FROM meta WHERE key='uncertain_vendor_command'")
            with pytest.raises((OSError, RecoveryRequiredError)):
                await operator.run("inspect", config, strategy, None, native_port, kind="vendor-write")
            counter = s.journal.get("next_transaction")
            pause_revision = c.requests.intent().revision
            await c.charging.close()
            await crash(s)
        before = list(rig.bench.device_calls)
        assert await operator.run("inspect", config, strategy, None, native_port, kind="vendor-write") == 0
        candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
        assert candidate["mutation_token"] == token
        evidence = candidate | {
            "command_quiescent": True,
            "original_state_independently_verified": True,
            "evidence_sha256": fingerprint(trace),
            "verified_at_epoch": time.time(),
        }
        path = tmp_path / "operator-evidence.json"
        for changed in (
            {"command_quiescent": False},
            {"original_state_independently_verified": 1},
            {"candidate_digest": "0" * 64},
            {"mutation_token": "0" * 32},
            {"payload_digest": "0" * 64},
            {"verified_at_epoch": time.time() - 7200},
            {"verified_at_epoch": time.time() + 60},
        ):
            save_private(path, evidence | changed)
            with pytest.raises((SecurityError, RecoveryRequiredError)):
                await operator.run("reconcile", config, strategy, path, native_port, kind="vendor-write")
        save_private(path, evidence)
        for key, changed in (
            ("managed_control_fault", {"classification": "native_authority_or_preservation_failed"}),
            ("managed_current_prepare_intent", {"stage": "pending"}),
            ("uncertain_vendor_command", {"action": "Other", "digest": "0" * 64}),
            ("vendor_start_grant", {"mutation": token, "local": 1001}),
            ("next_transaction", counter + 1),
        ):
            j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
            original = j.get(key)
            j.put(key, changed)
            j.close()
            with pytest.raises((SecurityError, RecoveryRequiredError)):
                await operator.run("reconcile", config, strategy, path, native_port, kind="vendor-write")
            j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
            if original is None:
                j.db.execute("DELETE FROM meta WHERE key=?", (key,))
            else:
                j.put(key, original)
            assert j.get(KEY) is None
            j.close()
        j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        j.db.execute(
            "CREATE TRIGGER refuse_operator_proof BEFORE INSERT ON meta "
            "WHEN NEW.key='managed_operator_debt_reconciliations' "
            "BEGIN SELECT RAISE(ABORT,'synthetic proof storage failure'); END"
        )
        j.close()
        import sqlite3

        with pytest.raises(sqlite3.IntegrityError, match="synthetic proof storage failure"):
            await operator.run("reconcile", config, strategy, path, native_port, kind="vendor-write")
        j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        assert j.db.execute("SELECT state FROM mutations WHERE token=?", (token,)).fetchone()[0] == "uncertain"
        assert j.get(KEY) is None
        j.db.execute("DROP TRIGGER refuse_operator_proof")
        j.close()
        assert await operator.run("reconcile", config, strategy, path, native_port, kind="vendor-write") == 0
        assert await operator.run("reconcile", config, strategy, path, native_port, kind="vendor-write") == 0
        assert rig.bench.device_calls == before and rig.bench.firmware.effective_limit() == 0
        j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        assert j.get("next_transaction") == counter and j.get("uncertain_vendor_command") is None
        assert j.get("vendor_start_grant") is None and j.get("managed_control_fault")
        assert j.db.execute("SELECT state FROM mutations WHERE token=?", (token,)).fetchone()[0] == "confirmed"
        # A bounded pool must not prune an unconsumed operator proof's exact reconciled mutation on restart.
        j.capacity = max(2, j.db.execute("SELECT COUNT(*) FROM mutations").fetchone()[0])
        for value in range(2):
            extra = j.mutation_intent("local", "SetChargingProfile", {"synthetic": value})
            j.mutation_finished(extra, True)
        assert j.db.execute("SELECT 1 FROM mutations WHERE token=?", (token,)).fetchone()
        j.close()
        replacement = managed_session(rig)
        await replacement.start()
        c = ManagedController(replacement)
        async with rig.charger(replacement) as charger:
            await introduced(rig, replacement, charger, "reconciled-vendor-boot")
            assert (await c.tick())["state"] == "attention"
            # Replaying the old Pause UUID does not acknowledge the later independent proof.
            c.command(pause, "pause", pause_revision - 1)
            assert (await c.tick())["state"] == "attention"
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            if c.pause_task is not None:
                await c.pause_task
            assert (await healthy(c))["local_hold_verified"]
            assert all(entry["completed"] is not None for entry in replacement.journal.get(KEY).values())
            assert rig.bench.firmware.effective_limit() == 0 and len(starts(rig)) == 1
            request = uuid.uuid4().hex
            c.command(request, "app_control", c.requests.intent().revision)
            assert (await healthy(c))["station_ceiling_a"] == 6
            assert c.requests.receipt(request)["state"] == "completed" and len(starts(rig)) == 1


# Intent: two real same-tag lifecycles and the full startup budget cannot themselves consume a pending owner Start.
# Why: explicit independent command settlement is required before retiring ambiguous attribution at zero.
async def test_unattributed_start_requires_exact_operator_evidence(tmp_path, tls, unused_tcp_port, monkeypatch, capsys):
    from datetime import UTC, datetime

    async with managed_bench(tmp_path, tls, unused_tcp_port) as rig:
        strategy = strategy_file(rig, tmp_path)
        config, native_port = tmp_path / "trial.json", rig.prepared.observer.port
        s = managed_session(rig)
        await s.start()
        c = ManagedController(s)
        rig.route_to_trial()
        async with rig.charger(s) as charger:
            await introduced(rig, s, charger, "unattributed-owner-boot")
            await healthy(c)
            request = uuid.uuid4().hex
            c.command(request, "start", c.requests.intent().revision)
            await c.charging.settle()
            tag = starts(rig)[0][3]["idTag"]
            original_deadline = c.charging.state()["hard_deadline_epoch"]
            local_ids = []
            for number in range(2):
                stamp = datetime.fromtimestamp(time.time() + number, UTC).isoformat()
                opened = await rig.bench.call(
                    charger,
                    f"unobserved-start-{number}",
                    "StartTransaction",
                    START | {"idTag": tag, "timestamp": stamp},
                )
                local = opened[2]["transactionId"]
                local_ids.append(local)
                rig.peer.plan_session = (local, 9 if number == 0 else 987654)
                assert (
                    await rig.bench.call(
                        charger,
                        f"unobserved-stop-{number}",
                        "StopTransaction",
                        {
                            "transactionId": local,
                            "timestamp": stamp,
                            "meterStop": 1100,
                        },
                    )
                )[0] == 3
                await until(lambda: s.journal.next_delivery() is None)
            assert s.journal.get("control")["start_pending"]
            assert c.charging.state().get("local_transaction") is None
            pause = uuid.uuid4().hex
            c.command(pause, "pause", c.requests.intent().revision)
            await c.pause_task
            assert rig.bench.firmware.effective_limit() == 0
            # Keep the actual production deadline. No test clock or engine deadline is replaced.
            await asyncio.sleep(max(0, original_deadline - time.time()) + 0.1)
            await c.tick()
            assert s.journal.get("control")["start_pending"] and len(starts(rig)) == 1
            await c.charging.close()
            await crash(s)
        before = list(rig.bench.device_calls)
        assert await operator.run("inspect", config, strategy, None, native_port, kind="local-start") == 0
        candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
        assert candidate["request_id"] == request
        evidence = candidate | {
            "command_no_longer_pending": True,
            "evidence_sha256": fingerprint(local_ids),
            "verified_at_epoch": time.time(),
        }
        path = tmp_path / "operator-start-evidence.json"
        for changed in (
            {"command_no_longer_pending": False},
            {"receipt_digest": "0" * 64},
            {"state_digest": "0" * 64},
            {"request_id": "0" * 32},
        ):
            save_private(path, evidence | changed)
            with pytest.raises((SecurityError, RecoveryRequiredError)):
                await operator.run("reconcile", config, strategy, path, native_port, kind="local-start")
        save_private(path, evidence)
        assert await operator.run("reconcile", config, strategy, path, native_port, kind="local-start") == 0
        assert rig.bench.device_calls == before and rig.bench.firmware.effective_limit() == 0
        j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        assert j.get("managed_test_charge")["state"] == "completed"
        assert j.get("local_start_receipt")["stage"] == "reconciled" and not j.get("control")["start_pending"]
        assert not j.get("managed_deferred_start")
        assert len(j.db.execute("SELECT * FROM transactions WHERE stop IS NOT NULL").fetchall()) == 2
        j.close()
        replacement = managed_session(rig)
        await replacement.start()
        c = ManagedController(replacement)
        async with rig.charger(replacement) as charger:
            await introduced(rig, replacement, charger, "reconciled-local-start-boot")
            assert (await c.tick())["state"] == "attention"
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            if c.pause_task is not None:
                await c.pause_task
            assert (await healthy(c))["local_hold_verified"]
            c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
            assert (await healthy(c))["station_ceiling_a"] == 6
            assert len(starts(rig)) == 1


# Intent: a genuine drained daily closure can settle its exact uncertain vendor Stop with the owner Start proof.
# Why: each debt used to block the other even after the restrictive command had no possible remaining target.
@pytest.mark.parametrize("reuse", [False, True])
async def test_owner_start_reconciliation_retires_exact_closed_vendor_stop(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys, reuse
):
    monkeypatch.setattr(managed_charging, "STARTUP_BUDGET", 3)
    trace = []
    actual = Bench.read_charger

    # The charger sends an ambiguous error for the real vendor Stop while all other replies stay ordinary.
    async def charger_peer(bench, connection):
        await actual(bench, ErrorReplyPeer(connection, trace, "RemoteStopTransaction"))

    monkeypatch.setattr(Bench, "read_charger", charger_peer)
    async with managed_bench(tmp_path, tls, unused_tcp_port) as rig:
        strategy = strategy_file(rig, tmp_path)
        config, native_port = tmp_path / "trial.json", rig.prepared.observer.port
        journal_path, identity = rig.prepared.config.journal, rig.prepared.config.identity
        session = managed_session(rig)
        await session.start()
        c, j = ManagedController(session), session.journal
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "overlapping-debts")
            await healthy(c)
            if reuse:
                earlier = await rig.bench.call(
                    charger, "earlier-daily", "StartTransaction", START | {"idTag": "NATIVE_TIMER", "meterStart": 800}
                )
                previous = earlier[2]["transactionId"]
                await until(lambda: j.vendor_id(previous) is not None)
                reused_vendor = j.vendor_id(previous)
                assert reused_vendor is not None
                assert (
                    await rig.bench.call(
                        charger,
                        "earlier-stop",
                        "StopTransaction",
                        {"transactionId": previous, "timestamp": START["timestamp"], "meterStop": 900},
                    )
                )[0] == 3
                await until(lambda: j.next_delivery() is None)
                rig.bench.next_vendor = reused_vendor - 1
                await healthy(c)
            request = uuid.uuid4().hex
            c.command(request, "start", c.requests.intent().revision)
            await c.charging.settle()
            tag = starts(rig)[0][3]["idTag"]
            assert j.get("local_start_receipt")["stage"] == "accepted"
            deadline = c.charging.state()["hard_deadline_epoch"]
            started = await rig.bench.call(charger, "independent-daily", "StartTransaction", START | {"idTag": tag})
            local = started[2]["transactionId"]
            rig.peer.plan_session = (local, 9)
            assert j.get("local_start_receipt")["local"] is None
            await until(lambda: j.vendor_id(local) is not None)
            vendor = j.vendor_id(local)
            if reuse:
                assert vendor == j.vendor_id(previous)
            cloud_stop = await rig.bench.cloud_call(
                "daily-vendor-stop", "RemoteStopTransaction", {"transactionId": vendor}
            )
            assert cloud_stop[0] == 4 and [call[3] for call in trace] == [{"transactionId": local}]
            mutation = j.get("uncertain_vendor_command")["mutation"]
            stop = {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100}
            assert (await rig.bench.call(charger, "daily-closed", "StopTransaction", stop))[0] == 3
            await until(lambda: j.next_delivery() is None)
            assert any(frame[0] == 2 and frame[2] == "StopTransaction" for frame in rig.bench.calls)
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            await c.pause_task
            await asyncio.sleep(max(0, deadline - time.time()) + 0.1)
            assert j.get("control")["start_pending"] and rig.bench.firmware.effective_limit() == 0
            assert j.db.execute("SELECT state FROM mutations WHERE token=?", (mutation,)).fetchone()[0] == "uncertain"
            await c.charging.close()
            await crash(session)

        for mapping in (vendor + 1, None):
            with closing(Journal(journal_path, identity)) as journal:
                journal.db.execute("UPDATE transactions SET vendor=? WHERE local=?", (mapping, local))
            with pytest.raises(RecoveryRequiredError, match="another unsettled operation"):
                await operator.run("inspect", config, strategy, None, native_port, kind="local-start")
        with closing(Journal(journal_path, identity)) as journal:
            journal.db.execute("UPDATE transactions SET vendor=? WHERE local=?", (vendor, local))
            # Contradictory modern scope cannot fall back to a matching historical cloud mapping.
            journal.db.execute("UPDATE vendor_command_scope SET local=? WHERE mutation=?", (local + 1, mutation))
        with pytest.raises(RecoveryRequiredError, match="another unsettled operation"):
            await operator.run("inspect", config, strategy, None, native_port, kind="local-start")
        with closing(Journal(journal_path, identity)) as journal:
            journal.db.execute("UPDATE vendor_command_scope SET local=? WHERE mutation=?", (local, mutation))
            assert journal.get(KEY) is None and journal.get("control")["start_pending"]

        assert await operator.run("inspect", config, strategy, None, native_port, kind="local-start") == 0
        candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
        assert candidate["request_id"] == request
        evidence = candidate | {
            "command_no_longer_pending": True,
            "evidence_sha256": fingerprint(request),
            "verified_at_epoch": time.time(),
        }
        path = tmp_path / "operator-overlap-evidence.json"
        save_private(path, evidence)
        commands = list(rig.bench.device_calls)
        assert await operator.run("reconcile", config, strategy, path, native_port, kind="local-start") == 0
        assert await operator.run("reconcile", config, strategy, path, native_port, kind="local-start") == 0
        assert rig.bench.device_calls == commands and rig.bench.firmware.effective_limit() == 0
        with closing(Journal(journal_path, identity)) as journal:
            proof = journal.get(KEY)[fingerprint(evidence)]["proof"]
            assert proof["prior"]["closed_vendor_stop"] == {
                "mutation_token": mutation,
                "local_transaction_id": local,
                "vendor_transaction_id": vendor,
                "stop_digest": fingerprint(stop),
                "cloud_stop_acknowledged": True,
                "command_reply": "unknown",
            }
            assert (
                journal.db.execute("SELECT state FROM mutations WHERE token=?", (mutation,)).fetchone()[0]
                == "confirmed"
            )
            assert not journal.get("uncertain_vendor_command") and not journal.get("control")["start_pending"]
            assert journal.get("local_start_receipt")["stage"] == "reconciled"
        replacement = managed_session(rig)
        await replacement.start()
        c = ManagedController(replacement)
        async with rig.charger(replacement) as charger:
            await introduced(rig, replacement, charger, "overlap-after-reconciliation")
            assert (await c.tick())["state"] == "attention"
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            if c.pause_task is not None:
                await c.pause_task
            assert (await healthy(c))["local_hold_verified"]
            assert len(trace) == 1 and len(starts(rig)) == 1 and rig.bench.firmware.effective_limit() == 0


class HeldReportPeer:
    """The cloud withholds one ordinary report while the charger accepts and closes its own real lifecycle."""

    # Keep the real cloud protocol implementation and expose only a peer-owned gate.
    def __init__(self, connection, gate):
        self.connection, self.gate = connection, gate
        self.targets = set()

    # Observe the first routine report on its actual upstream transport.
    def __aiter__(self):
        # The receiver remains independent of the proxy journal and control state.
        async def frames():
            async for raw in self.connection:
                frame = json.loads(raw)
                if self.gate["armed"] and frame[0] == 2 and frame[2] == "StatusNotification":
                    self.targets.add(frame[1])
                    self.gate["seen"].set()
                yield raw

        return frames()

    # Hold the reply until transport retirement, leaving the exact later lifecycle in the proxy FIFO.
    async def send(self, raw):
        frame = json.loads(raw)
        if frame[0] == 3 and frame[1] in self.targets:
            await self.gate["release"].wait()
        await self.connection.send(raw)


# Intent: legacy rejected telemetry is retired exactly once, leaving queued genuine Start/Stop to deliver normally.
# Why: migration must clear the poison head without waiving ambiguous lifecycle or releasing its latched zero hold.
@pytest.mark.parametrize("later", ["success", "start_lost", "start_refused", "stop_refused"])
async def test_legacy_observation_recovery_preserves_queued_lifecycle(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys, later
):
    from tests.prototype_rig import START

    gate = {"armed": False, "seen": asyncio.Event(), "release": asyncio.Event()}
    actual = StartReplyBench.vendor

    # Delay only the independent cloud peer; real local charger acceptance continues over its own TLS connection.
    async def cloud(bench, connection):
        await actual(bench, HeldReportPeer(connection, gate))

    monkeypatch.setattr(StartReplyBench, "vendor", cloud)
    monkeypatch.setattr(managed_start_delivery, "RETRY_INTERVAL", 0.02)
    async with managed_bench(tmp_path, tls, unused_tcp_port, bench_type=StartReplyBench) as rig:
        strategy = strategy_file(rig, tmp_path)
        config, native_port = tmp_path / "trial.json", rig.prepared.observer.port
        s = managed_session(rig)
        await s.start()
        c = ManagedController(s)
        rig.route_to_trial()
        try:
            async with rig.charger(s) as charger:
                await introduced(rig, s, charger, "legacy-observation-boot")
                await healthy(c)
                gate["armed"] = True
                assert (
                    await rig.bench.call(
                        charger,
                        "poison-report",
                        "StatusNotification",
                        {"connectorId": 1, "status": "Available", "errorCode": "NoError"},
                    )
                )[0] == 3
                await asyncio.wait_for(gate["seen"].wait(), 2)
                opened = await rig.bench.call(
                    charger, "queued-real-start", "StartTransaction", START | {"idTag": "NATIVE_TIMER"}
                )
                local = opened[2]["transactionId"]
                assert (
                    await rig.bench.call(
                        charger,
                        "queued-real-stop",
                        "StopTransaction",
                        {"transactionId": local, "meterStop": 1100, "timestamp": START["timestamp"]},
                    )
                )[0] == 3
                c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
                await c.pause_task
                await c.tick()
                await c.charging.close()
                await crash(s)
            gate["armed"] = False
            gate["release"].set()
            j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
            head = j.next_delivery()
            assert head.action == "StatusNotification"
            # Recreate only the serialized pre-upgrade rejection state, from an actually transmitted routine row.
            j.db.execute("UPDATE outbox SET state='rejected' WHERE token=?", (head.token,))
            j.put(
                "managed_control_fault",
                {"classification": "owner_control_or_delivery_uncertain", "at_epoch": time.time()},
            )
            assert j.vendor_id(local) is None
            tail = [dict(row) for row in j.db.execute("SELECT * FROM outbox WHERE local=?", (local,))]
            assert [row["action"] for row in tail] == ["StartTransaction", "StopTransaction"]
            j.close()
            assert await operator.run("inspect", config, strategy, None, native_port, kind="observation") == 0
            candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
            path = tmp_path / "report-evidence.json"
            save_private(path, candidate | {"evidence_sha256": fingerprint(tail), "verified_at_epoch": time.time()})
            before = list(rig.bench.device_calls)
            assert await operator.run("reconcile", config, strategy, path, native_port, kind="observation") == 0
            assert rig.bench.device_calls == before
            j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
            assert [dict(row) for row in j.db.execute("SELECT * FROM outbox WHERE local=?", (local,))] == tail
            assert (
                j.get("managed_control_fault")
                and j.db.execute("SELECT code FROM rejected_observations").fetchone()[0] is None
            )
            j.close()
            if later != "success":
                if later == "stop_refused":
                    rig.bench.stop_replies = ["error"]
                elif later == "start_refused":
                    rig.bench.start_replies = ["error"] * 3
                else:
                    rig.bench.start_replies = ["error", "silent"]
                async with running(rig, "legacy-observation-later-failure") as (failed, _, _):
                    states = ("ambiguous",) if later == "start_lost" else ("rejected",)
                    await until(
                        lambda: (
                            failed.journal.next_delivery() is not None
                            and failed.journal.next_delivery().state in states
                        ),
                        timeout=10,
                    )
                    assert rig.bench.firmware.effective_limit() == 0
                    if later == "start_lost":
                        start_token = tail[0]["token"]
                        old_retry = failed.journal.get(managed_start_delivery.RETRIES, {})[start_token]
                if later == "start_lost":
                    await map_unknown_retry(rig, tmp_path, capsys)
                    with closing(Journal(rig.prepared.config.journal, rig.prepared.config.identity)) as j:
                        assert start_token not in j.get(managed_start_delivery.RETRIES, {})
                        # Older builds can reopen the same attestation and retire the exact leftover retry.
                        j.put(managed_start_delivery.RETRIES, {start_token: old_retry})
                    path = tmp_path / "mapping-evidence.json"
                    assert await mapping.run("reconcile-start", config, strategy, path, native_port) == 0
                    with closing(Journal(rig.prepared.config.journal, rig.prepared.config.identity)) as j:
                        assert start_token not in j.get(managed_start_delivery.RETRIES, {})
                elif later == "start_refused":
                    await approve_retry(rig, tmp_path, capsys)
                else:
                    await approve_stop_after_start(rig, tmp_path, capsys)
            replacement = managed_session(rig)
            await replacement.start()
            c = ManagedController(replacement)
            async with rig.charger(replacement) as charger:
                await introduced(rig, replacement, charger, "migrated-observation-boot")
                await until(lambda: replacement.journal.next_delivery() is None)
                assert replacement.journal.vendor_id(local) is not None
                assert (await c.tick())["state"] == "attention" and rig.bench.firmware.effective_limit() == 0
                c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
                if c.pause_task is not None:
                    await c.pause_task
                assert (await healthy(c))["local_hold_verified"]
                c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
                assert (await healthy(c))["station_ceiling_a"] == 6 and not starts(rig)
        finally:
            gate["release"].set()
