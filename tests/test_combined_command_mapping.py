"""Independent command settlement and accepted cloud identity compose without waiving other debt."""

import asyncio
import copy
import json
import sqlite3
import time
import uuid
from contextlib import closing

import pytest
from ocpp_hub_core.frames import build_call_error
from ocpp_hub_core.prototype.managed_debt_recovery import KEY
from ocpp_hub_core.prototype.store import ConflictError, RecoveryRequiredError, fingerprint
from ocpp_hub_core.prototype.trial_session import save_private

from tests import conftest as _core_setup  # noqa: F401 - load the HA-free source package.
from tests.cloud_start_helpers import two_later_sessions
from tests.handover_helpers import crash
from tests.test_combined_recovery_proofs import selected_service
from tests.test_managed_debt_reconciliation import strategy_file
from tests.test_managed_proxy import healthy, starts
from tests.test_managed_stop_reply_recovery import pause, running
from tests.test_managed_test_charge import ChargeRig
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests.test_trial_session import Bench
from tools import reconcile_managed_debt as debt_tool
from tools import reconcile_managed_delivery as delivery_tool

KIND = "vendor-start-mapping"


class CompletedCommandReply:
    """The independent firmware finishes a real command, then its real reply is controlled separately."""

    # Preserve complete command bytes and the genuine firmware answer independently from the gateway's records.
    def __init__(self, connection, completed):
        self.connection, self.completed, self.targets = connection, completed, {}

    # Identify only commands which actually reached the independent physical peer on its TLS socket.
    async def __aiter__(self):
        async for raw in self.connection:
            frame = json.loads(raw)
            if frame[0] == 2 and frame[2] == "RemoteStartTransaction":
                self.targets[frame[1]] = frame
            yield raw

    # Once the ordinary firmware has produced Accepted, retain that actual answer for independent attestation.
    async def send(self, raw):
        frame = json.loads(raw)
        if frame[0] == 3 and frame[1] in self.targets:
            self.completed.append({"command": self.targets[frame[1]], "answer": frame})
            return
        await self.connection.send(raw)


# Inspect independently committed rows without invoking Journal startup recovery or taking ownership.
def committed_state(path):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        return {
            "mutations": db.execute("SELECT * FROM mutations ORDER BY token").fetchall(),
            "transactions": db.execute("SELECT * FROM transactions ORDER BY local").fetchall(),
            "outbox": db.execute("SELECT * FROM outbox ORDER BY seq").fetchall(),
            "meta": db.execute("SELECT * FROM meta ORDER BY key").fetchall(),
        }


# Install a real statement or deferred-COMMIT refusal on the offline scratch journal before the tool owns it.
def install_failure(path, boundary):
    with closing(sqlite3.connect(path)) as db:
        if boundary == "mutation":
            trigger = "BEFORE UPDATE ON mutations WHEN NEW.state='confirmed'"
            effect = "SELECT RAISE(ABORT,'synthetic combined command settlement refusal')"
        elif boundary == "mapping":
            trigger = "BEFORE UPDATE ON transactions WHEN NEW.vendor IS NOT NULL"
            effect = "SELECT RAISE(ABORT,'synthetic combined mapping refusal')"
        elif boundary == "proof":
            trigger = f"BEFORE INSERT ON meta WHEN NEW.key='{KEY}'"
            effect = "SELECT RAISE(ABORT,'synthetic combined proof refusal')"
        else:
            db.execute("CREATE TABLE combined_parent(id INTEGER PRIMARY KEY)")
            db.execute("CREATE TABLE combined_child(id REFERENCES combined_parent(id) DEFERRABLE INITIALLY DEFERRED)")
            trigger, effect = f"AFTER INSERT ON meta WHEN NEW.key='{KEY}'", "INSERT INTO combined_child VALUES(1)"
        db.execute(f"CREATE TRIGGER refuse_combined {trigger} BEGIN {effect}; END")
        db.commit()


# Remove only the test's fault injection; no state repair, mapping, receipt retirement or control write is performed.
def remove_failure(path):
    with closing(sqlite3.connect(path)) as db:
        db.execute("DROP TRIGGER refuse_combined")
        db.commit()


# Reach the actual correlated command/report failures before obtaining a genuine local Stop and explicit zero hold.
async def fail_one_cycle(b, completed, uncertain_command, lost_delivery, number):
    rig, s, c, sock = b.rig, b.session, b.controller, b.charger
    before = len(completed)
    rig.bench.start_replies = ["silent"] if lost_delivery else []
    command = asyncio.create_task(
        rig.bench.cloud_call(uuid.uuid4().hex, "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"})
    )
    try:
        await until(lambda: len(completed) == before + 1)
        observed = completed[-1]
        assert observed["answer"][2] == {"status": "Accepted"}
        minute = number * 20
        opened = await rig.bench.call(
            sock,
            uuid.uuid4().hex,
            "StartTransaction",
            START
            | {
                "idTag": "CLOUD_OWNER",
                "meterStart": 2000 + 1000 * number,
                "timestamp": f"2026-01-01T00:{minute:02d}:00Z",
            },
        )
        assert opened[0] == 3
        local = opened[2]["transactionId"]
        await until(
            lambda: any(
                row["body"]["timestamp"] == f"2026-01-01T00:{minute:02d}:00Z" for row in rig.bench.backend_starts
            )
        )
        backend = next(
            row for row in rig.bench.backend_starts if row["body"]["timestamp"] == f"2026-01-01T00:{minute:02d}:00Z"
        )
        assert backend["answer"]["idTagInfo"]["status"] == "Accepted"
        rig.peer.plan_session, rig.peer.permission_enabled, rig.peer.paused = (local, 987654), True, False
        rig.bench.cp, rig.bench.connection = "C", "Charging"
        rig.bench.firmware.requested_current = c.current.selected()
        assert rig.bench.firmware.current == c.current.selected()
        answer = observed["answer"]
        raw = (
            build_call_error(answer[1], "InternalError", "synthetic uncertain command reply")
            if uncertain_command
            else json.dumps(answer)
        )
        await sock.send(raw)
        assert (await command)[0] == (4 if uncertain_command else 3)
        rig.peer.permission_enabled = False
        assert (
            await rig.bench.call(
                sock,
                uuid.uuid4().hex,
                "StopTransaction",
                {
                    "transactionId": local,
                    "meterStop": 2100 + 1000 * number,
                    "timestamp": f"2026-01-01T00:{minute + 1:02d}:00Z",
                },
            )
        )[0] == 3
        if lost_delivery:
            await until(
                lambda: s.journal.next_delivery() is not None and s.journal.next_delivery().state == "ambiguous",
                timeout=12,
            )
        else:
            await until(lambda: s.journal.next_delivery() is None)
        await c.tick()
        await pause(c)
        assert rig.bench.firmware.effective_limit() == 0 and s.journal.active() is None
        assert not s.journal.get("vendor_start_grant") and not rig.bench.backend_errors
        return local, copy.deepcopy(backend), copy.deepcopy(observed)
    finally:
        if not command.done():
            command.cancel()
        await asyncio.gather(command, return_exceptions=True)


# Invoke only supported offline inspection and explicit attestation while no service owns the closed journal.
async def reconcile_cycle(
    rig, tmp_path, capsys, local, backend, observed, uncertain_command, lost_delivery, boundary=None
):
    config, strategy, port = tmp_path / "trial.json", strategy_file(rig, tmp_path), rig.prepared.observer.port
    device_calls = copy.deepcopy(rig.bench.device_calls)
    if uncertain_command:
        kind = KIND if lost_delivery else "vendor-write"
        assert await debt_tool.run("inspect", config, strategy, None, port, kind=kind) == 0
        candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
        evidence = candidate | {
            "command_quiescent": True,
            "original_state_independently_verified": True,
            "evidence_sha256": fingerprint({"backend": backend, "physical": observed}),
            "verified_at_epoch": time.time(),
        }
        if lost_delivery:
            assert candidate["local_transaction_id"] == local and candidate["delivery_token"] == backend["token"]
            assert candidate["payload_digest"] == fingerprint(observed["command"][3])
            evidence |= {
                "vendor_transaction_id": backend["answer"]["transactionId"],
                "command_session_independently_verified": True,
                "cloud_start_accepted": True,
            }
        tool_args, kwargs = ("reconcile", config, strategy), {"kind": kind}
        invoke = debt_tool.run
    else:
        assert await delivery_tool.run("inspect", config, strategy, None, port) == 0
        candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
        evidence = candidate | {
            "vendor": backend["answer"]["transactionId"],
            "evidence_sha256": fingerprint(backend),
            "verified_at_epoch": time.time(),
        }
        tool_args, kwargs, invoke = ("reconcile-start", config, strategy), {}, delivery_tool.run
    proof_path = tmp_path / "combined-independent-evidence.json"
    save_private(proof_path, evidence)
    if boundary is not None:
        install_failure(rig.prepared.config.journal, boundary)
        before = committed_state(rig.prepared.config.journal)
        try:
            with pytest.raises((sqlite3.IntegrityError, ConflictError)):
                await invoke(*tool_args, proof_path, port, **kwargs)
            assert committed_state(rig.prepared.config.journal) == before
            assert rig.bench.device_calls == device_calls
        finally:
            remove_failure(rig.prepared.config.journal)
    assert await invoke(*tool_args, proof_path, port, **kwargs) == 0
    capsys.readouterr()
    assert rig.bench.device_calls == device_calls
    return fingerprint(evidence)


# Keep the engine genuine while controlling only an actual independent firmware reply on the TLS wire.
def observe_completed_commands(monkeypatch):
    completed, actual = [], Bench.read_charger

    # The independent firmware still implements the command and every ordinary profile/composite request itself.
    async def read_charger(bench, connection):
        await actual(bench, CompletedCommandReply(connection, completed))

    monkeypatch.setattr(Bench, "read_charger", read_charger)
    return completed


# Intent: each separate failure and the combined failure recover through actual tools, real Stop ACK and fresh Pause.
# Why: two individually valid checks must have a safe composed recovery order and retain honest command history.
@pytest.mark.parametrize("selected", [6, 32])
@pytest.mark.parametrize("uncertain_command,lost_delivery", [(True, False), (False, True), (True, True)])
async def test_actual_combined_tool_and_individual_controls(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys, selected, uncertain_command, lost_delivery
):
    completed = observe_completed_commands(monkeypatch)
    async with selected_service(tmp_path, tls, unused_tcp_port, monkeypatch, selected) as seed:
        rig = seed.rig
        await seed.controller.tests.close()
        await crash(seed.session)
        for number in range(2 if uncertain_command and lost_delivery else 1):
            async with running(rig, f"combined-prefix-{number}") as (s, c, sock):
                await healthy(c)
                local, backend, observed = await fail_one_cycle(
                    ChargeRig(rig, s, c, sock), completed, uncertain_command, lost_delivery, number
                )
                assert backend["closed"] is (not lost_delivery)
            proof_id = await reconcile_cycle(
                rig, tmp_path, capsys, local, backend, observed, uncertain_command, lost_delivery
            )
            async with running(rig, f"combined-recovered-{number}") as (s, c, sock):
                await until(lambda: s.journal.next_delivery() is None)
                assert s.journal.vendor_id(local) == backend["answer"]["transactionId"]
                actual = next(row for row in rig.bench.backend_starts if row["token"] == backend["token"])
                assert actual["closed"] and actual["stop"]["body"]["transactionId"] == actual["answer"]["transactionId"]
                assert (await c.tick())["state"] == "attention" and rig.bench.firmware.effective_limit() == 0
                with pytest.raises(RecoveryRequiredError):
                    c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
                if uncertain_command:
                    entry = s.journal.get(KEY)[proof_id]
                    assert entry["completed"] is None
                    if lost_delivery:
                        assert entry["proof"]["wire_ack"]["delivery_token"] == actual["stop"]["token"]
                await pause(c)
                assert s.journal.get("managed_control_fault") is None
                assert (await healthy(c))["local_hold_verified"]
                if uncertain_command:
                    assert s.journal.get(KEY)[proof_id]["completed"] is not None
                await two_later_sessions(
                    ChargeRig(rig, s, c, sock),
                    selected=selected,
                    original_starts=number + 1,
                    first_minute=10 + 20 * number,
                )
                assert len(starts(rig)) == number + 1 and not rig.bench.backend_errors


# Intent: command retirement, mapping, proof insertion and final COMMIT roll back as one real offline tool operation.
# Why: separately committed halves can either lose command uncertainty or strand the accepted cloud transaction.
@pytest.mark.parametrize("boundary", ["mutation", "mapping", "proof", "commit"])
async def test_combined_tool_atomic_failures_then_exact_recovery(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys, boundary
):
    completed = observe_completed_commands(monkeypatch)
    async with selected_service(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as seed:
        rig = seed.rig
        await seed.controller.tests.close()
        await crash(seed.session)
        async with running(rig, "combined-atomic-prefix") as (s, c, sock):
            await healthy(c)
            local, backend, observed = await fail_one_cycle(ChargeRig(rig, s, c, sock), completed, True, True, 0)
        proof_id = await reconcile_cycle(rig, tmp_path, capsys, local, backend, observed, True, True, boundary)
        async with running(rig, "combined-atomic-recovery") as (s, c, sock):
            await until(lambda: s.journal.next_delivery() is None)
            assert s.journal.get(KEY)[proof_id]["proof"]["wire_ack"] is not None
            await pause(c)
            assert s.journal.get("managed_control_fault") is None
            assert (await healthy(c))["local_hold_verified"]
            await two_later_sessions(ChargeRig(rig, s, c, sock), selected=6, original_starts=1)


# Queue an earlier native denial and this command's distinct genuine session before either cloud outcome is known.
async def fail_prior_denial_and_combined(b, completed):
    rig, s, c, sock = b.rig, b.session, b.controller, b.charger
    before = len(completed)
    gate = asyncio.Event()
    rig.bench.start_reply_gate, rig.bench.start_replies = gate, ["Blocked", "silent"]
    command = asyncio.create_task(
        rig.bench.cloud_call(uuid.uuid4().hex, "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"})
    )
    try:
        await until(lambda: len(completed) == before + 1)
        observed, ids = copy.deepcopy(completed[-1]), []
        for number, tag in enumerate(("NATIVE_TIMER", "CLOUD_OWNER")):
            opened = await rig.bench.call(
                sock,
                uuid.uuid4().hex,
                "StartTransaction",
                START
                | {
                    "idTag": tag,
                    "meterStart": 1000 + 100 * number,
                    "timestamp": f"2026-01-01T00:{number * 2:02d}:00Z",
                },
            )
            assert opened[0] == 3 and opened[2]["idTagInfo"]["status"] == "Accepted"
            local = opened[2]["transactionId"]
            ids.append(local)
            assert (
                await rig.bench.call(
                    sock,
                    uuid.uuid4().hex,
                    "StopTransaction",
                    {
                        "transactionId": local,
                        "meterStop": 1100 + 100 * number,
                        "timestamp": f"2026-01-01T00:{number * 2 + 1:02d}:00Z",
                    },
                )
            )[0] == 3
        assert ids[0] != ids[1] and not s.journal.get("control").get("inhibit")
        assert [row[0] for row in s.journal.db.execute("SELECT action FROM outbox ORDER BY seq")] == (
            ["StartTransaction", "StopTransaction"] * 2
        )
        await sock.send(build_call_error(observed["answer"][1], "InternalError", "synthetic uncertain command reply"))
        rig.bench.start_reply_gate = None
        gate.set()
        assert (await command)[0] == 4
        await until(
            lambda: (
                s.journal.next_delivery() is not None
                and s.journal.next_delivery().transaction == ids[1]
                and s.journal.next_delivery().state == "ambiguous"
            ),
            timeout=12,
        )
        await c.tick()
        await pause(c)
        fault = copy.deepcopy(s.journal.get("managed_control_fault"))
        prior = s.journal.get("managed_start_denials")[str(ids[0])]
        assert prior["fault"] == fault and prior["completed"] is None
        assert prior["wire_ack"]["local_transaction_id"] == ids[0]
        assert s.journal.vendor_id(ids[0]) is not None and s.journal.vendor_id(ids[1]) is None
        assert rig.bench.firmware.effective_limit() == 0
        backend = next(row for row in rig.bench.backend_starts if row["body"]["idTag"] == "CLOUD_OWNER")
        return ids, copy.deepcopy(backend), observed, fault
    finally:
        rig.bench.start_reply_gate = None
        gate.set()
        if not command.done():
            command.cancel()
        await asyncio.gather(command, return_exceptions=True)


# Prove every active exact operator proof is ready before allowing any associated denial to authorize fault clear.
async def finish_prior_denial_and_operator(b, prior_local, proof_id, selected):
    from ocpp_hub_core.prototype.managed_release_recovery import ACK, release_evidence
    from ocpp_hub_core.prototype.managed_start_delivery import DENIALS, denied_start_evidence

    from tests.test_combined_recovery_proofs import pause_before_reconciliation

    c, j, rig = b.controller, b.session.journal, b.rig
    await until(lambda: j.next_delivery() is None)
    current_fault = copy.deepcopy(j.get("managed_control_fault"))
    assert (await c.tick())["state"] == "attention" and rig.bench.firmware.effective_limit() == 0
    records = copy.deepcopy(j.get(KEY))
    assert records[proof_id]["proof"]["wire_ack"] is not None
    original_denial = copy.deepcopy(j.get(DENIALS)[str(prior_local)])
    assert denied_start_evidence(c, current_fault) is not None
    broken = copy.deepcopy(records)
    broken[proof_id]["proof"]["wire_ack"] = None
    j.put(KEY, broken)
    assert release_evidence(c, current_fault) is None
    early = await pause_before_reconciliation(c)
    assert (await c.tick())["state"] == "attention"
    assert j.get(ACK, {}).get("pause_request") != early
    assert j.get(DENIALS)[str(prior_local)] == original_denial
    assert j.get("managed_control_fault") == current_fault
    j.put(KEY, records)
    final = await pause_before_reconciliation(c)
    ack = j.get(ACK)
    assert ack["pause_request"] == final and "operator_reconciliation" in ack and "denied_start_reconciliation" in ack
    await c.tick()
    assert j.get("managed_control_fault") is None
    assert j.get(DENIALS)[str(prior_local)]["completed"]["pause"] == final
    assert j.get(KEY)[proof_id]["completed"]["pause"] == final
    assert not any(row.get("completed") is None and not row.get("superseded") for row in j.get(KEY).values())
    assert (await healthy(c))["local_hold_verified"]
    await two_later_sessions(b, selected=selected, original_starts=1)


# Intent: a prior denied lifecycle remains in the all-proof Pause across combined mapping and actual Stop refusal.
# Why: changing fault classification or completing operator evidence first must not silently hide an older denial.
@pytest.mark.parametrize("selected", [6, 32])
@pytest.mark.parametrize("stop_mode", ["accepted", "error", "bad-info"])
async def test_prior_denial_composes_with_combined_and_refused_stop_recovery(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys, selected, stop_mode
):
    from tests.test_managed_stop_reply_recovery import reconcile as reconcile_stop

    completed = observe_completed_commands(monkeypatch)
    async with selected_service(tmp_path, tls, unused_tcp_port, monkeypatch, selected) as seed:
        rig = seed.rig
        await seed.controller.tests.close()
        await crash(seed.session)
        async with running(rig, "denial-before-combined") as (s, c, sock):
            await healthy(c)
            ids, backend, observed, original_fault = await fail_prior_denial_and_combined(
                ChargeRig(rig, s, c, sock), completed
            )
        composite = await reconcile_cycle(rig, tmp_path, capsys, ids[1], backend, observed, True, True)
        if stop_mode != "accepted":
            rig.bench.stop_replies = [stop_mode]
            async with running(rig, "composite-own-stop-refused") as (s, c, sock):
                await until(
                    lambda: s.journal.next_delivery() is not None and s.journal.next_delivery().state == "rejected"
                )
                assert s.journal.get(KEY)[composite]["superseded"]
                assert s.journal.get("managed_control_fault") == original_fault
                assert s.journal.get("managed_start_denials")[str(ids[0])]["fault"] == original_fault
                await pause(c)
                assert s.journal.get("managed_control_fault") == original_fault
            path = await reconcile_stop(rig, tmp_path, capsys)
            active = fingerprint(json.loads(path.read_text()))
        else:
            active = composite
        rig.bench.hold_stop_number = 1
        rig.bench.release_stop.clear()
        rig.bench.stop_entered.clear()
        async with running(rig, "denial-combined-final-wire") as (s, c, sock):
            try:
                await asyncio.wait_for(rig.bench.stop_entered.wait(), 3)
                assert s.journal.get(KEY)[active]["proof"]["wire_ack"] is None
                before = copy.deepcopy(s.journal.get("managed_start_denials"))
                await pause(c)
                assert s.journal.get("managed_control_fault") is not None
                assert s.journal.get("managed_start_denials") == before
            finally:
                rig.bench.release_stop.set()
            await finish_prior_denial_and_operator(ChargeRig(rig, s, c, sock), ids[0], active, selected)
            assert not rig.bench.backend_errors and all(row["closed"] for row in rig.bench.backend_starts)
