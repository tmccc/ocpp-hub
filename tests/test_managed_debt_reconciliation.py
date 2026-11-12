"""Real TLS failures and explicit offline operator evidence recover only their exact debt at physical zero."""

import asyncio
import json
import time
import uuid

import pytest
from ocpp_hub_core.frames import build_call_error
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_debt_recovery import KEY
from ocpp_hub_core.prototype.native_bounded import NativeBoundedSpec
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError, fingerprint
from ocpp_hub_core.prototype.trial_session import save_private

from tests.handover_helpers import crash
from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session, starts
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests.test_trial_session import Bench
from tools import reconcile_managed_debt as operator


class ErrorReplyPeer:
    """An independent charger returns CALLERROR after its ordinary fake implementation received the command."""

    # Keep fault injection on the peer's transport, with no replacement of an engine method.
    def __init__(self, connection, trace):
        self.connection, self.trace = connection, trace
        self.targets = set()

    # Observe genuine forwarded vendor commands on the encrypted charger socket.
    def __aiter__(self):
        # All unrelated station-profile, configuration and composite calls keep their ordinary implementation.
        async def frames():
            async for raw in self.connection:
                frame = json.loads(raw)
                if frame[0] == 2 and frame[2] == "RemoteStartTransaction":
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


# Preserve native strategy binding through CLI preflight without changing the actual production route contract.
def strategy_file(rig, tmp_path):
    value = rig.spec.value()
    value["trial_route"].update(sDomain=rig.prepared.config.listener_name, sURL="/charger/")
    rig.spec = NativeBoundedSpec.parse(value)
    strategy = tmp_path / "native-spec.json"
    save_private(strategy, value)
    return strategy


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
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
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
            await c.tests.close()
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

    from tests.test_prototype import START

    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
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
            await c.tests.settle()
            tag = starts(rig)[0][3]["idTag"]
            original_deadline = c.tests.state()["hard_deadline_epoch"]
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
            assert c.tests.state().get("local_transaction") is None
            pause = uuid.uuid4().hex
            c.command(pause, "pause", c.requests.intent().revision)
            await c.pause_task
            assert rig.bench.firmware.effective_limit() == 0
            # Keep the actual production deadline. No test clock or engine deadline is replaced.
            await asyncio.sleep(max(0, original_deadline - time.time()) + 0.1)
            await c.tick()
            assert s.journal.get("control")["start_pending"] and len(starts(rig)) == 1
            await c.tests.close()
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
async def test_legacy_observation_recovery_preserves_queued_lifecycle(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys
):
    from tests.test_prototype import START

    gate = {"armed": False, "seen": asyncio.Event(), "release": asyncio.Event()}
    actual = Bench.vendor

    # Delay only the independent cloud peer; real local charger acceptance continues over its own TLS connection.
    async def cloud(bench, connection):
        await actual(bench, HeldReportPeer(connection, gate))

    monkeypatch.setattr(Bench, "vendor", cloud)
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
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
                await c.tests.close()
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
