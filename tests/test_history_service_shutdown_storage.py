"""A real SQLite history commit refusal must not prevent the standalone service's protective cleanup."""

import asyncio
import json
import sqlite3
import uuid
from contextlib import closing

import pytest
from ocpp_hub_core.prototype.native_history import NativeHistoryArchive
from ocpp_hub_core.prototype.trial_session import TrialSession, save_private

from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import crash
from tests.process_helpers import ServiceProcess
from tests.test_managed_api import TOKEN
from tests.test_managed_debt_reconciliation import strategy_file
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_native_history_archive import unassigned
from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import FAST, until
from tests.test_prototype_tls import tls as tls
from tools import managed_proxy


class HistoryFailureConnection(sqlite3.Connection):
    """A real failed SQL commit is observed after SQLite rejects it; only the fault's child row is repaired."""

    failure_observations = None

    # Preserve every actual engine SQL operation and its original failure while repairing only the injected refusal.
    def execute(self, sql, parameters=(), /):
        try:
            return super().execute(sql, parameters)
        except sqlite3.IntegrityError:
            if self.failure_observations is not None and sql in ("COMMIT", "RELEASE native_history_observation"):
                self.failure_observations.append({"sql": sql, "transaction_open": self.in_transaction})
                super().execute("DELETE FROM service_history_child")
            raise


# Read only actual autonomous service publications; do not manually tick its controller.
async def publication(service, status_path, predicate):
    async with asyncio.timeout(18):
        while True:
            if service.done():
                await service
                raise AssertionError("service exited before the requested observation")
            if status_path.exists():
                status = json.loads(status_path.read_text())
                if status.get("running") and predicate(status):
                    return status
            await asyncio.sleep(0.05)


# Intent: a real history-commit refusal during actual32A service polling still sends zero/Stop and retains uncertainty.
# Why: a repaired SQLite failure can leave an open transaction which previously blocked every protective shutdown write.
async def test_service_history_commit_failure_allows_zero_stop_and_uncertainty(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with commissioned(tmp_path, tls, unused_tcp_port_factory(), monkeypatch, 32, cli_route=True) as original:
        await original.controller.current.close()
        await original.controller.tests.close()
        await crash(original.session)
        rig, sessions, callbacks = original.rig, [], {}
        loop = asyncio.get_running_loop()
        original_connect = sqlite3.connect

        # Use a real SQLite subclass only for the service journal, retaining its ordinary WAL and foreign-key modes.
        def connect(database, *args, **kwargs):
            if str(database) == str(rig.prepared.config.journal):
                kwargs["factory"] = HistoryFailureConnection
            return original_connect(database, *args, **kwargs)

        monkeypatch.setattr(sqlite3, "connect", connect)

        # Supply only the commissioned loopback preparation and real signal-callback registration.
        def create_session(prepared, **kwargs):
            session = TrialSession(prepared, FAST, **kwargs)
            sessions.append(session)
            rig.sessions.append(session)
            return session

        monkeypatch.setattr(managed_proxy, "preflight", lambda _: rig.prepared)
        monkeypatch.setattr(managed_proxy, "TrialSession", create_session)
        monkeypatch.setattr(managed_proxy, "notify", lambda _: None)
        monkeypatch.setattr(loop, "add_signal_handler", lambda sig, callback: callbacks.update({sig: callback}))
        monkeypatch.setattr(loop, "remove_signal_handler", lambda _: True)
        spec, management, secret = (
            tmp_path / name for name in ("service-native.json", "management.json", "token.json")
        )
        save_private(spec, rig.spec.value())
        save_private(secret, {"token": TOKEN})
        save_private(management, {"host": "127.0.0.1", "port": unused_tcp_port_factory(), "token_file": str(secret)})
        status_path = rig.prepared.config.journal.with_suffix(".status.json")
        service = asyncio.create_task(managed_proxy.serve(tmp_path / "trial.json", spec, management))
        db = None
        failures, statements = [], []
        try:
            await until(lambda: sessions and sessions[0].started)
            session = sessions[0]
            db = session.journal.db
            async with rig.charger(session) as charger:
                assert (await rig.bench.call(charger, "history-storage-service-Boot", "BootNotification", BOOT))[0] == 3
                await publication(service, status_path, lambda status: status.get("owner_control_enabled"))
                start = START | {"idTag": "NATIVE_TIMER", "timestamp": "2026-01-01T02:00:00Z"}
                response = await rig.bench.call(charger, uuid.uuid4().hex, "StartTransaction", start)
                assert response[0] == 3 and response[2]["idTagInfo"]["status"] == "Accepted"
                local = response[2]["transactionId"]
                await until(
                    lambda: session.journal.vendor_id(local) is not None and session.journal.next_delivery() is None
                )
                rig.peer.plan_session, rig.peer.permission_enabled, rig.peer.paused = (local, 9), True, False
                rig.bench.cp, rig.bench.connection = "C", "Charging"
                rig.bench.firmware.fire_plan(9)
                await publication(service, status_path, lambda status: status.get("current_a") == 32)
                assert rig.bench.firmware.current == 32
                NativeHistoryArchive(session.journal)
                db.execute("CREATE TABLE service_history_parent(id INTEGER PRIMARY KEY)")
                db.execute(
                    "CREATE TABLE service_history_child(id REFERENCES service_history_parent(id) "
                    "DEFERRABLE INITIALLY DEFERRED)"
                )
                db.execute(
                    "CREATE TRIGGER service_history_commit AFTER INSERT ON native_start_attempts "
                    "BEGIN INSERT INTO service_history_child VALUES(1); END"
                )
                db.failure_observations = failures
                db.set_trace_callback(statements.append)
                before = len(rig.bench.device_calls)
                # A genuine native HTTP history response introduces one complete unassigned attempt under strict policy.
                rig.bench.history.append(unassigned("history-service-storage.json"))
                with pytest.raises(sqlite3.DatabaseError, match="FOREIGN KEY"):
                    await asyncio.wait_for(service, 18)
                assert len(failures) == 1
                commands = rig.bench.device_calls[before:]
                zeros = [
                    f
                    for f in commands
                    if f[2] == "SetChargingProfile"
                    and f[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
                ]
                stops = [f for f in commands if f[2] == "RemoteStopTransaction"]
                assert len(zeros) >= 1
                assert len(stops) == 1 and stops[0][3] == {"transactionId": local}
                assert rig.bench.firmware.effective_limit() == 0 and rig.bench.firmware.current == 0
                # The peer intentionally withholds StopTransaction; command acceptance cannot prove physical closure.
                with closing(sqlite3.connect(session.journal.path.as_uri() + "?mode=ro", uri=True)) as reader:
                    value = reader.execute("SELECT value FROM meta WHERE key='trial_shutdown_uncertain'").fetchone()
                    assert value is not None and json.loads(value[0]) is True
                    assert reader.execute("SELECT stop FROM transactions WHERE local=?", (local,)).fetchone()[0] is None
                    assert reader.execute("SELECT COUNT(*) FROM native_start_attempts").fetchone()[0] == 0
                assert any(text == "ROLLBACK" for text in statements)
                assert failures[0]["transaction_open"] is True
                # Repair only injected schema; the replacement still needs genuine lifecycle closure.
                with closing(sqlite3.connect(session.journal.path)) as repair:
                    repair.execute("DROP TRIGGER service_history_commit")
                    repair.execute("DROP TABLE service_history_child")
                    repair.execute("DROP TABLE service_history_parent")
                    repair.commit()
                config = json.loads((tmp_path / "trial.json").read_text())
                config["local_tags"] = ["APP_OWNER", "NATIVE_TIMER"]
                save_private(tmp_path / "trial.json", config)
                strategy = strategy_file(rig, tmp_path)
                save_private(tmp_path / "process-strategy.json", json.loads(strategy.read_text()))
                child = ServiceProcess(rig, tmp_path, unused_tcp_port_factory())
                try:
                    await child.start()
                    closed = await rig.bench.call(
                        child.socket,
                        uuid.uuid4().hex,
                        "StopTransaction",
                        {"transactionId": local, "meterStop": 1500, "timestamp": "2026-01-01T02:01:00Z"},
                    )
                    assert closed[0] == 3
                    await child.delivered(local, closed=True)
                    # Reuse the real API/peer suffix: verified Pause/Return, two fresh sessions and real native outages.
                    await child.ordinary_recovery(32, (True, False), start_minute=10)
                    assert child.durable()["metadata"]["trial_shutdown_uncertain"] is True
                finally:
                    await child.close()
        finally:
            (tmp_path / "history-sql-observations.json").write_text(
                json.dumps({"failures": failures, "statements": statements}, indent=2)
            )
            if not service.done():
                service.cancel()
                await asyncio.gather(service, return_exceptions=True)
