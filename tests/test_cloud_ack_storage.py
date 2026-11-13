"""Real TLS replies plus real SQLite failures at the cloud-acknowledgement boundary.

The independent cloud holds its actual correlated response until the charger CALL
has durably enqueued the event. No Gateway, Peer.call, or Journal method is replaced.
A durably saved reply survives replacement without wire replay. A BEGIN refusal
before receipt persistence loses that certainty: non-Start delivery is at least
once, while an accepted Start needs the real closed-session mapping tool.
"""

import asyncio
import json
import sqlite3
import time
import uuid
from contextlib import AsyncExitStack, asynccontextmanager, closing
from dataclasses import dataclass

import pytest
from ocpp_hub_core.prototype.delivery_reply import DeliveryReply
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_start_delivery import DENIALS
from ocpp_hub_core.prototype.managed_stop_delivery import REJECTION
from ocpp_hub_core.prototype.store import Delivery, Journal, RecoveryRequiredError, encoded, fingerprint
from ocpp_hub_core.prototype.trial_session import save_private

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench, two_later_sessions
from tests.handover_helpers import crash
from tests.test_managed_debt_reconciliation import strategy_file
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_stop_reply_recovery import pause
from tests.test_managed_test_charge import ChargeRig, positive_flow
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tools import reconcile_managed_delivery as mapping

pytestmark = pytest.mark.asyncio
ACTIONS = (
    "StatusNotification",
    "MeterValues",
    "FirmwareStatusNotification",
    "DiagnosticsStatusNotification",
    "StartTransaction",
    "StopTransaction",
)


@dataclass
class HeldReply:
    action: str
    arrived: asyncio.Event
    release: asyncio.Event
    answer: object = None
    token: str | None = None


class ReplyGate:
    """Only the independent cloud's actual socket response is delayed."""

    # Keep independent peer state and explicit gates separate from the engine.
    def __init__(self, connection, bench):
        self.connection, self.bench, self.actions = connection, bench, {}

    # Preserve the real websocket interface used by the ordinary independent cloud.
    def __getattr__(self, name):
        return getattr(self.connection, name)

    # Observe actual request identities without changing their payloads or delivery order.
    async def __aiter__(self):
        async for raw in self.connection:
            frame = decode(raw)
            if frame[0] == 2:
                self.actions[frame[1]] = frame[2]
            yield raw

    # Hold the correlated cloud reply until the test has installed its real SQLite failure.
    async def send(self, raw):
        frame = decode(raw)
        held = self.bench.held_reply
        if held is not None and frame[0] in (3, 4) and self.actions.get(frame[1]) == held.action:
            self.bench.held_reply = None
            held.answer, held.token = frame, frame[1]
            held.arrived.set()
            await held.release.wait()
        await self.connection.send(raw)


class StorageBench(StartReplyBench):
    """Retain the independent backend's ID allocation, duplicate checks, and lifecycle ledger."""

    # Keep independent peer state and explicit gates separate from the engine.
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.held_reply = None

    # Run the independent backend through its actual reply gate.
    async def vendor(self, connection):
        await super().vendor(ReplyGate(connection, self))


class SqlFault:
    """Exercise actual BEGIN authorization, DELETE abort, or deferred FK failure at COMMIT."""

    # Keep independent peer state and explicit gates separate from the engine.
    def __init__(self, kind, token):
        self.kind, self.token, self.refusals, self.begins = kind, token, 0, 0

    # Refuse the selected real SQLite boundary without replacing a journal method.
    def install(self, db):
        if self.kind in ("begin", "ack-begin"):
            # Deny the actual BEGIN while preserving ordinary reads and peer transport.
            def authorize(action, arg1, arg2, database, trigger):
                if action == sqlite3.SQLITE_TRANSACTION and arg1 == "BEGIN":
                    self.begins += 1
                    self.refusals += 1
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            if self.kind == "begin":
                db.set_authorizer(authorize)
            else:
                # Authorizers run on statement preparation, not every cached BEGIN execution.
                # Arm after the real receipt-save COMMIT, expiring prepared statements before ACK BEGIN.
                armed = False

                # Arm the next BEGIN failure after the actual receipt-save COMMIT.
                def after_saved(sql):
                    nonlocal armed
                    if sql.startswith("BEGIN"):
                        self.begins += 1
                    if sql == "COMMIT" and not armed:
                        armed = True
                        db.set_authorizer(authorize)

                db.set_trace_callback(after_saved)
            return
        predicate = "OLD.token='" + self.token + "'"
        if self.kind == "delete":
            db.execute(
                "CREATE TRIGGER ack_storage_refusal BEFORE DELETE ON outbox WHEN "
                + predicate
                + " BEGIN SELECT RAISE(ABORT,'synthetic cloud ACK storage refusal'); END"
            )
        else:
            db.execute("CREATE TABLE ack_storage_parent(id INTEGER PRIMARY KEY)")
            db.execute(
                "CREATE TABLE ack_storage_child(id REFERENCES ack_storage_parent(id) DEFERRABLE INITIALLY DEFERRED)"
            )
            db.execute(
                "CREATE TRIGGER ack_storage_refusal AFTER DELETE ON outbox WHEN "
                + predicate
                + " BEGIN INSERT INTO ack_storage_child VALUES(1); END"
            )

    # Restore writes and verify that the failed transaction left no partial rows.
    def remove(self, db):
        db.set_trace_callback(None)
        db.set_authorizer(None)
        if self.kind not in ("begin", "ack-begin"):
            db.execute("DROP TRIGGER ack_storage_refusal")
        if self.kind == "commit":
            assert db.execute("SELECT COUNT(*) FROM ack_storage_child").fetchone()[0] == 0
            db.execute("DROP TABLE ack_storage_child")
            db.execute("DROP TABLE ack_storage_parent")


# Count real cloud requests for this exact durable event identity.
def wire_count(rig, token):
    return sum(frame[0] == 2 and frame[1] == token for frame in rig.bench.calls)


# Verify committed SQLite from a separate reader, including the exact peer-owned received frame.
def assert_retained(j, held, *, saved):
    with closing(sqlite3.connect(j.path.as_uri() + "?mode=ro", uri=True)) as reader:
        head = reader.execute("SELECT token,action,state FROM outbox ORDER BY seq LIMIT 1").fetchone()
        assert head == (held.token, held.action, "inflight")
        reply = reader.execute("SELECT action,answer FROM delivery_replies WHERE token=?", (held.token,)).fetchone()
        assert (reply is not None) is saved
        if saved:
            assert reply[0] == held.action and json.loads(reply[1]) == held.answer
        assert not reader.execute("PRAGMA foreign_key_check").fetchall()


# Publish the independent charger closure through its actual TLS socket.
async def close_local(b, local, minute):
    b.rig.peer.permission_enabled = False
    b.rig.bench.firmware.requested_current = 0
    answer = await b.rig.bench.call(
        b.charger,
        uuid.uuid4().hex,
        "StopTransaction",
        {
            "transactionId": local,
            "timestamp": f"2026-01-01T00:{minute:02d}:00Z",
            "meterStop": 1800 + minute,
        },
    )
    assert answer[0] == 3
    b.rig.peer.plan_session = None


# Create the selected real delivery head and hold only its independent cloud reply.
async def enqueue(b, action, cycle):
    minute = 2 + cycle * 3
    local = None
    if action in ("MeterValues", "StopTransaction"):
        local = await positive_flow(
            b, "daily", call_id=uuid.uuid4().hex, timestamp=f"2026-01-01T00:{minute:02d}:00Z", meter_start=1200 + minute
        )
        await mp.healthy(b.controller)
    held = HeldReply(action, asyncio.Event(), asyncio.Event())
    b.rig.bench.held_reply = held
    if action == "StartTransaction":
        answer = await b.rig.bench.call(
            b.charger,
            uuid.uuid4().hex,
            action,
            START
            | {"idTag": "NATIVE_TIMER", "meterStart": 1200 + minute, "timestamp": f"2026-01-01T00:{minute:02d}:00Z"},
        )
        assert answer[0] == 3
        local = answer[2]["transactionId"]
        # Persist the genuine Stop while the cloud Start reply is still withheld.
        # This admits the supported mapping tool after a replacement loses reply certainty.
        await close_local(b, local, minute + 1)
    elif action == "StopTransaction":
        await close_local(b, local, minute + 1)
    else:
        payload = {
            "StatusNotification": {"connectorId": 1, "status": "Preparing", "errorCode": "NoError"},
            "MeterValues": {
                "connectorId": 1,
                "transactionId": local,
                "meterValue": [{"timestamp": f"2026-01-01T00:{minute:02d}:30Z", "sampledValue": [{"value": "1250"}]}],
            },
            "FirmwareStatusNotification": {"status": "Downloaded"},
            "DiagnosticsStatusNotification": {"status": "Uploaded"},
        }[action]
        assert (await b.rig.bench.call(b.charger, uuid.uuid4().hex, action, payload))[0] == 3
    await asyncio.wait_for(held.arrived.wait(), 3)
    return held, local


# Start a new journal owner against the same independent physical and cloud peers.
@asynccontextmanager
async def replacement(b, label):
    session = mp.managed_session(b.rig)
    await session.start()
    c = ManagedController(session)
    async with b.rig.charger(session) as charger:
        try:
            await mp.introduced(b.rig, session, charger, label)
            yield ChargeRig(b.rig, session, c, charger)
        finally:
            await c.tests.close()
            if c.pause_task is not None:
                await asyncio.gather(c.pause_task, return_exceptions=True)
            await session.close()


# Recover only from saved wire evidence or independently verified actual cloud mapping.
async def replace_and_recover(stack, b, fault, held, action, tmp_path, capsys, cycle):
    await b.controller.tests.close()
    await crash(b.session)
    j = Journal(b.rig.prepared.config.journal, b.rig.prepared.config.identity)
    try:
        fault.remove(j.db)
        if action == "StartTransaction" and fault.kind == "begin":
            assert j.next_delivery().state == "ambiguous"
            assert j.vendor_id(j.next_delivery().transaction) is None
    finally:
        j.close()
    fresh = await stack.enter_async_context(replacement(b, f"storage-replacement-{cycle}"))
    if action != "StartTransaction" or fault.kind != "begin":
        await until(lambda: fresh.session.journal.next_delivery() is None)
        assert wire_count(b.rig, held.token) == (2 if fault.kind == "begin" else 1)
        return fresh

    # No retained in-memory answer survives a replacement, so the original Start is never silently replayed.
    await until(lambda: fresh.session.runtime.status.delivery_blocked)
    await pause(fresh.controller)
    assert fresh.session.journal.get("managed_control_fault")
    assert wire_count(b.rig, held.token) == 1
    await fresh.controller.tests.close()
    await crash(fresh.session)
    strategy_file(b.rig, tmp_path)
    args = (tmp_path / "trial.json", tmp_path / "native-spec.json")
    port = b.rig.prepared.observer.port
    assert await mapping.run("inspect", *args, None, port) == 0
    candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
    actual = next(row for row in b.rig.bench.backend_starts if row["token"] == held.token)
    evidence = candidate | {
        "vendor": actual["answer"]["transactionId"],
        "evidence_sha256": fingerprint(actual),
        "verified_at_epoch": time.time(),
    }
    path = tmp_path / f"storage-mapping-{cycle}.json"
    save_private(path, evidence)
    assert await mapping.run("reconcile-start", *args, path, port) == 0
    capsys.readouterr()
    recovered = await stack.enter_async_context(replacement(b, f"storage-mapped-{cycle}"))
    await until(lambda: recovered.session.journal.next_delivery() is None)
    await pause(recovered.controller)
    assert recovered.session.journal.get("managed_control_fault") is None
    recovered.controller.command(uuid.uuid4().hex, "app_control", recovered.controller.requests.intent().revision)
    await mp.healthy(recovered.controller)
    assert wire_count(b.rig, held.token) == 1
    return recovered


# Intent: two real storage faults per case recover authority, exact mapping and later outage protection.
# Why: a repaired DELETE/COMMIT must not leave either inflight debt, a permanent generic latch, or duplicate Start.
@pytest.mark.parametrize("selected", [6, 16, 32])
@pytest.mark.parametrize("action", ACTIONS)
@pytest.mark.parametrize("failure", ["begin", "delete", "commit"])
@pytest.mark.parametrize("replaced", [False, True], ids=["same-process", "replacement"])
async def test_cloud_ack_storage_recovery(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys, selected, action, failure, replaced
):
    await storage_recovery_case(
        tmp_path, tls, unused_tcp_port, monkeypatch, capsys, selected, action, failure, replaced
    )


# Repeat storage failure and recovery before checking later mapped sessions and native outages.
async def storage_recovery_case(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys, selected, action, failure, replaced
):
    monkeypatch.setattr(mp, "OwnerBench", StorageBench)
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with AsyncExitStack() as stack:
        b = await stack.enter_async_context(
            commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, selected, cli_route=True)
        )
        strategy_file(b.rig, tmp_path)
        original_configuration = dict(b.rig.peer.configuration)
        original_plan = json.loads(json.dumps(b.rig.bench.firmware.plans))
        for cycle in range(2):
            assert (await mp.healthy(b.controller))["station_ceiling_a"] == selected
            held, local = await enqueue(b, action, cycle)
            j = b.session.journal
            head = j.next_delivery()
            assert head.token == held.token and head.action == action and head.state == "inflight"
            before = [tuple(row) for row in j.db.execute("SELECT * FROM transactions ORDER BY local")]
            fault = SqlFault(failure, held.token)
            fault.install(j.db)
            try:
                held.release.set()
                await until(lambda: b.session.runtime.status.delivery_blocked)
                assert b.session.runtime.status.delivery_blocked_cause == "storage"
                assert j.next_delivery().token == held.token and j.next_delivery().state == "inflight"
                assert b.session.gateway._delivery_reply is not None
                assert_retained(j, held, saved=failure != "begin")
                assert wire_count(b.rig, held.token) == 1 and not j.db.in_transaction
                assert [tuple(row) for row in j.db.execute("SELECT * FROM transactions ORDER BY local")] == before
                if failure not in ("begin", "ack-begin"):
                    # BEGIN denial blocks every writer, including the supervisor; other faults are event-specific.
                    assert (await b.controller.tick())["state"] == "degraded"
                assert j.get("managed_control_fault") is None
                assert b.rig.bench.firmware.effective_limit() == selected
                if failure in ("begin", "ack-begin"):
                    assert fault.refusals > 0
                    if failure == "ack-begin":
                        assert fault.begins >= 2
                if replaced:
                    b = await replace_and_recover(stack, b, fault, held, action, tmp_path, capsys, cycle)
                else:
                    fault.remove(j.db)
                    await until(lambda: j.next_delivery() is None)
                    assert wire_count(b.rig, held.token) == 1
            finally:
                held.release.set()
            assert (await mp.healthy(b.controller))["station_ceiling_a"] == selected
            assert b.session.journal.get("managed_control_fault") is None
            if action == "StartTransaction":
                actual = next(row for row in b.rig.bench.backend_starts if row["token"] == held.token)
                assert b.session.journal.vendor_id(local) == actual["answer"]["transactionId"]
                assert actual["closed"]
            if action == "MeterValues":
                await close_local(b, local, 4 + cycle * 3)
                await until(lambda: b.session.journal.next_delivery() is None)
            assert not b.rig.bench.backend_errors and not mp.starts(b.rig)
        await two_later_sessions(b, selected=selected, original_starts=0, first_minute=20)
        assert b.rig.peer.configuration == original_configuration
        assert json.loads(json.dumps(b.rig.bench.firmware.plans)) == original_plan
        assert b.session.journal.next_delivery() is None and not b.session.journal.get("managed_control_fault")
        assert b.session.journal.db.execute("SELECT COUNT(*) FROM delivery_replies").fetchone()[0] == 0
        assert all(row["closed"] for row in b.rig.bench.backend_starts)
        assert not b.session.journal.db.execute("PRAGMA foreign_key_check").fetchall()


# Intent: known negative lifecycle replies remain restrictive after their exact persistence retry.
# Why: a storage-only exception must not classify a denial or genuine Stop refusal as accepted delivery.
@pytest.mark.parametrize("selected", [6, 16, 32])
@pytest.mark.parametrize(
    "failure,replaced", [("begin", False), ("delete", False), ("commit", False), ("delete", True), ("commit", True)]
)
@pytest.mark.parametrize("negative", ["denied-start", "refused-stop"])
async def test_negative_cloud_reply_storage_keeps_quarantine(
    tmp_path, tls, unused_tcp_port, monkeypatch, selected, failure, replaced, negative
):
    monkeypatch.setattr(mp, "OwnerBench", StorageBench)
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with AsyncExitStack() as stack:
        b = await stack.enter_async_context(
            commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, selected, cli_route=True)
        )
        if negative == "denied-start":
            b.rig.bench.start_replies = ["Blocked"]
            action = "StartTransaction"
        else:
            b.rig.bench.stop_replies = ["error"]
            action = "StopTransaction"
        held, local = await enqueue(b, action, 0)
        fault = SqlFault(failure, held.token)
        j = b.session.journal
        # A refusal persists by UPDATE rather than DELETE; select that same real transaction boundary.
        fault.install(j.db)
        if negative == "refused-stop" and failure != "begin":
            j.db.execute("DROP TRIGGER ack_storage_refusal")
            effect = (
                "SELECT RAISE(ABORT,'synthetic cloud ACK storage refusal')"
                if failure == "delete"
                else "INSERT INTO ack_storage_child VALUES(1)"
            )
            j.db.execute(
                "CREATE TRIGGER ack_storage_refusal AFTER UPDATE ON outbox WHEN NEW.token='"
                + held.token
                + "' AND NEW.state='rejected' BEGIN "
                + effect
                + "; END"
            )
        try:
            held.release.set()
            await until(lambda: b.session.runtime.status.delivery_blocked)
            assert b.session.runtime.status.delivery_blocked_cause == "storage"
            assert b.session.gateway._delivery_reply is not None
            assert_retained(j, held, saved=failure != "begin")
            assert wire_count(b.rig, held.token) == 1
            if failure != "begin":
                if negative == "denied-start":
                    # Production itself must try zero before any test-owned supervisor call.
                    await until(lambda: b.rig.bench.firmware.effective_limit() == 0)
                    assert j.vendor_id(local) is None
                await b.controller.tick()
                assert j.get("managed_control_fault")
            if replaced:
                await b.controller.tests.close()
                await crash(b.session)
                j = Journal(b.rig.prepared.config.journal, b.rig.prepared.config.identity)
                try:
                    # The actual received denial/refusal is durable, so replacement cannot call it unknown.
                    assert j.next_delivery().token == held.token and j.next_delivery().state == "inflight"
                    fault.remove(j.db)
                finally:
                    j.close()
                b = await stack.enter_async_context(replacement(b, "negative-storage-replacement"))
                j = b.session.journal
            else:
                fault.remove(j.db)
            await until(
                lambda: (
                    j.next_delivery() is None
                    if negative == "denied-start"
                    else j.next_delivery() is not None and j.next_delivery().state == "rejected"
                )
            )
            await b.controller.tick()
            await b.controller.tick()
            await until(lambda: b.rig.bench.firmware.effective_limit() == 0)
            assert j.get("managed_control_fault")
            assert wire_count(b.rig, held.token) == 1
            if negative == "denied-start":
                assert j.get(DENIALS)[str(local)]["completed"] is None
            else:
                assert j.get(REJECTION)["delivery_token"] == held.token
                await pause(b.controller)
                assert j.get("managed_control_fault") and j.next_delivery().state == "rejected"
            assert not mp.starts(b.rig)
        finally:
            held.release.set()
            await b.controller.tests.close()
            await crash(b.session)


# Intent: distinguish refusing receipt persistence from refusing the later ACK transaction's BEGIN.
# Why: a saved actual reply must survive this second-BEGIN cut without retransmission or mapping speculation.
@pytest.mark.parametrize("action", ACTIONS)
@pytest.mark.parametrize("replaced", [False, True], ids=["same-process", "replacement"])
async def test_cloud_ack_begin_after_reply_saved(tmp_path, tls, unused_tcp_port, monkeypatch, capsys, action, replaced):
    await storage_recovery_case(tmp_path, tls, unused_tcp_port, monkeypatch, capsys, 16, action, "ack-begin", replaced)


# Intent: exact row/frame identity is mandatory, and quarantine/dequeue cannot retain receipt-table garbage.
# Why: these labeled stored-state contracts complement actual TLS cases without inventing a peer outcome.
@pytest.mark.parametrize("tamper", ["action", "payload", "local", "frame-shape", "frame-id", "state"])
async def test_saved_reply_identity_and_cleanup_contract(tmp_path, tamper):
    j = Journal(tmp_path / "receipt-contract.db", "SYNTHETIC_STORAGE_ONLY")
    try:
        body = {"connectorId": 1, "status": "Preparing", "errorCode": "NoError"}
        token = j.enqueue("StatusNotification", body)
        j.mark_sent(j.next_delivery())
        item = j.next_delivery()
        reply = DeliveryReply.received(item, [3, token, {}])
        reply.save(j, item)
        reply.save(j, item)
        assert j.db.execute("SELECT COUNT(*) FROM delivery_replies").fetchone()[0] == 1
        tail = j.enqueue("FirmwareStatusNotification", {"status": "Downloaded"})
        wrong = next(row for row in j.db.execute("SELECT * FROM outbox") if row["token"] == tail)
        tail_item = Delivery(tail, wrong["action"], json.loads(wrong["payload"]), wrong["local"], "inflight")
        with pytest.raises(RecoveryRequiredError):
            DeliveryReply.received(tail_item, [3, tail, {}]).save(j, tail_item)
        assert j.db.execute("SELECT COUNT(*) FROM delivery_replies").fetchone()[0] == 1
        if tamper == "state":
            j.db.execute("UPDATE outbox SET state='queued' WHERE token=?", (token,))
            assert DeliveryReply.restore(j, j.next_delivery()) is None
            with pytest.raises(RecoveryRequiredError):
                reply.require(j.next_delivery())
        else:
            column, value = {
                "action": ("action", "FirmwareStatusNotification"),
                "payload": ("payload", encoded(body | {"status": "Available"})),
                "local": ("local", 987654),
                "frame-shape": ("answer", encoded([2, token, "StatusNotification", body])),
                "frame-id": ("answer", encoded([3, "e-" + "0" * 32, {}])),
            }[tamper]
            j.db.execute("UPDATE delivery_replies SET " + column + "=? WHERE token=?", (value, token))
            with pytest.raises(RecoveryRequiredError):
                DeliveryReply.restore(j, item)
            with pytest.raises(RecoveryRequiredError):
                reply.save(j, item)
        # Reconstruct an explicitly labeled contract state, then exercise real trigger/FK cleanup repeatedly.
        j.db.execute("DELETE FROM delivery_replies WHERE token=?", (token,))
        for state in ("queued", "ambiguous", "rejected"):
            j.db.execute("UPDATE outbox SET state='inflight' WHERE token=?", (token,))
            reply.save(j, j.next_delivery())
            j.db.execute("UPDATE outbox SET state=? WHERE token=?", (state, token))
            assert j.db.execute("SELECT COUNT(*) FROM delivery_replies").fetchone()[0] == 0
        j.db.execute("UPDATE outbox SET state='inflight' WHERE token=?", (token,))
        reply.save(j, j.next_delivery())
        j.acknowledge(j.next_delivery(), {})
        assert j.db.execute("SELECT COUNT(*) FROM delivery_replies").fetchone()[0] == 0
        assert j.next_delivery().token == tail
        with pytest.raises(sqlite3.IntegrityError):
            j.db.execute(
                "INSERT INTO delivery_replies VALUES(?,?,?,?,?)",
                (token, reply.action, reply.payload, None, encoded(reply.answer)),
            )
        assert not j.db.execute("PRAGMA foreign_key_check").fetchall()
    finally:
        j.close()


# Intent: malformed but genuinely received Start metadata is quarantined, not restored in a reconnect loop.
# Why: a durable response cannot bypass unknown-mapping zero or keep restarting the vendor session forever.
@pytest.mark.parametrize("selected", [6, 16, 32])
@pytest.mark.parametrize("replaced", [False, True], ids=["same-process", "replacement"])
async def test_malformed_start_reply_cache_quarantines_without_replay(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys, selected, replaced
):
    monkeypatch.setattr(mp, "OwnerBench", StorageBench)
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with AsyncExitStack() as stack:
        b = await stack.enter_async_context(
            commissioned(tmp_path, tls, unused_tcp_port, monkeypatch, selected, cli_route=True)
        )
        strategy_file(b.rig, tmp_path)
        b.rig.bench.start_replies = ["bad-info"]
        held, local = await enqueue(b, "StartTransaction", 0)
        held.release.set()
        await until(lambda: b.session.runtime.status.delivery_blocked)
        assert b.session.runtime.status.delivery_blocked_cause == "reconciliation"
        assert b.session.journal.next_delivery().state == "ambiguous"
        if replaced:
            await b.controller.tests.close()
            await crash(b.session)
            b = await stack.enter_async_context(replacement(b, "malformed-reply-replacement"))
            await until(lambda: b.session.runtime.status.delivery_blocked)
        j, c = b.session.journal, b.controller
        await c.tick()
        await c.tick()
        await until(lambda: b.rig.bench.firmware.effective_limit() == 0)
        assert j.get("managed_control_fault") and j.vendor_id(local) is None
        assert j.next_delivery().state == "ambiguous" and wire_count(b.rig, held.token) == 1
        assert b.session.gateway._delivery_reply is None
        assert j.db.execute("SELECT COUNT(*) FROM delivery_replies").fetchone()[0] == 0
        assert str(local) not in j.get(DENIALS, {})
        heartbeats = sum(frame[0] == 2 and frame[2] == "Heartbeat" for frame in b.rig.bench.calls)
        await until(
            lambda: sum(frame[0] == 2 and frame[2] == "Heartbeat" for frame in b.rig.bench.calls) >= heartbeats + 1,
            timeout=3,
        )
        assert b.session.runtime.status.vendor_connected
        await pause(c)
        assert j.get("managed_control_fault") and j.next_delivery().state == "ambiguous"
        with pytest.raises(RecoveryRequiredError, match="latched fault"):
            c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
        # Actual cloud allocation remains independent evidence; only the real mapping tool may settle it.
        b = await replace_and_recover(
            stack, b, SqlFault("begin", held.token), held, "StartTransaction", tmp_path, capsys, 99
        )
        assert (await mp.healthy(b.controller))["station_ceiling_a"] == selected
        await two_later_sessions(b, selected=selected, original_starts=0, first_minute=20)
        assert wire_count(b.rig, held.token) == 1 and not b.rig.bench.backend_errors
