"""Real cloud Stop replies keep immutable lifecycle evidence and recover only through exact proof and wire ACK."""

import asyncio
import json
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest
from ocpp_hub_core.frames import build_call_error, build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_debt_recovery import KEY
from ocpp_hub_core.prototype.managed_stop_delivery import REJECTION, WIRE_ACK
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError, fingerprint
from ocpp_hub_core.prototype.trial_session import save_private

from tests import test_managed_proxy as mp
from tests.handover_helpers import crash
from tests.test_managed_debt_reconciliation import strategy_file
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tools import reconcile_managed_debt as operator


class CloudReplyPeer:
    """Only the independent cloud's Stop response changes; engine, journal and TLS transport stay real."""

    # Record actions by actual correlated RPC identity before the peer sends its response.
    def __init__(self, connection, bench):
        self.connection, self.bench, self.actions = connection, bench, {}

    # Keep the websocket interface and its ordinary independent vendor behavior intact.
    def __getattr__(self, name):
        return getattr(self.connection, name)

    # Observe actual Stop transmissions without replacing a gateway or RPC method.
    async def __aiter__(self):
        async for raw in self.connection:
            frame = decode(raw)
            if frame[0] == 2:
                self.actions[frame[1]] = frame[2]
                if frame[2] == "StopTransaction":
                    self.bench.stop_wires = getattr(self.bench, "stop_wires", []) + [frame]
            yield raw

    # A retry still needs a real response on this socket; operator proof cannot manufacture that response.
    async def send(self, raw):
        frame = decode(raw)
        if frame[0] == 3 and self.actions.get(frame[1]) == "StopTransaction":
            gate = getattr(self.bench, "stop_gate", None)
            if gate is not None:
                await gate.wait()
            info = getattr(self.bench, "stop_info", None)
            if getattr(self.bench, "stop_error", False):
                raw = build_call_error(frame[1], "InternalError", "synthetic cloud refusal")
            elif info is not None:
                raw = build_call_result(frame[1], {"idTagInfo": info})
        await self.connection.send(raw)


class ReplyBench(mp.OwnerBench):
    """Use the same independent cloud implementation behind its transparent response wrapper."""

    # Keep ordinary cloud Boot/Authorize/Start behavior, changing only the final Stop reply at the peer.
    async def vendor(self, connection):
        await super().vendor(CloudReplyPeer(connection, self))


# Record a genuine native-timer lifecycle so Stop has its actual local/cloud mapping and immutable body.
async def stop_lifecycle(rig, session, charger, info):
    opened = await rig.bench.call(charger, "original-start", "StartTransaction", START | {"idTag": "NATIVE_TIMER"})
    local = opened[2]["transactionId"]
    await until(lambda: session.journal.vendor_id(local) is not None and session.journal.next_delivery() is None)
    rig.bench.stop_info = info
    await rig.bench.call(
        charger,
        "original-stop",
        "StopTransaction",
        {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
    )
    return local


# Make a fresh restrictive intent and wait for its actual physical effect, without assuming it acknowledges debt.
async def pause(c):
    identifier = uuid.uuid4().hex
    revision = c.requests.intent().revision
    c.command(identifier, "pause", revision)
    if c.pause_task is not None:
        await c.pause_task
    await c.tick()
    return identifier, revision


# Construct the private operator statement from an exact inspected candidate and independent cloud evidence.
def statement(candidate):
    return candidate | {
        "cloud_stop_independently_verified": True,
        "fault_origin_independently_verified": True,
        "original_state_independently_verified": True,
        "evidence_sha256": fingerprint({"independent_cloud_stop_record": candidate}),
        "verified_at_epoch": time.time(),
    }


# Every round starts the actual service and charger socket, then crash-closes its journal for exclusive tooling.
@asynccontextmanager
async def running(rig, label, *, owner_tag=None, release_on_exit=True):
    strategy_file(rig, rig.prepared.config.journal.parent)
    s = mp.managed_session(rig)
    if owner_tag is not None:
        s.gateway.policy = replace(
            s.gateway.policy, allowed_tags=frozenset({owner_tag, *s.gateway.policy.allowed_tags})
        )
        s.gateway.controls.policy = s.gateway.policy
    await s.start()
    c = ManagedController(s)
    rig.route_to_trial()
    async with rig.charger(s) as charger:
        try:
            await mp.introduced(rig, s, charger, label)
            yield s, c, charger
        finally:
            if release_on_exit and getattr(rig.bench, "stop_gate", None) is not None:
                rig.bench.stop_gate.set()
            await c.tests.close()
            if c.pause_task is not None:
                await asyncio.gather(c.pause_task, return_exceptions=True)
            await crash(s)


# Inspect and attest only a stopped service's current exact candidate; no recovery step sends a charger command.
async def reconcile(rig, tmp_path, capsys, kind="stop-delivery"):
    config, strategy, port = tmp_path / "trial.json", strategy_file(rig, tmp_path), rig.prepared.observer.port
    before = list(rig.bench.device_calls)
    assert await operator.run("inspect", config, strategy, None, port, kind=kind) == 0
    candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
    path = tmp_path / "retry-evidence.json"
    save_private(path, statement(candidate))
    assert await operator.run("reconcile", config, strategy, path, port, kind=kind) == 0
    assert rig.bench.device_calls == before
    return path


# Consume recovery with a new intent only after real delivery, then separately return authority without Start.
async def return_after_ack(rig, s, c, *, expected_starts=0):
    await until(lambda: s.journal.next_delivery() is None)
    assert (await c.tick())["state"] == "attention"
    await pause(c)
    assert (await mp.healthy(c))["local_hold_verified"]
    assert rig.bench.firmware.effective_limit() == 0 and len(mp.starts(rig)) == expected_starts
    c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
    assert (await mp.healthy(c))["station_ceiling_a"] == 6


# Intent: valid optional authorization metadata, including RFC3339 precision beyond microseconds, drains normally.
# Why: a Stop repair must preserve the ordinary lifecycle and not label a valid fractional timestamp malformed.
@pytest.mark.parametrize("info", [None, {"status": "Accepted", "expiryDate": "2030-01-01T00:00:00.123456789Z"}])
async def test_valid_stop_reply_needs_no_reconciliation(tmp_path, tls, unused_tcp_port, monkeypatch, info):
    monkeypatch.setattr(mp, "OwnerBench", ReplyBench)
    async with mp.managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        s = mp.managed_session(rig)
        await s.start()
        c = ManagedController(s)
        rig.route_to_trial()
        async with rig.charger(s) as charger:
            await mp.introduced(rig, s, charger, "valid-stop-boot")
            await mp.healthy(c)
            await stop_lifecycle(rig, s, charger, info)
            await until(lambda: s.journal.next_delivery() is None)
            identifier, revision = await pause(c)
            assert (await mp.healthy(c))["local_hold_verified"]
            assert c.requests.receipt(identifier)["state"] == "completed"
            assert not s.journal.get(REJECTION) and not s.journal.get(KEY)


# Intent: malformed Stop metadata retains its event until exact offline proof and a genuine matching wire reply.
# Why: neither an operator statement nor a pre-ACK Pause can fabricate delivery or cache invalid authorization.
@pytest.mark.parametrize(
    "info",
    [
        {"status": "Accepted", "expiryDate": "2030-01-01T00:00:00+0000"},
        {"status": "Accepted", "parentIdTag": "P" * 21},
        {"status": "not-a-status"},
    ],
)
async def test_stop_retry_requires_exact_evidence_then_wire_ack(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys, info
):
    monkeypatch.setattr(mp, "OwnerBench", ReplyBench)
    async with mp.managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        strategy = strategy_file(rig, tmp_path)
        config, port = tmp_path / "trial.json", rig.prepared.observer.port
        s = mp.managed_session(rig)
        await s.start()
        c = ManagedController(s)
        rig.route_to_trial()
        async with rig.charger(s) as charger:
            await mp.introduced(rig, s, charger, "rejected-stop-boot")
            await mp.healthy(c)
            local = await stop_lifecycle(rig, s, charger, info)
            await until(lambda: s.runtime.status.delivery_blocked)
            await c.tick()
            old_pause, revision = await pause(c)
            j = s.journal
            retained = j.next_delivery()
            assert retained.state == "rejected" and retained.action == "StopTransaction"
            assert j.get(REJECTION)["delivery_token"] == retained.token
            assert c.requests.receipt(old_pause)["state"] == "accepted"
            counter, original = (
                j.get("next_transaction"),
                dict(j.db.execute("SELECT * FROM transactions WHERE local=?", (local,)).fetchone()),
            )
            await c.tests.close()
            await crash(s)
        before = list(rig.bench.device_calls)
        assert await operator.run("inspect", config, strategy, None, port, kind="stop-delivery") == 0
        candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
        evidence = statement(candidate)
        path = tmp_path / "stop-evidence.json"
        for changed in (
            {"cloud_stop_independently_verified": False},
            {"fault_origin_independently_verified": 1},
            {"original_state_independently_verified": False},
            {"delivery_token": "e-" + "0" * 32},
            {"reply_digest": "0" * 64},
            {"payload_digest": "0" * 64},
            {"local_transaction_id": local + 1},
            {"vendor_transaction_id": original["vendor"] + 1},
            {"candidate_digest": "0" * 64},
        ):
            save_private(path, evidence | changed)
            with pytest.raises((SecurityError, RecoveryRequiredError)):
                await operator.run("reconcile", config, strategy, path, port, kind="stop-delivery")
        save_private(path, evidence)
        j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        j.db.execute(
            "CREATE TRIGGER fail_stop_proof BEFORE INSERT ON meta "
            "WHEN NEW.key='managed_operator_debt_reconciliations' "
            "BEGIN SELECT RAISE(ABORT,'synthetic Stop proof failure'); END"
        )
        j.close()
        with pytest.raises(sqlite3.IntegrityError, match="synthetic Stop proof failure"):
            await operator.run("reconcile", config, strategy, path, port, kind="stop-delivery")
        j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        assert j.next_delivery().state == "rejected" and j.get(KEY) is None
        j.db.execute("DROP TRIGGER fail_stop_proof")
        j.close()
        assert await operator.run("reconcile", config, strategy, path, port, kind="stop-delivery") == 0
        assert await operator.run("reconcile", config, strategy, path, port, kind="stop-delivery") == 0
        assert rig.bench.device_calls == before
        rig.bench.stop_gate = asyncio.Event()
        replacement = mp.managed_session(rig)
        await replacement.start()
        c = ManagedController(replacement)
        async with rig.charger(replacement) as charger:
            await mp.introduced(rig, replacement, charger, "approved-stop-retry-boot")
            await until(lambda: len(rig.bench.stop_wires) == 2)
            early_pause, early_revision = await pause(c)
            assert c.requests.receipt(early_pause)["state"] == "accepted"
            assert replacement.journal.next_delivery() is not None
            rig.bench.stop_gate.set()
            await until(lambda: replacement.journal.next_delivery() is None)
            j = replacement.journal
            assert j.get("next_transaction") == counter
            assert dict(j.db.execute("SELECT * FROM transactions WHERE local=?", (local,)).fetchone()) == original
            assert next(iter(j.get(KEY).values()))["proof"]["wire_ack"] is not None
            assert (
                j.db.execute("SELECT 1 FROM auth_info WHERE digest=?", (fingerprint("native_timer"),)).fetchone()
                is None
            )
            assert (await c.tick())["state"] == "attention"
            c.command(early_pause, "pause", early_revision)
            assert (await c.tick())["state"] == "attention"
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            if c.pause_task is not None:
                await c.pause_task
            assert (await mp.healthy(c))["local_hold_verified"]
            assert rig.bench.firmware.effective_limit() == 0 and not mp.starts(rig)
            returning = uuid.uuid4().hex
            c.command(returning, "app_control", c.requests.intent().revision)
            assert (await mp.healthy(c))["station_ceiling_a"] == 6
            assert c.requests.receipt(returning)["state"] == "completed"


# Intent: a changed malformed reply or CALLERROR revokes only its exact retry permission and permits fresh proof.
# Why: no approval can ignore a different reply, while a newer independently verified retry must remain possible.
@pytest.mark.parametrize("call_error", [False, True])
async def test_changed_stop_reply_requires_new_proof(tmp_path, tls, unused_tcp_port, monkeypatch, capsys, call_error):
    monkeypatch.setattr(mp, "OwnerBench", ReplyBench)
    async with mp.managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        async with running(rig, "first-rejection") as (s, c, charger):
            await mp.healthy(c)
            await stop_lifecycle(rig, s, charger, {"status": "invalid-first"})
            await until(lambda: s.runtime.status.delivery_blocked)
            await pause(c)
            token = s.journal.next_delivery().token
        path = await reconcile(rig, tmp_path, capsys)
        with pytest.raises(RecoveryRequiredError, match="previous operator debt proof"):
            await operator.run(
                "reconcile",
                tmp_path / "trial.json",
                strategy_file(rig, tmp_path),
                path,
                rig.prepared.observer.port,
                kind="stop-fault",
            )
        rig.bench.stop_error = call_error
        rig.bench.stop_info = {"status": "invalid-second"}
        async with running(rig, "second-rejection") as (s, c, charger):
            await until(lambda: s.runtime.status.delivery_blocked)
            await c.tick()
            assert s.journal.next_delivery().token == token
            first = next(iter(s.journal.get(KEY).values()))
            assert first.get("superseded") and first["proof"]["wire_ack"] is None
            assert s.journal.get("managed_control_fault")["classification"] == "owner_control_or_delivery_uncertain"
            assert s.journal.get(REJECTION)["reason"] == (
                "cloud_call_error" if call_error else "authorization_metadata_invalid"
            )
        await reconcile(rig, tmp_path, capsys)
        rig.bench.stop_error, rig.bench.stop_info = False, None
        async with running(rig, "fresh-approved-retry") as (s, c, charger):
            await return_after_ack(rig, s, c)
            assert len(rig.bench.stop_wires) == 3
            records = list(s.journal.get(KEY).values())
            assert len(records) == 2
            settled = [entry for entry in records if not entry.get("superseded")]
            assert len(settled) == 1 and settled[0]["proof"]["wire_ack"] is not None


# Intent: actual wire evidence and dequeue roll back together on a real SQLite deletion failure.
# Why: receiving a CallResult cannot manufacture a committed delivery proof when its journal transaction failed.
async def test_stop_wire_ack_and_dequeue_are_atomic(tmp_path, tls, unused_tcp_port, monkeypatch, capsys):
    monkeypatch.setattr(mp, "OwnerBench", ReplyBench)
    async with mp.managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        async with running(rig, "atomic-first-rejection") as (s, c, charger):
            await mp.healthy(c)
            await stop_lifecycle(rig, s, charger, {"status": "invalid"})
            await until(lambda: s.runtime.status.delivery_blocked)
            await pause(c)
        await reconcile(rig, tmp_path, capsys)
        rig.bench.stop_gate = asyncio.Event()
        async with running(rig, "atomic-approved-retry") as (s, c, charger):
            s.journal.db.execute(
                "CREATE TRIGGER fail_actual_stop_ack BEFORE DELETE ON outbox "
                "WHEN OLD.action='StopTransaction' "
                "BEGIN SELECT RAISE(ABORT,'synthetic actual ACK commit failure'); END"
            )
            rig.bench.stop_gate.set()
            await until(lambda: s.runtime.status.delivery_blocked)
            assert s.journal.next_delivery() is not None
            proof = next(iter(s.journal.get(KEY).values()))["proof"]
            assert proof["wire_ack"] is None and proof["resolved"]["retained"]
            assert (await c.tick())["state"] == "attention"
            s.journal.db.execute("DROP TRIGGER fail_actual_stop_ack")
        async with running(rig, "atomic-replacement-retry") as (s, c, charger):
            await return_after_ack(rig, s, c)


# Intent: a retained pre-upgrade Stop may really drain after replacement, then its recorded fault can be reconciled.
# Why: a drained unexplained latch must not be waived by selecting an arbitrary closed transaction.
async def test_drained_stop_fault_needs_preserved_wire_receipt(tmp_path, tls, unused_tcp_port, monkeypatch, capsys):
    monkeypatch.setattr(mp, "OwnerBench", ReplyBench)
    async with mp.managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        async with running(rig, "legacy-retained-stop") as (s, c, charger):
            await mp.healthy(c)
            await stop_lifecycle(rig, s, charger, {"status": "invalid"})
            await until(lambda: s.runtime.status.delivery_blocked)
            await pause(c)
            token = s.journal.next_delivery().token
            # Represent the old validator's retained inflight event, without any new rejection metadata or proof.
            s.journal.db.execute("DELETE FROM meta WHERE key=?", (REJECTION,))
            s.journal.db.execute("UPDATE outbox SET state='inflight' WHERE token=?", (token,))
        rig.bench.stop_info = None
        async with running(rig, "real-legacy-retry-ack") as (s, c, charger):
            await until(lambda: s.journal.next_delivery() is None)
            await c.tick()
            wire = s.journal.get(WIRE_ACK)
            assert wire["delivery_token"] == token and not s.journal.get(KEY)
        j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        j.db.execute("DELETE FROM meta WHERE key=?", (WIRE_ACK,))
        j.close()
        with pytest.raises(RecoveryRequiredError, match="preserved actual wire"):
            await operator.run(
                "inspect",
                tmp_path / "trial.json",
                strategy_file(rig, tmp_path),
                None,
                rig.prepared.observer.port,
                kind="stop-fault",
            )
        j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        j.put(WIRE_ACK, wire)
        j.close()
        await reconcile(rig, tmp_path, capsys, "stop-fault")
        async with running(rig, "settled-drained-fault") as (s, c, charger):
            await return_after_ack(rig, s, c)


# Intent: an overlapping later closed native lifecycle stays intact while each malformed Stop needs its own proof.
# Why: exact retry approval must neither discard queued pairs nor block recovery of the next actual rejected Stop.
@pytest.mark.parametrize("invalid", [False, True])
async def test_stop_recovery_preserves_closed_queued_tail(tmp_path, tls, unused_tcp_port, monkeypatch, capsys, invalid):
    monkeypatch.setattr(mp, "OwnerBench", ReplyBench)
    async with mp.managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        async with running(rig, "queued-tail-boot") as (s, c, charger):
            await mp.healthy(c)
            rig.bench.stop_gate = asyncio.Event()
            first = await stop_lifecycle(rig, s, charger, {"status": "invalid"} if invalid else None)
            await until(lambda: len(getattr(rig.bench, "stop_wires", [])) == 1)
            second_reply = await rig.bench.call(
                charger,
                "overlap-start",
                "StartTransaction",
                START | {"idTag": "NATIVE_TIMER", "meterStart": 1200, "timestamp": "2026-11-10T02:00:00Z"},
            )
            second = second_reply[2]["transactionId"]
            await rig.bench.call(
                charger,
                "overlap-stop",
                "StopTransaction",
                {"transactionId": second, "timestamp": "2026-11-10T03:00:00Z", "meterStop": 1300},
            )
            assert s.journal.vendor_id(second) is None
            originals = [dict(row) for row in s.journal.db.execute("SELECT * FROM transactions ORDER BY local")]
            counter = s.journal.get("next_transaction")
            rig.bench.stop_gate.set()
            if not invalid:
                await until(lambda: s.journal.next_delivery() is None)
                assert s.journal.vendor_id(first) is not None and s.journal.vendor_id(second) is not None
                assert not s.journal.get(KEY)
                return
            await until(lambda: s.runtime.status.delivery_blocked)
            await pause(c)
        # Reject an ambiguous tail without moving its head, then restore the definite queued pair.
        j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        tail = j.db.execute(
            "SELECT token FROM outbox WHERE local=? AND action='StartTransaction'", (second,)
        ).fetchone()[0]
        j.db.execute("UPDATE outbox SET state='ambiguous' WHERE token=?", (tail,))
        j.close()
        with pytest.raises(RecoveryRequiredError, match="ambiguous or orphaned"):
            await operator.run(
                "inspect",
                tmp_path / "trial.json",
                strategy_file(rig, tmp_path),
                None,
                rig.prepared.observer.port,
                kind="stop-delivery",
            )
        j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
        j.db.execute("UPDATE outbox SET state='queued' WHERE token=?", (tail,))
        j.close()
        await reconcile(rig, tmp_path, capsys)
        async with running(rig, "first-tail-retry") as (s, c, charger):
            await until(lambda: s.journal.vendor_id(second) is not None and s.runtime.status.delivery_blocked)
            await c.tick()
            assert s.journal.next_delivery().transaction == second
            first_proof = next(iter(s.journal.get(KEY).values()))
            assert first_proof.get("superseded") and first_proof["proof"]["wire_ack"]
        await reconcile(rig, tmp_path, capsys)
        async with running(rig, "second-tail-retry") as (s, c, charger):
            await return_after_ack(rig, s, c)
            assert s.journal.get("next_transaction") == counter
            final = [dict(row) for row in s.journal.db.execute("SELECT * FROM transactions ORDER BY local")]
            for before, after in zip(originals, final, strict=True):
                assert {key: value for key, value in before.items() if key != "vendor"} == {
                    key: value for key, value in after.items() if key != "vendor"
                }
            assert len(rig.bench.stop_wires) == 4
