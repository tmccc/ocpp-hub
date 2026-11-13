"""Two actual SQLite refusals, queued real lifecycles, changed Stop reply and complete later recovery.

Independent TLS peers and real SQLite statement/COMMIT failures preserve exact FIFO debt and recovery.
"""

import asyncio
import copy
import json
import re
import sqlite3
import time
import uuid
from contextlib import AsyncExitStack
from datetime import UTC, datetime, timedelta

import pytest
from ocpp_hub_core.frames import build_call_error, build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.managed_debt_recovery import KEY
from ocpp_hub_core.prototype.managed_stop_delivery import REJECTION, WIRE_ACK
from ocpp_hub_core.prototype.store import Journal, fingerprint
from ocpp_hub_core.prototype.trial_session import save_private

from tests import conftest as _engine  # noqa: F401 - initialize standalone core package
from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import crash
from tests.recovery_barriers import returned_to_app, witnessed_native_loss
from tests.test_managed_debt_reconciliation import strategy_file
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_stop_reply_recovery import CloudReplyPeer, pause, running
from tests.test_managed_test_charge import ChargeRig, positive_flow
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests_model.catalog import input_digest
from tests_model.storage_debt_cases import storage_fifo_candidates
from tests_model.test_schedule_sequences import listener_port, tls_for_case
from tests_model.test_schedule_sequences import model_tls as model_tls
from tests_model.test_start_recovery_sequences import close_daily
from tools import reconcile_managed_debt as operator

FAULT_TRIGGER = "deep_storage_failure"
PARENT = "deep_storage_parent"
CHILD = "deep_storage_child"


class ScriptedCloud(CloudReplyPeer):
    """Keep the actual cloud handler, changing only named Stop peer replies and barriers."""

    # Retain actual peer calls and create per-token Stop response barriers.
    async def __aiter__(self):
        async for raw in super().__aiter__():
            frame = decode(raw)
            if frame[0] == 2:
                self.bench.all_wires.append(copy.deepcopy(frame))
                if frame[2] == "StopTransaction" and frame[1] not in self.bench.stop_gates:
                    gate = new_gate(self.bench, frame[1])
                    # Hold exactly the first real Stop, so later real closed pairs can queue behind it.
                    if self.bench.first_stop is None:
                        self.bench.first_stop = frame[1]
                    else:
                        gate.set()
            yield raw

    # Emit only the declared peer reply and retain independent wire evidence.
    async def send(self, raw):
        frame = decode(raw)
        if frame[0] == 3 and self.actions.get(frame[1]) == "StopTransaction":
            await self.bench.stop_gates[frame[1]].wait()
            mode = self.bench.stop_modes.get(frame[1], getattr(self.bench, "default_stop_mode", "bad0"))
            if mode == "error":
                raw = build_call_error(frame[1], "InternalError", "independent changed Stop refusal")
            elif mode in ("bad0", "bad1"):
                raw = build_call_result(frame[1], {"idTagInfo": {"status": "invalid-" + mode}})
            else:
                assert mode == "valid"
            await self.connection.send(raw)
            self.bench.stop_replies.append(decode(raw))  # only a response actually emitted on this socket
            return
        await self.connection.send(raw)
        if frame[0] == 3 and self.actions.get(frame[1]) == "StartTransaction":
            self.bench.start_replies.append(copy.deepcopy(frame))


class ScriptedBench(mp.OwnerBench):
    # Keep peer input, mappings and barriers separate from the controller.
    def __init__(self):
        super().__init__()
        self.all_wires, self.stop_replies = [], []
        self.start_replies = []
        self.reported = []
        self.stop_gates, self.stop_modes = {}, {}
        self.all_stop_gates = []
        self.first_stop = None

    # Run the ordinary independent cloud peer behind its response wrapper.
    async def vendor(self, connection):
        await super().vendor(ScriptedCloud(connection, self))

    # Capture original external device input independently of journal encoding and cloud translation.
    async def call(self, charger, token, action, payload):
        if action in ("StartTransaction", "StopTransaction"):
            self.reported.append((action, copy.deepcopy(payload)))
        return await super().call(charger, token, action, payload)


# Require exact immutable payloads, independent emitted cloud mappings and FIFO first-dispatch order.
def assert_cloud_fifo(bench, initial):
    vendor_by_local = {}
    for transaction in initial["transactions"]:
        start, stop = json.loads(transaction["start"]), json.loads(transaction["stop"])
        assert ("StartTransaction", start) in bench.reported and ("StopTransaction", stop) in bench.reported
        calls = [frame for frame in bench.all_wires if frame[2] == "StartTransaction" and frame[3] == start]
        assert len(calls) == 1
        replies = [frame for frame in bench.start_replies if frame[1] == calls[0][1]]
        assert len(replies) == 1 and type(replies[0][2]["transactionId"]) is int
        vendor_by_local[transaction["local"]] = replies[0][2]["transactionId"]
    tokens = [row["token"] for row in initial["outbox"]]
    dispatched = list(dict.fromkeys(frame[1] for frame in bench.all_wires if frame[1] in tokens))
    assert dispatched == tokens
    for row in initial["outbox"]:
        expected = json.loads(row["payload"])
        if row["action"] == "StopTransaction":
            expected["transactionId"] = vendor_by_local[row["local"]]
        calls = [frame for frame in bench.all_wires if frame[1] == row["token"]]
        assert calls and all(frame[2] == row["action"] and frame[3] == expected for frame in calls)


# Retain every barrier so superseded socket handlers can also be released.
def new_gate(bench, token):
    gate = asyncio.Event()
    bench.stop_gates[token] = gate
    bench.all_stop_gates.append(gate)  # old closed-socket handlers retain their original barrier
    return gate


# Quote only generated synthetic tokens in exact-target fault fixtures.
def sql_literal(value):
    # These are production-generated UUID tokens, not caller SQL; retain explicit quoting in the fixture.
    return "'" + value.replace("'", "''") + "'"


# Make SQLite itself refuse the declared write or its deferred COMMIT.
def install_failure(db, case, head_token):
    assert re.fullmatch(r"e-[a-f0-9]{32}", head_token)
    target = (
        "BEFORE INSERT ON meta WHEN NEW.key=" + sql_literal(KEY)
        if case.cut == "proof"
        else "BEFORE DELETE ON outbox WHEN OLD.token=" + sql_literal(head_token)
    )
    if case.sqlite_fault == "write":
        body = "SELECT RAISE(ABORT,'independent storage statement refusal');"
    else:
        db.execute(f"CREATE TABLE {PARENT}(id INTEGER PRIMARY KEY)")
        db.execute(f"CREATE TABLE {CHILD}(id INTEGER REFERENCES {PARENT}(id) DEFERRABLE INITIALLY DEFERRED)")
        body = f"INSERT INTO {CHILD} VALUES(1);"
    db.execute(f"CREATE TRIGGER {FAULT_TRIGGER} {target} BEGIN {body} END")


# Repair only the declared storage fixture without changing any debt.
def drop_failure(db):
    db.execute(f"DROP TRIGGER {FAULT_TRIGGER}")


# Never construct a Journal merely to inspect an inflight head: initialization itself performs recovery.
def read_stopped(path):
    db = sqlite3.connect("file:" + str(path) + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    return db


# Capture protected lifecycle, FIFO, proof and fault preimages.
def state(db):
    # Read one actual JSON metadata value through the supplied connection.
    def meta(key):
        row = db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    return {
        "transactions": [dict(r) for r in db.execute("SELECT * FROM transactions ORDER BY local")],
        "outbox": [dict(r) for r in db.execute("SELECT * FROM outbox ORDER BY seq")],
        "counter": meta("next_transaction"),
        "proofs": meta(KEY),
        "fault": meta("managed_control_fault"),
        "stop_wire_ack": meta(WIRE_ACK),
        "stop_rejection": meta(REJECTION),
    }


# Close the read-only observation connection after its durable snapshot.
def stopped_state(path):
    db = read_stopped(path)
    try:
        return state(db)
    finally:
        db.close()


# Select the exact non-superseded proof for this actual Stop token.
def proof_for(j, token):
    matches = [
        e
        for e in j.get(KEY, {}).values()
        if e["proof"]["attestation"].get("delivery_token") == token and not e.get("superseded")
    ]
    assert len(matches) == 1
    return matches[0]


# Identify only an actual ACK transaction ending in rollback, excluding a prior reply-save COMMIT.
def rolled_back_ack_transactions(trace, token):
    target = "DELETE FROM outbox WHERE token=" + sql_literal(token)
    active, matches = None, []
    for statement in trace:
        sql = statement.strip()
        if sql == "BEGIN IMMEDIATE":
            active = []
        if active is not None:
            active.append(sql)
            if sql == "ROLLBACK":
                if target in active:
                    matches.append(tuple(active))
                active = None
    return tuple(matches)


# A COMMIT trace is an attempt, so deferred-FK refusal must be followed by rollback in this same ACK.
def require_ack_rollback(trace, token, sqlite_fault):
    attempts = rolled_back_ack_transactions(trace, token)
    assert attempts, "no rolled-back ACK transaction for the exact head"
    assert all(("COMMIT" in attempt) == (sqlite_fault == "commit") for attempt in attempts)
    return attempts


# Protect the saved response separately from the ACK state that must roll back atomically.
def require_saved_stop_reply(db, head, actual):
    rows = [dict(row) for row in db.execute("SELECT * FROM delivery_replies ORDER BY token")]
    assert len(rows) == 1
    row = rows[0]
    assert (row["token"], row["action"], row["payload"], row["local"]) == (
        head["token"],
        "StopTransaction",
        head["payload"],
        head["local"],
    )
    assert actual[0] == 3 and actual[1] == head["token"]
    assert json.loads(row["answer"]) == actual
    return row


# Count actual socket requests independently of the gateway's saved delivery reply.
def head_wires(bench, token):
    return [frame for frame in bench.all_wires if frame[1] == token and frame[2] == "StopTransaction"]


# Permit only independently delivered vendor mapping to fill historical rows.
def same_original_transactions(before, after):
    # Only an actual Start wire reply may fill a previously missing vendor mapping.
    assert len(before) == len(after)
    for old, new in zip(before, after, strict=True):
        assert {k: v for k, v in old.items() if k != "vendor"} == {k: v for k, v in new.items() if k != "vendor"}
        if old["vendor"] is not None:
            assert new["vendor"] == old["vendor"]


# Bind synthetic operator evidence to actual independent peer messages and physical originals.
async def make_evidence(rig, tmp_path, capsys):
    assert rig.bench.firmware.plans == rig.bench.original_plans
    assert rig.peer.configuration == rig.bench.original_config
    assert rig.bench.firmware.current == 0 and rig.bench.firmware.effective_limit() == 0
    config = tmp_path / "trial.json"
    strategy = strategy_file(rig, tmp_path)
    port = rig.prepared.observer.port
    assert await operator.run("inspect", config, strategy, None, port, kind="stop-delivery") == 0
    candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
    token = candidate["delivery_token"]
    # Witness from actual independent peer traffic, not a hash of the engine candidate alone.
    received = [f for f in rig.bench.stop_wires if f[1] == token]
    emitted = [f for f in rig.bench.stop_replies if f[1] == token]
    assert received and emitted
    evidence = candidate | {
        "cloud_stop_independently_verified": True,
        "fault_origin_independently_verified": True,
        "original_state_independently_verified": True,
        "evidence_sha256": fingerprint({"actual_cloud_Stop_calls": received, "actual_cloud_Stop_replies": emitted}),
        "verified_at_epoch": time.time(),
    }
    path = tmp_path / ("proof-" + token + ".json")
    save_private(path, evidence)
    return config, strategy, port, path


# Call the actual exclusive operator workflow with its inspected evidence.
async def reconcile_evidence(args):
    config, strategy, port, path = args
    return await operator.run("reconcile", config, strategy, path, port, kind="stop-delivery")


# Require a fresh supported exact proof without any physical commands.
async def fresh_proof(rig, tmp_path, capsys):
    before = list(rig.bench.device_calls)
    args = await make_evidence(rig, tmp_path, capsys)
    assert await reconcile_evidence(args) == 0
    assert rig.bench.device_calls == before
    return args


# Await the actual runtime delivery state of one immutable token.
async def head_is(s, token, status):
    await until(lambda: (h := s.journal.next_delivery()) is not None and h.token == token and h.state == status)


# Verify two new positive sessions with distinct physical outage and recovery epochs.
async def two_fresh_lifecycles(b, selected, events):
    c, fw = b.controller, b.rig.bench.firmware
    for number in range(2):
        beginning = datetime(2026, 1, 1, 1, number * 2, tzinfo=UTC)
        local = await positive_flow(
            b,
            "daily",
            call_id=uuid.uuid4().hex,
            meter_start=3000 + number * 100,
            timestamp=beginning.isoformat().replace("+00:00", "Z"),
        )
        assert (await mp.healthy(c))["current_a"] == selected and fw.current == selected
        events.append(f"lifecycle-{number + 1}")
        assert await witnessed_native_loss(b, selected, local) >= 3
        events.append(f"outage-{number + 1}")
        assert fw.current == selected
        await close_daily(
            b,
            local,
            "native" if number == 0 else "ledger",
            timestamp=(beginning + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
            meter_stop=3100 + number * 100,
        )
        assert b.session.journal.active() is None and not b.session.journal.get("managed_control_fault")


# Intent: two real storage failures and queued tails require exact new proofs and two later physical recoveries.
# Why: one repaired head cannot transfer proof or hide a changed refusal on a later real lifecycle.
@pytest.mark.parametrize("case", storage_fifo_candidates(), ids=lambda c: c.identifier)
async def test_storage_fifo_sequences(tmp_path, model_tls, monkeypatch, capsys, record_property, case):
    monkeypatch.setattr(mp, "OwnerBench", ScriptedBench)
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    events, failures, suffix_complete = [], 0, False
    queued_pairs_verified = changed_reply_refused = False
    stack = AsyncExitStack()
    try:
        async with commissioned(
            tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch, case.selected, cli_route=True
        ) as original:
            try:
                rig, s, c, sock = original.rig, original.session, original.controller, original.charger
                bench, fw = rig.bench, rig.bench.firmware
                original_plans, original_config = copy.deepcopy(fw.plans), copy.deepcopy(rig.peer.configuration)
                bench.original_plans, bench.original_config = original_plans, original_config
                local = await positive_flow(original, "daily")
                await mp.healthy(c)
                rig.peer.permission_enabled, fw.requested_current = False, 0
                rig.bench.cp, rig.bench.connection = "B", "Plug"
                await bench.call(
                    sock,
                    uuid.uuid4().hex,
                    "StopTransaction",
                    {"transactionId": local, "timestamp": "2026-01-01T00:01:00Z", "meterStop": 1100},
                )
                await until(lambda: bench.first_stop is not None)
                head_token = bench.first_stop
                locals_ = [local]
                for index in range(1, case.tail_pairs + 1):
                    start = datetime(2026, 1, 1, 0, index * 2, tzinfo=UTC)
                    reply = await bench.call(
                        sock,
                        uuid.uuid4().hex,
                        "StartTransaction",
                        START
                        | {
                            "meterStart": 1100 + index * 100,
                            "idTag": "NATIVE_TIMER",
                            "timestamp": start.isoformat().replace("+00:00", "Z"),
                        },
                    )
                    assert reply[0] == 3 and reply[2]["idTagInfo"]["status"] == "Accepted"
                    tail_local = reply[2]["transactionId"]
                    assert s.journal.vendor_id(tail_local) is None
                    locals_.append(tail_local)
                    await bench.call(
                        sock,
                        uuid.uuid4().hex,
                        "StopTransaction",
                        {
                            "transactionId": tail_local,
                            "meterStop": 1200 + index * 100,
                            "timestamp": (start + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
                        },
                    )
                initial = state(s.journal.db)
                tokens = [r["token"] for r in initial["outbox"] if r["action"] == "StopTransaction"]
                assert len(tokens) == case.tail_pairs + 1 and tokens[0] == head_token
                for tail_local in locals_[1:]:
                    rows = [r for r in initial["outbox"] if r["local"] == tail_local]
                    assert [r["action"] for r in rows] == ["StartTransaction", "StopTransaction"]
                    assert all(r["state"] == "queued" for r in rows)
                queued_pairs_verified = True
                events.append("tails-queued")
                bench.stop_gates[head_token].set()
                await head_is(s, head_token, "rejected")
                await pause(c)
                assert fw.effective_limit() == 0
                events.append("head-refused")
                calls_at_pause = len(bench.device_calls)
                await c.tests.close()
                await crash(s)
                # Install the exact fault only while stopped; the rejected head is already durable.
                j = Journal(rig.prepared.config.journal, rig.prepared.config.identity)
                try:
                    install_failure(j.db, case, head_token)
                finally:
                    j.close()
                if case.cut == "proof":
                    args = await make_evidence(rig, tmp_path, capsys)
                    before = stopped_state(rig.prepared.config.journal)
                    device_before = list(bench.device_calls)
                    for _ in range(2):
                        with pytest.raises(sqlite3.IntegrityError) as error:
                            await reconcile_evidence(args)
                        expected = (
                            sqlite3.SQLITE_CONSTRAINT_TRIGGER
                            if case.sqlite_fault == "write"
                            else sqlite3.SQLITE_CONSTRAINT_FOREIGNKEY
                        )
                        assert error.value.sqlite_errorcode == expected
                        assert stopped_state(rig.prepared.config.journal) == before
                        assert bench.device_calls == device_before
                        failures += 1
                        events.append(f"failure-{failures}")
                else:
                    await fresh_proof(rig, tmp_path, capsys)
                    bench.stop_modes[head_token] = "valid"
                    wire_before = len(head_wires(bench, head_token))
                    replies_before = len([frame for frame in bench.stop_replies if frame[1] == head_token])
                    saved_reply = None
                    for attempt in range(2):
                        if attempt == 0:
                            new_gate(bench, head_token)
                        async with running(rig, f"storage-ack-failure-{attempt}", release_on_exit=False) as (
                            s,
                            c,
                            sock,
                        ):
                            await head_is(s, head_token, "inflight")
                            before = state(s.journal.db)
                            head = before["outbox"][0]
                            trace = []
                            s.journal.db.set_trace_callback(trace.append)
                            try:
                                if attempt == 0:
                                    bench.stop_gates[head_token].set()
                                # Replacement retries the saved ACK; a new cloud reply is forbidden, not required.
                                await until(lambda: bool(rolled_back_ack_transactions(trace, head_token)))
                                require_ack_rollback(trace, head_token, case.sqlite_fault)
                                assert s.runtime.status.delivery_blocked
                                assert s.runtime.status.delivery_blocked_cause == "storage"
                                assert state(s.journal.db) == before and not s.journal.db.in_transaction
                                assert proof_for(s.journal, head_token)["proof"]["wire_ack"] is None
                                actual = [frame for frame in bench.stop_replies if frame[1] == head_token]
                                assert len(actual) == replies_before + 1
                                assert len(head_wires(bench, head_token)) == wire_before + 1
                                retained = require_saved_stop_reply(s.journal.db, head, actual[-1])
                                if saved_reply is not None:
                                    assert retained == saved_reply
                                saved_reply = retained
                                if case.sqlite_fault == "commit":
                                    assert s.journal.db.execute(f"SELECT COUNT(*) FROM {CHILD}").fetchone()[0] == 0
                            finally:
                                s.journal.db.set_trace_callback(None)
                            failures += 1
                            events.append(f"failure-{failures}")
                assert failures == 2
                # A saved head ACK can settle immediately after repair. Hold the first tail response instead.
                new_gate(bench, tokens[1])
                if case.cut == "proof":
                    new_gate(bench, head_token)
                s, c, sock = await stack.enter_async_context(
                    running(rig, "storage-event-prefix", release_on_exit=False)
                )
                repaired = False
                for event in case.order:
                    if event == "repair":
                        drop_failure(s.journal.db)
                        repaired = True
                    elif event == "pause":
                        await pause(c)
                    else:
                        await stack.aclose()
                        stack = AsyncExitStack()
                        s, c, sock = await stack.enter_async_context(
                            running(rig, "storage-event-replacement", release_on_exit=False)
                        )
                    assert fw.effective_limit() == 0 and c.requests.intent().mode == "paused"
                    if case.cut == "ack" and repaired:
                        # Require autonomous cached-response settlement while the next real Stop is withheld.
                        await head_is(s, tokens[1], "inflight")
                        assert (
                            s.journal.db.execute("SELECT 1 FROM outbox WHERE token=?", (head_token,)).fetchone() is None
                        )
                        assert (
                            s.journal.db.execute(
                                "SELECT 1 FROM delivery_replies WHERE token=?", (head_token,)
                            ).fetchone()
                            is None
                        )
                        settled_proof = proof_for(s.journal, head_token)["proof"]
                        wire_ack = settled_proof["wire_ack"]
                        assert wire_ack["delivery_token"] == head_token
                        assert wire_ack["reply_digest"] == fingerprint(actual[-1][2])
                        assert settled_proof["resolved"]["retained"] is False
                        assert settled_proof["resolved"]["wire_ack"] == wire_ack
                    else:
                        assert s.journal.next_delivery().token == head_token
                        if case.cut == "ack":
                            assert proof_for(s.journal, head_token)["proof"]["wire_ack"] is None
                            assert require_saved_stop_reply(s.journal.db, head, actual[-1]) == saved_reply
                    if case.cut == "ack":
                        assert len(head_wires(bench, head_token)) == wire_before + 1
                        assert (
                            len([frame for frame in bench.stop_replies if frame[1] == head_token]) == replies_before + 1
                        )
                    assert not any(frame[1] == tokens[1] for frame in bench.stop_replies)
                    assert fw.plans == original_plans and rig.peer.configuration == original_config
                    events.append(event)
                await stack.aclose()
                stack = AsyncExitStack()
                if case.cut == "proof":
                    # Earlier evidence has an old control snapshot; inspect fresh after actual Pause/Boot events.
                    await fresh_proof(rig, tmp_path, capsys)
                    bench.stop_modes[head_token] = "valid"
                    new_gate(bench, head_token)
                s, c, sock = await stack.enter_async_context(
                    running(rig, "storage-head-final-retry", release_on_exit=False)
                )
                if case.cut == "proof":
                    await head_is(s, head_token, "inflight")
                    bench.stop_gates[head_token].set()
                await head_is(s, tokens[1], "inflight")
                if case.cut == "ack":
                    assert len(head_wires(bench, head_token)) == wire_before + 1
                bench.stop_gates[tokens[1]].set()
                await head_is(s, tokens[1], "rejected")
                events.append("head-settlement-verified")
                records = s.journal.get(KEY)
                head_records = [
                    e for e in records.values() if e["proof"]["attestation"]["delivery_token"] == head_token
                ]
                assert any(e.get("superseded") and e["proof"]["wire_ack"] for e in head_records)
                # Every queued tail first refuses independently. The first tail then changes its real reply after proof.
                for index, token in enumerate(tokens[1:], start=1):
                    await stack.aclose()
                    stack = AsyncExitStack()
                    await fresh_proof(rig, tmp_path, capsys)
                    if index == 1:
                        bench.stop_modes[token] = "bad1" if case.changed_reply == "changed-info" else "error"
                        s, c, sock = await stack.enter_async_context(
                            running(rig, "changed-tail-refusal", release_on_exit=False)
                        )
                        await head_is(s, token, "rejected")
                        entries = [
                            e
                            for e in s.journal.get(KEY).values()
                            if e["proof"]["attestation"]["delivery_token"] == token
                        ]
                        assert (
                            len(entries) == 1
                            and entries[0].get("superseded")
                            and entries[0]["proof"]["wire_ack"] is None
                        )
                        assert fw.effective_limit() == 0
                        changed_reply_refused = True
                        await stack.aclose()
                        stack = AsyncExitStack()
                        await fresh_proof(rig, tmp_path, capsys)
                    bench.stop_modes[token] = "valid"
                    s, c, sock = await stack.enter_async_context(
                        running(rig, f"tail-{index}-valid-retry", release_on_exit=False)
                    )
                    if index + 1 < len(tokens):
                        await head_is(s, tokens[index + 1], "rejected")
                    else:
                        await until(lambda: s.journal.next_delivery() is None)
                    assert fw.effective_limit() == 0
                final = state(s.journal.db)
                events.append("tails-reconciled")
                same_original_transactions(initial["transactions"], final["transactions"])
                assert final["counter"] == initial["counter"] and not final["outbox"]
                # Derive cloud mappings solely from independently emitted real Start replies.
                assert_cloud_fifo(bench, initial)
                # Cloud Start tail calls never auto replay, even through repeated ACK failures and replacement.
                for row in initial["outbox"]:
                    if row["action"] == "StartTransaction":
                        assert sum(f[1] == row["token"] and f[2] == "StartTransaction" for f in bench.all_wires) == 1
                assert (await c.tick())["state"] == "attention"
                await pause(c)
                assert (await mp.healthy(c))["local_hold_verified"] and fw.effective_limit() == 0
                events.append("pause")
                for frame in bench.device_calls[calls_at_pause:]:
                    if frame[2] == "SetChargingProfile":
                        profile = frame[3]["csChargingProfiles"]
                        if profile["chargingProfileId"] == 7001:
                            assert all(
                                row["limit"] == 0 for row in profile["chargingSchedule"]["chargingSchedulePeriod"]
                            )
                await returned_to_app(ChargeRig(rig, s, c, sock), case.selected)
                events.append("return")
                bench.default_stop_mode = "valid"
                await two_fresh_lifecycles(ChargeRig(rig, s, c, sock), case.selected, events)
                assert not mp.starts(rig)
                assert not any(f[2] == "RemoteStopTransaction" for f in bench.device_calls)
                assert fw.plans == original_plans and rig.peer.configuration == original_config
                assert events == list(case.events)
                suffix_complete = True
            finally:
                if "bench" in locals():
                    for gate in bench.all_stop_gates:
                        gate.set()
                await stack.aclose()
    finally:
        # Release every old peer gate so closed-connection handlers cannot survive test teardown.
        if "bench" in locals():
            for gate in bench.all_stop_gates:
                gate.set()
        await stack.aclose()
        record_property("model_case", case.identifier)
        record_property("model_family", "storage_fifo")
        record_property("model_input_sha256", input_digest("storage_fifo", case))
        record_property("completed_events", events)
        record_property("storage_failures_observed", failures == 2)
        record_property("queued_pairs_verified", queued_pairs_verified)
        record_property("changed_reply_refused", changed_reply_refused)
        record_property("two_fresh_lifecycles_complete", suffix_complete)
        record_property("suffix_complete", suffix_complete)
