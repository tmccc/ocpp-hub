"""Real HTTP/TLS recovery proves every acknowledged lifecycle before later charging resumes."""

import asyncio
import copy
import json
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager

import pytest
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.managed_delivery_recovery import KEY as DELIVERY_KEY
from ocpp_hub_core.prototype.managed_delivery_recovery import delivery_recovery_evidence
from ocpp_hub_core.prototype.managed_release_recovery import ACK, acknowledged_evidence, reconcile_owner_release
from ocpp_hub_core.prototype.managed_start_delivery import DENIALS, denied_start_evidence
from ocpp_hub_core.prototype.store import RecoveryRequiredError, fingerprint
from ocpp_hub_core.prototype.trial_session import save_private

from tests import conftest as _core_setup  # noqa: F401 - load the HA-free source package.
from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench, two_later_sessions
from tests.handover_helpers import crash
from tests.test_managed_debt_reconciliation import strategy_file
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_stop_reply_recovery import pause, running
from tests.test_managed_test_charge import ChargeRig, ready
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tools import reconcile_managed_delivery as mapping


class HeldStopReply:
    """The independent cloud can withhold one genuine Stop result after closing its own matching session."""

    # Every request and reply continues on the same TLS connection; only the chosen result is held.
    def __init__(self, connection, bench):
        self.connection, self.bench, self.stop_count, self.held_token = connection, bench, 0, None

    # Preserve the ordinary websocket surface, including cloud-originated commands.
    def __getattr__(self, name):
        return getattr(self.connection, name)

    # Correlate the held result to an actually received immutable Stop request.
    async def __aiter__(self):
        async for raw in self.connection:
            frame = decode(raw)
            if frame[0] == 2 and frame[2] == "StopTransaction":
                self.stop_count += 1
                if self.stop_count == self.bench.hold_stop_number:
                    self.held_token = frame[1]
            yield raw

    # Backend allocation and closure run normally before this actual response boundary is reached.
    async def send(self, raw):
        frame = decode(raw)
        if frame[0] == 3 and frame[1] == self.held_token:
            self.bench.stop_entered.set()
            await self.bench.release_stop.wait()
        await self.connection.send(raw)


class ProofBench(StartReplyBench):
    """Real peer implementation supplies distinct backend IDs, real denial replies and a final-Stop gate."""

    # Keep gates disabled throughout commissioning and ordinary healthy setup.
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hold_stop_number = None
        self.stop_entered, self.release_stop = asyncio.Event(), asyncio.Event()

    # Reuse the complete ordinary cloud implementation behind the correlated response gate.
    async def vendor(self, connection):
        await super().vendor(HeldStopReply(connection, self))


class HeldHTTPWriter:
    """Hold an actual native HTTP response after normal parsing and response construction."""

    # The normal native peer retains its genuine request parsing, authentication and response body.
    def __init__(self, writer, peer):
        self.writer, self.peer, self.parts = writer, peer, []

    # Preserve close, wait_closed and transport inspection on the real server socket.
    def __getattr__(self, name):
        return getattr(self.writer, name)

    # Buffer precisely the response bytes which the independent native implementation produced.
    def write(self, body):
        self.parts.append(body)

    # Release the unaltered response only after the test has changed a real committed proof record.
    async def drain(self):
        self.peer.http_entered.set()
        await self.peer.release_http.wait()
        for part in self.parts:
            self.writer.write(part)
        self.parts.clear()
        await self.writer.drain()


class ProofNativePeer(InterruptedNativePeer):
    """Normal native HTTP, actual outage replies and one independently held native audit response."""

    # Independent native state and outage observations continue to come from the established fixture.
    def __init__(self, bench):
        super().__init__(bench)
        self.hold_http = False
        self.http_entered, self.release_http = asyncio.Event(), asyncio.Event()

    # Gate only the next actual response and leave every later request on the real handler.
    async def handle(self, reader, writer):
        if self.hold_http:
            self.hold_http = False
            await super().handle(reader, HeldHTTPWriter(writer, self))
        else:
            await super().handle(reader, writer)


# Use original native6 or real explicit native32 preparation before selecting 16/32 through the normal worker.
@asynccontextmanager
async def selected_service(tmp_path, tls, port, monkeypatch, selected):
    monkeypatch.setattr(mp, "OwnerBench", ProofBench)
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", ProofNativePeer)
    factory = (
        ready(tmp_path, tls, port, monkeypatch, cli_route=True)
        if selected == 6
        else commissioned(tmp_path, tls, port, monkeypatch, selected, cli_route=True)
    )
    async with factory as b:
        strategy_file(b.rig, tmp_path)
        try:
            yield b
        finally:
            b.rig.bench.release_stop.set()
            b.rig.peer.release_http.set()


# Queue distinct real immutable closed pairs while the first actual cloud Start reply is still withheld.
async def queue_closed_pairs(b, replies):
    rig, s, sock = b.rig, b.session, b.charger
    gate = asyncio.Event()
    rig.bench.start_reply_gate, rig.bench.start_replies = gate, list(replies)
    locals_seen = []
    for number in range(len(replies)):
        minute = 2 * number
        opened = await rig.bench.call(
            sock,
            uuid.uuid4().hex,
            "StartTransaction",
            START
            | {
                "idTag": "NATIVE_TIMER",
                "meterStart": 1000 + 100 * number,
                "timestamp": f"2026-01-01T00:{minute:02d}:00Z",
            },
        )
        assert opened[0] == 3 and opened[2]["idTagInfo"]["status"] == "Accepted"
        local = opened[2]["transactionId"]
        locals_seen.append(local)
        assert (
            await rig.bench.call(
                sock,
                uuid.uuid4().hex,
                "StopTransaction",
                {
                    "transactionId": local,
                    "meterStop": 1100 + 100 * number,
                    "timestamp": f"2026-01-01T00:{minute + 1:02d}:00Z",
                },
            )
        )[0] == 3
    assert len(set(locals_seen)) == len(replies)
    assert not s.journal.get("control").get("inhibit")
    assert [row[0] for row in s.journal.db.execute("SELECT action FROM outbox ORDER BY seq")] == (
        ["StartTransaction", "StopTransaction"] * len(replies)
    )
    rig.bench.start_reply_gate = None
    gate.set()
    return locals_seen


# Accept one fresh restrictive intent and wait for its effect without running the fault reconciliation tick.
async def pause_before_reconciliation(c):
    identifier = uuid.uuid4().hex
    c.command(identifier, "pause", c.requests.intent().revision)
    if c.pause_task is not None:
        await c.pause_task
    return identifier


# Build both real proofs using the actual offline mapping tool while no service owns the journal.
@asynccontextmanager
async def mixed_proofs(tmp_path, tls, port, monkeypatch, capsys, selected, first_reply="Blocked"):
    async with selected_service(tmp_path, tls, port, monkeypatch, selected) as original:
        rig, s, c = original.rig, original.session, original.controller
        ids = await queue_closed_pairs(original, [first_reply, "silent"])
        await until(
            lambda: (
                s.journal.next_delivery() is not None
                and s.journal.next_delivery().transaction == ids[1]
                and s.journal.next_delivery().state == "ambiguous"
            ),
            timeout=12,
        )
        await c.tick()
        await until(lambda: s.gateway.controls.ready and rig.bench.firmware.effective_limit() == 0)
        fault = copy.deepcopy(s.journal.get("managed_control_fault"))
        assert fault and s.journal.vendor_id(ids[0]) is not None and s.journal.vendor_id(ids[1]) is None
        await pause(c)
        assert s.journal.get("managed_control_fault") == fault
        second = rig.bench.backend_starts[1]
        assert second["token"] == s.journal.next_delivery().token
        assert second["answer"]["idTagInfo"]["status"] == "Accepted"
        cloud_id, evidence_digest = second["answer"]["transactionId"], fingerprint(second)
        await c.tests.close()
        await crash(s)
        commands = copy.deepcopy(rig.bench.device_calls)
        args, native_port = (tmp_path / "trial.json", strategy_file(rig, tmp_path)), rig.prepared.observer.port
        assert await mapping.run("inspect", *args, None, native_port) == 0
        candidate = json.loads(capsys.readouterr().out.splitlines()[-1])["candidate"]
        assert candidate["local"] == ids[1] and candidate["start_token"] == second["token"]
        evidence = candidate | {
            "vendor": cloud_id,
            "evidence_sha256": evidence_digest,
            "verified_at_epoch": time.time(),
        }
        path = tmp_path / "independent-cloud-proof.json"
        save_private(path, evidence)
        assert await mapping.run("reconcile-start", *args, path, native_port) == 0
        capsys.readouterr()
        assert rig.bench.device_calls == commands
        async with running(rig, "mixed-proof-replacement") as (fresh, controller, sock):
            await until(lambda: fresh.journal.next_delivery() is None)
            assert fresh.journal.vendor_id(ids[1]) == cloud_id
            assert not rig.bench.backend_errors and all(row["closed"] for row in rig.bench.backend_starts)
            assert fresh.journal.get("managed_control_fault") == fault
            assert delivery_recovery_evidence(controller, fault) is not None
            assert (denied_start_evidence(controller, fault) is not None) == (first_reply == "Blocked")
            yield ChargeRig(rig, fresh, controller, sock), ids, fault


# Intent: supported independent mapping plus denial both finish under one fresh Pause at each real selected ceiling.
# Why: recording only one proof creates a permanently mismatched Pause although every lifecycle really closed.
@pytest.mark.parametrize("selected", [6, 16, 32])
@pytest.mark.parametrize("first_reply", ["Accepted", "Blocked"])
async def test_mapping_and_denial_capture_all_proofs_then_two_later_sessions(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys, selected, first_reply
):
    async with mixed_proofs(tmp_path, tls, unused_tcp_port, monkeypatch, capsys, selected, first_reply) as (
        b,
        ids,
        fault,
    ):
        c, j = b.controller, b.session.journal
        identifier = await pause_before_reconciliation(c)
        ack = j.get(ACK)
        assert ack["pause_request"] == identifier and "delivery_reconciliation" in ack
        assert ("denied_start_reconciliation" in ack) == (first_reply == "Blocked")
        assert acknowledged_evidence(c, fault, ack)
        await c.tick()
        assert j.get("managed_control_fault") is None
        assert j.get(DELIVERY_KEY)[str(ids[1])]["completed"]["pause"] == identifier
        if first_reply == "Blocked":
            assert j.get(DENIALS)[str(ids[0])]["completed"]["pause"] == identifier
        assert (await mp.healthy(c))["local_hold_verified"] and b.rig.bench.firmware.effective_limit() == 0
        await two_later_sessions(b, selected=selected, original_starts=0)


# Intent: added, removed and changed actual proof records revoke a Pause before and during its real native read.
# Why: acknowledging one snapshot cannot authorize different evidence after any asynchronous boundary.
@pytest.mark.parametrize("change", ["added", "removed", "changed"])
@pytest.mark.parametrize("timing", ["after-accept", "held-native-read"])
async def test_all_proof_snapshot_rejects_committed_change(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys, change, timing
):
    async with mixed_proofs(tmp_path, tls, unused_tcp_port, monkeypatch, capsys, 6) as (b, ids, fault):
        c, j, peer = b.controller, b.session.journal, b.rig.peer
        original_denials, original_delivery = copy.deepcopy(j.get(DENIALS)), copy.deepcopy(j.get(DELIVERY_KEY))
        if change == "added":
            # Contract control: temporarily remove one genuine already-produced proof, then restore its exact bytes.
            hidden = copy.deepcopy(original_denials)
            hidden[str(ids[0])]["completed"] = {"pause": "contract-control", "at_epoch": time.time()}
            j.put(DENIALS, hidden)
        await pause_before_reconciliation(c)
        ack = copy.deepcopy(j.get(ACK))
        task = None
        try:
            if timing == "held-native-read":
                peer.hold_http = True
                task = asyncio.create_task(reconcile_owner_release(c))
                await asyncio.wait_for(peer.http_entered.wait(), 3)
            if change == "added":
                j.put(DENIALS, original_denials)
            else:
                altered = copy.deepcopy(original_delivery)
                if change == "removed":
                    altered[str(ids[1])]["completed"] = {"pause": "contract-control", "at_epoch": time.time()}
                else:
                    altered[str(ids[1])]["proof"]["native_digest"] = fingerprint("another committed native proof")
                j.put(DELIVERY_KEY, altered)
            assert not acknowledged_evidence(c, fault, ack)
            peer.release_http.set()
            if task is not None:
                with pytest.raises(RecoveryRequiredError):
                    await task
            else:
                assert not await reconcile_owner_release(c)
            assert j.get("managed_control_fault") == fault and j.get(ACK) == ack
            assert b.rig.bench.firmware.effective_limit() == 0 and c.requests.intent().mode == "paused"
        finally:
            peer.release_http.set()
            if task is not None and not task.done():
                await asyncio.gather(task, return_exceptions=True)
            j.put(DENIALS, original_denials)
            j.put(DELIVERY_KEY, original_delivery)
        await pause(c)
        assert j.get("managed_control_fault") is None
        assert (await mp.healthy(c))["local_hold_verified"]
        await two_later_sessions(b, selected=6, original_starts=0)


# Intent: every queued denied session requires its own real Stop reply before a fresh Pause finishes the whole group.
# Why: a singleton Stop receipt cannot stand for two or three distinct local/cloud lifecycles.
@pytest.mark.parametrize("selected", [6, 16, 32])
@pytest.mark.parametrize("count", [2, 3])
async def test_multiple_denials_require_last_wire_ack_and_then_recover(
    tmp_path, tls, unused_tcp_port, monkeypatch, selected, count
):
    async with selected_service(tmp_path, tls, unused_tcp_port, monkeypatch, selected) as b:
        rig, c, j = b.rig, b.controller, b.session.journal
        rig.bench.hold_stop_number = count
        ids = await queue_closed_pairs(b, ["Blocked"] * count)
        try:
            await asyncio.wait_for(rig.bench.stop_entered.wait(), 3)
            await c.tick()
            fault = copy.deepcopy(j.get("managed_control_fault"))
            assert rig.bench.firmware.effective_limit() == 0
            early, _ = await pause(c)
            assert j.get("managed_control_fault") == fault
            assert j.get(ACK, {}).get("pause_request") != early
            assert all(row["completed"] is None for row in j.get(DENIALS).values())
            assert j.next_delivery().transaction == ids[-1] and j.next_delivery().action == "StopTransaction"
        finally:
            rig.bench.release_stop.set()
        await until(lambda: j.next_delivery() is None)
        denials = j.get(DENIALS)
        for local in ids:
            denial, backend = (
                denials[str(local)],
                next(row for row in rig.bench.backend_starts if row["answer"]["transactionId"] == j.vendor_id(local)),
            )
            wire = denial["wire_ack"]
            assert wire["fault"] == fault and wire["local_transaction_id"] == local
            assert wire["vendor_transaction_id"] == backend["answer"]["transactionId"]
            assert wire["delivery_token"] == backend["stop"]["token"]
            assert wire["reply_digest"] == fingerprint({})
        final, _ = await pause(c)
        assert j.get("managed_control_fault") is None
        assert {row["completed"]["pause"] for row in j.get(DENIALS).values()} == {final}
        assert not rig.bench.backend_errors and all(row["closed"] for row in rig.bench.backend_starts)
        assert (await mp.healthy(c))["local_hold_verified"]
        await two_later_sessions(b, selected=selected, original_starts=0)


# Intent: final denial completion and fault clearing roll back together on genuine metadata/delete/COMMIT failures.
# Why: partially completing a group can lose its exact Pause authority or falsely clear a retained fault.
@pytest.mark.parametrize("boundary", ["denial-update", "fault-delete", "commit"])
async def test_full_reconciliation_rollback_preserves_denials_and_pause(
    tmp_path, tls, unused_tcp_port, monkeypatch, boundary
):
    async with selected_service(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j, rig = b.controller, b.session.journal, b.rig
        ids = await queue_closed_pairs(b, ["Blocked", "Expired", "Invalid"])
        await until(lambda: j.next_delivery() is None)
        await c.tick()
        await pause_before_reconciliation(c)
        before = copy.deepcopy(j.get(DENIALS)), copy.deepcopy(j.get("managed_control_fault")), copy.deepcopy(j.get(ACK))
        completed = f"NEW.key='{DENIALS}' AND json_extract(NEW.value,'$.\"{ids[-1]}\".completed') IS NOT NULL"
        if boundary == "fault-delete":
            trigger = "BEFORE DELETE ON meta WHEN OLD.key='managed_control_fault'"
            effect = "SELECT RAISE(ABORT,'synthetic fault deletion refusal')"
        elif boundary == "denial-update":
            trigger = f"BEFORE INSERT ON meta WHEN {completed}"
            effect = "SELECT RAISE(ABORT,'synthetic final denial refusal')"
        else:
            j.db.execute("CREATE TABLE proof_parent(id INTEGER PRIMARY KEY)")
            j.db.execute("CREATE TABLE proof_child(id REFERENCES proof_parent(id) DEFERRABLE INITIALLY DEFERRED)")
            trigger, effect = f"AFTER INSERT ON meta WHEN {completed}", "INSERT INTO proof_child VALUES(1)"
        j.db.execute(f"CREATE TRIGGER fail_denial_completion {trigger} BEGIN {effect}; END")
        try:
            with pytest.raises(sqlite3.IntegrityError):
                await c.tick()
            assert not j.db.in_transaction
            assert (j.get(DENIALS), j.get("managed_control_fault"), j.get(ACK)) == before
            assert j.next_delivery() is None and rig.bench.firmware.effective_limit() == 0
            assert all(row["completed"] is None for row in j.get(DENIALS).values())
        finally:
            j.db.execute("DROP TRIGGER fail_denial_completion")
        await c.tick()
        assert j.get("managed_control_fault") is None and all(row["completed"] for row in j.get(DENIALS).values())
        assert (await mp.healthy(c))["local_hold_verified"]
        await two_later_sessions(b, selected=6, original_starts=0)
