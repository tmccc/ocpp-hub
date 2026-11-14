"""Out-of-order real native reads retain the newest source; current failures still remove freshness."""

import asyncio
import copy
import json
import sqlite3
import uuid
from datetime import datetime

import pytest
from ocpp_hub_core.prototype import managed_schedule
from ocpp_hub_core.prototype.native_bounded_handover import AuditSupersededError
from ocpp_hub_core.prototype.native_faults import NativeContentError
from ocpp_hub_core.prototype.native_http import NativeTransportUnavailableError

from tests import test_managed_schedule_cutoff as schedule_bench
from tests.test_managed_proxy import healthy, starts
from tests.test_managed_schedule import SchedulePeer
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_prototype_tls import tls as tls


class OrderedNativePeer(SchedulePeer):
    """Only the independent HTTP peer holds or replaces its selected final native response."""

    # Arm after commissioning so every preflight, profile and genuine TLS lifecycle remains ordinary.
    def __init__(self, bench):
        super().__init__(bench)
        self.mode = None
        self.configuration_reads = 0
        self.entered, self.release, self.finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.held_request = None
        self.sent_outcomes = []
        self.unavailable = False
        self.unavailable_requests = []

    # Read exact incoming bytes before holding the second getConfig or returning an actual HTTP503.
    async def handle(self, reader, writer):
        if self.mode is None and not self.unavailable:
            await super().handle(reader, writer)
            return
        header = await reader.readuntil(b"\r\n\r\n")
        fields = {
            line.split(b":", 1)[0].lower(): line.split(b":", 1)[1].strip()
            for line in header.split(b"\r\n")[1:]
            if b":" in line
        }
        raw = await reader.readexactly(int(fields[b"content-length"]))
        request = json.loads(raw)
        selected = self.unavailable
        if self.mode is not None and request["action"] == "getConfig":
            self.configuration_reads += 1
            selected = selected or self.configuration_reads == 2
        if not selected:
            replay = asyncio.StreamReader()
            replay.feed_data(header + raw)
            replay.feed_eof()
            await super().handle(replay, writer)
            return
        self.requests.append(request["action"])
        if self.unavailable:
            self.unavailable_requests.append(request)
            response = b"HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n"
        else:
            mode, self.mode = self.mode, None
            self.held_request = request
            body = (
                b'{"code":200,"code":200,"data":{}}'
                if mode == "strict-content"
                else json.dumps({"code": 200, "data": json.dumps(copy.deepcopy(self.configuration))}).encode()
            )
            response = (
                b"HTTP/1.0 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n"
                if mode == "transport503"
                else b"HTTP/1.0 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body
            )
            self.entered.set()
            await self.release.wait()
            self.sent_outcomes.append(mode)
        try:
            writer.write(response)
            await writer.drain()
        except ConnectionError:
            pass  # An actually cancelled old reader may already have retired this one fake-peer socket.
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            self.finished.set()


# Preserve the result of the real old task without accidentally cancelling its newer peer or swallowing success.
async def old_result(task, mode, *, superseded):
    result = (await asyncio.gather(task, return_exceptions=True))[0]
    if mode == "valid-other-source" and not superseded:
        assert not isinstance(result, BaseException)
        return result
    expected = (
        asyncio.CancelledError if mode == "cancelled"
        else AuditSupersededError if superseded
        else NativeContentError if mode == "strict-content"
        else NativeTransportUnavailableError
    )
    assert isinstance(result, expected), result
    return result


# After the witnessed order, finish this genuine daily and prove the next original occurrence remains usable.
async def later_daily(b, local, selected):
    b.rig.peer.unavailable = False
    b.rig.peer.plan_session = (local, 9)
    assert (await healthy(b.c))["station_ceiling_a"] == selected
    await b.close_transaction(local)
    await healthy(b.c)
    assert set(b.device.plans) == {9}
    following = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
    assert following not in (None, local)
    assert (await healthy(b.c))["current_a"] == selected
    assert not b.stops() and not starts(b.rig)


# Intent: obsolete read outcomes cannot erase a newer fresh daily source; current failures still require zero.
# Why: handling an old exception before checking sequence can create an unnecessary hold or borrowed Stop at cutoff.
@pytest.mark.parametrize("selected", [6, 32])
@pytest.mark.parametrize("newer_read", [False, True], ids=["current-failure-control", "obsolete-finishes-last"])
@pytest.mark.parametrize(
    "mode", ["valid-other-source", "strict-content", "transport503", "cancelled"]
)
async def test_native_read_outcome_belongs_to_its_sequence(
    tmp_path, tls, unused_tcp_port, monkeypatch, selected, newer_read, mode
):
    monkeypatch.setattr(schedule_bench, "SchedulePeer", OrderedNativePeer)
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        if selected != 6:
            b.c.command(uuid.uuid4().hex, "set_current", b.c.requests.intent().revision, current_a=selected)
            await b.c.current.settle()
            await healthy(b.c)
        await b.arm()
        b.clock.now = b.start
        local = await b.start_plan(9, demand=True)
        await healthy(b.c)
        c, peer, j = b.c, b.rig.peer, b.session.journal
        intent = c.requests.intent()
        source = j.get(managed_schedule.KEY)
        assert source.get("other_local") == local and source.get("bound_local") is None
        assert c.native.last_verified_at > 0 and b.device.current == selected
        peer.mode = mode
        if mode == "valid-other-source":
            peer.plan_session = (local, 987654)
        task = asyncio.create_task(c.native.read())
        try:
            await asyncio.wait_for(peer.entered.wait(), 3)
            sequence = c.native.read_sequence
            assert not task.done()
            if newer_read:
                peer.plan_session = (local, 9)
                newest = await c.native.read()
                newest_source = copy.deepcopy(j.get(managed_schedule.KEY))
                verified_at, resolved = c.native.last_verified_at, c.native.last_resolved_read
                assert resolved > sequence and verified_at == newest.physical.observed_at > 0
                assert newest_source.get("other_local") == local and newest_source.get("bound_local") is None
            if mode == "cancelled":
                task.cancel()
            peer.release.set()
            result = await old_result(task, mode, superseded=newer_read)
            await asyncio.wait_for(peer.finished.wait(), 3)
            witness = {
                "mode": mode,
                "newer_read": newer_read,
                "selected_a": selected,
                "old_sequence": sequence,
                "last_resolved_read": c.native.last_resolved_read,
                "last_verified_at": c.native.last_verified_at,
                "result_type": type(result).__name__,
                "current_source": j.get(managed_schedule.KEY),
                "physical_limit_a": b.device.effective_limit(),
                "physical_current_a": b.device.current,
                "remote_stops": b.stops(),
            }
            (tmp_path / "read-order-witness.json").write_text(json.dumps(witness, indent=2) + "\n")
            assert peer.held_request == {"action": "getConfig"}
            assert peer.sent_outcomes == [mode]
            if newer_read:
                assert c.native.last_verified_at == verified_at, (
                    "obsolete read erased the newer verified native timestamp"
                )
                assert c.native.last_resolved_read == resolved and j.get(managed_schedule.KEY) == newest_source
                assert not c.schedule.needs_source(j.get(managed_schedule.KEY))
            elif mode == "valid-other-source":
                assert result.session_plan_id == 987654 and c.native.last_verified_at > 0
                assert j.get(managed_schedule.KEY).get("other_local") is None
            else:
                assert c.native.last_verified_at == 0, "a genuine current failure must invalidate earlier freshness"
                assert c.schedule.needs_source(j.get(managed_schedule.KEY))
            assert c.requests.intent() == intent and j.active() == local and not b.stops()
            peer.unavailable = not newer_read
            b.clock.now = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
            await c.schedule.protect_deadline(j.get(managed_schedule.KEY))
            assert not b.stops() and b.device.effective_limit() == (selected if newer_read else 0)
            await later_daily(b, local, selected)
        finally:
            peer.release.set()
            await asyncio.gather(task, return_exceptions=True)
            peer.unavailable = False


# Intent: a genuine current read's real SQL authority failure still invalidates its earlier positive source proof.
# Why: preserving obsolete completion must not suppress a new unknown outcome or authorize cutoff continuation.
@pytest.mark.parametrize("selected", [6, 32])
async def test_current_native_authority_sql_refusal_removes_freshness(
    tmp_path, tls, unused_tcp_port, monkeypatch, selected
):
    monkeypatch.setattr(schedule_bench, "SchedulePeer", OrderedNativePeer)
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        if selected != 6:
            b.c.command(uuid.uuid4().hex, "set_current", b.c.requests.intent().revision, current_a=selected)
            await b.c.current.settle()
            await healthy(b.c)
        await b.arm()
        b.clock.now = b.start
        local = await b.start_plan(9, demand=True)
        await healthy(b.c)
        c, peer, j = b.c, b.rig.peer, b.session.journal
        source, intent = copy.deepcopy(j.get(managed_schedule.KEY)), c.requests.intent()
        assert source.get("other_local") == local and c.native.last_verified_at > 0
        attempts = []

        # Deny only the first actual authority SELECT after the already-started full native HTTP observation.
        def deny_authority(action, first, second, database, trigger):
            if action == sqlite3.SQLITE_READ and first == "trial_resources" and not attempts:
                attempts.append((first, second))
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        peer.mode = "valid-other-source"
        task = asyncio.create_task(c.native.read())
        try:
            await asyncio.wait_for(peer.entered.wait(), 3)
            j.db.set_authorizer(deny_authority)
            peer.release.set()
            try:
                with pytest.raises(sqlite3.DatabaseError):
                    await task
            finally:
                j.db.set_authorizer(None)
            assert attempts and not j.db.in_transaction
            assert c.native.last_verified_at == 0 and c.schedule.needs_source(j.get(managed_schedule.KEY))
            assert j.get(managed_schedule.KEY) == source and c.requests.intent() == intent
            assert j.active() == local and not b.stops()
            peer.unavailable = True
            b.clock.now = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
            await c.schedule.protect_deadline(j.get(managed_schedule.KEY))
            assert b.device.effective_limit() == 0 and not b.stops()
            await later_daily(b, local, selected)
        finally:
            j.db.set_authorizer(None)
            peer.release.set()
            peer.unavailable = False
            await asyncio.gather(task, return_exceptions=True)
