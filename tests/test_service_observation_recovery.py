"""Production service polls recover optional-storage, Pause and cancelled-Start failures without helper ticks."""

import asyncio
import json
import sqlite3
import threading
import time
import uuid

import pytest
from ocpp_hub_core.prototype import managed_view
from ocpp_hub_core.prototype.local_control import LocalControl
from ocpp_hub_core.prototype.ordinary_recovery import SOURCE

from tests.charge_rig import positive_flow
from tests.managed_rig import starts
from tests.management_peer import call
from tests.runtime_rig import until
from tests.service_recovery_helpers import running_service
from tests.test_worker_storage_recovery import failing_cleanup
from tests.tls_support import tls as tls
from tools import managed_proxy


# Intent: a failing status projection leaves the running command supervisor and charge untouched.
# Why: rendering follows supervision and may access reporting-only journal data.
async def test_failed_poll_projection_keeps_observing_charge(tmp_path, tls, unused_tcp_port_factory, monkeypatch):
    async with running_service(tmp_path, tls, unused_tcp_port_factory, monkeypatch, 6) as service:
        b = service.charge
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        before = await service.published(state="healthy", power=True)
        original = managed_view.observed_status

        # Fail only the disposable view. The actual service poll, native evidence and wire paths run unchanged.
        def unavailable(*args):
            raise sqlite3.OperationalError("simulated reporting read failure")

        with monkeypatch.context() as patch:
            patch.setattr(managed_view, "observed_status", unavailable)
            observed = await service.published(before["checked_at"], state="degraded")
            assert observed["fault"] == "status_unavailable"
            assert observed["owner_control_enabled"] is False
            live = await call(service.api, "status")
            assert live["ok"] and live["result"]["fault"] == "status_unavailable"
            assert live["result"]["owner_control_enabled"] is False
            assert b.session.journal.active() == local
            assert b.rig.bench.firmware.current == 6 and not b.rig.bench.remote_stop.is_set()
        assert managed_view.observed_status is original
        await service.published(observed["checked_at"], state="healthy", power=True)


# Intent: a failed source-record write keeps an existing verified charge running.
# Why: failing to renew observation evidence must preserve both the earlier proof and the physical session.
async def test_failed_source_publication_keeps_existing_charge(tmp_path, tls, unused_tcp_port_factory, monkeypatch):
    async with running_service(tmp_path, tls, unused_tcp_port_factory, monkeypatch, 6) as service:
        b = service.charge
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        before = await service.published(state="healthy", power=True)
        journal = b.session.journal
        saved = journal.get(SOURCE)
        journal.db.execute(
            f"CREATE TEMP TRIGGER block_source BEFORE INSERT ON meta WHEN NEW.key='{SOURCE}' "
            "BEGIN SELECT RAISE(ABORT, 'simulated source write failure'); END"
        )
        try:
            degraded = await service.published(before["checked_at"], state="degraded")
            assert degraded["fault"] == "native_observation_unavailable"
            assert journal.get(SOURCE) == saved
            assert journal.active() == local and b.rig.bench.firmware.current == 6
            assert not b.rig.bench.remote_stop.is_set() and not service.task.done() and not journal.db.in_transaction
        finally:
            if not service.task.done():
                journal.db.execute("DROP TRIGGER block_source")
        await service.published(degraded["checked_at"], state="healthy", power=True)


# Intent: real SQLite refusal while counting queued reports does not stop a healthy service.
# Why: the count describes an observation and carries no charger authority.
@pytest.mark.parametrize("charging", [False, True], ids=["idle", "daily"])
async def test_reporting_count_refusal_keeps_service(tmp_path, tls, unused_tcp_port_factory, monkeypatch, charging):
    async with running_service(tmp_path, tls, unused_tcp_port_factory, monkeypatch, 6) as service:
        b = service.charge
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex) if charging else None
        before = await service.published(state="healthy", power=charging)
        db = b.session.journal.db
        refusals = []

        # Deny the aggregate while the idle reporting pass runs. Ordinary lifecycle SQL remains available.
        def authorize(action, first, second, database, trigger):
            if action == sqlite3.SQLITE_FUNCTION and second == "count":
                refusals.append(second)
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        db.set_authorizer(authorize)
        try:
            observed = await service.published(before["checked_at"], state="healthy", power=charging)
            assert refusals and observed["pending_lifecycle_events"] is None
            assert observed["diagnostics"]["available"] is False
            assert db.in_transaction is False and not service.task.done()
            live = await call(service.api, "status")
            assert live["ok"] and live["result"]["healthy"] and live["result"]["can_pause"]
            assert not live["result"]["can_start"]
            assert b.session.journal.active() == local and not b.rig.bench.remote_stop.is_set()
        finally:
            if not service.task.done():
                db.set_authorizer(None)
        recovered = await service.published(observed["checked_at"], state="healthy", power=charging)
        assert recovered["pending_lifecycle_events"] == 0 and recovered["diagnostics"]["available"] is True


# Intent: a failed Start authorization status read leaves an active scheduled charge healthy.
# Why: the dashboard's explanation must not turn an optional cache read into a service shutdown and Stop.
async def test_start_authorization_status_read_failure_keeps_service(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch
):
    async with running_service(tmp_path, tls, unused_tcp_port_factory, monkeypatch, 6) as service:
        b = service.charge
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        before = await service.published(state="healthy", power=True)
        assert before["start_authorization"] == "ready"
        db = b.session.journal.db
        cache_refused = False

        # Refuse only reads of the authorization cache. The admitted transaction and its evidence remain available.
        def authorize(action, table, column, database, trigger):
            nonlocal cache_refused
            if action == sqlite3.SQLITE_READ and (table, column) == ("auth_info", "info"):
                cache_refused = True
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        db.set_authorizer(authorize)
        try:
            observed = await service.published(before["checked_at"], state="healthy", power=True)
            assert cache_refused and observed["start_authorization"] is None
            live = await call(service.api, "status")
            assert live["ok"] and live["result"]["healthy"] and live["result"]["can_pause"]
            assert live["result"]["start_authorization"] is None and not live["result"]["can_start"]
            assert b.session.journal.active() == local and not b.rig.bench.remote_stop.is_set()
        finally:
            if not service.task.done():
                db.set_authorizer(None)
        recovered = await service.published(observed["checked_at"], state="healthy", power=True)
        assert recovered["start_authorization"] == "ready"


# Intent: a held status-file write cannot delay an actual Pause or its verified result.
# Why: disk output and charger supervision used to execute on the same thread.
async def test_blocked_status_write_does_not_delay_pause(tmp_path, tls, unused_tcp_port_factory, monkeypatch):
    loop = asyncio.get_running_loop()
    held, release = asyncio.Event(), threading.Event()
    delay = threading.Event()
    save = managed_proxy.save_private

    # Block only the real reporting sink. Never replace a charger or engine operation.
    def slow_status(path, status):
        if path.name.endswith(".status.json") and delay.is_set():
            loop.call_soon_threadsafe(held.set)
            assert release.wait(18)
        save(path, status)

    monkeypatch.setattr(managed_proxy, "save_private", slow_status)
    async with running_service(tmp_path, tls, unused_tcp_port_factory, monkeypatch, 6) as service:
        b = service.charge
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        await service.published(state="healthy", power=True)
        delay.set()
        try:
            await asyncio.wait_for(held.wait(), 8)
            request = uuid.uuid4().hex
            revision = b.controller.requests.intent().revision
            assert (await call(service.api, "pause", id=request, revision=revision))["ok"]
            await asyncio.wait_for(b.rig.bench.remote_stop.wait(), 3)
            await service.close_daily(local)
            await until(lambda: b.controller.requests.receipt(request)["state"] == "completed", 8)
            assert b.rig.bench.firmware.effective_limit() == 0
            row = b.session.journal.db.execute("SELECT result FROM managed_requests WHERE id=?", (request,)).fetchone()
            evidence = json.loads(row["result"])
            assert evidence["_pause"]["local"] == evidence["_stop"]["local"] == local
            assert evidence["_stop"]["boot"] == b.session.gateway.boot_observation
            assert "_stop" not in b.controller.requests.receipt(request)["result"]
        finally:
            release.set()
        assert (await service.published(state="healthy", desired="paused"))["local_hold_verified"]


# Intent: a failed optional diagnostic never exits serve or stops its independently known daily session.
# Why: direct controller tests omit the shutdown path that used to convert a note failure into real Stop.
@pytest.mark.parametrize("current", [6, 32])
async def test_actual_service_recovers_optional_observation_storage(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, current
):
    async with running_service(tmp_path, tls, unused_tcp_port_factory, monkeypatch, current) as service:
        b = service.charge
        c, j, rig = b.controller, b.session.journal, b.rig
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        before = await service.published(state="healthy", power=True)
        intent = c.requests.intent()
        reached, attempts = failing_cleanup(j, "managed_native_failure", minimum_attempts=1)
        try:
            rig.peer.unavailable = True
            await asyncio.wait_for(reached.wait(), 8)
            await until(lambda: c.native_unavailable >= 3, 17)
            await until(lambda: rig.bench.firmware.effective_limit() == 0)
            assert len(attempts) >= 3 and not service.task.done()
            assert not rig.bench.remote_stop.is_set() and j.active() == local
            rig.peer.unavailable = False
            recovered = await service.published(before["checked_at"], state="healthy", power=True)
            assert recovered["station_ceiling_a"] == current and c.requests.intent() == intent
            assert j.active() == local and not starts(rig) and not rig.bench.remote_stop.is_set()
            assert not j.get("managed_control_fault")
        finally:
            rig.peer.unavailable = False
            j.db.execute("DROP TRIGGER IF EXISTS review_cleanup_unavailable")


# Intent: the actual faulted service retries the same explicit Pause after a native503 and waits for genuine closure.
# Why: a second Pause or manual tick would hide a stranded original request and an exited poll loop.
async def test_actual_service_completes_original_faulted_pause(tmp_path, tls, unused_tcp_port_factory, monkeypatch):
    async with running_service(tmp_path, tls, unused_tcp_port_factory, monkeypatch, 6) as service:
        b = service.charge
        c, j, rig = b.controller, b.session.journal, b.rig
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        await service.published(state="healthy", power=True)
        j.put(
            "managed_native_failure",
            {
                "type": "NativeTransportUnavailableError",
                "detail": "synthetic legacy availability fault",
                "at_epoch": time.time(),
            },
        )
        c.fault("native_authority_or_preservation_failed")
        await until(lambda: rig.bench.firmware.effective_limit() == 0, 8)
        rig.peer.unavailable = True
        request, revision = uuid.uuid4().hex, c.requests.intent().revision
        result = await call(service.api, "pause", id=request, revision=revision)
        assert result["ok"]
        await asyncio.wait_for(c.pause_task, 3)
        assert rig.peer.failed_reads.qsize() and not rig.bench.remote_stop.is_set()
        rig.peer.unavailable = False
        await asyncio.wait_for(rig.bench.remote_stop.wait(), 8)
        await service.published(state="attention")
        stops = [f for f in rig.bench.device_calls if f[2] == "RemoteStopTransaction"]
        assert len(stops) == 1 and stops[0][3] == {"transactionId": local}
        assert c.requests.receipt(request)["state"] == "accepted" and rig.bench.firmware.current == 0
        await service.close_daily(local)
        after = await service.published(state="healthy", desired="paused")
        assert after["local_hold_verified"] and c.requests.receipt(request)["state"] == "completed"
        assert not starts(rig)


# Intent: storage refusal during both the first Stop and the service's retry leaves the original Pause active.
# Why: a foreground retry must keep supervision running until the same guarded Stop can be sent.
async def test_actual_service_retries_pause_when_stop_write_fails(tmp_path, tls, unused_tcp_port_factory, monkeypatch):
    async with running_service(tmp_path, tls, unused_tcp_port_factory, monkeypatch, 6) as service:
        b = service.charge
        c, j, rig = b.controller, b.session.journal, b.rig
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        await service.published(state="healthy", power=True)
        reached, attempts = failing_cleanup(j, "managed_owner_stop")
        request, revision = uuid.uuid4().hex, c.requests.intent().revision
        try:
            assert (await call(service.api, "pause", id=request, revision=revision))["ok"]
            claim = j.get("managed_owner_pause")
            await asyncio.wait_for(reached.wait(), 15)
            assert len(attempts) >= 2 and not service.task.done()
            assert j.get("managed_owner_pause") == claim and c.requests.receipt(request)["state"] == "accepted"
            assert rig.bench.firmware.effective_limit() == 0 and not rig.bench.remote_stop.is_set()
        finally:
            if not service.task.done():
                j.db.execute("DROP TRIGGER review_cleanup_unavailable")
        await asyncio.wait_for(rig.bench.remote_stop.wait(), 10)
        assert c.requests.receipt(request)["state"] == "accepted"
        await service.close_daily(local)
        paused = await service.published(state="healthy", desired="paused")
        assert paused["local_hold_verified"] and j.get("managed_owner_pause") == claim
        assert c.requests.receipt(request)["state"] == "completed"
        assert [frame[3] for frame in rig.bench.device_calls if frame[2] == "RemoteStopTransaction"] == [
            {"transactionId": local}
        ]


# Intent: cancelling an unprepared Start after initial zero restores original app authority through serve alone.
# Why: an explicit Pause before the first poll used to conceal this cancellation's permanent zero/fault state.
@pytest.mark.parametrize("current", [6, 32])
async def test_actual_service_recovers_cancelled_start_initial_zero(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, current
):
    async with running_service(tmp_path, tls, unused_tcp_port_factory, monkeypatch, current) as service:
        b = service.charge
        c, s, rig = b.controller, b.session, b.rig
        await service.published(state="healthy", desired="app")
        intent = c.requests.intent()
        loop, factory, victims = asyncio.get_running_loop(), asyncio.get_running_loop().get_task_factory(), []
        captured = asyncio.Event()

        # Cancel only the actual Start child before its first instruction, after the real outer zero sequence.
        def cancel_start_child(loop, coroutine, **kwargs):
            task = asyncio.Task(coroutine, loop=loop, eager_start=False, **kwargs)
            if (
                getattr(coroutine, "cr_code", None) is LocalControl._start_owned.__code__
                and coroutine.cr_frame.f_locals.get("self") is s.gateway.controls
            ):
                task.cancel()
                victims.append(task)
                captured.set()
            return task

        request = uuid.uuid4().hex
        try:
            loop.set_task_factory(cancel_start_child)
            response = await call(service.api, "start", id=request, revision=intent.revision)
            assert response["ok"]
            await asyncio.wait_for(captured.wait(), 3)
            await asyncio.wait_for(asyncio.gather(c.charging.worker, *victims, return_exceptions=True), 3)
        finally:
            loop.set_task_factory(factory)
        assert c.charging.worker.cancelled() and len(victims) == 1
        assert rig.bench.firmware.effective_limit() == 0 and not starts(rig)
        boundary, zero_mark = time.time(), len(rig.bench.received_limits)
        recovered = await service.published(boundary, state="healthy", desired="app")
        assert c.requests.intent() == intent and recovered["station_ceiling_a"] == current
        assert rig.bench.firmware.effective_limit() == current
        assert current in rig.bench.received_limits[zero_mark:]
        assert c.requests.receipt(request)["state"] == "failed" and c.charging.lease is None
        assert not s.journal.get("managed_control_fault") and not rig.bench.remote_stop.is_set()
        assert s.journal.get("local_start_receipt") is None and not starts(rig)
