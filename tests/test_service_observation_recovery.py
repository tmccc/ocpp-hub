"""Production service polls recover optional-storage, Pause and cancelled-Start failures without helper ticks."""

import asyncio
import time
import uuid

import pytest
from ocpp_hub_core.prototype.local_control import LocalControl

from tests.service_recovery_helpers import running_service
from tests.test_managed_api import call
from tests.test_managed_proxy import starts
from tests.test_managed_test_charge import positive_flow
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests.test_worker_storage_recovery import failing_cleanup


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
