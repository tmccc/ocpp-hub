"""Known-unsent, actually issued and cancelled Start work keep distinct outcomes through real peer recovery."""

import asyncio
import uuid

import pytest
from ocpp_hub_core.prototype.local_control import LocalControl

from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import two_later_sessions
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_normal_start import owner_rig
from tests.test_managed_proxy import healthy, starts
from tests.test_managed_test_charge import actual_stop, positive_flow
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests.test_worker_storage_recovery import failing_cleanup


# Intent: actual HTTP failure before effects cannot acquire Pause/Stop authority through a diagnostic outage.
# Why: keeping a dead worker's optional diagnostic alive must not turn a known-unsent request into a timeout hold.
async def test_start_preflight_failure_settles_before_optional_diagnostic(tmp_path, tls, unused_tcp_port, monkeypatch):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch, startup=1) as b:
        c, j, peer = b.controller, b.session.journal, b.rig.peer
        await healthy(c)
        intent, control, wires = c.requests.intent(), j.get("control"), list(b.rig.bench.device_calls)
        reached, attempts = failing_cleanup(j, "managed_test_failure", minimum_attempts=1)
        peer.unavailable = True
        request = uuid.uuid4().hex
        c.command(request, "start", intent.revision)
        try:
            await asyncio.wait_for(reached.wait(), 3)
            await asyncio.wait_for(c.tests.settle(), 3)
            assert attempts and peer.failed_reads.qsize() >= 1
            assert c.requests.intent() == intent and j.get("control") == control
            assert b.rig.bench.device_calls == wires and starts(b.rig) == []
            assert j.get("local_start_receipt") is None and c.tests.lease is None
            receipt = c.requests.receipt(request)
            assert receipt["state"] == "failed" and receipt["result"]["classification"] == "preflight_refused_unsent"
            cause = c.tests.state()["failure"]
            assert (cause["id"], cause["action"], cause["revision"], cause["effects_started"]) == (
                request,
                "start",
                intent.revision,
                False,
            )
            peer.unavailable = False
            assert (await healthy(c))["station_ceiling_a"] == 6
            assert j.db.execute("SELECT 1 FROM sqlite_temp_master WHERE name='review_cleanup_unavailable'").fetchone()
            j.db.execute("DROP TRIGGER review_cleanup_unavailable")
            await two_later_sessions(b, selected=6, original_starts=0)
        finally:
            peer.unavailable = False
            j.db.execute("DROP TRIGGER IF EXISTS review_cleanup_unavailable")
            if c.tests.worker is not None:
                await asyncio.wait_for(asyncio.gather(c.tests.worker, return_exceptions=True), 3)


# Intent: the same service reconciles a lost unsent task, while cancellation after real dispatch retains unknown debt.
# Why: cancellation supplies no command outcome; exact unsent markers or genuine Start/Stop and ACKs must settle it.
@pytest.mark.parametrize("issued", [False, True], ids=["before-task-execution", "after-real-Start-wire"])
async def test_cancelled_start_worker_keeps_actual_dispatch_outcome(
    tmp_path, tls, unused_tcp_port, monkeypatch, issued
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch, startup=1) as b:
        c, s, rig = b.controller, b.session, b.rig
        loop, captured, wire_entered, release = (
            asyncio.get_running_loop(),
            asyncio.Event(),
            asyncio.Event(),
            asyncio.Event(),
        )
        factory, victims = loop.get_task_factory(), []

        # Capture the real owned LocalControl coroutine and cancel it only at this test's declared boundary.
        def observe_task(loop, coroutine, **kwargs):
            task = asyncio.Task(coroutine, loop=loop, eager_start=False, **kwargs)
            if (
                getattr(coroutine, "cr_code", None) is LocalControl._start.__code__
                and coroutine.cr_frame.f_locals.get("self") is s.gateway.controls
            ):
                victims.append(task)
                if not issued:
                    task.cancel()
                captured.set()
            return task

        # The independent charger applies the actual Start before withholding its real TLS acknowledgement.
        async def hold_reply(action, payload):
            if issued and action == "RemoteStartTransaction":
                wire_entered.set()
                await release.wait()

        rig.bench.before_reply = hold_reply
        loop.set_task_factory(observe_task)
        request = uuid.uuid4().hex
        try:
            c.command(request, "start", c.requests.intent().revision)
            await asyncio.wait_for(captured.wait(), 3)
            if issued:
                await asyncio.wait_for(wire_entered.wait(), 3)
                assert len(starts(rig)) == 1 and s.journal.get("local_start_receipt")["stage"] == "issued"
                victims[0].cancel()
            await asyncio.wait_for(asyncio.gather(c.tests.worker, *victims, return_exceptions=True), 3)
            assert c.tests.worker.cancelled() and len(victims) == 1 and victims[0].cancelled()
        finally:
            loop.set_task_factory(factory)
            release.set()
            rig.bench.before_reply = None
        assert c.tests.state()["start_attempted"] is issued
        assert c.tests.state()["start_rpc_accepted"] is False
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        await asyncio.wait_for(c.tick(), 3)
        if c.pause_task is not None:
            await asyncio.wait_for(asyncio.shield(c.pause_task), 3)
        assert c.requests.intent().mode == "paused" and rig.bench.firmware.effective_limit() == 0
        if issued:
            for _ in range(2):
                status = await c.tick()
                assert not status.get("local_hold_verified")
            assert s.journal.get("local_start_receipt")["stage"] == "issued"
            assert s.journal.get("control")["start_pending"] is True
            assert c.requests.receipt(request)["state"] in ("accepted", "running", "uncertain")
            assert len(starts(rig)) == 1
            await until(lambda: c.requests.receipt(request)["state"] == "uncertain")
            assert c.tests.timer.done()
            local = await positive_flow(b)
            assert rig.bench.firmware.current == 0
            for _ in range(3):
                await c.tick()
            await actual_stop(b, local)
        else:
            assert s.journal.get("local_start_receipt") is None and not starts(rig)
            assert (await healthy(c))["local_hold_verified"]
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        terminal = c.requests.receipt(request)
        assert terminal["state"] == ("completed" if issued else "failed")
        if issued:
            assert terminal["result"] == {"local_session_verified": True, "charging": False, "timed": False}
            assert s.journal.get("local_start_receipt")["stage"] == "issued"
        c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
        assert (await healthy(c))["station_ceiling_a"] == 6
        await two_later_sessions(b, selected=6, original_starts=int(issued))
        assert c.requests.receipt(request) == terminal
        assert s.journal.next_delivery() is None and not s.journal.get("managed_control_fault")
