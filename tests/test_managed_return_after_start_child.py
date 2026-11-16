"""Return between owned Start completion and app-intent publication retains zero and the original lifecycle."""

import asyncio
import copy
import sys
import uuid

import pytest
from ocpp_hub_core.prototype.local_control import LocalControl

from tests import test_native_bounded_session as native_bench
from tests.recovery_barriers import completed_control
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_normal_start import owner_rig
from tests.test_managed_proxy import healthy, starts
from tests.test_managed_test_charge import actual_stop, positive_flow
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls

pytestmark = pytest.mark.asyncio


# Intent: Return after the real owned Start commits still restores zero if its outer worker loses permission.
# Why: shielding the child leaves a genuine management-input window before the worker publishes app authority.
async def test_return_after_start_child_commit_restores_zero_without_native_http(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch, startup=10) as b:
        c, g, j, rig = b.controller, b.session.gateway, b.session.journal, b.rig
        assert isinstance(rig.peer, InterruptedNativePeer)
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        intent = c.requests.intent()
        owner_pause = copy.deepcopy(j.get("managed_owner_pause"))
        plans, configuration = copy.deepcopy(rig.bench.firmware.plans), copy.deepcopy(rig.peer.configuration)
        scope = (g._charger_epoch, g.boot_observation, g.boot_metadata_revision, g.controls._revision)
        identifier, returning = uuid.uuid4().hex, uuid.uuid4().hex
        scheduled, callback_errors, witness = [], [], {}
        injected = asyncio.Event()
        loop = asyncio.get_running_loop()

        # Submit the ordinary owner request only after the observed SQL has actually executed and the child has exited.
        def after_child_commit():
            j.db.set_trace_callback(None)
            try:
                child = scheduled[0]["child"]
                witness.update(
                    child_done=child.done(),
                    child_cancelled=child.cancelled(),
                    child_error=repr(child.exception()) if child.done() and not child.cancelled() else None,
                    worker_pending=c.tests.worker is not None and not c.tests.worker.done(),
                    control=copy.deepcopy(j.get("control")),
                    receipt=copy.deepcopy(j.get("local_start_receipt")),
                    intent=c.requests.intent(),
                    owner_pause=copy.deepcopy(j.get("managed_owner_pause")),
                    in_transaction=j.db.in_transaction,
                    actual_limit=rig.bench.firmware.effective_limit(),
                    wire=len(rig.bench.device_calls),
                )
                witness["return"] = c.command(returning, "app_control", intent.revision)
                witness["pending_return"] = c.requests.pending_return()
                rig.peer.unavailable = True
            except Exception as error:
                callback_errors.append(repr(error))  # Assert callback failures in the test, not a loop log.
            finally:
                injected.set()

        # Observe only the real final shared-control INSERT; never execute SQL, assertions or owner requests in trace.
        def trace(statement):
            if scheduled or not statement.startswith("INSERT OR REPLACE INTO meta "):
                return
            frame = sys._getframe(1)
            completion, starting = {}, {}
            try:
                while frame is not None:
                    if frame.f_code is LocalControl._complete.__code__:
                        completion = dict(frame.f_locals)
                    elif frame.f_code is LocalControl._start.__code__:
                        starting = dict(frame.f_locals)
                    frame = frame.f_back
            finally:
                del frame
            changes = completion.get("changes", {})
            if (
                completion.get("self") is g.controls
                and changes == {"uncertain": False, "owner": "shared"}
                and starting.get("self") is g.controls
                and starting.get("request_id") == identifier
                and starting.get("shared") is True
                and completion.get("ticket") == starting.get("ticket")
            ):
                scheduled.append({"child": asyncio.current_task(), "ticket": starting["ticket"]})
                loop.call_soon(after_child_commit)

        j.db.set_trace_callback(trace)
        try:
            assert c.command(identifier, "start", intent.revision)["state"] == "accepted"
            await asyncio.wait_for(injected.wait(), 5)
            assert not callback_errors and len(scheduled) == 1, callback_errors
            assert witness["child_done"] and not witness["child_cancelled"] and witness["child_error"] == "None"
            assert witness["worker_pending"] and not witness["in_transaction"]
            receipt, control = witness["receipt"], witness["control"]
            assert receipt["request_id"] == identifier and receipt["stage"] == "accepted"
            assert receipt["local"] is None and receipt["ticket"] == scheduled[0]["ticket"]
            assert control["ticket"] == receipt["ticket"] and control["owner"] == "shared"
            assert control["inhibit"] is False and control["uncertain"] is False and control["start_pending"] is True
            assert witness["actual_limit"] == 6 and witness["intent"] == intent and intent.mode == "paused"
            assert witness["owner_pause"] == owner_pause
            assert witness["return"]["id"] == returning and witness["return"]["state"] == "accepted"
            assert witness["pending_return"] == (returning, intent.revision)
            assert len(starts(rig)) == 1 and j.active() is None
            await asyncio.wait_for(c.tests.settle(), 4)
            assert c.tests.state()["state"] == "stopping" and c.tests.state()["failure"]["type"] == "DeniedError"
            assert c.requests.receipt(identifier)["state"] == "uncertain" and c.pause_task is not None
            await asyncio.wait_for(asyncio.shield(c.pause_task), 3)
            failed = await asyncio.wait_for(rig.peer.failed_reads.get(), 2)
            assert failed["action"] == "getConfig" and rig.peer.unavailable
            assert (g._charger_epoch, g.boot_observation, g.boot_metadata_revision, g.controls._revision) == scope
            assert c.requests.intent() == intent and j.get("managed_owner_pause") == owner_pause
            assert j.get("local_start_receipt") == receipt and j.get("control")["start_pending"] is True
            assert c.requests.receipt(returning)["state"] != "completed"
            assert not any(frame[2] == "RemoteStopTransaction" for frame in rig.bench.device_calls)
            zeros = [
                frame
                for frame in rig.bench.device_calls[witness["wire"] :]
                if frame[2] == "SetChargingProfile"
                and frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
            ]
            assert zeros, {
                "witness": witness,
                "control": j.get("control"),
                "limit": rig.bench.firmware.effective_limit(),
            }
            assert rig.bench.firmware.effective_limit() == 0
            current = j.get("control")
            assert current["owner"] == "local" and current["inhibit"] is True and current["uncertain"] is True
            assert current["start_pending"] is True and current["stop_epoch"] == control["stop_epoch"]
            assert j.get("local_start_receipt") == receipt and c.requests.intent() == intent
            assert j.get("managed_owner_pause") == owner_pause
            assert rig.bench.firmware.plans == plans and rig.peer.configuration == configuration
            assert len(starts(rig)) == 1 and j.active() is None
            rig.peer.unavailable = False
            await c.tick()
            assert c.requests.intent() == intent and c.requests.receipt(returning)["state"] == "accepted"
            assert c.requests.receipt(identifier)["state"] == "uncertain"
            assert j.get("local_start_receipt") == receipt and j.get("control")["start_pending"]
            assert rig.bench.firmware.effective_limit() == 0

            # The original accepted command reports its own real lifecycle later, while station zero still holds.
            local = await positive_flow(b, call_id=uuid.uuid4().hex)
            bound = j.get("local_start_receipt")
            assert bound["request_id"] == identifier and bound["ticket"] == receipt["ticket"]
            assert bound["local"] == local and bound["stage"] == "accepted"
            assert not j.get("control")["start_pending"] and j.active() == local
            assert rig.bench.firmware.effective_limit() == 0 and c.requests.intent() == intent
            assert c.requests.receipt(returning)["state"] == "accepted"
            assert c.requests.receipt(identifier)["state"] == "uncertain" and len(starts(rig)) == 1
            await c.tick()
            await until(lambda: any(frame[2] == "RemoteStopTransaction" for frame in rig.bench.device_calls))
            assert [
                frame[3]["transactionId"] for frame in rig.bench.device_calls if frame[2] == "RemoteStopTransaction"
            ] == [local]
            assert j.active() == local and rig.bench.firmware.effective_limit() == 0
            assert c.requests.receipt(returning)["state"] == "accepted"
            await actual_stop(b, local)
            status = await completed_control(b, returning, mode="app", revision=intent.revision + 1, ceiling=6)
            assert status["owner_control_enabled"] and j.active() is None
            assert c.requests.receipt(identifier)["state"] == "completed"
            assert c.requests.receipt(identifier)["result"] == {
                "local_session_verified": True,
                "charging": False,
                "timed": False,
            }
            assert j.get("local_start_receipt")["request_id"] == identifier
            assert j.get("local_start_receipt")["local"] == local
            assert c.requests.receipt(returning)["state"] == "completed" and len(starts(rig)) == 1
            assert [
                frame[3]["transactionId"] for frame in rig.bench.device_calls if frame[2] == "RemoteStopTransaction"
            ] == [local]
            assert rig.bench.firmware.plans == plans and rig.peer.configuration == configuration
        finally:
            j.db.set_trace_callback(None)
            rig.peer.unavailable = False
            if c.tests.worker is not None:
                await asyncio.gather(c.tests.worker, return_exceptions=True)
            if c.pause_task is not None:
                await asyncio.gather(c.pause_task, return_exceptions=True)
