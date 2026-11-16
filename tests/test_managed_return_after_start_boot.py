"""Actual ACK/identical-Boot burst probes current Pause protection after old Start child completion."""

import asyncio
import copy
import json
import sqlite3
import sys
import uuid

import pytest
from ocpp_hub_core.frames import build_call
from ocpp_hub_core.prototype.connection_scope import current_scope
from ocpp_hub_core.prototype.local_control import LocalControl

from tests import test_native_bounded_session as native_bench
from tests.recovery_barriers import completed_control
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_normal_start import owner_rig
from tests.test_managed_proxy import healthy, starts
from tests.test_managed_test_charge import actual_stop, positive_flow
from tests.test_prototype import BOOT
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls

pytestmark = pytest.mark.asyncio


# Intent: current Pause protects a real ACK/Boot/Return burst without borrowing the completed old Start scope.
# Why: identical Boot may run after the child commits shared control but before its parent publishes app intent.
@pytest.mark.parametrize("storage_failure", ["none", "statement", "commit"])
async def test_current_pause_protects_post_child_matching_boot_return(
    tmp_path, tls, unused_tcp_port, monkeypatch, storage_failure
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
        original_send = b.charger.send
        boot_reply, boot_id = loop.create_future(), uuid.uuid4().hex
        burst, zero_writers = {}, []
        sql_attempts = []
        fault_installed = False

        # Refuse only the new current-Pause claim after the original owned Start and Return acceptance are real.
        def install_reclaim_refusal(control):
            nonlocal fault_installed
            old_ticket = control["ticket"].replace("'", "''")
            if storage_failure == "commit":
                j.db.execute("CREATE TABLE uq11_reclaim_parent(id INTEGER PRIMARY KEY)")
                j.db.execute(
                    "CREATE TABLE uq11_reclaim_child("
                    "id REFERENCES uq11_reclaim_parent(id) DEFERRABLE INITIALLY DEFERRED)"
                )
                effect = "INSERT INTO uq11_reclaim_child VALUES(123);"
            else:
                effect = "SELECT RAISE(ABORT,'independent current Pause reclaim storage unavailable');"

            # Observe SQLite's actual candidate row and caller, without executing SQL or engine work in the callback.
            def observed(value):
                frame = sys._getframe(1)
                reclaim = False
                try:
                    while frame is not None:
                        if (
                            frame.f_code is type(c).reclaim_failed_start_pause.__code__
                            and frame.f_locals.get("self") is c
                        ):
                            reclaim = True
                        frame = frame.f_back
                finally:
                    del frame
                sql_attempts.append(
                    {
                        "control": json.loads(value),
                        "actual_reclaim": reclaim,
                        "in_transaction": j.db.in_transaction,
                        "scope": (g._charger_epoch, g.boot_observation, g.boot_metadata_revision),
                    }
                )
                return 0

            j.db.create_function("uq11_observe_reclaim", 1, observed)
            j.db.execute(
                "CREATE TEMP TRIGGER uq11_reclaim_refusal BEFORE INSERT ON meta "
                "WHEN NEW.key='control' AND json_extract(NEW.value,'$.owner')='local' "
                "AND json_extract(NEW.value,'$.inhibit')=1 "
                "AND json_extract(NEW.value,'$.uncertain')=1 "
                "AND json_extract(NEW.value,'$.ticket')<>'" + old_ticket + "' "
                "AND json_extract(NEW.value,'$.stop_epoch')=" + str(control["stop_epoch"]) + " "
                "AND json_extract((SELECT value FROM meta WHERE key='control'),'$.ticket')='" + old_ticket + "' "
                "AND json_extract((SELECT value FROM meta WHERE key='control'),'$.owner')='shared' "
                "AND json_extract((SELECT value FROM meta WHERE key='control'),'$.inhibit')=0 "
                "BEGIN SELECT uq11_observe_reclaim(NEW.value); " + effect + " END"
            )
            fault_installed = True

        # Remove the external storage refusal while leaving all actual engine state and receipt/debt untouched.
        def clear_reclaim_refusal():
            nonlocal fault_installed
            if fault_installed:
                j.db.execute("DROP TRIGGER uq11_reclaim_refusal")
                fault_installed = False
            j.db.create_function("uq11_observe_reclaim", 1, None)

        # Send the real Accepted ACK then a genuine adjacent Boot; the ordinary reader resolves its actual reply.
        async def send_reply_then_boot(raw):
            reply = json.loads(raw)
            command = next((item for item in rig.bench.device_calls if item[1] == reply[1]), None)
            if not burst and reply[0] == 3 and command is not None and command[2] == "RemoteStartTransaction":
                assert reply[2]["status"] == "Accepted"
                rig.bench.replies[boot_id] = boot_reply
                burst.update(reply=copy.deepcopy(reply), command=copy.deepcopy(command))
                await original_send(raw)
                await original_send(build_call(boot_id, "BootNotification", BOOT))
                return
            if burst and command is not None and command[2] == "SetChargingProfile":
                limit = command[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit == 0:
                    child = scheduled[0]["child"] if scheduled else None
                    zero_writers.append(
                        {
                            "old_child_done": child is not None and child.done(),
                            "current_pause_pending": c.pause_task is not None and not c.pause_task.done(),
                            "scope": current_scope(g),
                        }
                    )
            await original_send(raw)

        monkeypatch.setattr(b.charger, "send", send_reply_then_boot)

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
                    current_scope=current_scope(g),
                    old_start_scope=scheduled[0]["start_scope"],
                )
                witness["return"] = c.command(returning, "app_control", intent.revision)
                witness["pending_return"] = c.requests.pending_return()
                if storage_failure != "none":
                    install_reclaim_refusal(witness["control"])
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
                lease = c.tests.lease
                scheduled.append(
                    {
                        "child": asyncio.current_task(),
                        "ticket": starting["ticket"],
                        "start_scope": lease.start_scope if lease is not None else None,
                    }
                )
                loop.call_soon(after_child_commit)

        j.db.set_trace_callback(trace)
        try:
            assert c.command(identifier, "start", intent.revision)["state"] == "accepted"
            await asyncio.wait_for(injected.wait(), 5)
            assert not callback_errors and len(scheduled) == 1, callback_errors
            assert witness["child_done"] and not witness["child_cancelled"] and witness["child_error"] == "None"
            assert witness["worker_pending"] and not witness["in_transaction"]
            old_scope, current = witness["old_start_scope"], witness["current_scope"]
            assert old_scope is not None and current is not None
            assert (old_scope.epoch, old_scope.boot, old_scope.metadata_revision) == scope[:3]
            assert current.boot != old_scope.boot and current.lineage == old_scope.lineage
            assert g.controls._revision == scope[3]
            assert burst and (await asyncio.wait_for(asyncio.shield(boot_reply), 3))[2]["status"] == "Accepted"
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
            if storage_failure == "none":
                await asyncio.wait_for(asyncio.shield(c.pause_task), 3)
            else:
                failed_state = copy.deepcopy(c.tests.state())
                with pytest.raises(sqlite3.IntegrityError) as refused:
                    await asyncio.wait_for(asyncio.shield(c.pause_task), 3)
                expected_code = (
                    sqlite3.SQLITE_CONSTRAINT_FOREIGNKEY
                    if storage_failure == "commit"
                    else sqlite3.SQLITE_CONSTRAINT_TRIGGER
                )
                assert refused.value.sqlite_errorcode == expected_code
                assert len(sql_attempts) == 1 and sql_attempts[0]["actual_reclaim"]
                assert sql_attempts[0]["in_transaction"] and not j.db.in_transaction
                staged = sql_attempts[0]["control"]
                assert staged["ticket"] != control["ticket"] and staged["owner"] == "local"
                assert staged["inhibit"] is True and staged["uncertain"] is True
                assert staged["start_pending"] is True and staged["stop_epoch"] == control["stop_epoch"]
                assert sql_attempts[0]["scope"] == (current.epoch, current.boot, current.metadata_revision)
                if storage_failure == "commit":
                    assert j.db.execute("SELECT COUNT(*) FROM uq11_reclaim_child").fetchone()[0] == 0
                else:
                    assert "independent current Pause reclaim storage unavailable" in str(refused.value)
                # Actual SQL refusal must preserve T1, original accepted debt and physical6 before retry.
                assert j.get("control") == control and j.get("local_start_receipt") == receipt
                assert c.tests.state() == failed_state and c.requests.intent() == intent
                assert j.get("managed_owner_pause") == owner_pause and j.get("trial_phase") == "active"
                assert c.requests.receipt(identifier)["state"] == "uncertain"
                assert c.requests.pending_return() == (returning, intent.revision)
                assert c.requests.receipt(returning)["state"] == "accepted"
                assert current_scope(g) == current and g.controls._revision == scope[3]
                assert not g.controls._operations and not g.charger._pending and j.active() is None
                assert not j.get("managed_control_fault") and rig.peer.unavailable
                assert not zero_writers and rig.bench.firmware.effective_limit() == 6
                assert len(starts(rig)) == 1 and not any(
                    frame[2] == "RemoteStopTransaction" for frame in rig.bench.device_calls
                )
                assert rig.bench.firmware.plans == plans and rig.peer.configuration == configuration
                clear_reclaim_refusal()
                # The real current-Pause service retries under B; no helper or authority state is invoked/forged.
                await c.tick()
                assert j.get("control")["ticket"] != control["ticket"]
            failed = await asyncio.wait_for(rig.peer.failed_reads.get(), 2)
            assert failed["action"] == "getConfig" and rig.peer.unavailable
            assert current_scope(g) == current and g.controls._revision == scope[3]
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
            if storage_failure == "none":
                assert zero_writers and all(
                    item["old_child_done"] and item["current_pause_pending"] and item["scope"] == current
                    for item in zero_writers
                ), zero_writers
            else:
                assert zero_writers and all(
                    item["old_child_done"] and not item["current_pause_pending"] and item["scope"] == current
                    for item in zero_writers
                ), zero_writers
            restricted = j.get("control")
            assert restricted["owner"] == "local" and restricted["inhibit"] is True
            assert restricted["uncertain"] is True and restricted["start_pending"] is True
            assert restricted["stop_epoch"] == control["stop_epoch"]
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
            clear_reclaim_refusal()
            monkeypatch.setattr(b.charger, "send", original_send)
            rig.peer.unavailable = False
            if c.tests.worker is not None:
                await asyncio.gather(c.tests.worker, return_exceptions=True)
            if c.pause_task is not None:
                await asyncio.gather(c.pause_task, return_exceptions=True)
            if not boot_reply.done():
                boot_reply.cancel()
            await asyncio.gather(boot_reply, return_exceptions=True)
