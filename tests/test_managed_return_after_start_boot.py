"""Atomic Start publication and real storage failures retain accepted debt across a newer Boot and owner Pause."""

import asyncio
import copy
import json
import sqlite3
import sys
import time
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


# Intent: atomic Start success or actual handoff refusal retains truthful debt across matching Boot and new Pause.
# Why: scope A cannot authorize scope B; only a new real Pause may own zero while the original accepted Start is late.
@pytest.mark.parametrize("storage_failure", ["none", "statement", "commit"])
async def test_current_pause_protects_post_child_matching_boot_return(
    tmp_path, tls, unused_tcp_port, monkeypatch, storage_failure
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch, startup=20) as b:
        c, g, j, rig = b.controller, b.session.gateway, b.session.journal, b.rig
        assert isinstance(rig.peer, InterruptedNativePeer)
        initial_pause = uuid.uuid4().hex
        c.command(initial_pause, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        initial_intent, released = c.requests.intent(), c.released_epoch
        initial_owner_pause = copy.deepcopy(j.get("managed_owner_pause"))
        plans, configuration = copy.deepcopy(rig.bench.firmware.plans), copy.deepcopy(rig.peer.configuration)
        scope_a, revision = current_scope(g), g.controls._revision
        assert scope_a is not None
        identifier, new_pause, returning = uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        original_send = b.charger.send
        boot_reply, boot_id = loop.create_future(), uuid.uuid4().hex
        burst, observations, scheduled, callback_errors, witness = {}, [], [], [], {}
        wire_replies, zero_writers = [], []
        injected = asyncio.Event()
        installed = True

        # Observe the actual accepted Start publisher and candidate SQL without changing any engine verdict.
        def publication_observed(value):
            frame = sys._getframe(1)
            attempt, start, child = None, {}, None
            try:
                while frame is not None:
                    if frame.f_code is LocalControl._start_owned.__code__ and frame.f_locals.get("self") is g.controls:
                        attempt, child = frame.f_locals.get("attempt"), asyncio.current_task()
                    if frame.f_code is LocalControl._start.__code__ and frame.f_locals.get("self") is g.controls:
                        start = {key: frame.f_locals.get(key) for key in ("request_id", "ticket", "scope")}
                    frame = frame.f_back
                lease = c.charging.lease
                observations.append(
                    {
                        "attempt": attempt,
                        "child": child,
                        "start": start,
                        "candidate": json.loads(value),
                        "transaction": j.db.in_transaction,
                        "intent_inside": c.requests.intent(),
                        "receipt": copy.deepcopy(j.get("local_start_receipt")),
                        "lease": lease,
                        "lease_mode_inside": lease.mode if lease is not None else None,
                        "lease_revision_inside": lease.revision if lease is not None else None,
                        "limit": rig.bench.firmware.effective_limit(),
                    }
                )
            except Exception as error:
                callback_errors.append(repr(error))  # A read-only observer must not replace the actual SQL refusal.
            finally:
                del frame
            return 0

        j.db.create_function("start_boot_publication_observed", 1, publication_observed)
        if storage_failure == "commit":
            j.db.execute("CREATE TABLE start_boot_parent(id INTEGER PRIMARY KEY)")
            j.db.execute(
                "CREATE TABLE start_boot_child(id REFERENCES start_boot_parent(id) DEFERRABLE INITIALLY DEFERRED)"
            )
        effect = (
            "SELECT RAISE(ABORT,'independent final Start publication unavailable');"
            if storage_failure == "statement"
            else "INSERT INTO start_boot_child VALUES(123);"
            if storage_failure == "commit"
            else ""
        )
        j.db.execute(
            "CREATE TEMP TRIGGER start_boot_publication_cut BEFORE INSERT ON meta "
            "WHEN NEW.key='managed_test_charge' AND json_extract(NEW.value,'$.state')='awaiting_power' "
            "BEGIN SELECT start_boot_publication_observed(NEW.value); " + effect + " END"
        )

        # Remove only the external fault producer; original accepted debt and all engine state remain untouched.
        def clear_publication_cut():
            nonlocal installed
            if installed:
                j.db.execute("DROP TRIGGER start_boot_publication_cut")
                installed = False
            j.db.create_function("start_boot_publication_observed", 1, None)

        # Admit a NEW Pause at the observed current revision, then the separate Return under that new paused revision.
        def admit_new_owner_requests():
            witness["pause"] = c.command(new_pause, "pause", witness["intent"].revision)
            witness["paused_intent"] = c.requests.intent()
            witness["paused_control"] = copy.deepcopy(j.get("control"))
            witness["current_owner_pause"] = copy.deepcopy(j.get("managed_owner_pause"))
            witness["return"] = c.command(returning, "app_control", witness["paused_intent"].revision)
            witness["pending_return"] = c.requests.pending_return()
            witness["new_owner_admitted"] = True
            rig.peer.unavailable = True

        # Snapshot a real completed child before admitting newer owner authority; no old scope is transferred.
        def observe_completed_child(child, *, require_parent_pending):
            lease = observations[0]["lease"]
            witness.update(
                child_done=child.done(),
                child_cancelled=child.cancelled(),
                child_error=repr(child.exception()) if child.done() and not child.cancelled() else None,
                worker_pending=c.charging.worker is not None and not c.charging.worker.done(),
                require_parent_pending=require_parent_pending,
                control=copy.deepcopy(j.get("control")),
                receipt=copy.deepcopy(j.get("local_start_receipt")),
                intent=c.requests.intent(),
                owner_pause=copy.deepcopy(j.get("managed_owner_pause")),
                state=copy.deepcopy(c.charging.state()),
                in_transaction=j.db.in_transaction,
                actual_limit=rig.bench.firmware.effective_limit(),
                wire=len(rig.bench.device_calls),
                current_scope=current_scope(g),
                old_start_scope=lease.start_scope,
                lease_mode=lease.mode,
                lease_revision=lease.revision,
                lease_deadline=lease.deadline,
                observed_at=time.monotonic(),
            )

        # Success has no await after COMMIT; the adjacent Boot is handled before this callback and the parent resumes.
        def after_successful_child_commit():
            j.db.set_trace_callback(None)
            try:
                observe_completed_child(scheduled[0]["child"], require_parent_pending=True)
                admit_new_owner_requests()
            except Exception as error:
                callback_errors.append(repr(error))  # The test asserts callback errors before any outcome oracle.
            finally:
                injected.set()

        # Observe only the actual final shared-control INSERT and queue the independent owner action afterwards.
        def trace(statement):
            if storage_failure != "none" or scheduled or not statement.startswith("INSERT OR REPLACE INTO meta "):
                return
            frame = sys._getframe(1)
            completion, starting, attempt = {}, {}, None
            try:
                while frame is not None:
                    if frame.f_code is LocalControl._complete.__code__:
                        completion = dict(frame.f_locals)
                    elif frame.f_code is LocalControl._start.__code__:
                        starting = dict(frame.f_locals)
                    elif frame.f_code is LocalControl._start_owned.__code__:
                        attempt = frame.f_locals.get("attempt")
                    frame = frame.f_back
            finally:
                del frame
            if (
                completion.get("self") is g.controls
                and completion.get("changes") == {"uncertain": False, "owner": "shared"}
                and starting.get("self") is g.controls
                and starting.get("request_id") == identifier
                and starting.get("shared") is True
                and completion.get("ticket") == starting.get("ticket")
            ):
                scheduled.append({"child": asyncio.current_task(), "attempt": attempt, "ticket": starting["ticket"]})
                loop.call_soon(after_successful_child_commit)

        # Preserve actual ACK bytes; only success appends Boot immediately, since failed publication must finish zero A.
        async def send_reply_then_boot(raw):
            reply = json.loads(raw)
            command = next((item for item in rig.bench.device_calls if item[1] == reply[1]), None)
            first_start = not burst and reply[0] == 3 and command is not None and command[2] == "RemoteStartTransaction"
            if first_start:
                burst.update(reply=copy.deepcopy(reply), command=copy.deepcopy(command), scope=current_scope(g))
            if burst and reply[0] == 3 and command is not None and command[2] == "SetChargingProfile":
                limit = command[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit == 0:
                    child = observations[0]["child"] if observations else None
                    zero_writers.append(
                        {
                            "frame": copy.deepcopy(command),
                            "old_child_done": child is not None and child.done(),
                            "current_pause_pending": c.pause_task is not None and not c.pause_task.done(),
                            "scope": current_scope(g),
                            "control": copy.deepcopy(j.get("control")),
                            "after_new_owner": witness.get("new_owner_admitted") is True,
                        }
                    )
            await original_send(raw)
            if reply[0] in (3, 4):
                wire_replies.append(copy.deepcopy(reply))
            if first_start and storage_failure == "none":
                rig.bench.replies[boot_id] = boot_reply
                await original_send(build_call(boot_id, "BootNotification", BOOT))

        monkeypatch.setattr(b.charger, "send", send_reply_then_boot)
        j.db.set_trace_callback(trace)
        try:
            assert c.command(identifier, "start", initial_intent.revision)["state"] == "accepted"
            if storage_failure == "none":
                await asyncio.wait_for(injected.wait(), 5)
                assert not callback_errors and len(scheduled) == 1 and len(observations) == 1, callback_errors
                assert witness["child_done"] and witness["worker_pending"]
                assert not witness["child_cancelled"] and witness["child_error"] == "None"
                assert not witness["in_transaction"] and witness["lease_deadline"] > witness["observed_at"]
                attempt = observations[0]["attempt"]
                result = attempt.result()
                assert result.failure is None and result.publication is not None and not result.zero_verified
                assert witness["intent"].mode == witness["lease_mode"] == "app"
                assert witness["intent"].revision == witness["lease_revision"] == initial_intent.revision + 1
                assert witness["state"]["state"] == "awaiting_power" and witness["state"]["start_rpc_accepted"]
                assert witness["actual_limit"] == 6
                assert witness["control"]["owner"] == "shared"
                assert not witness["control"]["inhibit"] and not witness["control"]["uncertain"]
                assert witness["owner_pause"] == initial_owner_pause
                assert (await asyncio.wait_for(asyncio.shield(boot_reply), 3))[2]["status"] == "Accepted"
                assert not [item for item in zero_writers if not item["after_new_owner"]]
            else:
                # Do not inject Boot during rollback: the genuine failed child first verifies zero under its own A.
                await asyncio.wait_for(c.charging.settle(), 5)
                assert not callback_errors and len(observations) == 1
                cut = observations[0]
                attempt, child = cut["attempt"], cut["child"]
                assert attempt is not None and attempt.finished and child.done() and not child.cancelled()
                result = attempt.result()
                assert isinstance(result.failure, sqlite3.IntegrityError)
                assert result.publication is None and result.zero_verified and result.remote_start_dispatched
                expected_code = (
                    sqlite3.SQLITE_CONSTRAINT_FOREIGNKEY
                    if storage_failure == "commit"
                    else sqlite3.SQLITE_CONSTRAINT_TRIGGER
                )
                assert result.failure.sqlite_errorcode == expected_code and not j.db.in_transaction
                if storage_failure == "commit":
                    assert j.db.execute("SELECT COUNT(*) FROM start_boot_child").fetchone()[0] == 0
                assert c.requests.intent() == initial_intent and c.released_epoch == released
                assert cut["lease"].mode == initial_intent.mode and cut["lease"].revision == initial_intent.revision
                assert (
                    c.charging.state()["state"] == "stopping"
                    and c.charging.state()["failure"]["type"] == "IntegrityError"
                )
                assert not c.charging.state().get("start_rpc_accepted")
                assert c.requests.receipt(identifier)["state"] == "uncertain"
                assert rig.bench.firmware.effective_limit() == 0 and current_scope(g) == scope_a
                assert j.get("local_start_receipt")["stage"] == "accepted"
                assert j.get("local_start_receipt")["ticket"] == j.get("control")["ticket"]
                owned_zero = [item for item in zero_writers if not item["after_new_owner"]]
                assert owned_zero
                assert all(not item["old_child_done"] and item["scope"] == scope_a for item in owned_zero)
                clear_publication_cut()
                # This is an actual later Boot, after owned rollback A, before the NEW Pause claim under B.
                answer = await rig.bench.call(b.charger, boot_id, "BootNotification", BOOT)
                assert answer[0] == 3 and answer[2]["status"] == "Accepted"
                observe_completed_child(child, require_parent_pending=False)
                assert witness["intent"] == initial_intent and witness["lease_mode"] == "paused"
                assert witness["control"]["owner"] == "local" and witness["control"]["inhibit"]
                assert witness["control"]["uncertain"] and witness["actual_limit"] == 0
                admit_new_owner_requests()

            # All paths first prove actual acceptance and the real producer/transaction before the common suffix.
            cut = observations[0]
            assert cut["transaction"] and cut["limit"] == 6 and cut["attempt"] is attempt
            assert cut["start"]["request_id"] == identifier and cut["start"]["scope"] == scope_a
            assert cut["receipt"]["request_id"] == identifier and cut["receipt"]["stage"] == "accepted"
            assert cut["receipt"]["ticket"] == cut["start"]["ticket"] and cut["receipt"]["local"] is None
            assert cut["candidate"]["state"] == "awaiting_power" and cut["candidate"]["start_rpc_accepted"]
            assert cut["candidate"]["revision"] == initial_intent.revision + 1
            assert cut["intent_inside"].mode == "app" and cut["intent_inside"].revision == initial_intent.revision + 1
            assert cut["lease_mode_inside"] == "paused" and cut["lease_revision_inside"] == initial_intent.revision
            assert result.remote_start_dispatched and result.answer[2] == {"status": "Accepted"}
            assert burst and burst["scope"] == scope_a and burst["reply"][2] == {"status": "Accepted"}
            remote = starts(rig)
            assert len(remote) == 1 and [3, remote[0][1], {"status": "Accepted"}] in wire_replies
            current = witness["current_scope"]
            assert current is not None and current.boot != scope_a.boot and current.lineage == scope_a.lineage
            assert witness["old_start_scope"] == scope_a and current_scope(g) == current
            assert g.controls._revision == revision
            receipt, control = witness["receipt"], witness["control"]
            assert receipt["request_id"] == identifier and receipt["stage"] == "accepted" and receipt["local"] is None
            assert receipt["ticket"] == cut["start"]["ticket"] == control["ticket"]
            assert control["start_pending"] and witness["pause"]["state"] == witness["return"]["state"] == "accepted"
            assert witness["pause"]["id"] == new_pause and witness["return"]["id"] == returning
            intent, owner_pause = witness["paused_intent"], witness["current_owner_pause"]
            assert intent.mode == "paused" and intent.revision == witness["intent"].revision + 1
            assert owner_pause == {"ticket": witness["paused_control"]["ticket"], "revision": intent.revision}
            assert witness["paused_control"]["ticket"] != control["ticket"]
            assert witness["paused_control"]["stop_epoch"] > control["stop_epoch"]
            assert witness["pending_return"] == (returning, intent.revision)
            assert rig.peer.unavailable and j.active() is None and len(starts(rig)) == 1
            clear_publication_cut()
            await asyncio.wait_for(c.charging.settle(), 4)
            assert c.charging.state()["state"] == "stopping"
            if storage_failure == "none":
                assert c.charging.state()["failure"]["type"] == "DeniedError"
            else:
                assert c.charging.state()["failure"]["type"] == "IntegrityError"
            assert c.requests.receipt(identifier)["state"] == "uncertain" and c.pause_task is not None
            await asyncio.wait_for(asyncio.shield(c.pause_task), 4)
            failed_reads = [await asyncio.wait_for(rig.peer.failed_reads.get(), 3)]
            while not rig.peer.failed_reads.empty():
                failed_reads.append(rig.peer.failed_reads.get_nowait())
            # A read begun before unavailability can fail on a later endpoint. Prove a fresh complete audit too.
            read_sequence = c.native.read_sequence
            await c.tick()
            assert c.native.read_sequence > read_sequence and failed_reads
            assert (await asyncio.wait_for(rig.peer.failed_reads.get(), 3))["action"] == "getConfig"
            new_zero = [item for item in zero_writers if item["after_new_owner"]]
            if storage_failure == "none":
                assert new_zero and new_zero[0]["current_pause_pending"]
            else:
                # Own zero A was already real; current Pause need not repeat it just because identical Boot arrived.
                assert result.zero_verified
            assert all(item["old_child_done"] and item["scope"] == current for item in new_zero)
            assert all(item["control"]["ticket"] == witness["paused_control"]["ticket"] for item in new_zero)
            assert c.requests.intent() == intent and j.get("managed_owner_pause") == owner_pause
            assert j.get("local_start_receipt") == receipt and j.get("control")["start_pending"]
            assert j.get("control")["owner"] == "local" and j.get("control")["inhibit"]
            assert j.get("control")["uncertain"] and rig.bench.firmware.effective_limit() == 0
            assert not j.get("managed_control_fault") and not any(
                frame[2] == "RemoteStopTransaction" for frame in rig.bench.device_calls
            )
            assert c.requests.receipt(returning)["state"] == "accepted"
            assert current_scope(g) == current and g.controls._revision == revision
            assert rig.bench.firmware.plans == plans and rig.peer.configuration == configuration
            rig.peer.unavailable = False
            await c.tick()
            assert c.requests.intent() == intent and c.requests.receipt(returning)["state"] == "accepted"
            assert c.requests.receipt(identifier)["state"] == "uncertain"
            assert j.get("local_start_receipt") == receipt and rig.bench.firmware.effective_limit() == 0

            # The original real accepted Start supplies its own delayed lifecycle while the new Pause retains zero.
            local = await positive_flow(b, call_id=uuid.uuid4().hex)
            bound = j.get("local_start_receipt")
            assert bound["request_id"] == identifier and bound["ticket"] == receipt["ticket"]
            assert bound["local"] == local and bound["stage"] == "accepted"
            assert not j.get("control")["start_pending"] and j.active() == local
            assert rig.bench.firmware.effective_limit() == 0 and c.requests.intent() == intent
            assert c.requests.receipt(returning)["state"] == "accepted" and len(starts(rig)) == 1
            await c.tick()
            await until(lambda: any(frame[2] == "RemoteStopTransaction" for frame in rig.bench.device_calls))
            stops = [
                frame[3]["transactionId"] for frame in rig.bench.device_calls if frame[2] == "RemoteStopTransaction"
            ]
            assert stops == [local]
            assert j.active() == local and rig.bench.firmware.effective_limit() == 0
            assert c.requests.receipt(returning)["state"] == "accepted"
            # Native closure alone cannot settle Pause/Return or replace the genuine OCPP/cloud closure.
            rig.bench.firmware.active = None
            rig.bench.firmware.requested_current = 0
            rig.peer.permission_enabled = False
            rig.bench.cp, rig.bench.connection = "B", "Plug"
            await c.tick()
            assert j.active() == local and c.requests.receipt(returning)["state"] == "accepted"
            assert c.requests.receipt(new_pause)["state"] != "completed"
            assert rig.bench.firmware.effective_limit() == 0
            await actual_stop(b, local)
            status = await completed_control(b, returning, mode="app", revision=intent.revision + 1, ceiling=6)
            assert status["owner_control_enabled"] and j.active() is None
            assert c.requests.receipt(new_pause)["state"] == "completed"
            assert c.requests.receipt(identifier)["state"] == "completed"
            assert c.requests.receipt(identifier)["result"] == {
                "local_session_verified": True,
                "charging": False,
                "timed": False,
            }
            assert j.get("local_start_receipt")["request_id"] == identifier
            assert j.get("local_start_receipt")["local"] == local
            assert c.requests.receipt(returning)["state"] == "completed" and len(starts(rig)) == 1
            stops = [
                frame[3]["transactionId"] for frame in rig.bench.device_calls if frame[2] == "RemoteStopTransaction"
            ]
            assert stops == [local]
            assert current_scope(g) == current and rig.bench.firmware.plans == plans
            assert rig.peer.configuration == configuration
        finally:
            j.db.set_trace_callback(None)
            clear_publication_cut()
            monkeypatch.setattr(b.charger, "send", original_send)
            rig.peer.unavailable = False
            if c.charging.worker is not None:
                await asyncio.gather(c.charging.worker, return_exceptions=True)
            if c.pause_task is not None:
                await asyncio.gather(c.pause_task, return_exceptions=True)
            if not boot_reply.done():
                boot_reply.cancel()
            await asyncio.gather(boot_reply, return_exceptions=True)
