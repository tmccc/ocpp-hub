"""A genuine rejected Stop remains independently recoverable across every fault-latch acquisition boundary."""

import asyncio
import copy
import sqlite3
import uuid

import pytest
from ocpp_hub_core.frames import build_call_error
from ocpp_hub_core.prototype.gateway import decode

from tests import test_managed_proxy as mp
from tests.test_managed_stop_reply_recovery import ReplyBench, pause, reconcile, return_after_ack, running
from tests.test_managed_test_charge import ChargeRig, positive_flow
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests_model.test_schedule_sequences import listener_port, tls_for_case
from tests_model.test_schedule_sequences import model_tls as model_tls


# Intent: a crash before the first fault-publishing tick must not remove the exact retained Stop's recovery route.
# Why: actual closed local Starts and ordinary timetables need the same durable delivery evidence after replacement.
@pytest.mark.parametrize("origin", ["daily", "local", "local-unobserved"])
@pytest.mark.parametrize("boundary", ["before-tick", "after-tick", "after-pause"])
@pytest.mark.parametrize("reply", ["invalid-info", "call-error"])
async def test_stop_rejection_replacement_retains_recovery(
    tmp_path, model_tls, monkeypatch, capsys, origin, boundary, reply
):
    monkeypatch.setattr(mp, "OwnerBench", ReplyBench)
    async with mp.managed_bench(tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch) as rig:
        async with running(rig, "stop-before-crash", owner_tag="APP_OWNER") as (s, c, sock):
            await mp.healthy(c)
            if origin.startswith("local"):
                c.command(uuid.uuid4().hex, "start", c.requests.intent().revision)
                await c.tests.settle()
            local = await positive_flow(ChargeRig(rig, s, c, sock), "local" if origin.startswith("local") else "daily")
            if origin.startswith("local"):
                assert s.journal.get("local_start_receipt")["local"] == local
                assert c.tests.state()["local_transaction"] == local
            if origin != "local-unobserved":
                await mp.healthy(c)
            else:
                rig.peer.permission_enabled = False
                rig.bench.firmware.requested_current = 0
            rig.bench.stop_info = {"status": "invalid"} if reply == "invalid-info" else None
            rig.bench.stop_error = reply == "call-error"
            await rig.bench.call(
                sock,
                "autonomous-stop",
                "StopTransaction",
                {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
            )
            rig.peer.permission_enabled = False
            await until(lambda: s.runtime.status.delivery_blocked)
            if boundary == "after-tick":
                await c.tick()
            elif boundary == "after-pause":
                await pause(c)
        async with running(rig, "stop-after-crash") as (s, c, sock):
            await until(lambda: s.runtime.status.delivery_blocked)
            await pause(c)
            assert rig.bench.firmware.effective_limit() == 0 and s.journal.active() is None
        await reconcile(rig, tmp_path, capsys)
        rig.bench.stop_error, rig.bench.stop_info = False, None
        async with running(rig, "stop-after-proof") as (s, c, sock):
            await return_after_ack(rig, s, c, expected_starts=int(origin.startswith("local")))


# Intent: a real accepted Start reply after newer Pause can settle only its exact genuinely closed bound session.
# Why: the protocol receipt commits before the worker publishes success; an unknown issued reply remains debt.
@pytest.mark.parametrize("answer", ["accepted", "unknown"])
@pytest.mark.parametrize("replace_process", [False, True])
async def test_bound_lifecycle_before_start_success_publication(
    tmp_path, model_tls, monkeypatch, answer, replace_process
):
    async with mp.managed_bench(tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch) as rig:
        identifier, retained = uuid.uuid4().hex, []
        async with running(rig, "held-start-reply", owner_tag="APP_OWNER") as (s, c, sock):
            await mp.healthy(c)
            send = sock.send

            # Hold only the independent charger's actual RemoteStart reply after applying its command.
            async def hold_start(raw):
                frame = decode(raw)
                call = next((row for row in rig.bench.device_calls if row[1] == frame[1]), None)
                if frame[0] == 3 and call is not None and call[2] == "RemoteStartTransaction":
                    retained.append(raw)
                else:
                    await send(raw)

            monkeypatch.setattr(sock, "send", hold_start)
            c.command(identifier, "start", c.requests.intent().revision)
            await until(lambda: bool(retained))
            local = await positive_flow(ChargeRig(rig, s, c, sock))
            assert s.journal.get("local_start_receipt")["local"] == local
            assert c.tests.state()["local_transaction"] == local
            rig.peer.permission_enabled, rig.bench.firmware.requested_current = False, 0
            await rig.bench.call(
                sock,
                "closed-before-start-ack",
                "StopTransaction",
                {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
            )
            await until(lambda: s.journal.next_delivery() is None)
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            raw = retained[0]
            if answer == "unknown":
                raw = build_call_error(decode(raw)[1], "InternalError", "synthetic uncertain Start reply")
            await send(raw)
            await c.tests.settle()
            if c.pause_task is not None:
                await c.pause_task
            assert c.tests.state()["start_rpc_accepted"] is False
            assert s.journal.get("local_start_receipt")["stage"] == ("accepted" if answer == "accepted" else "issued")
            if not replace_process:
                await verify_bound_result(rig, s, c, identifier, answer)
        if replace_process:
            async with running(rig, "replaced-held-start") as (s, c, sock):
                await verify_bound_result(rig, s, c, identifier, answer)


# Require positive recovery only from a real accepted exact receipt; zero and closure cannot settle an unknown send.
async def verify_bound_result(rig, s, c, identifier, answer):
    await pause(c)
    for _ in range(4):
        await c.tick()
        await asyncio.sleep(0)
    assert rig.bench.firmware.effective_limit() == 0 and len(mp.starts(rig)) == 1
    if answer == "accepted":
        assert c.tests.state()["state"] == "completed"
        assert c.requests.receipt(identifier)["state"] == "completed"
        c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
        assert (await mp.healthy(c))["normal_start_ready"]
    else:
        assert c.requests.receipt(identifier)["state"] != "completed"
        assert s.journal.get("local_start_receipt")["stage"] == "issued"


# Intent: journal failure commits neither half of exact closed-session recovery, and a later retry completes both.
# Why: a recovered command receipt without its matching lifecycle state, or the reverse, can strand later recovery.
@pytest.mark.parametrize("boundary", ["request", "metadata"])
async def test_closed_session_retirement_is_atomic(tmp_path, model_tls, monkeypatch, boundary):
    async with mp.managed_bench(tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch) as rig:
        async with running(rig, "closed-atomic-prefix", owner_tag="APP_OWNER") as (s, c, sock):
            await mp.healthy(c)
            identifier = uuid.uuid4().hex
            c.command(identifier, "start", c.requests.intent().revision)
            await c.tests.settle()
            local = await positive_flow(ChargeRig(rig, s, c, sock))
            assert c.tests.state()["local_transaction"] == local
            rig.peer.permission_enabled, rig.bench.firmware.requested_current = False, 0
            await rig.bench.call(
                sock,
                "closed-atomic-stop",
                "StopTransaction",
                {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
            )
            await until(lambda: s.journal.next_delivery() is None)
        async with running(rig, "closed-atomic-replacement") as (s, c, sock):
            native = await c.native.read()
            c.require_paused(native)
            state, receipt = copy.deepcopy(c.tests.state()), c.requests.receipt(identifier)
            assert receipt["state"] == "uncertain"
            sql = (
                "BEFORE UPDATE ON managed_requests WHEN NEW.id='" + identifier + "'"
                if boundary == "request"
                else "BEFORE INSERT ON meta WHEN NEW.key='managed_test_charge'"
            )
            s.journal.db.execute(
                "CREATE TRIGGER fail_closed_retire "
                + sql
                + " BEGIN SELECT RAISE(ABORT,'synthetic closed retirement write failure'); END"
            )
            with pytest.raises(sqlite3.IntegrityError, match="synthetic closed retirement"):
                c.tests.retire_closed_at_zero(native)
            assert c.tests.state() == state and c.requests.receipt(identifier) == receipt
            assert rig.bench.firmware.effective_limit() == 0
            s.journal.db.execute("DROP TRIGGER fail_closed_retire")
            c.tests.retire_closed_at_zero(native)
            assert c.tests.state()["state"] == c.requests.receipt(identifier)["state"] == "completed"
            c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
            assert (await mp.healthy(c))["normal_start_ready"]
