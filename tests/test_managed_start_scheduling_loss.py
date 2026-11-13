"""Controlled scheduler loss before an owned Start task runs cannot create false issued-command provenance."""

import asyncio
import sqlite3
import uuid

import pytest
from ocpp_hub_core.prototype.local_control import LocalControl
from ocpp_hub_core.prototype.managed import ManagedController

from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import crash
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_proxy import healthy, introduced, managed_session, starts
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_test_charge import ChargeRig, positive_flow
from tests_model.test_schedule_sequences import listener_port, tls_for_case
from tests_model.test_schedule_sequences import model_tls as model_tls
from tests_model.test_start_recovery_sequences import close_daily, observe


# Intent: losing the scheduled real task, or its preparation write, retains exact no-dispatch provenance and recovery.
# Why: marking an attempt before the owned coroutine begins can permanently suppress a later daily's outage zero.
@pytest.mark.parametrize("action,current", [("start", 6), ("start", 32), ("start_test", 6)])
@pytest.mark.parametrize("loss", ["scheduled-task", "preparation-write", "retirement-request", "retirement-metadata"])
async def test_start_preparation_survives_controlled_task_loss(tmp_path, model_tls, monkeypatch, action, current, loss):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with commissioned(tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch, current) as b:
        c, s, rig = b.controller, b.session, b.rig
        loop, interrupted = asyncio.get_running_loop(), asyncio.Event()
        original_factory = loop.get_task_factory()
        victims = []

        # Cancel only this real non-eager process-owned task before execution; no engine method or outcome is mocked.
        def lose_start(loop, coroutine, **kwargs):
            task = asyncio.Task(coroutine, loop=loop, eager_start=False, **kwargs)
            if (
                getattr(coroutine, "cr_code", None) is LocalControl._start.__code__
                and coroutine.cr_frame.f_locals.get("self") is s.gateway.controls
            ):
                task.cancel()
                victims.append(task)
                interrupted.set()
            return task

        if loss != "preparation-write":
            loop.set_task_factory(lose_start)
        else:
            s.journal.db.execute(
                "CREATE TRIGGER fail_start_marker BEFORE INSERT ON meta WHEN NEW.key='managed_test_charge' "
                "AND json_extract(NEW.value,'$.start_attempted')=1 "
                "BEGIN SELECT RAISE(ABORT,'synthetic preparation marker failure'); END"
            )
        identifier = uuid.uuid4().hex
        try:
            c.command(identifier, action, c.requests.intent().revision)
            if loss != "preparation-write":
                await asyncio.wait_for(interrupted.wait(), 3)
                await asyncio.gather(c.tests.worker, *victims, return_exceptions=True)
                assert len(victims) == 1 and victims[0].cancelled()
            else:
                with pytest.raises(sqlite3.IntegrityError, match="synthetic preparation marker failure"):
                    await c.tests.settle()
                s.journal.db.execute("DROP TRIGGER fail_start_marker")
        finally:
            loop.set_task_factory(original_factory)
        assert c.tests.state()["start_attempted"] is False and c.tests.state()["start_rpc_accepted"] is False
        assert s.journal.get("local_start_receipt") is None and not starts(rig)
        await c.tests.close()
        await crash(s)
        replacement = managed_session(rig)
        await replacement.start()
        c = ManagedController(replacement)
        try:
            async with rig.charger(replacement) as sock:
                await introduced(rig, replacement, sock, "scheduled-start-loss-replacement")
                if loss.startswith("retirement-"):
                    if loss == "retirement-request":
                        sql = "BEFORE UPDATE ON managed_requests WHEN NEW.id='" + identifier + "'"
                    else:
                        sql = "BEFORE INSERT ON meta WHEN NEW.key='managed_test_charge' AND "
                        sql += "json_extract(NEW.value,'$.state')='completed'"
                    replacement.journal.db.execute(
                        "CREATE TRIGGER fail_unsent_retire "
                        + sql
                        + " BEGIN SELECT RAISE(ABORT,'synthetic unsent retirement failure'); END"
                    )
                    c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
                    with pytest.raises(sqlite3.IntegrityError, match="synthetic unsent retirement failure"):
                        await c.pause_task
                    assert c.requests.receipt(identifier)["state"] == "uncertain"
                    assert c.tests.state()["state"] == "interrupted" and rig.bench.firmware.effective_limit() == 0
                    replacement.journal.db.execute("DROP TRIGGER fail_unsent_retire")
                c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
                assert (await healthy(c))["local_hold_verified"]
                assert c.tests.state()["state"] == "completed"
                assert c.requests.receipt(identifier)["state"] == "failed"
                c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
                assert (await healthy(c))["normal_start_ready"]
                fresh = ChargeRig(rig, replacement, c, sock)
                local = await positive_flow(fresh, "daily")
                await healthy(c)
                rig.peer.unavailable = True
                await observe(c, 5)
                assert rig.bench.firmware.effective_limit() == 0
                rig.peer.unavailable = False
                await healthy(c)
                await close_daily(fresh, local, "native")
                assert not starts(rig)
        finally:
            rig.peer.unavailable = False
            await c.tests.close()
            await replacement.close()
