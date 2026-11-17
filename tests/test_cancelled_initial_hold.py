"""Cancelled initial restrictive work must recover only its exact original app claim."""

import asyncio
import copy
import uuid
from contextlib import contextmanager

import pytest
from ocpp_hub_core.prototype.local_control import LocalControl

from tests.test_managed_proxy import healthy, starts
from tests.test_managed_start_cancelled_outer import selected_source
from tests.test_prototype import BOOT
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Cancel the genuine owned coroutine only at scheduling; the outer worker and every native/wire gate remain real.
async def cancelled_child(b, boundary):
    c, g = b.controller, b.session.gateway
    loop, factory, victims = asyncio.get_running_loop(), asyncio.get_running_loop().get_task_factory(), []
    captured = asyncio.Event()
    code = LocalControl._stop.__code__ if boundary == "zero_child" else LocalControl._start_owned.__code__

    # A pre-first-instruction child cancellation cannot be mistaken for a dispatched physical Start.
    def cancel_selected(loop, coroutine, **kwargs):
        task = asyncio.Task(coroutine, loop=loop, eager_start=False, **kwargs)
        if getattr(coroutine, "cr_code", None) is code and coroutine.cr_frame.f_locals.get("self") is g.controls:
            victims.append(task)
            task.cancel()
            captured.set()
        return task

    identifier, intent = uuid.uuid4().hex, c.requests.intent()
    try:
        loop.set_task_factory(cancel_selected)
        c.command(identifier, "start", intent.revision)
        await asyncio.wait_for(captured.wait(), 3)
        await asyncio.wait_for(asyncio.gather(c.tests.worker, *victims, return_exceptions=True), 3)
    finally:
        loop.set_task_factory(factory)
    assert c.tests.worker.cancelled() and len(victims) == 1
    assert c.tests.state()["start_attempted"] is False and not starts(b.rig)
    assert c.tests.lease.initial_zero_ticket == b.session.journal.get("control")["ticket"]
    return identifier, intent


# A real refusal at UPDATE or deferred COMMIT must roll back the receipt and recovery requirement together.
@contextmanager
def refused_retirement(j, identifier, boundary):
    if boundary == "commit":
        j.db.execute("CREATE TABLE cancel_parent(id INTEGER PRIMARY KEY)")
        j.db.execute("CREATE TABLE cancel_child(id REFERENCES cancel_parent(id) DEFERRABLE INITIALLY DEFERRED)")
        effect = "INSERT INTO cancel_child VALUES(1);"
    else:
        effect = "SELECT RAISE(ABORT,'synthetic retirement refusal');"
    j.db.execute(
        "CREATE TRIGGER refuse_retirement BEFORE UPDATE ON managed_requests WHEN NEW.id='"
        + identifier
        + "' AND NEW.state='failed' BEGIN "
        + effect
        + " END"
    )
    try:
        yield
    finally:
        j.db.execute("DROP TRIGGER refuse_retirement")
        assert not j.db.in_transaction
        if boundary == "commit":
            assert j.db.execute("SELECT COUNT(*) FROM cancel_child").fetchone()[0] == 0
            j.db.execute("DROP TABLE cancel_child")
            j.db.execute("DROP TABLE cancel_parent")


# Intent: either owned child may disappear before execution without stranding a zero claim or inventing Pause.
# Why: capturing only after an awaited Stop misses the shielded-child and pre-first-instruction boundaries.
@pytest.mark.parametrize("current", [6, 32])
@pytest.mark.parametrize("boundary", ["zero_child", "start_child"])
@pytest.mark.parametrize("newer_pause", [False, True])
async def test_cancelled_initial_hold_respects_exact_or_newer_intent(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, boundary, newer_pause
):
    async with selected_source(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        c, j, rig = b.controller, b.session.journal, b.rig
        identifier, intent = await cancelled_child(b, boundary)
        request_revision = intent.revision
        pause = None
        if newer_pause:
            pause = uuid.uuid4().hex
            c.command(pause, "pause", intent.revision)
            intent = c.requests.intent()
        status = await healthy(c)
        assert c.requests.intent() == intent and not j.get("managed_control_fault")
        assert c.requests.receipt(identifier)["state"] == "failed" and c.tests.lease is None
        assert rig.bench.firmware.effective_limit() == (0 if newer_pause else current)
        assert not starts(rig) and not rig.bench.remote_stop.is_set()
        assert j.get("local_start_receipt") is None
        if pause is not None:
            assert status["local_hold_verified"] and c.requests.receipt(pause)["state"] == "completed"
        before = copy.deepcopy(c.requests.receipt(identifier))
        assert c.command(identifier, "start", request_revision) == before


# Intent: a failed exact retirement stays pending and later recovers atomically under the same app intent.
# Why: a storage retry cannot publish readiness, lose its restrictive marker, or manufacture deadline Stop authority.
@pytest.mark.parametrize("boundary", ["statement", "commit"])
async def test_cancelled_zero_retirement_storage_is_atomic(tmp_path, tls, unused_tcp_port, monkeypatch, boundary):
    async with selected_source(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j, rig = b.controller, b.session.journal, b.rig
        identifier, intent = await cancelled_child(b, "start_child")
        state, request, phase = c.tests.state(), c.requests.receipt(identifier), j.get("trial_phase")
        with refused_retirement(j, identifier, boundary):
            status = await c.tick()
            assert status["fault"] == "start_operation_pending" and not status.get("local_hold_verified")
            assert c.tests.state() == state and c.requests.receipt(identifier) == request
            assert j.get("trial_phase") == phase and c.tests.lease is not None
            assert c.requests.intent() == intent and rig.bench.firmware.effective_limit() == 0
            assert not starts(rig) and not rig.bench.remote_stop.is_set()
        assert (await healthy(c))["station_ceiling_a"] == 6 and c.requests.intent() == intent
        assert c.requests.receipt(identifier)["state"] == "failed" and not j.get("managed_control_fault")


# Intent: a fresh identical Boot cannot inherit the cancelled operation's original positive recovery claim.
# Why: proving that no Start was dispatched does not prove authority under a later physical introduction.
async def test_cancelled_zero_does_not_restore_across_new_boot(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with selected_source(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j, rig = b.controller, b.session.journal, b.rig
        identifier, intent = await cancelled_child(b, "start_child")
        old_boot = b.session.gateway.boot_observation
        assert (await rig.bench.call(b.charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
        assert b.session.gateway.boot_observation != old_boot
        mark = len(rig.bench.received_limits)
        for _ in range(4):
            await c.tick()
        await until(lambda: rig.bench.firmware.effective_limit() == 0)
        assert c.requests.intent() == intent and c.requests.receipt(identifier)["state"] == "failed"
        assert not any(limit > 0 for limit in rig.bench.received_limits[mark:]) and not starts(rig)
        assert not rig.bench.remote_stop.is_set() and j.get("local_start_receipt") is None
