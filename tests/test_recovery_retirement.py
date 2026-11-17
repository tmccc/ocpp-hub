"""Real transport and SQLite regressions for owned retirement and timer publication."""

import asyncio
import copy
import sqlite3
import sys
import uuid
from contextlib import AsyncExitStack

import pytest
from ocpp_hub_core.prototype.local_control import LocalControl
from ocpp_hub_core.prototype.managed_occurrences import KEY
from ocpp_hub_core.prototype.managed_schedule import ManagedSchedule
from ocpp_hub_core.prototype.native_bounded_handover import NativeBoundedHandover
from ocpp_hub_core.prototype.recovery_contracts import WireReply
from ocpp_hub_core.prototype.recovery_coordinator import RecoveryCoordinator

from tests.managed_scenario_helpers import next_daily, originals, require_originals
from tests.native_fault_support import FinalReleaseHttpPeer, coroutine_locals
from tests.native_schedule_support import timer_bench
from tests.peer_mutation_helpers import observe_device_replies
from tests.test_managed_proxy import healthy, introduced, starts
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls

pytestmark = pytest.mark.asyncio


# Read actual CALLs at the independent device; this helper grants no authority or expected outcome.
def profile_limit(frame):
    return frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]


# Intent: session retirement must finish owned Return SQL before propagating repeated caller cancellation.
# Why: shielding only the drain leaves a live native completion using a journal that its caller just closed.
async def test_cancelled_close_joins_held_return_before_closing_journal(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch, peer_type=FinalReleaseHttpPeer) as b:
        c, g, j, peer = b.c, b.session.gateway, b.session.journal, b.rig.peer
        pause = uuid.uuid4().hex
        c.command(pause, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        assert c.requests.receipt(pause)["state"] == "completed"
        intent = c.requests.intent()
        identifier = uuid.uuid4().hex
        c.command(identifier, "app_control", intent.revision)
        replies = observe_device_replies(b, monkeypatch)
        sql_witnesses = []

        # Select only the real owned Return preflight on the actual HTTP getConfig request.
        def select_return_preflight():
            owned = c.recovery._task
            if owned is None or owned not in g.controls._completions:
                return None
            restore = coroutine_locals(owned, RecoveryCoordinator._restore_owned.__code__)
            read = coroutine_locals(owned, NativeBoundedHandover._read.__code__)
            if (
                restore.get("self") is not c.recovery
                or restore.get("expected_intent") != intent
                or restore.get("return_request") != identifier
                or read.get("self") is not c.native
                or g.controls.release_attempt is not None
                or g.controls._operations
                or c.requests.receipt(identifier)["state"] != "running"
            ):
                return None
            return {"owned": owned, "sequence": read["sequence"], "intent": intent}

        peer.select = select_return_preflight
        returning = asyncio.create_task(c.return_requested(identifier, intent.revision))
        closing, owned = None, None
        try:
            await asyncio.wait_for(peer.entered.wait(), 5)
            assert peer.witness is not None and peer.witness["http_action"] == "getConfig"
            owned = peer.witness["owned"]
            assert owned in g.controls._completions and not owned.done()
            assert g.controls.release_attempt is None and g.controls.desired() == 0

            # The original waiter can disappear; only the actual owned completion now retains its SQL duty.
            returning.cancel()
            with pytest.raises(asyncio.CancelledError):
                await returning
            assert not owned.done() and owned in g.controls._completions

            # Trace only the real request-specific terminal UPDATE; do not execute SQL or alter its result here.
            def trace(statement):
                if (
                    statement.startswith("UPDATE managed_requests SET state='uncertain'")
                    and identifier in statement
                    and "return_not_confirmed" in statement
                ):
                    sql_witnesses.append(
                        {
                            "owned_task": asyncio.current_task() is owned,
                            "closing": b.session.closing,
                            "closed": b.session.closed,
                        }
                    )

            j.db.set_trace_callback(trace)
            wire_mark = len(b.rig.bench.device_calls)
            closing = asyncio.create_task(b.session.close())

            # Both protocol peers detach only after the independent shutdown zero completed its real sequence.
            await until(lambda: g.charger.sender is None and g.vendor.sender is None)
            calls = b.rig.bench.device_calls[wire_mark:]
            zero = [frame for frame in calls if frame[2] == "SetChargingProfile" and profile_limit(frame) == 0]
            assert zero and all([3, frame[1], {"status": "Accepted"}] in replies for frame in zero)
            composites = [frame for frame in calls if frame[2] == "GetCompositeSchedule"]
            assert composites and any(reply[0] == 3 and reply[1] == composites[-1][1] for reply in replies)
            assert b.device.effective_limit() == 0 and not starts(b.rig)
            assert b.session.closing and not b.session.closed and not owned.done()
            assert j.db.execute("SELECT 1").fetchone()[0] == 1

            # A real held HTTP request keeps the drain live; neither cancellation may let the caller escape it.
            for _ in range(2):
                closing.cancel()
                done, _ = await asyncio.wait((closing,), timeout=0.05)
                assert not done, "close propagated cancellation before the held journal user retired"
                assert not b.session.closed and not owned.done()
                assert owned in g.controls._completions
                assert j.db.execute("SELECT 1").fetchone()[0] == 1

            peer.release.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(asyncio.shield(closing), 5)
            assert b.session.closed and owned.done() and not owned.cancelled()
            assert not g.controls._completions and not g.controls._operations
            assert sql_witnesses == [{"owned_task": True, "closing": True, "closed": False}]
            with pytest.raises(sqlite3.ProgrammingError, match="closed"):
                j.db.execute("SELECT 1")
            assert b.device.effective_limit() == 0 and not starts(b.rig)
        finally:
            peer.release.set()
            if not returning.done():
                returning.cancel()
            await asyncio.gather(returning, return_exceptions=True)
            if closing is not None:
                await asyncio.gather(closing, return_exceptions=True)
            if owned is not None:
                await asyncio.gather(owned, return_exceptions=True)
            await b.session.close()


# Intent: an uninstalled requested timer may expire in the real release child's final publication transaction.
# Why: the timer's own finish transaction must join that caller instead of faulting a valid verified release.
async def test_requested_timer_expiry_joins_release_publication_after_reconnect(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    async with (
        timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b,
        AsyncExitStack() as connections,
    ):
        saved = originals(b)
        identifier = uuid.uuid4().hex
        b.request(identifier)
        c, g, j = b.c, b.session.gateway, b.session.journal
        requested = copy.deepcopy(j.get(KEY))
        assert requested["id"] == identifier and requested["state"] == "requested"
        assert c.requests.receipt(identifier)["state"] == "accepted"
        assert set(b.device.plans) == {9} and b.timer not in b.device.plans
        previous_epoch = g._charger_epoch

        # Genuine new charger transport/Boot; no SQL seed or process-state replacement creates this prefix.
        await b.sock.close()
        await until(lambda: not b.session.runtime.status.charger_connected)
        b.sock = await connections.enter_async_context(b.rig.charger(b.session))
        await introduced(b.rig, b.session, b.sock, "requested-timer-real-reconnect")
        await until(lambda: c.native.introduced_epoch == g._charger_epoch and g.controls.ready)
        assert g._charger_epoch != previous_epoch and c.released_epoch != g._charger_epoch
        assert g.controls.desired() == b.device.effective_limit() == 0
        assert j.get(KEY)["id"] == identifier and j.get(KEY)["state"] == "requested"
        assert b.clock.now < requested["start_epoch"] - 10
        require_originals(b, saved)
        selected, wire_mark = c.current.selected(), len(b.rig.bench.device_calls)
        replies = observe_device_replies(b, monkeypatch)
        entered, release = asyncio.Event(), asyncio.Event()
        held, sql_witnesses, statements = [], [], []
        before_reply = b.rig.bench.before_reply

        # Hold a real Composite only AFTER its own positive Set reached firmware and its genuine ACK arrived.
        async def hold_positive_composite(action, payload):
            attempt = g.controls.release_attempt
            if (
                not held
                and action == "GetCompositeSchedule"
                and attempt is not None
                and attempt.purpose == "automatic"
                and attempt.positive_may_have_dispatched
                and attempt.positive_reply is WireReply.ACCEPTED
                and not attempt.positive_confirmed
                and not attempt.zero_may_have_dispatched
                and j.get(KEY).get("id") == identifier
                and j.get(KEY).get("state") == "requested"
            ):
                sent = [
                    frame
                    for frame in b.rig.bench.device_calls[wire_mark:]
                    if frame[2] == "SetChargingProfile" and profile_limit(frame) == selected
                ]
                assert len(sent) == 1 and [3, sent[0][1], {"status": "Accepted"}] in replies
                assert b.device.effective_limit() == selected and not j.db.in_transaction
                held.append(
                    {
                        "attempt": attempt,
                        "set": copy.deepcopy(sent[0]),
                        "profiles": copy.deepcopy(b.device.profiles),
                        "calendar": b.clock.now,
                    }
                )
                entered.set()
                await release.wait()
            if before_reply is not None:
                await before_reply(action, payload)

        # Inspect the real SQLite writer stack; observations neither mutate SQL nor return a policy verdict.
        def trace(statement):
            statements.append(statement)
            if not (
                statement.startswith("INSERT OR REPLACE INTO meta ")
                and "('" + KEY + "'," in statement
                and identifier in statement
                and "cancelled_before_install" in statement
            ):
                return
            frame, attempt, publisher, finisher = sys._getframe(1), None, False, False
            try:
                while frame is not None:
                    if frame.f_code is LocalControl._release.__code__:
                        attempt = frame.f_locals.get("attempt")
                    if frame.f_code is ManagedSchedule.finish.__code__ and frame.f_locals.get("self") is c.schedule:
                        finisher = True
                    if frame.f_code.co_name == "publish" and frame.f_code.co_filename.endswith(
                        "recovery_coordinator.py"
                    ):
                        publisher = True
                    frame = frame.f_back
            finally:
                del frame
            if not finisher:
                return  # Later cleanup may write the same body; this cut selects actual retirement.
            sql_witnesses.append(
                {
                    "transaction": j.db.in_transaction,
                    "publisher": publisher,
                    "attempt": attempt,
                    "verified": attempt is not None and attempt.positive_confirmed,
                    "published": attempt is not None and attempt.published,
                    "begins": sum(sql.strip().upper() == "BEGIN IMMEDIATE" for sql in statements),
                }
            )

        b.rig.bench.before_reply = hold_positive_composite
        driving = asyncio.create_task(c.tick())
        try:
            await asyncio.wait_for(entered.wait(), 5)
            assert len(held) == 1 and held[0]["calendar"] < requested["start_epoch"] - 10
            assert not held[0]["attempt"].finished and j.get(KEY)["state"] == "requested"
            b.clock.now = requested["start_epoch"] - 9
            j.db.set_trace_callback(trace)
            release.set()
            status = await asyncio.wait_for(driving, 5)
            attempt = held[0]["attempt"]
            assert attempt.finished
            assert attempt.result().failure is None, repr(attempt.result().failure)
            assert attempt.result().published
            assert len(sql_witnesses) == 1
            assert sql_witnesses[0] == {
                "transaction": True,
                "publisher": True,
                "attempt": held[0]["attempt"],
                "verified": True,
                "published": False,
                "begins": 1,
            }
            assert attempt.result().effects.positive_verified and not attempt.result().effects.zero_dispatched
            assert j.get(KEY)["state"] == "complete" and j.get(KEY)["result"] == "cancelled_before_install"
            receipt = c.requests.receipt(identifier)
            assert receipt["state"] == "failed"
            assert receipt["result"]["classification"] == "cancelled_before_install"
            assert not j.get("managed_control_fault") and not j.db.in_transaction
            assert c.released_epoch == g._charger_epoch
            assert status["state"] == "healthy" and b.device.effective_limit() == selected
            calls = b.rig.bench.device_calls[wire_mark:]
            assert [profile_limit(frame) for frame in calls if frame[2] == "SetChargingProfile"] == [selected]
            assert not any(frame[2] == "DataTransfer" and frame[3].get("messageId") == "setPlan" for frame in calls)
            assert not starts(b.rig) and not b.stops()
            require_originals(b, saved)
        finally:
            release.set()
            b.rig.bench.before_reply = before_reply
            if not driving.done():
                await asyncio.gather(driving, return_exceptions=True)
            j.db.set_trace_callback(None)

        # Rejoin the unchanged complete original midnight-to06:00 lifecycle, never a manufactured permission tail.
        await next_daily(b, selected, saved)
