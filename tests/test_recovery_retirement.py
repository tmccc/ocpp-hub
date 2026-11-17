"""Real transport and SQLite regressions for owned retirement and timer publication."""

import asyncio
import sqlite3
import uuid

import pytest
from ocpp_hub_core.prototype.native_bounded_handover import NativeBoundedHandover
from ocpp_hub_core.prototype.recovery_coordinator import RecoveryCoordinator

from tests.native_fault_support import FinalReleaseHttpPeer, coroutine_locals
from tests.native_schedule_support import timer_bench
from tests.peer_mutation_helpers import observe_device_replies
from tests.test_managed_proxy import healthy, starts
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

            await until(lambda: g.charger.sender is None and g.vendor.sender is None)
            calls = b.rig.bench.device_calls[wire_mark:]
            zero = [frame for frame in calls if frame[2] == "SetChargingProfile" and profile_limit(frame) == 0]
            assert zero and all([3, frame[1], {"status": "Accepted"}] in replies for frame in zero)
            composites = [frame for frame in calls if frame[2] == "GetCompositeSchedule"]
            assert composites and any(reply[0] == 3 and reply[1] == composites[-1][1] for reply in replies)
            assert b.device.effective_limit() == 0 and not starts(b.rig)
            assert b.session.closing and not b.session.closed and not owned.done()
            assert j.db.execute("SELECT 1").fetchone()[0] == 1

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
