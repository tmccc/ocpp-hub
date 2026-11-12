"""Completed connection receipts preserve exact-id results without consuming live handler slots."""

import asyncio

from ocpp_hub_core.frames import build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.rpc import InboundCache
from ocpp_hub_core.prototype.store import Journal, fingerprint


# Intent: completed requests do not consume the live-handler bound, and mutation replies never disappear to make room.
# Why: compact connection receipts must preserve exact mutation idempotency after an arbitrary read history.
async def test_completed_reads_leave_capacity_and_keep_mutation_receipts(tmp_path):
    j = Journal(tmp_path / "receipts.db", "OFFLINE001")
    try:
        cache = InboundCache(capacity=1, journal=j)
        calls = []

        # Standalone cache work records which handler actually executes and returns a bounded protocol result.
        async def handle():
            calls.append(True)
            return build_call_result("write", {"status": "Accepted"})

        write = [2, "write", "RemoteStopTransaction", {"transactionId": 12}]
        expected = await cache.response("vendor", 1, write, handle)
        for index in range(100):
            await cache.response("vendor", 1, [2, str(index), "GetConfiguration", {}], handle)
        assert len(calls) == 101
        assert cache._items == {}
        assert await cache.response("vendor", 1, write, handle) == expected
        changed = await cache.response("vendor", 1, write[:3] + [{"transactionId": 13}], handle)
        assert decode(changed)[2] == "FormationViolation" and len(calls) == 101
    finally:
        j.close()


# Intent: a real SQLite failure recording a terminal result cannot make a physically executed handler replayable.
# Why: the write-ahead pending receipt must survive a failed reply commit even after all live futures are gone.
async def test_failed_terminal_receipt_commit_keeps_pending_no_replay_marker(tmp_path):
    j = Journal(tmp_path / "receipts.db", "OFFLINE001")
    try:
        cache = InboundCache(journal=j)
        frame = [2, "executed-once", "RemoteStopTransaction", {"transactionId": 12}]
        calls = []
        j.db.execute(
            "CREATE TRIGGER fail_receipt_commit BEFORE UPDATE ON inbound_receipts "
            "BEGIN SELECT RAISE(ABORT,'synthetic terminal receipt failure'); END"
        )

        # One completed side effect supplies the observed successful result before its receipt update fails.
        async def execute():
            calls.append(True)
            return build_call_result(frame[1], {"status": "Accepted"})

        failed = await cache.response("vendor", 1, frame, execute)
        assert decode(failed)[2] == "InternalError" and len(calls) == 1 and not cache._items
        assert j.db.execute("SELECT response FROM inbound_receipts").fetchone()[0] is None
        j.db.execute("DROP TRIGGER fail_receipt_commit")
        assert await cache.response("vendor", 1, frame, execute) == failed
        assert len(calls) == 1
    finally:
        j.close()


# Intent: inability to save the initial receipt refuses work before its handler can have any external side effect.
# Why: storage failure must never create a command whose later duplicate has no no-replay boundary.
async def test_failed_initial_receipt_refuses_handler_execution(tmp_path):
    j = Journal(tmp_path / "receipts.db", "OFFLINE001")
    try:
        cache = InboundCache(journal=j)
        j.db.execute(
            "CREATE TRIGGER fail_receipt_begin BEFORE INSERT ON inbound_receipts "
            "BEGIN SELECT RAISE(ABORT,'synthetic initial receipt failure'); END"
        )
        calls = []

        # This handler would represent an external side effect and must remain unreachable.
        async def execute():
            calls.append(True)
            return build_call_result("unsent", {"status": "Accepted"})

        result = await cache.response("vendor", 1, [2, "unsent", "RemoteStopTransaction", {}], execute)
        assert decode(result)[2] == "InternalError" and not calls and not cache._items
    finally:
        j.close()


# Intent: a live duplicate shares one interrupted handler and both later retries retain a stable failure.
# Why: cancelling its first caller must neither cancel a duplicate's future nor execute the mutation again.
async def test_cancelled_live_handler_completes_duplicates_without_replay(tmp_path):
    j = Journal(tmp_path / "receipts.db", "OFFLINE001")
    try:
        cache = InboundCache(capacity=1, journal=j)
        entered, release = asyncio.Event(), asyncio.Event()
        frame = [2, "cancelled", "RemoteStopTransaction", {}]
        calls = []

        # An interrupted operation stays pending long enough for a real concurrent duplicate to share it.
        async def execute():
            calls.append(True)
            entered.set()
            await release.wait()
            return build_call_result(frame[1], {"status": "Accepted"})

        first = asyncio.create_task(cache.response("vendor", 1, frame, execute))
        await entered.wait()
        duplicate = asyncio.create_task(cache.response("vendor", 1, frame, execute))
        await asyncio.sleep(0)
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        failed = await duplicate
        assert decode(failed)[2] == "InternalError" and len(calls) == 1
        assert await cache.response("vendor", 1, frame, execute) == failed
        assert not cache._items and len(calls) == 1
    finally:
        j.close()


# Intent: a process replacement clears only obsolete connection receipts while preserving all physical uncertainty.
# Why: vendor epochs restart at one, but durable mutation and Start debt must never be reset with the reply cache.
async def test_new_cache_retires_connection_results_without_clearing_command_debt(tmp_path):
    j = Journal(tmp_path / "receipts.db", "OFFLINE001")
    try:
        cache = InboundCache(journal=j)
        frame = [2, "old-pending", "RemoteStartTransaction", {"idTag": "OLD"}]
        j.db.execute("INSERT INTO inbound_receipts VALUES('vendor',1,?,?,NULL)", (frame[1], fingerprint(frame[2:])))
        mutation = j.mutation_intent("vendor", "RemoteStartTransaction", frame[3])
        j.put("vendor_start_grant", {"token": "old-physical-grant"})
        j.put("uncertain_vendor_command", {"action": "RemoteStartTransaction", "digest": fingerprint(frame[3])})
        InboundCache(journal=j)
        assert not j.db.execute("SELECT 1 FROM inbound_receipts").fetchone()
        assert j.db.execute("SELECT state FROM mutations WHERE token=?", (mutation,)).fetchone()[0] == "pending"
        assert j.get("vendor_start_grant") == {"token": "old-physical-grant"}
        assert j.get("uncertain_vendor_command")["action"] == "RemoteStartTransaction"
        assert not cache._items
    finally:
        j.close()
