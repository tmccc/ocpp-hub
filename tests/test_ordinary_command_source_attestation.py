"""Recovery reuses exact confirmed manual-command source evidence without borrowing timer identity."""

import asyncio
import uuid
from datetime import UTC, datetime

import pytest

from tests.managed_rig import healthy, pending_cloud_offer, starts
from tests.native_schedule_support import timer_bench
from tests.prototype_rig import START
from tests.runtime_rig import until
from tests.tls_support import tls as tls


# Intent: exact manual source survives a real native outage; absent, changed and contradictory sources stay zero.
# Why: acceptance and recovery must share command provenance without turning arbitrary native numbers into authority.
@pytest.mark.parametrize(
    "source", ["distinct", "distinct_marker", "distinct_changed", "shared", "absent", "foreign", "changed"]
)
async def test_manual_command_source_is_stable_across_native_outage(
    tmp_path, tls, unused_tcp_port, monkeypatch, source
):
    async with timer_bench(tmp_path, tls, unused_tcp_port) as b:
        c, j = b.c, b.session.journal
        c.command(uuid.uuid4().hex, "set_current", c.requests.intent().revision, current_a=32)
        await c.current.settle()
        await healthy(c)
        shared = source in ("shared", "absent", "changed")
        tag = "NATIVE_TIMER" if shared else "CLOUD_OWNER"
        reply = await b.rig.bench.cloud_call(
            "owner-command", "RemoteStartTransaction", {"connectorId": 1, "idTag": tag}
        )
        assert reply[0] == 3 and reply[2]["status"] == "Accepted"
        opened = await b.rig.bench.call(
            b.sock,
            "owner-report",
            "StartTransaction",
            START | {"idTag": tag, "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()},
        )
        local = opened[2]["transactionId"]
        b.rig.peer.plan_session = (
            (local, 987659)
            if source in ("shared", "changed", "distinct_marker", "distinct_changed")
            else (local, 9)
            if source == "foreign"
            else None
        )
        b.rig.peer.paused, b.rig.peer.permission_enabled = False, True
        b.rig.bench.cp, b.rig.bench.connection = "C", "Charging"
        b.device.requested_current = 32
        await until(lambda: j.vendor_id(local) is not None and j.next_delivery() is None)
        if source == "absent":
            await pending_cloud_offer(c, 32)
        else:
            await healthy(c)
        grant = j.get("vendor_start_grant")
        if source in ("shared", "changed"):
            assert grant["local"] == local
        elif source == "absent":
            assert grant.get("local") is None
        original_port = b.session.prepared.observer.port

        # The independent endpoint closes genuine native reads, leaving charger/cloud TLS paths intact.
        async def unavailable(reader, writer):
            await reader.readuntil(b"\r\n\r\n")
            writer.close()
            await writer.wait_closed()

        async with await asyncio.start_server(unavailable, "127.0.0.1", 0) as server:
            b.session.prepared.observer.port = server.sockets[0].getsockname()[1]
            try:
                for _ in range(3):
                    await c.tick()
            finally:
                b.session.prepared.observer.port = original_port
        assert b.device.effective_limit() == 0 and not b.stops()
        if source in ("changed", "distinct_changed"):
            b.rig.peer.plan_session = (local, 987660)
        for _ in range(5):
            await c.tick()
        assert b.device.effective_limit() == (32 if source in ("distinct", "distinct_marker", "shared") else 0)
        assert j.active() == local and j.get("vendor_start_grant") == grant
        assert len(starts(b.rig)) == 1 and not b.stops()
