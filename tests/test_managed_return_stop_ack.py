"""Real managed Return while a genuine cloud Stop acknowledgement is delayed."""

import asyncio
import json
import uuid
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from ocpp_hub_core.frames import build_call_error

from tests.charge_rig import ChargeRig
from tests.current_rig import release_bench
from tests.managed_rig import healthy, starts
from tests.native_schedule_support import timer_bench
from tests.prototype_rig import START
from tests.recovery_barriers import completed_control
from tests.runtime_rig import until
from tests.tls_support import tls as tls
from tests.trial_peer import Bench


@pytest.mark.parametrize("closing_read", [False, True], ids=["initial-read", "closed-zero-read"])
async def test_return_waits_for_replaced_cloud_before_positive_send(
    tmp_path, tls, unused_tcp_port, monkeypatch, closing_read
):
    async with release_bench(tmp_path, tls, unused_tcp_port, 6) as b:
        c, runtime = b.c, b.session.runtime
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        identifier = uuid.uuid4().hex
        revision = c.requests.intent().revision
        c.command(identifier, "app_control", revision)
        read = c.native.read_result
        before = list(b.rig.bench.received_limits)
        native_reads = []

        async def disconnect_after_read(*, closed=False):
            result = await read(closed=closed)
            native_reads.append(closed)
            if closed == closing_read:
                assert runtime.status.vendor_connected and b.session.gateway.vendor_registration == "Accepted"
                await b.rig.bench.cloud.close()
                await until(lambda: not runtime.status.vendor_connected)
            return result

        with monkeypatch.context() as patch:
            patch.setattr(runtime, "vendor_enabled", lambda: False)
            patch.setattr(c.native, "read_result", disconnect_after_read)
            await c.return_requested(identifier, revision)
            assert native_reads == ([False, True] if closing_read else [False])
            assert c.requests.receipt(identifier)["state"] == "accepted"
            await c.return_requested(identifier, revision)
            assert c.requests.receipt(identifier)["state"] == "accepted"
            assert native_reads == ([False, True] if closing_read else [False])
            assert b.rig.bench.received_limits == before and b.rig.bench.firmware.effective_limit() == 0
            assert not b.session.journal.fault.latched()

        test = ChargeRig(b.rig, b.session, c, b.charger)
        await completed_control(test, identifier, mode="app", revision=revision + 1, ceiling=6)
        assert c.requests.receipt(identifier)["state"] == "completed"


# Intent: one accepted Return waits at zero for a known Stop ACK and completes without a new UUID.
# Why: pending delivery of an already closed mapped session does not imply an ambiguous positive write.
@pytest.mark.parametrize("outcome", ["fast", "delayed", "with_observation", "new_pause", "rejected"])
async def test_return_with_pending_stop_ack(tmp_path, tls, unused_tcp_port, monkeypatch, outcome):
    delayed = outcome != "fast"
    stop_seen, release_ack = asyncio.Event(), asyncio.Event()
    targets = set()
    vendor = Bench.vendor
    if not delayed:
        release_ack.set()

    class GateConnection:
        # Keep the actual fake cloud responsible for mapping and ordinary replies.
        def __init__(self, connection):
            self.connection = connection

        # Observe the genuine Stop request on the transport.
        def __aiter__(self):
            # Receive real frames without reading engine pending-call state.
            async def frames():
                async for raw in self.connection:
                    frame = json.loads(raw)
                    if frame[0] == 2 and frame[2] == "StopTransaction":
                        targets.add(frame[1])
                        stop_seen.set()
                    yield raw

            return frames()

        # Delay only the actual correlated Stop acknowledgement.
        async def send(self, raw):
            frame = json.loads(raw)
            if frame[0] == 3 and frame[1] in targets:
                async with asyncio.timeout(5):
                    await release_ack.wait()
                if outcome == "rejected":
                    raw = build_call_error(frame[1], "InternalError", "synthetic rejected Stop")
            await self.connection.send(raw)

    # Use the existing independent backend through this transport-only gate.
    async def gated_vendor(bench, connection):
        await vendor(bench, GateConnection(connection))

    monkeypatch.setattr(Bench, "vendor", gated_vendor)
    report = {"delayed": delayed, "stages": []}
    try:
        async with timer_bench(tmp_path, tls, unused_tcp_port) as b:
            j, g = b.session.journal, b.session.gateway
            g.policy = replace(g.policy, allowed_tags=frozenset({"APP_OWNER", *g.policy.allowed_tags}))
            g.controls.policy = g.policy
            identifier = uuid.uuid4().hex
            b.c.command(identifier, "start", b.c.requests.intent().revision)
            await b.c.charging.settle()
            tag = starts(b.rig)[0][3]["idTag"]
            reply = await b.rig.bench.call(
                b.sock,
                "actual-owner",
                "StartTransaction",
                START | {"idTag": tag, "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat()},
            )
            local = reply[2]["transactionId"]
            b.rig.peer.plan_session = (local, 987654)
            b.rig.peer.paused = False
            b.rig.peer.permission_enabled = True
            b.rig.bench.cp, b.rig.bench.connection = "C", "Charging"
            b.device.requested_current = 32
            await healthy(b.c)
            await until(lambda: j.next_delivery() is None and not g.vendor._pending)
            pause, returning = uuid.uuid4().hex, uuid.uuid4().hex
            b.c.command(pause, "pause", b.c.requests.intent().revision)
            await b.c.pause_task
            b.c.command(returning, "app_control", b.c.requests.intent().revision)
            await b.close_transaction(local)
            await asyncio.wait_for(stop_seen.wait(), 3)
            if outcome == "with_observation":
                observation = {"connectorId": 1, "errorCode": "NoError", "status": "Finishing"}
                assert (await b.rig.bench.call(b.sock, "status-after-stop", "StatusNotification", observation))[0] == 3
                assert j.db.execute("SELECT 1 FROM outbox WHERE action='StatusNotification'").fetchone()
            if not delayed:
                await until(lambda: j.next_delivery() is None and not g.vendor._pending)

            # Retain diagnostic snapshots for assertion failures without external file paths.
            def snapshot(label, status=None):
                report["stages"].append(
                    {
                        "label": label,
                        "status": status,
                        "pause": b.c.requests.receipt(pause),
                        "return": b.c.requests.receipt(returning),
                        "control": j.get("control"),
                        "owner": j.get("local_start_receipt"),
                        "outbox": [dict(row) for row in j.db.execute("SELECT * FROM outbox")],
                        "release_failure": j.get("managed_release_failure"),
                        "native_failure": j.get("managed_native_failure"),
                        "phase": j.get("trial_phase"),
                        "limit": b.device.effective_limit(),
                    }
                )

            snapshot("before_ticks")
            for tick in range(3):
                status = await b.c.tick()
                snapshot(f"tick_{tick}", status)
                if delayed:
                    assert b.c.requests.receipt(returning)["state"] == "accepted", report
                    assert b.device.effective_limit() == 0
            if outcome == "new_pause":
                b.c.command(uuid.uuid4().hex, "pause", b.c.requests.intent().revision)
                await b.c.pause_task
                assert b.c.requests.receipt(returning)["state"] == "failed"
            release_ack.set()
            if outcome == "rejected":
                await until(lambda: j.next_delivery() is not None and j.next_delivery().state == "rejected")
                await b.c.tick()
                assert b.c.requests.receipt(returning)["state"] != "completed"
                assert b.device.effective_limit() == 0 and len(starts(b.rig)) == 1
                await b.c.charging.close()
                return
            await until(lambda: j.next_delivery() is None and not g.vendor._pending)
            status = await healthy(b.c)
            snapshot("after_cloud_ACK", status)
            assert b.c.requests.receipt(returning)["state"] == ("failed" if outcome == "new_pause" else "completed"), (
                report
            )
            assert b.device.effective_limit() == (0 if outcome == "new_pause" else 6)
            assert len(starts(b.rig)) == 1
            await b.c.charging.close()
    finally:
        release_ack.set()
