"""Vendor authority and exact-id recovery use real TLS and independent native HTTP peers."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from ocpp_hub_core.frames import build_call
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.managed import ManagedController

import tests.test_managed_proxy as managed_helpers
import tests.test_native_bounded_session as native_bench
import tests.test_trial_session as independent_peer
from tests.handover_helpers import NativeVehiclePeer
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session, starts
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


class GatedNativePeer(NativeVehiclePeer):
    """The independent HTTP endpoint can delay its complete response without altering engine decisions."""

    # Ordinary native contents remain owned by the fake device while the test controls only transport timing.
    def __init__(self, bench: Any) -> None:
        super().__init__(bench)
        self.gate_enabled = False
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    # Hold the actual HTTP request until the independent race has occurred, then serve its normal native body.
    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self.gate_enabled:
            self.entered.set()
            await self.release.wait()
        await super().handle(reader, writer)


class CorrelatedOwnerBench(managed_helpers.OwnerBench):
    """The independent fake cloud correlates actual replies without adding a polling delay per read."""

    # These futures belong only to the peer's wire receiver, never to the engine under test.
    def __init__(self) -> None:
        super().__init__()
        self.vendor_replies: dict[str, asyncio.Future[list[Any]]] = {}

    # Keep the original fake cloud responses while observing its actual incoming correlated response frames.
    async def vendor(self, connection: Any) -> None:
        pending = self.vendor_replies

        class Observed:
            # Delegate the real websocket transport unchanged.
            def __getattr__(self, name: str) -> Any:
                return getattr(connection, name)

            # Return an asynchronous view of the real received frames.
            def __aiter__(self) -> Any:
                return self.frames()

            # Correlate replies before the ordinary independent peer records and processes the same raw frame.
            async def frames(self) -> Any:
                async for raw in connection:
                    frame = decode(raw)
                    if frame[0] in (3, 4) and frame[1] in pending:
                        future = pending.pop(frame[1])
                        if not future.done():
                            future.set_result(frame)
                    yield raw

        await super().vendor(Observed())

    # Send one real vendor request and await the actual reply received by its independent TLS reader.
    async def actual_cloud_call(self, token: str, action: str, payload: dict[str, Any]) -> list[Any]:
        future = asyncio.get_running_loop().create_future()
        self.vendor_replies[token] = future
        await self.cloud.send(build_call(token, action, payload))
        return await asyncio.wait_for(future, 2)


# Intent: expiry, a retired originating vendor and a newer Pause revoke a Start while real native HTTP is awaited.
# Why: fresh physical evidence does not preserve time-dependent or connection-specific permission to dispatch.
@pytest.mark.parametrize("crossed", ["expiry", "vendor", "pause"])
async def test_vendor_start_rechecks_authority_after_native_http(tmp_path, tls, unused_tcp_port, monkeypatch, crossed):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", GatedNativePeer)
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        c = ManagedController(session)
        rig.route_to_trial()
        try:
            async with rig.charger(session) as charger:
                await introduced(rig, session, charger, "final-authority-boot")
                await healthy(c)
                if crossed == "expiry":
                    # This changes only the independent vendor's actual Authorize reply.
                    def short_info():
                        return {
                            "status": "Accepted",
                            "expiryDate": (datetime.now(UTC) + timedelta(seconds=0.2)).isoformat(),
                        }

                    monkeypatch.setattr(independent_peer, "info", short_info)
                g, j = session.gateway, session.journal
                old_socket, old_epoch = rig.bench.cloud, g._vendor_epoch
                token = "superseded-start"
                rig.peer.gate_enabled = True
                await old_socket.send(
                    build_call(token, "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"})
                )
                await asyncio.wait_for(rig.peer.entered.wait(), 2)
                assert not starts(rig) and j.get("vendor_start_grant")
                if crossed == "expiry":
                    await asyncio.sleep(0.25)
                    assert g.auth_info.get("CLOUD_OWNER")["status"] == "Expired"
                elif crossed == "vendor":
                    await old_socket.close()
                    await until(lambda: rig.bench.cloud is not old_socket and g.vendor_registration == "Accepted")
                    assert g._vendor_epoch != old_epoch
                else:
                    c.command("a" * 32, "pause", c.requests.intent().revision)
                rig.peer.gate_enabled = False
                rig.peer.release.set()
                await g.drain()
                if c.pause_task is not None:
                    await c.pause_task
                assert not starts(rig)
                assert not j.get("vendor_start_grant")
                assert not j.db.execute("SELECT 1 FROM mutations WHERE origin='vendor'").fetchone()
                if crossed == "vendor":
                    assert not any(f[0] != 2 and f[1] == token for f in rig.bench.calls)
                else:
                    await until(lambda: any(f[0] == 4 and f[1] == token for f in rig.bench.calls))
                if crossed == "pause":
                    assert c.requests.intent().mode == "paused" and rig.bench.firmware.effective_limit() == 0
                else:
                    monkeypatch.setattr(
                        independent_peer,
                        "info",
                        lambda: {
                            "status": "Accepted",
                            "expiryDate": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
                        },
                    )
                    result = await rig.bench.cloud_call(
                        "new-valid-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}
                    )
                    assert result[0] == 3 and result[2] == {"status": "Accepted"}
                    assert len(starts(rig)) == 1
        finally:
            rig.peer.gate_enabled = False
            rig.peer.release.set()
            await session.close()


# Intent: one real HTTP503 yields a bounded failure that same-id retries receive without executing the Start.
# Why: expected native availability errors must not cancel a cached response or turn a retry into a physical command.
async def test_vendor_native503_has_stable_correlated_failure(tmp_path, tls, unused_tcp_port, monkeypatch):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        c = ManagedController(session)
        rig.route_to_trial()
        try:
            async with rig.charger(session) as charger:
                await introduced(rig, session, charger, "native503-reply-boot")
                await healthy(c)
                token = "native503-start"
                raw = build_call(token, "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"})
                rig.peer.unavailable = True
                await rig.bench.cloud.send(raw)
                async with asyncio.timeout(2):
                    assert (await rig.peer.failed_reads.get())["action"] == "getConfig"
                await until(lambda: any(f[0] == 4 and f[1] == token for f in rig.bench.calls), timeout=2)
                first = next(f for f in rig.bench.calls if f[0] == 4 and f[1] == token)
                assert first[2] == "InternalError" and not starts(rig)
                assert not session.journal.get("vendor_start_grant")
                assert not session.journal.db.execute("SELECT 1 FROM mutations WHERE origin='vendor'").fetchone()
                rig.peer.unavailable = False
                await rig.bench.cloud.send(raw)
                await until(lambda: len([f for f in rig.bench.calls if f[0] != 2 and f[1] == token]) == 2)
                assert [f for f in rig.bench.calls if f[0] != 2 and f[1] == token] == [first, first]
                assert not starts(rig)
                result = await rig.bench.cloud_call(
                    "fresh-native-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"}
                )
                assert result[0] == 3 and result[2] == {"status": "Accepted"} and len(starts(rig)) == 1
        finally:
            rig.peer.unavailable = False
            await session.close()


# Intent: cancellation after an actual Start dispatch cannot replay the request or discard its unknown outcome.
# Why: a late peer ACK cannot make a cancelled request id reusable for a second physical Start.
async def test_cancelled_dispatched_vendor_call_has_stable_no_replay_result(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        c = ManagedController(session)
        rig.route_to_trial()
        entered, release = asyncio.Event(), asyncio.Event()
        try:
            async with rig.charger(session) as charger:
                await introduced(rig, session, charger, "cancelled-call-boot")
                await healthy(c)
                g = session.gateway

                # The fake charger applies the received command, then withholds only its actual TLS acknowledgement.
                async def hold_ack(action, payload):
                    if action == "RemoteStartTransaction":
                        entered.set()
                        await release.wait()

                rig.bench.before_reply = hold_ack
                previous = set(g._tasks)
                token = "cancelled-wire-start"
                raw = build_call(token, "RemoteStartTransaction", {"connectorId": 1, "idTag": "CLOUD_OWNER"})
                await rig.bench.cloud.send(raw)
                await asyncio.wait_for(entered.wait(), 2)
                assert len(starts(rig)) == 1
                tasks = set(g._tasks) - previous
                assert len(tasks) == 1
                task = tasks.pop()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                release.set()
                rig.bench.before_reply = None
                await rig.bench.cloud.send(raw)
                await until(lambda: any(f[0] == 4 and f[1] == token for f in rig.bench.calls), timeout=2)
                first = next(f for f in rig.bench.calls if f[0] == 4 and f[1] == token)
                await rig.bench.cloud.send(raw)
                await until(lambda: len([f for f in rig.bench.calls if f[0] != 2 and f[1] == token]) == 2)
                assert [f for f in rig.bench.calls if f[0] != 2 and f[1] == token] == [first, first]
                assert len(starts(rig)) == 1 and session.journal.get("vendor_start_grant")
                assert [
                    r[0] for r in session.journal.db.execute("SELECT state FROM mutations WHERE origin='vendor'")
                ] == ["uncertain"]
        finally:
            release.set()
            await session.close()


# Intent: old harmless reads cannot deny the mapped cloud Stop or erase its completed exact-id outcome.
# Why: a healthy long-lived connection must keep accepting Stop after thousands of completed read requests.
async def test_vendor_read_history_does_not_exhaust_stop_capacity(tmp_path, tls, unused_tcp_port, monkeypatch):
    monkeypatch.setattr(managed_helpers, "OwnerBench", CorrelatedOwnerBench)
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        c = ManagedController(session)
        rig.route_to_trial()
        try:
            async with rig.charger(session) as charger:
                await introduced(rig, session, charger, "many-reads-boot")
                await healthy(c)
                await rig.bench.call(charger, "daily-authorize", "Authorize", {"idTag": "NATIVE_TIMER"})
                report = await rig.bench.call(
                    charger, "daily-start", "StartTransaction", START | {"idTag": "NATIVE_TIMER"}
                )
                local = report[2]["transactionId"]
                await until(
                    lambda: session.journal.vendor_id(local) is not None and session.journal.next_delivery() is None
                )
                rig.peer.paused = False
                rig.peer.permission_enabled = True
                rig.bench.cp, rig.bench.connection = "C", "Charging"
                rig.bench.firmware.fire_plan(9)
                assert (await healthy(c))["current_a"] == 6
                for index in range(4096):
                    response = await rig.bench.actual_cloud_call(
                        "read-" + str(index), "GetConfiguration", {"key": ["SupportedFeatureProfiles"]}
                    )
                    assert response[0] == 3
                assert session.journal.db.execute("SELECT COUNT(*) FROM managed_requests").fetchone()[0] == 0
                assert (await healthy(c))["owner_control_enabled"]
                vendor = session.journal.vendor_id(local)
                response = await rig.bench.actual_cloud_call(
                    "owner-stop", "RemoteStopTransaction", {"transactionId": vendor}
                )
                assert response[0] == 3 and response[2] == {"status": "Accepted"}
                duplicate = await rig.bench.actual_cloud_call(
                    "owner-stop", "RemoteStopTransaction", {"transactionId": vendor}
                )
                assert duplicate == response
                stops = [f for f in rig.bench.device_calls if f[2] == "RemoteStopTransaction"]
                assert len(stops) == 1 and stops[0][3] == {"transactionId": local}
                assert rig.bench.firmware.current == 0
                await rig.bench.call(
                    charger,
                    "actual-stop-report",
                    "StopTransaction",
                    {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
                )
                rig.peer.permission_enabled = False
                await until(lambda: session.journal.active() is None and session.journal.next_delivery() is None)
        finally:
            await session.close()
