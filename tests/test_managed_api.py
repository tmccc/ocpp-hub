"""Authenticated real management sockets against the real controller and independent charger/native peers."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import ssl
import uuid
from contextlib import AsyncExitStack
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from ocpp_hub_core.prototype import managed_api
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings
from ocpp_hub_core.prototype.store import RecoveryRequiredError
from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus

from tests.current_rig import adjustable
from tests.managed_rig import healthy, introduced, managed_bench, managed_session, starts
from tests.management_peer import TOKEN, call
from tests.runtime_rig import until
from tests.tls_support import tls as tls


# Intent: a queued Pause wins against an earlier unprocessed Current request at the same observed revision.
# Why: ordinary work cannot acquire charger authority ahead of waiting restrictive work.
async def test_management_pause_overtakes_queued_current(tmp_path, tls, unused_tcp_port_factory, monkeypatch):
    async with adjustable(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        c = b.controller
        before = list(b.rig.bench.received_limits)
        api = ManagementServer(ManagementSettings(19001, tmp_path / "unused", TOKEN), c, lambda: {})
        current, pause = uuid.uuid4().hex, uuid.uuid4().hex
        revision = c.requests.intent().revision
        ordinary = asyncio.create_task(
            api.dispatch(json.dumps({"id": current, "command": "set_current", "revision": revision, "current_a": 16})),
            eager_start=True,
        )
        restrictive = asyncio.create_task(
            api.dispatch(json.dumps({"id": pause, "command": "pause", "revision": revision})), eager_start=True
        )
        assert (await restrictive)["result"]["state"] == "accepted"
        with pytest.raises(RecoveryRequiredError):
            await ordinary
        assert c.requests.receipt(current)["state"] == "not_found"
        assert (await healthy(c))["local_hold_verified"]
        assert c.requests.receipt(pause)["state"] == "completed"
        assert b.rig.bench.received_limits[len(before) :] == [0]


# Intent: an ordinary client at its read limit leaves an authenticated owner's Pause available.
# Why: dashboard refreshes must never consume the separate budget for physically restricting the charger.
async def test_management_status_limit_leaves_pause_available(tmp_path, tls, unused_tcp_port_factory, monkeypatch):
    async with adjustable(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        c = b.controller
        published = {**await healthy(c), "running": True, "certificate_remaining_seconds": 86400}
        port = unused_tcp_port_factory()
        api = ManagementServer(ManagementSettings(port, tmp_path / "unused", TOKEN), c, lambda: published)
        await api.start()
        options = {
            "additional_headers": {"Authorization": "Bearer " + TOKEN},
            "subprotocols": ["ics-proxy.v1"],
            "proxy": None,
        }
        url = f"ws://127.0.0.1:{port}/management"
        try:
            async with connect(url, **options) as dashboard, connect(url, **options) as owner:
                before = list(b.rig.bench.received_limits)
                identifier = uuid.uuid4().hex
                now = 10.0
                with monkeypatch.context() as clock:
                    clock.setattr(managed_api, "time", SimpleNamespace(monotonic=lambda: now))
                    for _ in range(30):
                        assert (await call(dashboard, "status"))["ok"]
                    assert await call(dashboard, "status") == {"ok": False, "error": "not_ready"}
                    assert b.rig.bench.received_limits == before
                    pause = await call(owner, "pause", id=identifier, revision=c.requests.intent().revision)
                    assert pause["ok"] and pause["result"]["state"] == "accepted"
                    assert await call(dashboard, "status") == {"ok": False, "error": "not_ready"}
                    now += 2.0
                    assert (await healthy(c))["local_hold_verified"]
                    receipt = (await call(owner, "receipt", request_id=identifier))["result"]
                    assert receipt["state"] == "completed"
                assert b.rig.bench.received_limits[len(before) :] == [0]
                assert not starts(b.rig)
        finally:
            await api.close()


# Intent: four reusable idle sockets cannot prevent an authenticated owner from pausing at once.
# Why: the reserved Pause budget only helps after the owner can reach the queue.
async def test_idle_management_connections_leave_pause_available(tmp_path, tls, unused_tcp_port_factory, monkeypatch):
    async with adjustable(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        c = b.controller
        published = {**await healthy(c), "running": True, "certificate_remaining_seconds": 86400}
        port = unused_tcp_port_factory()
        api = ManagementServer(ManagementSettings(port, tmp_path / "unused", TOKEN), c, lambda: published)
        await api.start()
        options = {
            "additional_headers": {"Authorization": "Bearer " + TOKEN},
            "subprotocols": ["ics-proxy.v1"],
            "proxy": None,
        }
        url = f"ws://127.0.0.1:{port}/management"
        try:
            async with AsyncExitStack() as clients:
                idle = [await clients.enter_async_context(connect(url, **options)) for _ in range(4)]
                for client in idle:
                    assert (await call(client, "status"))["ok"]
                await until(lambda: len(api.idle) == 4)
                identifier = uuid.uuid4().hex
                async with connect(url, **options) as owner:
                    reply = await call(owner, "pause", id=identifier, revision=c.requests.intent().revision)
                    assert reply["ok"] and reply["result"]["state"] == "accepted"
                    assert (await healthy(c))["local_hold_verified"]
                    assert c.requests.receipt(identifier)["state"] == "completed"
                    await asyncio.wait_for(idle[0].wait_closed(), 2)
                    assert (await call(idle[-1], "status"))["ok"]
        finally:
            await api.close()


# Intent: authenticated status/history are passive, and duplicate Pause is one durable/physical operation.
# Why: UI refreshes and a lost network response must never introduce charger writes or repeat control.
async def test_real_management_socket_reads_and_idempotent_pause(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port_factory: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with managed_bench(tmp_path, tls, unused_tcp_port_factory()) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "api-boot")
            published = {**await healthy(controller), "running": True, "certificate_remaining_seconds": 86400}
            port = unused_tcp_port_factory()
            api = ManagementServer(
                ManagementSettings(port, tmp_path / "unused-secret", TOKEN), controller, lambda: published
            )
            await api.start()
            try:
                async with connect(
                    f"ws://127.0.0.1:{port}/management",
                    additional_headers={"Authorization": "Bearer " + TOKEN},
                    subprotocols=["ics-proxy.v1"],
                ) as socket:
                    before = list(rig.bench.received_limits)
                    status = (await call(socket, "status"))["result"]
                    assert status["healthy"] and status["api_version"] == 1
                    assert (await call(socket, "history"))["result"] == {"sessions": []}
                    assert rig.bench.received_limits == before
                    retired = await call(socket, "temporary_schedule", revision=0, start_epoch=1794384000)
                    assert retired["ok"] is False and retired["error"] == "invalid_request"
                    assert controller.requests.intent().revision == 0
                    assert session.journal.db.execute("SELECT COUNT(*) FROM managed_requests").fetchone()[0] == 0
                    assert rig.bench.received_limits == before
                    request = uuid.uuid4().hex
                    first = await call(socket, "pause", id=request, revision=0)
                    second = await call(socket, "pause", id=request, revision=0)
                    assert first["ok"] and second["ok"]
                    assert controller.requests.intent().revision == 1
                    pending = (await call(socket, "status"))["result"]
                    assert pending["desired_control"] == "paused" and not pending["healthy"]
                    published = {**await healthy(controller), "running": True, "certificate_remaining_seconds": 86400}
                    assert (await call(socket, "receipt", request_id=request))["result"]["state"] == "completed"
                    status = (await call(socket, "status"))["result"]
                    assert status["healthy"] and status["local_hold_verified"] and status["can_return"]

                    # Fail the real delivery pump after the last complete poll.
                    async def refuse_flush():
                        raise sqlite3.OperationalError("delivery storage unavailable")

                    runtime = session.runtime.status
                    with monkeypatch.context() as blocked:
                        blocked.setattr(session.gateway, "flush", refuse_flush)
                        await until(lambda: runtime.delivery_blocked_cause == "storage")
                        waiting = (await call(socket, "status"))["result"]
                        assert waiting["checked_at"] == status["checked_at"]
                        assert waiting["local_hold_verified"] and waiting["cloud_connected"]
                        assert not waiting["owner_control_enabled"]
                        assert not waiting["healthy"] and not waiting["local_healthy"]
                        assert not waiting["can_return"] and not waiting["can_start"]
                        assert waiting["state"] == "degraded"
                    await until(lambda: not runtime.delivery_blocked)
                    assert (await call(socket, "status"))["result"]["can_return"]
                    assert rig.bench.received_limits[len(before) :] == [0] and not starts(rig)
                    await socket.send(
                        '{"id":"' + uuid.uuid4().hex + '","command":"pause","command":"app_control","revision":1}'
                    )
                    assert json.loads(await socket.recv()) == {"ok": False, "error": "invalid_request"}
                    assert controller.requests.intent().mode == "paused"
            finally:
                await api.close()


# Intent: missing/wrong/duplicate credentials, browser origins and modified paths cannot reach command dispatch.
# Why: only the same-host authenticated HA client is permitted to request control or inspect private status.
@pytest.mark.parametrize("case", ["missing", "wrong", "duplicate", "origin", "query"])
async def test_management_rejects_unauthorized_handshakes_without_mutation(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port_factory: Any,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    async with managed_bench(tmp_path, tls, unused_tcp_port_factory()) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        port = unused_tcp_port_factory()
        api = ManagementServer(ManagementSettings(port, tmp_path / "unused-secret", TOKEN), controller, lambda: {})
        await api.start()
        before = session.journal.get("control")
        headers = [] if case == "missing" else [("Authorization", "Bearer " + ("b" * 64 if case == "wrong" else TOKEN))]
        if case == "duplicate":
            headers.append(("Authorization", "Bearer " + TOKEN))
        path = "/management?command=pause" if case == "query" else "/management"
        try:
            with pytest.raises(InvalidStatus):
                async with connect(
                    f"ws://127.0.0.1:{port}{path}",
                    additional_headers=headers,
                    origin="http://untrusted.invalid" if case == "origin" else None,
                    subprotocols=["ics-proxy.v1"],
                ):
                    pass
            assert session.journal.get("control") == before
            assert session.journal.db.execute("SELECT COUNT(*) FROM managed_requests").fetchone()[0] == 0
            assert rig.bench.device_calls == []
        finally:
            await api.close()


# Intent: arbitrary RPC, Start, extra arguments and bad revisions are refused without changing durable intent.
# Why: authentication grants only the narrow management operations, not unrestricted charger administration.
@pytest.mark.parametrize(
    "payload",
    [
        {"command": "start"},
        {"command": "Reset"},
        {"command": "pause", "revision": True},
        {"command": "pause", "revision": -1},
        {"command": "pause", "revision": 0, "extra": 1},
        {"command": "app_control", "revision": "0"},
    ],
)
async def test_management_validation_precedes_durable_mutation(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    payload: dict[str, Any],
) -> None:
    async with managed_bench(tmp_path, tls, unused_tcp_port) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        api = ManagementServer(ManagementSettings(19001, tmp_path / "unused", TOKEN), controller, lambda: {})
        from ocpp_hub_core.prototype.security import SecurityError

        with pytest.raises(SecurityError):
            await api.dispatch(json.dumps({"id": uuid.uuid4().hex, **payload}))
        assert controller.requests.intent().mode == "app"
        assert session.journal.db.execute("SELECT COUNT(*) FROM managed_requests").fetchone()[0] == 0
