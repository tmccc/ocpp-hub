"""Authenticated real management sockets against the real controller and independent charger/native peers."""

from __future__ import annotations

import json
import ssl
import uuid
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings
from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus

from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session, starts
from tests.test_prototype_tls import tls as tls

TOKEN = "a" * 64  # Synthetic bearer used only by loopback test peers.


# Send one bounded protocol message and correlate its real socket response.
async def call(socket: Any, command: str, **fields: Any) -> dict[str, Any]:
    identifier = fields.pop("id", uuid.uuid4().hex)
    await socket.send(json.dumps({"id": identifier, "command": command, **fields}))
    value = json.loads(await socket.recv())
    assert value.get("id") == identifier or value.get("ok") is False
    return value


# Intent: authenticated status/history are passive, and duplicate Pause is one durable/physical operation.
# Why: UI refreshes and a lost network response must never introduce charger writes or repeat control.
async def test_real_management_socket_reads_and_idempotent_pause(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port_factory: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with managed_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as rig:
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
    async with managed_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as rig:
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
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        api = ManagementServer(ManagementSettings(19001, tmp_path / "unused", TOKEN), controller, lambda: {})
        from ocpp_hub_core.prototype.security import SecurityError

        with pytest.raises(SecurityError):
            api.dispatch(json.dumps({"id": uuid.uuid4().hex, **payload}))
        assert controller.requests.intent().mode == "app"
        assert session.journal.db.execute("SELECT COUNT(*) FROM managed_requests").fetchone()[0] == 0
