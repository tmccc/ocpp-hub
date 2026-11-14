"""Unavailable actual journal reads return no fabricated state and recover on the same management connection."""

import sqlite3
import uuid

import pytest
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings
from websockets.asyncio.client import connect

from tests.test_managed_api import TOKEN, call
from tests.test_managed_current import adjustable
from tests.test_managed_proxy import healthy
from tests.test_prototype_tls import tls as tls


# Intent: a real failed SQL read cannot return a cached successful status, guessed history or a lost receipt.
# Why: read-only callers need an honest temporary refusal and must recover without repeating a physical command.
@pytest.mark.parametrize("command,table", [
    ("status", "meta"), ("status", "transactions"), ("history", "transactions"), ("receipt", "managed_requests")
])
async def test_management_read_storage_refusal_recovers_same_socket(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, command, table
):
    async with adjustable(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        c, j = b.controller, b.session.journal
        pause = uuid.uuid4().hex
        c.command(pause, "pause", c.requests.intent().revision)
        published = {**await healthy(c), "running": True, "certificate_remaining_seconds": 86400}
        before = c.requests.intent(), j.get("control"), list(b.rig.bench.device_calls)
        attempts = []

        # Refuse SQLite's real table read, including already prepared statements, without replacing engine methods.
        def authorize(action, first, second, database, trigger):
            if action == sqlite3.SQLITE_READ and first == table:
                attempts.append((first, second))
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        port = unused_tcp_port_factory()
        api = ManagementServer(ManagementSettings(port, tmp_path / "unused-token", TOKEN), c, lambda: published)
        await api.start()
        try:
            async with connect(
                f"ws://127.0.0.1:{port}/management", additional_headers={"Authorization": "Bearer " + TOKEN},
                subprotocols=["ics-proxy.v1"]
            ) as socket:
                assert (await call(socket, "status"))["result"]["healthy"]
                j.db.set_authorizer(authorize)
                try:
                    response = await call(socket, command, **({"request_id": pause} if command == "receipt" else {}))
                    assert attempts
                    assert response == {"ok": False, "error": "not_ready"}
                finally:
                    j.db.set_authorizer(None)
                restored = (await call(socket, "status"))["result"]
                assert restored["healthy"] and restored["desired_control"] == "paused"
                assert restored["control_revision"] == before[0].revision
                assert (await call(socket, "history"))["result"] == {"sessions": []}
                assert (await call(socket, "receipt", request_id=pause))["result"]["state"] == "completed"
                assert (c.requests.intent(), j.get("control"), b.rig.bench.device_calls) == before
                assert b.rig.bench.firmware.effective_limit() == 0
        finally:
            j.db.set_authorizer(None)
            await api.close()
