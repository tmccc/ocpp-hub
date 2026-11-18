"""Unavailable actual journal reads return no fabricated state and recover on the same management connection."""

import sqlite3
import uuid

import pytest
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings
from websockets.asyncio.client import connect

from tests.current_rig import adjustable
from tests.managed_rig import healthy
from tests.management_peer import TOKEN, call
from tests.runtime_rig import until
from tests.tls_support import tls as tls


# Intent: a real failed SQL read cannot return a cached successful status, guessed history or a lost receipt.
# Why: read-only callers need an honest temporary refusal and must recover without repeating a physical command.
@pytest.mark.parametrize(
    "command,table",
    [("status", "meta"), ("status", "transactions"), ("history", "transactions"), ("receipt", "managed_requests")],
)
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
                f"ws://127.0.0.1:{port}/management",
                additional_headers={"Authorization": "Bearer " + TOKEN},
                subprotocols=["ics-proxy.v1"],
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


# Intent: an accepted current update keeps its worker when SQLite refuses the first read after COMMIT.
# Why: the same socket must report uncertainty and allow a receipt lookup, and no stale Return survives disconnect.
async def test_current_reply_storage_and_cached_disconnect(tmp_path, tls, unused_tcp_port_factory, monkeypatch):
    async with adjustable(tmp_path, tls, unused_tcp_port_factory(), monkeypatch) as b:
        c, j = b.controller, b.session.journal
        published = {**await healthy(c), "running": True, "certificate_remaining_seconds": 86400}
        identifier, revision = uuid.uuid4().hex, c.requests.intent().revision
        committed, refused = False, []

        # Invalidate SQLite's prepared reads after the exact acceptance transaction.
        def authorize(action, table, column, database, trigger):
            if not refused and action == sqlite3.SQLITE_READ and table in ("meta", "managed_requests"):
                refused.append(table)
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        # Arm the refused read only for the commit that inserted this current selection.
        def trace(sql):
            nonlocal committed
            if sql.startswith("INSERT INTO managed_current_requests"):
                committed = True
            if committed and sql == "COMMIT" and not refused:
                j.db.set_authorizer(authorize)

        port = unused_tcp_port_factory()
        api = ManagementServer(ManagementSettings(port, tmp_path / "unused-token", TOKEN), c, lambda: published)
        await api.start()
        try:
            async with connect(
                f"ws://127.0.0.1:{port}/management",
                additional_headers={"Authorization": "Bearer " + TOKEN},
                subprotocols=["ics-proxy.v1"],
            ) as socket:
                j.db.set_authorizer(authorize)
                refused_reply = await call(socket, "set_current", id=identifier, revision=revision, current_a=16)
                j.db.set_authorizer(None)
                assert refused_reply == {"id": identifier, "ok": False, "error": "not_ready"}
                assert c.current.worker is None and c.requests.receipt(identifier)["state"] == "not_found"
                refused.clear()
                j.db.set_trace_callback(trace)
                reply = await call(socket, "set_current", id=identifier, revision=revision, current_a=16)
                j.db.set_trace_callback(None)
                j.db.set_authorizer(None)
                assert refused == ["managed_requests"]
                assert reply == {"id": identifier, "ok": False, "error": "outcome_unknown"}
                await c.current.settle()
                receipt = await call(socket, "receipt", request_id=identifier)
                assert receipt["result"]["state"] == "completed"
                assert (await call(socket, "set_current", id=identifier, revision=revision, current_a=16)) == {
                    "id": identifier,
                    "ok": True,
                    "result": receipt["result"],
                }
                assert b.rig.bench.firmware.effective_limit() == 16
                c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
                published.update(await healthy(c))
                before = (await call(socket, "status"))["result"]
                assert before["healthy"] and before["can_return"]
                await b.charger.close()
                await until(lambda: not b.session.runtime.status.charger_connected)
                after = (await call(socket, "status"))["result"]
                assert after["checked_at"] == before["checked_at"]
                assert after["control_revision"] == before["control_revision"]
                assert not any(after[key] for key in ("healthy", "local_hold_verified", "can_return", "boot_verified"))
        finally:
            j.db.set_authorizer(None)
            j.db.set_trace_callback(None)
            await api.close()
