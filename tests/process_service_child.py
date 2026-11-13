"""Test-only observation adapter around the real managed service; parent peers survive this process's SIGKILL."""

import asyncio
import json
import os
import signal
import sqlite3
import sys
from pathlib import Path

from tools import managed_proxy as service
from tools.offline_validate import network_guard

sys.addaudithook(network_guard)


class Gate:
    """An exact request-owned SQL boundary can freeze this process without running graceful engine cleanup."""

    phase = "none"
    identifier = ""
    fired = False

    # Emit only bounded synthetic identity/state, then wait for the parent's actual SIGKILL.
    @classmethod
    def freeze(cls, db, phase):
        if cls.fired or phase != cls.phase:
            return
        cls.fired = True
        print(
            json.dumps(
                {
                    "test_event": "gate",
                    "phase": phase,
                    "id": cls.identifier,
                    "pid": os.getpid(),
                    "transaction_open": db.in_transaction,
                }
            ),
            flush=True,
        )
        while True:
            signal.pause()


class ObservedConnection(sqlite3.Connection):
    """All actual SQL runs unchanged; before-statement and post-return observations never invent stored facts."""

    # Read only this actual connection's current private metadata without recursively entering the observation hook.
    def metadata(self, key):
        row = super().execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row is not None else {}

    # Pin each boundary to the exact real request before freezing the actual process.
    def checkpoint(self, sql, parameters, before):
        if Gate.phase == "none" or Gate.fired:
            return
        operation = sql.strip().upper()
        state = self.metadata("managed_test_charge")
        receipt = self.metadata("local_start_receipt")
        current = self.metadata("managed_current_change")
        request = super().execute("SELECT action,state FROM managed_requests WHERE id=?", (Gate.identifier,)).fetchone()
        owned = state.get("id") == Gate.identifier or current.get("id") == Gate.identifier or request is not None
        if not owned:
            return
        if before and operation.startswith("INSERT OR REPLACE INTO META") and parameters:
            value = json.loads(parameters[1])
            if parameters[0] == "local_start_receipt" and value.get("request_id") == Gate.identifier:
                if value.get("stage") == "prepared":
                    Gate.freeze(self, "before-prepared-sql")
            if parameters[0] == "managed_test_charge" and value.get("id") == Gate.identifier:
                if value.get("start_attempted") is True and state.get("start_attempted") is False:
                    Gate.freeze(self, "receipt-before-marker")
        if operation == "COMMIT":
            prefix = "before-" if before else "after-"
            if request is not None and request[0] == "app_control":
                if request[1] == "accepted":
                    Gate.freeze(self, prefix + "return-request-commit")
                if request[1] == "completed":
                    Gate.freeze(self, prefix + "return-success-commit")
            if receipt.get("request_id") == Gate.identifier:
                stage = receipt.get("stage")
                if stage in ("prepared", "issued", "accepted"):
                    Gate.freeze(self, prefix + stage + "-commit")
            if state.get("id") == Gate.identifier and state.get("start_rpc_accepted") is True:
                Gate.freeze(self, prefix + "managed-success-commit")
            if current.get("id") == Gate.identifier and current.get("state") == "completed":
                Gate.freeze(self, prefix + "current-success-commit")
            if before and receipt.get("request_id") != Gate.identifier:
                Gate.freeze(self, "before-request-commit")
            if not before and state.get("id") == Gate.identifier and state.get("start_attempted") is False:
                Gate.freeze(self, "after-request-commit")
            if not before and current.get("id") == Gate.identifier and current.get("state") == "pending":
                Gate.freeze(self, "after-current-request-commit")

    # Post-COMMIT means execute actually returned; SQLite's pre-statement trace alone cannot establish durability.
    def execute(self, sql, parameters=(), /):
        self.checkpoint(sql, parameters, True)
        result = super().execute(sql, parameters)
        self.checkpoint(sql, parameters, False)
        return result


# The inherited control pipe arms only observation; every real command still uses the authenticated management socket.
async def controls():
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=4096)
    transport, _ = await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
    try:
        while raw := await reader.readline():
            value = json.loads(raw)
            assert set(value) == {"phase", "id"} and len(raw) < 512
            Gate.phase, Gate.identifier, Gate.fired = value["phase"], value["id"], False
            print(json.dumps({"test_event": "armed", **value}), flush=True)
    finally:
        transport.close()


# Keep real service composition/signal handling; only the synthetic native port and SQLite observer are supplied.
async def main():
    config, spec, management, native_port = sys.argv[1:]
    original_preflight, original_connect = service.preflight, sqlite3.connect

    # NativeObserver normally uses port80; the parent owns an explicit ephemeral loopback endpoint instead.
    def preflight(path):
        prepared = original_preflight(path)
        prepared.observer.port = int(native_port)
        return prepared

    # Connection instrumentation delegates every SQL operation and preserves WAL/fsync/foreign-key behavior.
    def connect(*args, **kwargs):
        kwargs["factory"] = ObservedConnection
        return original_connect(*args, **kwargs)

    service.preflight, sqlite3.connect = preflight, connect
    reader = asyncio.create_task(controls())
    try:
        return await service.serve(Path(config), Path(spec), Path(management))
    finally:
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
