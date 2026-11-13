"""Parent-owned physical/native/cloud peers and real managed-service subprocesses share no engine objects."""

import asyncio
import json
import os
import signal
import sqlite3
import sys
import uuid
from contextlib import asynccontextmanager, closing
from types import SimpleNamespace

from ocpp_hub_core.prototype.trial_session import save_private
from websockets.asyncio.client import connect

from tests.handover_helpers import crash
from tests.test_managed_api import TOKEN, call
from tests.test_managed_debt_reconciliation import strategy_file
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_prototype import BOOT, START


class ServiceProcess:
    """A child owns the real journal/controller; this parent owns only its independent peers and control transport."""

    # Allocate distinct management paths while preserving the same genuine journal and physical installation.
    def __init__(self, rig, directory, port):
        self.rig, self.directory, self.port = rig, directory, port
        self.process = self.socket = self.reader = self.collector = None
        self.events = asyncio.Queue()
        self.generation = 0
        self.kills = []
        save_private(directory / "process-management-token.json", {"token": TOKEN})
        save_private(
            directory / "process-management.json",
            {"host": "127.0.0.1", "port": port, "token_file": str(directory / "process-management-token.json")},
        )

    # Start the real service, then connect the independent verified-TLS device to its actual listener.
    async def start(self):
        self.generation += 1
        self.events = asyncio.Queue()
        log = (self.directory / f"process-{self.generation}.stderr.log").open("w")
        self.process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-B",
            "-m",
            "tests.process_service_child",
            str(self.directory / "trial.json"),
            str(self.directory / "process-strategy.json"),
            str(self.directory / "process-management.json"),
            str(self.rig.prepared.observer.port),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=log,
            env={**os.environ, "NOTIFY_SOCKET": "", "PYTHONDONTWRITEBYTECODE": "1"},
        )
        log.close()
        self.collector = asyncio.create_task(self.collect())
        async with asyncio.timeout(15):
            while True:
                assert self.process.returncode is None, self.error_log()
                try:
                    self.socket = await self.rig.bench.connection_to(SimpleNamespace(prepared=self.rig.prepared))
                    break
                except OSError:
                    await asyncio.sleep(0.05)
        self.reader = asyncio.create_task(self.rig.bench.read_charger(self.socket))
        assert (await self.rig.bench.call(self.socket, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3

    # Collect only test observation markers; service status text remains a separately retained transcript.
    async def collect(self):
        assert self.process.stdout is not None
        with (self.directory / f"process-{self.generation}.stdout.log").open("w") as log:
            while raw := await self.process.stdout.readline():
                log.write(raw.decode())
                value = json.loads(raw)
                if "test_event" in value:
                    await self.events.put(value)

    # A classified service failure should expose its actual private synthetic stderr when a bounded test fails.
    def error_log(self):
        return (self.directory / f"process-{self.generation}.stderr.log").read_text()[-6000:]

    # Use the actual authenticated management protocol; the parent never calls a controller method.
    async def request(self, command, **fields):
        async with connect(
            f"ws://127.0.0.1:{self.port}/management",
            additional_headers={"Authorization": "Bearer " + TOKEN},
            subprotocols=["ics-proxy.v1"],
            proxy=None,
        ) as socket:
            return await call(socket, command, **fields)

    # Wait for real published service evidence under its unchanged five-second supervisor cadence.
    async def status(self, predicate=lambda status: status.get("healthy") is True):
        async with asyncio.timeout(25):
            while True:
                assert self.process.returncode is None, self.error_log()
                result = await self.request("status")
                if result.get("ok") is True and predicate(result["result"]):
                    return result["result"]
                await asyncio.sleep(0.25)

    # The marker is acknowledged before the management mutation is submitted; no timing sleep selects the kill point.
    async def arm(self, phase, identifier):
        self.process.stdin.write(json.dumps({"phase": phase, "id": identifier}).encode() + b"\n")
        await self.process.stdin.drain()
        async with asyncio.timeout(3):
            value = await self.events.get()
            assert value == {"test_event": "armed", "phase": phase, "id": identifier}

    # Observe the exact blocked real SQL boundary and prove the marker belongs to this actual child/request.
    async def gate(self, phase, identifier):
        async with asyncio.timeout(15):
            value = await self.events.get()
        assert value["test_event"] == "gate" and value["phase"] == phase and value["id"] == identifier
        assert value["pid"] == self.process.pid
        return value

    # An independent read-only connection observes only durable committed facts, including a live WAL.
    def durable(self):
        with closing(sqlite3.connect("file:" + str(self.rig.prepared.config.journal) + "?mode=ro", uri=True)) as db:
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            metadata = {key: json.loads(value) for key, value in db.execute("SELECT key,value FROM meta")}
            requests = [
                dict(zip(("id", "action", "revision", "state", "result"), row))
                for row in db.execute("SELECT id,action,revision,state,result FROM managed_requests")
            ]
            db.row_factory = sqlite3.Row
            transactions = [dict(row) for row in db.execute("SELECT * FROM transactions")]
            outbox = [dict(row) for row in db.execute("SELECT * FROM outbox")]
            return {"metadata": metadata, "requests": requests, "transactions": transactions, "outbox": outbox}

    # Observe genuine committed cloud mapping and ACK drain without manufacturing delivery evidence.
    async def delivered(self, local, *, closed=False):
        async with asyncio.timeout(10):
            while True:
                snapshot = self.durable()
                row = next((row for row in snapshot["transactions"] if row["local"] == local), None)
                if (
                    row is not None
                    and row["vendor"] is not None
                    and (not closed or row["stop"] is not None)
                    and not any(item["local"] == local for item in snapshot["outbox"])
                ):
                    return row
                await asyncio.sleep(0.05)

    # Four actual HTTP failures establish protective zero; fresh owner and physical evidence establish repair.
    async def native_outage(self, expected, demand):
        peer, firmware = self.rig.peer, self.rig.bench.firmware
        while not peer.failed_reads.empty():
            peer.failed_reads.get_nowait()
        peer.unavailable = True
        try:
            async with asyncio.timeout(25):
                for _ in range(4):
                    assert (await peer.failed_reads.get())["action"] == "getConfig"
            await self.status(lambda value: value.get("fault") == "native_observation_unavailable")
            assert firmware.effective_limit() == 0
        finally:
            peer.unavailable = False
        await self.status(
            lambda value: (
                value.get("owner_control_enabled") is True
                and value.get("station_ceiling_a") == expected
                and value.get("current_a") == (expected if demand else 0)
                and firmware.effective_limit() == expected
            )
        )

    # Independent real charger reports and native power retain lifecycle supervision after process replacement.
    async def ordinary_recovery(self, expected, demand, *, start_minute=0):
        state = await self.status(lambda value: value.get("charger_connected") is True)
        assert (await self.request("pause", revision=state["control_revision"]))["ok"]
        state = await self.status(lambda value: value.get("local_hold_verified") is True)
        assert self.rig.bench.firmware.effective_limit() == 0
        assert (await self.request("app_control", revision=state["control_revision"]))["ok"]
        await self.status(
            lambda value: value.get("owner_control_enabled") is True and value.get("station_ceiling_a") == expected
        )
        locals_seen = []
        demands = demand if isinstance(demand, tuple) else (demand, demand)
        for epoch, demand in enumerate(demands):
            result = await self.rig.bench.call(
                self.socket,
                uuid.uuid4().hex,
                "StartTransaction",
                START
                | {
                    "idTag": "NATIVE_TIMER",
                    "meterStart": 2000 + epoch * 100,
                    "timestamp": f"2026-01-01T00:{start_minute + epoch * 2:02}:00Z",
                },
            )
            assert result[0] == 3 and result[2]["idTagInfo"]["status"] == "Accepted"
            local = result[2]["transactionId"]
            assert local not in locals_seen
            locals_seen.append(local)
            self.rig.peer.plan_session = (local, 9)
            self.rig.peer.paused, self.rig.peer.permission_enabled = False, demand
            self.rig.bench.cp, self.rig.bench.connection = ("C", "Charging") if demand else ("B", "Plug")
            self.rig.bench.firmware.fire_plan(9)
            self.rig.bench.firmware.requested_current = 32 if demand else 0
            await self.status(
                lambda value: (
                    value.get("owner_control_enabled") is True
                    and value.get("station_ceiling_a") == expected
                    and value.get("current_a") == (expected if demand else 0)
                    and self.rig.bench.firmware.effective_limit() == expected
                )
            )
            await self.native_outage(expected, demand)
            stop = await self.rig.bench.call(
                self.socket,
                uuid.uuid4().hex,
                "StopTransaction",
                {
                    "transactionId": local,
                    "meterStop": 2100 + epoch * 100,
                    "timestamp": f"2026-01-01T00:{start_minute + epoch * 2 + 1:02}:00Z",
                },
            )
            assert stop[0] == 3
            self.rig.peer.permission_enabled = False
            await self.delivered(local, closed=True)
            await self.status(lambda value: value.get("can_start") is True)
        assert len(locals_seen) == len(set(locals_seen)) == 2
        return locals_seen

    # SIGKILL bypasses all service finally/signal cleanup while the parent device/cloud/native peers survive.
    async def kill(self):
        pid = self.process.pid
        self.process.kill()
        assert await asyncio.wait_for(self.process.wait(), 5) == -signal.SIGKILL
        self.kills.append({"pid": pid, "generation": self.generation, "returncode": self.process.returncode})
        await self.disconnect()

    # Retire only parent-owned socket readers after death or ordinary final service shutdown.
    async def disconnect(self):
        if self.socket is not None:
            await self.socket.close()
        if self.reader is not None:
            self.reader.cancel()
            await asyncio.gather(self.reader, return_exceptions=True)
        if self.collector is not None:
            await self.collector
        self.socket = self.reader = self.collector = None

    # Final teardown may gracefully stop a surviving replacement; the tested death never uses this path.
    async def close(self):
        if self.process is not None and self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), 15)
            except TimeoutError:
                self.process.kill()
                await self.process.wait()
        await self.disconnect()


# Commission actual bounded authority once, then release journal ownership before any child is created.
@asynccontextmanager
async def process_rig(directory, tls, listener, management, monkeypatch, current=6):
    async with commissioned(directory, tls, listener, monkeypatch, current, cli_route=True) as original:
        rig = original.rig
        value = json.loads((directory / "trial.json").read_text())
        value["local_tags"] = ["APP_OWNER", "NATIVE_TIMER"]
        save_private(directory / "trial.json", value)
        digest = rig.spec.digest
        strategy = strategy_file(rig, directory)
        assert rig.spec.digest == digest
        save_private(directory / "process-strategy.json", json.loads(strategy.read_text()))
        await original.controller.tests.close()
        await crash(original.session)
        child = ServiceProcess(rig, directory, management)
        try:
            yield child
        finally:
            await child.close()
