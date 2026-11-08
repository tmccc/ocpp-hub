"""Full offline supervisor exercised through real authenticated loopback TLS websocket peers."""

from __future__ import annotations

import asyncio
import sqlite3
import ssl
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.frames import build_call, build_call_result
from ocpp_hub_core.prototype.gateway import Gateway, decode
from ocpp_hub_core.prototype.rpc import RegistrationDeferredError
from ocpp_hub_core.prototype.runtime import OfflineRuntime, RuntimeSettings
from ocpp_hub_core.prototype.security import TRANSPORT_LOG, Admission, SecurityError
from ocpp_hub_core.prototype.store import Journal
from websockets.asyncio.client import connect
from websockets.asyncio.server import ServerConnection, serve

from tests.prototype_firmware import Firmware
from tests.test_prototype import BOOT, START
from tests.test_prototype_tls import accept_cap
from tests.test_prototype_tls import admission as admission
from tests.test_prototype_tls import tls as tls

NOW = "2030-01-01T00:00:00Z"
FAST = RuntimeSettings(
    retry_min=0.02, retry_max=0.1, pump_interval=0.01, ping_interval=0.2, ping_timeout=0.2, close_timeout=0.2
)


# Wait for an observable result with a single aggregate deadline; no test changes the engine under it.
async def until(check: Callable[[], bool], timeout: float = 5) -> None:
    async with asyncio.timeout(timeout):
        while not check():
            await asyncio.sleep(0.01)


class Bench:
    """One independent fake cloud and the actual supervisor, with configurable real peer replies."""

    # Allocate private real journal state; the only connection addresses are generated literal loopback ports.
    def __init__(
        self, path: Path, tls_pair: tuple[ssl.SSLContext, ssl.SSLContext, Path], credential: Admission
    ) -> None:
        self.server_tls, self.client_tls, _ = tls_pair
        self.admission = credential
        self.store = Journal(path, credential.identity)
        self.gateway = Gateway(self.store, timeout=0.15)
        self.calls: list[list[Any]] = []
        self.connections: list[ServerConnection] = []
        self.boot_status = "Accepted"
        self.boot_interval = 1
        self.drop_start = False
        self.firmware = Firmware()
        self.runtime: OfflineRuntime | None = None

    # The fake backend is a real websocket endpoint and never shares the gateway's correlation state.
    async def vendor(self, connection: ServerConnection) -> None:
        self.connections.append(connection)
        async for raw in connection:
            frame = decode(raw)
            self.calls.append(frame)
            if frame[0] != 2:
                continue
            if frame[2] == "BootNotification":
                answer = {"status": self.boot_status, "interval": self.boot_interval, "currentTime": NOW}
            elif frame[2] == "Heartbeat":
                answer = {"currentTime": NOW}
            elif frame[2] == "StartTransaction":
                if self.drop_start:
                    continue
                answer = {"transactionId": 9001, "idTagInfo": {"status": "Accepted"}}
            else:
                answer = {}
            await connection.send(build_call_result(frame[1], answer))

    # Start the real runtime only after the fake cloud's ephemeral port exists.
    async def start(self, port: int, settings: RuntimeSettings = FAST) -> OfflineRuntime:
        self.runtime = OfflineRuntime(
            self.gateway,
            self.admission,
            f"wss://127.0.0.1:{port}/ocpp",
            self.server_tls,
            self.client_tls,
            settings,
        )
        return await self.runtime.start()

    # Open an authenticated fake charger without starting any background work on the actual household.
    def charger(self) -> Any:
        assert self.runtime is not None
        return connect(
            f"wss://127.0.0.1:{self.runtime.port}/charger/{self.admission.identity}",
            ssl=self.client_tls,
            additional_headers=[("Authorization", self.admission.authorization)],
            subprotocols=["ocpp1.6"],
            logger=TRANSPORT_LOG,
            proxy=None,
        )

    # Supply a synthetic introduction; no actual Boot was available in the recorded device export.
    async def boot(self, charger: Any) -> None:
        await charger.send(build_call("boot", "BootNotification", BOOT))
        assert decode(await charger.recv())[2]["status"] == "Accepted"
        cap = decode(await charger.recv())
        assert cap[2] == "SetChargingProfile"
        await accept_cap(charger, cap, self.firmware)
        await until(lambda: self.gateway.controls.ready)

    # Keep runtime teardown before journal close so cancelled tasks never touch a closed database.
    async def close(self) -> None:
        if self.runtime is not None:
            await self.runtime.close()
        self.store.close()


# Intent: keep the backend online with negotiated OCPP heartbeats while the charger sends nothing.
# Why: websocket Ping/Pong alone cannot meet the backend's application heartbeat contract.
async def test_runtime_idle_heartbeats(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], admission: Admission
) -> None:
    bench = Bench(tmp_path / "journal.db", tls, admission)
    async with serve(
        bench.vendor, "127.0.0.1", 0, ssl=bench.server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
    ) as cloud:
        runtime = await bench.start(cloud.sockets[0].getsockname()[1])
        async with bench.charger() as charger:
            await bench.boot(charger)
            await until(lambda: runtime.status.heartbeats >= 3)
            assert runtime.status.charger_connected and runtime.status.boot_observed
            assert len([f for f in bench.calls if f[0] == 2 and f[2] == "BootNotification"]) == 1
    await bench.close()


# Intent: the cloud stops seeing a connected virtual charger once its actual local socket disappears.
# Why: an independent heartbeat task must not keep advertising stale charger availability indefinitely.
async def test_runtime_charger_loss_retires_vendor_session(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], admission: Admission
) -> None:
    bench = Bench(tmp_path / "journal.db", tls, admission)
    async with serve(
        bench.vendor, "127.0.0.1", 0, ssl=bench.server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
    ) as cloud:
        runtime = await bench.start(cloud.sockets[0].getsockname()[1])
        async with bench.charger() as charger:
            await bench.boot(charger)
            await until(lambda: bench.gateway.vendor_registration == "Accepted")
        await until(lambda: not runtime.status.charger_connected and not runtime.status.vendor_connected)
        count = runtime.status.reconnects
        await asyncio.sleep(0.15)
        assert runtime.status.reconnects == count
    await bench.close()


# Intent: a cloud socket which stops answering websocket Ping is replaced while the local charger stays usable.
# Why: an open TCP connection with no Pong must not leave the reconnect supervisor stuck forever.
async def test_runtime_missing_pong_reconnects_without_losing_local_control(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], admission: Admission
) -> None:
    bench = Bench(tmp_path / "journal.db", tls, admission)
    async with serve(
        bench.vendor,
        "127.0.0.1",
        0,
        ssl=bench.server_tls,
        subprotocols=["ocpp1.6"],
        logger=TRANSPORT_LOG,
        close_timeout=0.2,
    ) as cloud:
        runtime = await bench.start(cloud.sockets[0].getsockname()[1])
        async with bench.charger() as charger:
            await bench.boot(charger)
            await until(lambda: bench.gateway.vendor_registration == "Accepted")
            old = bench.connections[-1]
            old.transport.pause_reading()
            try:
                await until(lambda: runtime.status.reconnects >= 2)
                assert runtime.status.charger_connected and bench.gateway.controls.ready
                command = asyncio.create_task(bench.gateway.set_limit(6))
                await accept_cap(charger, decode(await charger.recv()), bench.firmware)
                assert (await command)[2]["status"] == "Accepted"
            finally:
                old.transport.resume_reading()
    await bench.close()


# Intent: a real SQLite read denial in the delivery pump does not disable the independent heartbeat task.
# Why: storage quarantine must remain observable while the cloud transport still meets its idle contract.
async def test_runtime_database_failure_does_not_kill_heartbeat(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], admission: Admission
) -> None:
    bench = Bench(tmp_path / "journal.db", tls, admission)
    async with serve(
        bench.vendor, "127.0.0.1", 0, ssl=bench.server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
    ) as cloud:
        runtime = await bench.start(cloud.sockets[0].getsockname()[1])
        async with bench.charger() as charger:
            await bench.boot(charger)
            await until(lambda: bench.gateway.vendor_registration == "Accepted")

            # Deny only actual outbox reads; transport and unrelated settings remain available.
            def deny_outbox(
                action: int, table: str | None, column: str | None, database: str | None, trigger: str | None
            ) -> int:
                return sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_READ and table == "outbox" else sqlite3.SQLITE_OK

            bench.store.db.set_authorizer(deny_outbox)
            try:
                await until(lambda: runtime.status.delivery_blocked)
                await until(lambda: runtime.status.heartbeats >= 2)
                assert runtime.status.charger_connected and runtime.status.vendor_connected
            finally:
                bench.store.db.set_authorizer(None)
    await bench.close()


# Intent: malformed or rejected registration cannot permit delivery or heartbeat until a fresh valid acceptance.
# Why: transport admission alone does not establish that the backend accepted this virtual charge point.
@pytest.mark.parametrize("malformed", [False, True])
async def test_runtime_rejected_or_malformed_boot_recovers(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], admission: Admission, malformed: bool
) -> None:
    bench = Bench(tmp_path / "journal.db", tls, admission)
    bench.boot_status = "Accepted" if malformed else "Rejected"
    bench.boot_interval = True if malformed else 1
    async with serve(
        bench.vendor, "127.0.0.1", 0, ssl=bench.server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
    ) as cloud:
        runtime = await bench.start(cloud.sockets[0].getsockname()[1])
        async with bench.charger() as charger:
            await bench.boot(charger)
            await until(lambda: len([f for f in bench.calls if f[0] == 2 and f[2] == "BootNotification"]) >= 2)
            assert runtime.status.heartbeats == 0
            bench.boot_status, bench.boot_interval = "Accepted", 1
            await until(lambda: runtime.status.heartbeats >= 1)
            assert runtime.status.charger_connected
    await bench.close()


# Intent: vendor transport reconnects without interrupting the local receiver or losing queued events.
# Why: a cloud outage must not disconnect the charger or discard an already accepted local transaction.
async def test_runtime_cloud_loss_local_control_and_replay(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], admission: Admission
) -> None:
    bench = Bench(tmp_path / "journal.db", tls, admission)
    async with serve(
        bench.vendor, "127.0.0.1", 0, ssl=bench.server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
    ) as cloud:
        runtime = await bench.start(cloud.sockets[0].getsockname()[1])
        async with bench.charger() as charger:
            await bench.boot(charger)
            await until(lambda: bench.gateway.vendor_registration == "Accepted")
            await bench.connections[-1].close()
            await charger.send(build_call("start", "StartTransaction", START))
            assert decode(await charger.recv())[2]["transactionId"] == 1001
            await until(lambda: bench.store.vendor_id(1001) == 9001)
            assert runtime.status.reconnects >= 2 and runtime.status.charger_connected
            command = asyncio.create_task(bench.gateway.set_limit(6))
            frame = decode(await charger.recv())
            assert frame[2] == "SetChargingProfile"
            await accept_cap(charger, frame, bench.firmware)
            assert (await command)[2]["status"] == "Accepted"
            assert bench.store.next_delivery() is None
    await bench.close()


# Intent: a lost cloud Start result quarantines delivery while application heartbeats continue independently.
# Why: reconnecting and resending an ambiguous Start could duplicate history; killing heartbeat hides the issue.
async def test_runtime_uncertain_start_keeps_heartbeat_alive(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], admission: Admission
) -> None:
    bench = Bench(tmp_path / "journal.db", tls, admission)
    bench.drop_start = True
    async with serve(
        bench.vendor, "127.0.0.1", 0, ssl=bench.server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
    ) as cloud:
        runtime = await bench.start(cloud.sockets[0].getsockname()[1])
        async with bench.charger() as charger:
            await bench.boot(charger)
            await charger.send(build_call("start", "StartTransaction", START))
            assert decode(await charger.recv())[2]["transactionId"] == 1001
            await until(lambda: runtime.status.delivery_blocked)
            await until(lambda: runtime.status.heartbeats >= 1)
            assert len([f for f in bench.calls if f[0] == 2 and f[2] == "StartTransaction"]) == 1
            assert runtime.status.charger_connected
    await bench.close()


# Intent: pending registration honours its interval even if the transport changes before retry.
# Why: otherwise reconnect bypasses backend retry timing and causes a rapid Boot loop.
async def test_runtime_registration_delay_survives_replacement(tmp_path: Path) -> None:
    store = Journal(tmp_path / "journal.db", "OFFLINE001")
    gateway = Gateway(store)
    store.put("boot", BOOT)

    # Give an actual correlated Pending response to each generated Boot request.
    async def pending(raw: str) -> None:
        frame = decode(raw)
        await gateway.receive(
            "vendor", build_call_result(frame[1], {"status": "Pending", "interval": 30, "currentTime": NOW})
        )

    gateway.attach_vendor(pending)
    with pytest.raises(RegistrationDeferredError):
        await gateway.flush()
    gateway.attach_vendor(pending)
    with pytest.raises(RegistrationDeferredError) as error:
        await gateway.flush()
    assert error.value.retry_after > 29
    await gateway.close()
    store.close()


# Intent: a stale charger reader's finally cannot detach a newer authenticated replacement.
# Why: session replacement and shutdown race on real websocket handlers.
async def test_runtime_replaced_charger_survives_old_reader_cleanup(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], admission: Admission
) -> None:
    bench = Bench(tmp_path / "journal.db", tls, admission)
    async with serve(
        bench.vendor, "127.0.0.1", 0, ssl=bench.server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
    ) as cloud:
        runtime = await bench.start(cloud.sockets[0].getsockname()[1])
        async with bench.charger() as first:
            await bench.boot(first)
            async with bench.charger() as second:
                await bench.boot(second)
                await second.send(build_call("live", "Heartbeat", {}))
                assert "currentTime" in decode(await second.recv())[2]
                assert runtime.status.charger_connected
    await bench.close()


# Intent: persisted Boot does not introduce a new charger; the optional read-only probe waits for an actual Boot.
# Why: restored history and current connection readiness are separate facts.
async def test_runtime_boot_probe_requires_current_epoch(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], admission: Admission
) -> None:
    bench = Bench(tmp_path / "journal.db", tls, admission)
    bench.store.put("boot", BOOT)
    settings = RuntimeSettings(retry_min=0.02, retry_max=0.1, pump_interval=0.01, boot_probe_after=0.1)
    async with serve(
        bench.vendor, "127.0.0.1", 0, ssl=bench.server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
    ) as cloud:
        runtime = await bench.start(cloud.sockets[0].getsockname()[1], settings)
        async with bench.charger() as charger:
            await charger.send(build_call("not-boot", "Heartbeat", {}))
            assert "currentTime" in decode(await charger.recv())[2]
            assert not runtime.status.boot_observed
            probe = decode(await charger.recv())
            assert probe[2:] == ["TriggerMessage", {"requestedMessage": "BootNotification"}]
            await charger.send(build_call_result(probe[1], {"status": "Accepted"}))
            await bench.boot(charger)
            await until(lambda: runtime.status.boot_observed)
    await bench.close()


# Intent: construction is dormant and rejects real-network endpoints before any coroutine or socket exists.
# Why: the offline prototype must not accidentally contact production through a CLI/config typo.
def test_runtime_constructor_is_dormant_and_rejects_live_urls(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], admission: Admission
) -> None:
    store = Journal(tmp_path / "journal.db", admission.identity)
    gateway = Gateway(store)
    server, client, _ = tls
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        OfflineRuntime(gateway, admission, "wss://127.0.0.1:12345/ocpp", server, client)
        for url in ("wss://example.invalid/ocpp", "wss://192.0.2.1/ocpp", "ws://127.0.0.1/ocpp"):
            with pytest.raises(SecurityError):
                OfflineRuntime(gateway, admission, url, server, client)
        assert not caught
    store.close()
