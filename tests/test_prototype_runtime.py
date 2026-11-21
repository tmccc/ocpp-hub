"""Full offline supervisor exercised through real authenticated loopback TLS websocket peers."""

from __future__ import annotations

import asyncio
import sqlite3
import ssl
import warnings
from pathlib import Path

import pytest
from ocpp_hub_core.frames import build_call, build_call_result
from ocpp_hub_core.prototype.gateway import Gateway, decode
from ocpp_hub_core.prototype.policy import Policy
from ocpp_hub_core.prototype.rpc import RegistrationDeferredError
from ocpp_hub_core.prototype.runtime import OfflineRuntime, RuntimeSettings
from ocpp_hub_core.prototype.security import TRANSPORT_LOG, Admission, SecurityError
from ocpp_hub_core.prototype.store import Journal
from websockets.asyncio.server import serve

from tests.prototype_rig import BOOT, START
from tests.runtime_rig import NOW, Bench, until
from tests.tls_support import accept_cap
from tests.tls_support import admission as admission
from tests.tls_support import tls as tls


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


# Intent: real TLS charger traffic retains bounded local Authorize while an open cloud has refused registration.
# Why: an accepted websocket is not accepted OCPP registration and must not make local permission less available.
@pytest.mark.parametrize("registration", ["Pending", "Rejected"])
async def test_runtime_unregistered_cloud_keeps_local_authorize(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    admission: Admission,
    registration: str,
) -> None:
    bench = Bench(tmp_path / "journal.db", tls, admission, Policy(strict_cloud_authorization=True))
    bench.boot_status, bench.boot_interval = registration, 30
    try:
        async with serve(
            bench.vendor, "127.0.0.1", 0, ssl=bench.server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
        ) as cloud:
            await bench.start(cloud.sockets[0].getsockname()[1])
            async with bench.charger() as charger:
                await bench.boot(charger)
                await until(lambda: bench.gateway.vendor_registration == registration)
                await charger.send(build_call("local-auth", "Authorize", {"idTag": "LOCAL_TEST"}))
                answer = decode(await charger.recv())
                assert answer[:2] == [3, "local-auth"]
                assert answer[2]["idTagInfo"]["status"] == "Accepted"
                assert set(answer[2]["idTagInfo"]) == {"status", "expiryDate"}
                assert not [frame for frame in bench.calls if frame[0] == 2 and frame[2] == "Authorize"]
    finally:
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


# Intent: the socket keeps receiving genuine replies while a Boot response or a refusal is waiting for the peer.
# Why: a stalled response cannot hide the charger reply to another command.
@pytest.mark.parametrize("blocked", ["malformed", "full_queue", "boot"])
async def test_runtime_reads_a_reply_while_an_outgoing_response_waits(tmp_path, tls, admission, blocked):
    bench = Bench(tmp_path / "reply-congestion.db", tls, admission)
    release, refusing, full_queue = asyncio.Event(), asyncio.Event(), asyncio.Event()
    queued = set()
    bench.gateway.use_command_processor()
    try:
        async with serve(
            bench.vendor, "127.0.0.1", 0, ssl=bench.server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
        ) as cloud:
            await bench.start(cloud.sockets[0].getsockname()[1])
            async with bench.charger() as charger:
                await bench.boot(charger)
                await until(lambda: bench.gateway.vendor_registration == "Accepted")
                peer = bench.gateway.charger
                sender = peer.sender
                assert sender is not None
                peer.timeout = 2

                # Delay only the selected actual reply sends while leaving the websocket and reads intact.
                async def congested(raw):
                    frame = decode(raw)
                    if frame[0] == 3 and frame[1].startswith("queued-"):
                        queued.add(frame[1])
                        if len(queued) == 32:
                            full_queue.set()
                        await release.wait()
                    if frame[0] in (3, 4) and frame[1] == "held":
                        refusing.set()
                        await release.wait()
                    await sender(raw)

                peer.sender = congested
                command = None
                try:
                    if blocked == "full_queue":
                        for index in range(32):
                            await charger.send(build_call(f"queued-{index}", "Heartbeat", {}))
                        await asyncio.wait_for(full_queue.wait(), 1)
                        await charger.send(build_call("held", "Heartbeat", {}))
                    elif blocked == "boot":
                        command = asyncio.create_task(peer.call("GetConfiguration", {"key": ["HeartbeatInterval"]}))
                        request = decode(await asyncio.wait_for(charger.recv(), 1))
                        await charger.send(build_call("held", "BootNotification", BOOT))
                    else:
                        await charger.send('[2,"held","Heartbeat",[]]')
                    await asyncio.wait_for(refusing.wait(), 1)
                    if blocked == "boot":
                        await bench.connections[-1].send(build_call("cloud-during-boot", "GetConfiguration", {}))
                        await until(lambda: any(frame[1] == "cloud-during-boot" for frame in bench.calls))
                        refusal = next(frame for frame in bench.calls if frame[1] == "cloud-during-boot")
                        assert refusal[:3] == [4, "cloud-during-boot", "InternalError"]
                        assert "command was not sent" in refusal[3]
                    if command is None:
                        command = asyncio.create_task(peer.call("GetConfiguration", {"key": ["HeartbeatInterval"]}))
                        request = decode(await asyncio.wait_for(charger.recv(), 1))
                    assert request[0] == 2 and request[2] == "GetConfiguration"
                    actual = {"configurationKey": []}
                    await charger.send(build_call_result(request[1], actual))
                    assert (await asyncio.wait_for(command, 1))[2] == actual
                    assert not release.is_set()
                finally:
                    release.set()
                    if command is not None:
                        await asyncio.gather(command, return_exceptions=True)
                    await bench.gateway.drain()
    finally:
        await bench.close()
