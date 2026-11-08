"""Dormant preflight, independent credentials, real TLS admission and bounded read-only native HTTP."""

from __future__ import annotations

import asyncio
import json
import socket
import ssl
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from ocpp_hub_core.frames import basic_auth_header
from ocpp_hub_core.prototype.security import TRANSPORT_LOG, SecurityError
from ocpp_hub_core.prototype.store import fingerprint
from ocpp_hub_core.prototype.trial_config import PreparedTLS, prepare_tls, read_file
from ocpp_hub_core.prototype.trial_evidence import NativeObserver
from ocpp_hub_core.prototype.trial_resources import Baseline
from ocpp_hub_core.prototype.trial_session import preflight, save_private
from ocpp_hub_core.prototype.trial_transport import TrialTransport
from websockets.asyncio.client import connect
from websockets.asyncio.server import ServerConnection, serve
from websockets.datastructures import Headers
from websockets.exceptions import InvalidStatus
from websockets.http11 import Request

from tests.test_prototype_tls import tls as tls
from tests.trial_helpers import config_files


# Intent: default CLI validation parses files/certificates but creates no journal, socket or DNS request.
# Why: preparing the package must never become an accidental live switch.
def test_default_cli_preflight_is_dormant(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], monkeypatch: Any, capsys: Any
) -> None:
    from tools.trial_proxy import main

    config, path = config_files(tmp_path, 19876, 19877)

    # Any attempted socket construction or DNS call is a meaningful preflight failure.
    def deny(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("preflight attempted a network operation")

    monkeypatch.setattr(socket, "socket", deny)
    monkeypatch.setattr(socket, "getaddrinfo", deny)
    assert main(["--config", str(path)]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["offline_preflight_passed"] and not output["network_contacted"]
    assert not output["hardware_admission_verified"] and not output["hardware_ceiling_verified"]
    assert not config.journal.exists()
    assert "SECRET" not in repr(preflight(path))


# Intent: a real-length OCPP URL identity stays separate from the physical serial, without an idTag's 20-byte cap.
# Why: all earlier synthetic peers reused a short hardware serial and hid this commissioning admission failure.
@pytest.mark.parametrize("length", [32, 64])
def test_preflight_preserves_long_ocpp_identity(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], length: int
) -> None:
    config, path = config_files(tmp_path, 19876, 19877)
    identity = "C" * length
    value = json.loads(path.read_text())
    value.update(
        identity=identity, charger_path="/charger/" + identity, vendor_url="wss://127.0.0.1:19877/ocpp/" + identity
    )
    save_private(path, value)
    baseline = json.loads(config.baseline.read_text())
    baseline["identity_digest"] = fingerprint(identity)
    save_private(config.baseline, baseline)
    for secret in (config.incoming_secret, config.upstream_secret):
        assert secret is not None
        credential = json.loads(secret.read_text())
        credential["username"] = identity
        save_private(secret, credential)
    prepared = preflight(path)
    assert prepared.config.identity == identity
    assert prepared.config.hardware_identity == "HARDWARE001"
    assert prepared.baseline == Baseline.parse(baseline)
    with pytest.raises(SecurityError):
        replace(prepared.config, identity="C" * 65)


# Intent: final failure status replaces the running status even when cleanup itself raises after retiring connections.
# Why: a storage failure during shutdown must not leave a stale healthy report or installed signal handlers.
async def test_cli_persists_final_status_when_close_raises(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], monkeypatch: Any, capsys: Any
) -> None:
    from tools import trial_proxy

    config, path = config_files(tmp_path, 19876, 19877)
    final = {"running": False, "phase": "unavailable", "shutdown_uncertain": True}
    removed = []
    loop = asyncio.get_running_loop()
    original_remove = loop.remove_signal_handler

    # Model the explicit outer CLI boundary after a real session has already produced its failure report.
    class FailedSession:
        # Accept the validated package without opening a socket in this boundary-only regression.
        def __init__(self, prepared: Any) -> None:
            self.final_status: dict[str, Any] | None = None

        # Enter cleanup immediately, as if the bounded startup deadline had expired.
        async def start(self) -> None:
            raise TimeoutError

        # Preserve the real shutdown contract: final status exists even if a later storage close raises.
        async def close(self) -> None:
            self.final_status = final
            raise RuntimeError("synthetic private failure")

    # Observe cleanup while preserving the event loop's actual signal-handler removal.
    def remove(signum: Any) -> bool:
        removed.append(signum)
        return original_remove(signum)

    monkeypatch.setattr(trial_proxy, "TrialSession", FailedSession)
    monkeypatch.setattr(loop, "remove_signal_handler", remove)
    save_private(config.journal.with_suffix(".status.json"), {"running": True, "shutdown_uncertain": False})
    with pytest.raises(RuntimeError):
        await trial_proxy.run(path, 60, False)
    assert json.loads(config.journal.with_suffix(".status.json").read_text()) == final
    assert len(removed) == 2
    output = capsys.readouterr().out
    assert json.loads(output.splitlines()[-1]) == {"trial_stopped": True, "shutdown_uncertain": True}
    assert "private" not in output


# Intent: secret symlinks, insecure modes, oversize data and writable trust material fail closed.
# Why: owner-local preflight must not consume a credential or trust anchor another user can replace.
@pytest.mark.parametrize("change", ["secret-mode", "secret-link", "oversize", "public-writable"])
def test_private_file_boundary(tmp_path: Path, change: str) -> None:
    path = tmp_path / "data"
    path.write_bytes(b"private credential")
    path.chmod(0o600)
    secret = True
    if change == "secret-mode":
        path.chmod(0o644)
    elif change == "secret-link":
        link = tmp_path / "link"
        link.symlink_to(path)
        path = link
    elif change == "oversize":
        path.write_bytes(b"x" * 131073)
    else:
        path.chmod(0o666)
        secret = False
    with pytest.raises(SecurityError) as error:
        read_file(path, secret=secret)
    assert "credential" not in str(error.value)


# Intent: a path typo cannot make baseline capture or status writing overwrite a credential/configuration input.
# Why: all derived output locations must remain independent before any live read or disk write begins.
@pytest.mark.parametrize("alias", ["baseline-secret", "journal-secret", "status-secret", "config-output"])
def test_trial_file_aliases_are_rejected(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], alias: str
) -> None:
    from ocpp_hub_core.prototype.trial_config import TrialConfig

    config, path = config_files(tmp_path, 19876, 19877)
    with pytest.raises(SecurityError):
        if alias == "baseline-secret":
            replace(config, baseline=config.incoming_secret)
        elif alias == "journal-secret":
            replace(config, journal=config.native_secret)
        elif alias == "status-secret":
            replace(config, upstream_secret=config.journal.with_suffix(".status.json"))
        else:
            value = json.loads(path.read_text())
            value["baseline"] = str(path)
            path.write_text(json.dumps(value))
            TrialConfig.load(path)


# Intent: listener identity validity and key pairing are checked before the trial can bind.
# Why: parsing a PEM alone does not show that the charger can verify the intended listener's name.
@pytest.mark.parametrize("change", ["name", "not-yet-valid", "expired", "client-as-server"])
def test_listener_tls_preflight(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], monkeypatch: Any, change: str
) -> None:
    import ocpp_hub_core.prototype.trial_config as module

    config, _ = config_files(tmp_path, 19876, 19877)
    if change == "name":
        config = replace(config, listener_name="wrong.invalid")
    elif change == "not-yet-valid":
        monkeypatch.setattr(module.time, "time", lambda: 0)
    elif change == "expired":
        now = time.time()
        monkeypatch.setattr(module.time, "time", lambda: now + 10000000)
    else:
        prepared = prepare_tls(config)
        with pytest.raises(SecurityError):
            TrialTransport(config, PreparedTLS(prepared.client, prepared.client, prepared.incoming, prepared.upstream))
        return
    with pytest.raises(SecurityError):
        prepare_tls(config)


# Intent: unsafe endpoint changes are rejected without implicit downgrade, redirect or wildcard binding.
# Why: a production adapter must not weaken the original offline adapter to reach the real backend.
@pytest.mark.parametrize(
    "changes",
    [
        {"listener_host": "0.0.0.0"},
        {"listener_host": "8.8.8.8"},
        {"listener_port": 0},
        {"vendor_url": "ws://127.0.0.1:1234/ocpp/OFFLINE001"},
        {"vendor_url": "wss://user:password@127.0.0.1:1234/ocpp/OFFLINE001"},
        {"vendor_url": "wss://127.0.0.1:1234/ocpp/OFFLINE001?token=x"},
        {"charger_path": "/other/OFFLINE001"},
        {"max_current_a": 7},
        {"incoming_auth": "none"},
        {"incoming_secret": None},
        {"upstream_auth": "none"},
        {"upstream_secret": None},
    ],
)
def test_trial_endpoint_validation(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], changes: dict[str, Any]
) -> None:
    config, _ = config_files(tmp_path, 19876, 19877)
    with pytest.raises(SecurityError):
        replace(config, **changes)


# Intent: a manually constructed TLS package cannot silently weaken either explicitly selected authentication mode.
# Why: validating the configuration alone is insufficient if another caller passes inconsistent prepared credentials.
@pytest.mark.parametrize("leg", ["incoming", "upstream"])
@pytest.mark.parametrize("mode", ["basic", "none"])
def test_trial_transport_refuses_inconsistent_prepared_authentication(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], leg: str, mode: str
) -> None:
    config, _ = config_files(tmp_path, 19876, 19877)
    if mode == "none":
        config = replace(config, **{leg + "_auth": "none", leg + "_secret": None})
    prepared = prepare_tls(config)
    inconsistent = replace(prepared, **{leg: None if mode == "basic" else "Basic SYNTHETIC"})
    with pytest.raises(SecurityError):
        TrialTransport(config, inconsistent)


# Intent: absent, malformed or wrong-source connection metadata is refused before it can replace an admitted charger.
# Why: closing or incomplete transports must not bypass address checks or crash the admission boundary.
@pytest.mark.parametrize("remote", [None, (), "unexpected", ("invalid", 1), ("127.0.0.2", 1)])
def test_trial_admission_refuses_malformed_or_wrong_peer(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], remote: Any
) -> None:
    config, _ = config_files(tmp_path, 19876, 19877)
    prepared = prepare_tls(config)
    transport = TrialTransport(config, prepared)

    # Preserve the rejection classification without requiring an impossible malformed operating-system peer tuple.
    def respond(status: Any, text: str) -> Any:
        return SimpleNamespace(status_code=int(status))

    connection = SimpleNamespace(remote_address=remote, respond=respond)
    request = Request(config.charger_path, Headers([("Authorization", prepared.incoming)]))
    assert transport.admit(connection, request).status_code == 401


# Intent: exact full-path admission and independent credentials protect both real synthetic TLS legs.
# Why: accepting merely a trailing identity or forwarding the local secret would violate the trial boundary.
@pytest.mark.parametrize("upstream_mode", ["basic", "none"])
@pytest.mark.parametrize("incoming_mode", ["basic", "none"])
async def test_trial_transport_exact_admission_and_independent_upstream_secret(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    upstream_mode: str,
    incoming_mode: str,
) -> None:
    server_tls, client_tls, _ = tls
    seen: list[tuple[str, list[str]]] = []

    # The fake backend sees only the upstream credential, never the incoming charger's different one.
    async def vendor(connection: ServerConnection) -> None:
        seen.append((connection.request.path, connection.request.headers.get_all("Authorization")))
        await connection.wait_closed()

    async with serve(vendor, "127.0.0.1", 0, ssl=server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        config, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        value = json.loads(path.read_text())
        if upstream_mode == "none":
            value.update(upstream_auth="none", upstream_secret=None)
        if incoming_mode == "none":
            value.update(incoming_auth="none", incoming_secret=None)
        save_private(path, value)
        prepared = preflight(path)
        config = prepared.config
        transport = TrialTransport(config, prepared.tls)
        admitted = asyncio.Event()

        # Only an admitted peer can reach this handler, so rejected attempts cannot replace it.
        async def charger_handler(connection: ServerConnection) -> None:
            admitted.set()
            await connection.wait_closed()

        listener = await transport.listen(charger_handler)
        try:
            base = f"wss://127.0.0.1:{config.listener_port}"
            valid_headers = [] if prepared.tls.incoming is None else [("Authorization", prepared.tls.incoming)]
            invalid_header = basic_auth_header("OFFLINE001", "wrong")[1]
            for suffix, headers in (
                ("/other/OFFLINE001", valid_headers),
                (config.charger_path + "?x=1", valid_headers),
                (config.charger_path, [("Authorization", invalid_header)]),
                (config.charger_path, [("Authorization", invalid_header)] * 2),
            ):
                with pytest.raises(InvalidStatus):
                    async with connect(
                        base + suffix,
                        ssl=client_tls,
                        additional_headers=headers,
                        subprotocols=["ocpp1.6"],
                        logger=TRANSPORT_LOG,
                        proxy=None,
                    ):
                        pytest.fail("invalid trial peer was admitted")
                assert not admitted.is_set()
            async with connect(
                base + config.charger_path,
                ssl=client_tls,
                additional_headers=valid_headers,
                subprotocols=["ocpp1.6"],
                logger=TRANSPORT_LOG,
                proxy=None,
            ):
                await asyncio.wait_for(admitted.wait(), 1)
                async with transport.connect():
                    await asyncio.sleep(0.02)
            expected = [] if prepared.tls.upstream is None else [prepared.tls.upstream]
            assert seen == [("/ocpp/OFFLINE001", expected)]
            if prepared.tls.incoming is not None:
                assert prepared.tls.incoming != prepared.tls.upstream
        finally:
            listener.close()
            await listener.wait_closed()


# Supply independent native device data for the existing read-only action shapes.
def native_values() -> dict[str, Any]:
    purposes = {"ChargePointMaxProfile": [], "TxDefaultProfile": [], "TxProfile": []}
    return {
        "config": {"Id": "OFFLINE001", "bPSN": "HARDWARE001", "installerCurrent": 32},
        "SMART": {"conn0": purposes, "conn1": purposes},
        "SETPLAN": {"planList": [], "defaultEco": "Fast"},
        "SESSION": {"saveData": {"sessionList": [{"_transactionId": -1}]}},
        "METER": [{"meterData": {"cur": [0, 0, 0], "power": [0, 0, 0, 0]}}],
        "CONNECTOR": [{"info": {"CP": "A"}, "connStatus": "Unplug"}],
        "OCPP": {"online": True},
        "TRANSACTIONS": [],
    }


# Intent: the native observer uses only bounded read requests and independently proves unplugged history and state.
# Why: OCPP SuspendedEV is not trustworthy vehicle-connection evidence on this charger.
@pytest.mark.parametrize("mismatch", [None, "ocpp", "hardware"])
@pytest.mark.parametrize("include_history", [False, True])
async def test_native_observer_real_http_is_read_only(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], mismatch: str | None, include_history: bool
) -> None:
    values = native_values()
    identity = "SYNTHETIC_OCPP_ID_" + "C" * 14
    values["config"]["Id"] = identity
    if mismatch == "ocpp":
        values["config"]["Id"] = "OTHER_OCPP_ID"
    elif mismatch == "hardware":
        values["config"]["bPSN"] = "OTHER_HARDWARE"
    requests: list[dict[str, Any]] = []

    # This separate HTTP peer rejects anything outside the actual read-only native action envelope.
    async def peer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        headers = await reader.readuntil(b"\r\n\r\n")
        length = int(
            next(
                line.split(b":", 1)[1] for line in headers.split(b"\r\n") if line.lower().startswith(b"content-length:")
            )
        )
        request = json.loads(await reader.readexactly(length))
        requests.append(request)
        key = (
            "config"
            if request["action"] == "getConfig"
            else "TRANSACTIONS"
            if request["action"] == "getTransaction"
            else request["data"]
        )
        body = json.dumps({"code": 200, "data": json.dumps(values[key])}).encode()
        writer.write(b"HTTP/1.0 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(peer, "127.0.0.1", 0)
    try:
        config, _ = config_files(tmp_path, 19876, 19877)
        config = replace(
            config,
            identity=identity,
            charger_path="/charger/" + identity,
            vendor_url="wss://127.0.0.1:19877/ocpp/" + identity,
        )
        observer = NativeObserver(config, server.sockets[0].getsockname()[1])
        if mismatch is not None:
            with pytest.raises(SecurityError):
                await observer.observe()
            assert len(requests) == 1
            return
        evidence = await (observer.observe_with_history() if include_history else observer.observe())
        evidence.require_unplugged(config.identity)
        assert len(requests) == (9 if include_history else 8) and evidence.upstream_connected
        assert (evidence.history is not None) == include_history
        with pytest.raises(SecurityError):
            await observer._read("setConfig")
        assert len(requests) == (9 if include_history else 8)
    finally:
        server.close()
        await server.wait_closed()


# Intent: duplicate native JSON keys on either layer cannot authorize removal of a zero-current hold.
# Why: last-key-wins interpretation can turn a contradictory physical state into a false idle observation.
@pytest.mark.parametrize(
    "raw", [b'{"code":200,"code":200,"data":{}}', b'{"code":200,"data":"{\\"CP\\":\\"B\\",\\"CP\\":\\"A\\"}"}']
)
async def test_native_json_duplicate_keys_rejected(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], raw: bytes
) -> None:
    from ocpp_hub_core.prototype.native_http import read_native

    # Return a syntactically complete successful response whose JSON is deliberately ambiguous.
    async def peer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b"\r\n\r\n")
        writer.write(b"HTTP/1.0 200 OK\r\nContent-Length: " + str(len(raw)).encode() + b"\r\n\r\n" + raw)
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(peer, "127.0.0.1", 0)
    try:
        with pytest.raises(SecurityError):
            await read_native(
                "127.0.0.1",
                server.sockets[0].getsockname()[1],
                basic_auth_header("admin", "synthetic")[1],
                "statistics",
                "CONNECTOR",
            )
    finally:
        server.close()
        await server.wait_closed()


# Intent: a peer dripping bytes cannot keep the native observation alive beyond its single aggregate deadline.
# Why: per-read socket timeouts do not bound a response and worker threads cannot be safely cancelled.
async def test_native_drip_response_has_aggregate_deadline() -> None:
    from ocpp_hub_core.prototype.native_http import read_native

    finished = asyncio.Event()

    # Keep the stream active while withholding the promised complete body, then notice the client's closure.
    async def peer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await reader.readuntil(b"\r\n\r\n")
            writer.write(b"HTTP/1.0 200 OK\r\nContent-Length: 1000\r\n\r\n")
            await writer.drain()
            for _ in range(80):
                writer.write(b" ")
                await writer.drain()
                await asyncio.sleep(0.1)
        except ConnectionError, OSError:
            pass
        finally:
            writer.close()
            finished.set()

    server = await asyncio.start_server(peer, "127.0.0.1", 0)
    try:
        started = asyncio.get_running_loop().time()
        with pytest.raises(SecurityError):
            async with asyncio.timeout(6):
                await read_native(
                    "127.0.0.1",
                    server.sockets[0].getsockname()[1],
                    basic_auth_header("admin", "synthetic")[1],
                    "getConfig",
                    None,
                )
        assert asyncio.get_running_loop().time() - started < 6
        await asyncio.wait_for(finished.wait(), 2)
    finally:
        server.close()
        await server.wait_closed()


# Intent: supported chunked framing works, while ambiguous or oversized HTTP framing cannot become evidence.
# Why: a strict JSON decoder alone does not protect the preceding network message boundary.
@pytest.mark.parametrize(
    ("raw", "valid"),
    [
        (b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\n\r\n", True),
        (b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 2\r\n\r\n{}", False),
        (b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 2\r\n\r\n{}", False),
        (b"HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\n\r\n{}", False),
        (b"HTTP/1.1 302 Found\r\nLocation: http://elsewhere.invalid/\r\n\r\n", False),
    ],
)
async def test_native_http_framing(raw: bytes, valid: bool) -> None:
    from ocpp_hub_core.prototype.native_http import response_body

    reader = asyncio.StreamReader()
    reader.feed_data(raw)
    reader.feed_eof()
    if valid:
        assert await response_body(reader) == b"{}"
    else:
        with pytest.raises((ValueError, SecurityError)):
            await response_body(reader)


# Intent: only the observed missing OCPP status file means disconnected; all other native failures remain errors.
# Why: the file is absent after restart until a backend connects, so requiring it before listening creates a deadlock.
@pytest.mark.parametrize(
    ("module", "message", "code", "valid"),
    [
        ("OCPP", "NO FOUND /tmp/Ioc/OCPP.json", 1004, True),
        ("CONNECTOR", "NO FOUND /tmp/Ioc/OCPP.json", 1004, False),
        ("OCPP", "unexpected native failure", 1004, False),
        ("OCPP", "NO FOUND /tmp/Ioc/OCPP.json", 1004.0, False),
    ],
)
async def test_native_missing_backend_file_is_explicitly_disconnected(
    module: str, message: str, code: int | float, valid: bool
) -> None:
    from ocpp_hub_core.prototype.native_http import read_native

    raw = json.dumps({"code": code, "message": message, "data": None}).encode()

    # An independent native HTTP peer returns the exact observed startup envelope or its negative controls.
    async def peer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b"\r\n\r\n")
        writer.write(b"HTTP/1.0 200 OK\r\nContent-Length: " + str(len(raw)).encode() + b"\r\n\r\n" + raw)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(peer, "127.0.0.1", 0)
    try:
        operation = read_native(
            "127.0.0.1", server.sockets[0].getsockname()[1], "Basic SYNTHETIC", "statistics", module
        )
        if valid:
            assert await operation == {"online": False}
        else:
            with pytest.raises(SecurityError):
                await operation
    finally:
        server.close()
        await server.wait_closed()
