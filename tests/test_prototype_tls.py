"""Real authenticated loopback WSS tests, using only temporary synthetic certificates."""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
import subprocess
from http import HTTPStatus
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.frames import basic_auth_header, build_call, build_call_result
from ocpp_hub_core.prototype.gateway import Gateway, decode
from ocpp_hub_core.prototype.security import (
    TRANSPORT_LOG,
    Admission,
    SecurityError,
    offline_server,
    offline_vendor,
)
from ocpp_hub_core.prototype.store import Journal
from websockets.asyncio.client import connect
from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import InvalidStatus

from tests.test_prototype_ics import CALLS, ICS_POLICY, STARTS


# Intent: create one synthetic trust root and hostname-valid test certificate.
# Why: TLS tests must retain verification rather than setting CERT_NONE to make them pass.
@pytest.fixture
def tls(tmp_path: Path) -> tuple[ssl.SSLContext, ssl.SSLContext, Path]:
    key, certificate = tmp_path / "key.pem", tmp_path / "certificate.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=offline.test",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-keyout",
            str(key),
            "-out",
            str(certificate),
        ],
        capture_output=True,
        check=True,
        timeout=20,
    )
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(certificate, key)
    client = ssl.create_default_context(cafile=str(certificate))
    return server, client, certificate


# Intent: declare a synthetic credential with no relationship to the household charger.
# Why: authentication forwarding can be tested without reading or copying a real secret.
@pytest.fixture
def admission() -> Admission:
    return Admission("OFFLINE001", basic_auth_header("OFFLINE001", "synthetic-secret-only")[1])


# Intent: run the actual gateway across two authenticated TLS websocket legs and replay a session.
# Why: unit-level message tests alone do not prove framing, encryption or upstream header/path preservation.
async def test_prototype_tls_two_leg_gateway(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], admission: Admission, caplog: Any
) -> None:
    caplog.set_level(logging.DEBUG)
    server_context, client_context, _ = tls
    journal = Journal(tmp_path / "journal.db", admission.identity)
    gateway = Gateway(journal, timeout=0.5)
    vendor_headers: list[list[str]] = []
    vendor_paths: list[str] = []
    vendor_calls: list[list[Any]] = []

    # Record the admitted synthetic identity and answer as a separate OCPP cloud backend.
    async def vendor_handler(connection: ServerConnection) -> None:
        vendor_headers.append(connection.request.headers.get_all("Authorization"))
        vendor_paths.append(connection.request.path)
        async for raw in connection:
            frame = decode(raw)
            vendor_calls.append(frame)
            result = (
                {"transactionId": 9001, "idTagInfo": {"status": "Accepted"}} if frame[2] == "StartTransaction" else {}
            )
            if frame[2] == "BootNotification":
                result = {"status": "Accepted", "interval": 60}
            await connection.send(build_call_result(frame[1], result))

    # Authentication already ran before this handler can attach the charger.
    async def charger_handler(connection: ServerConnection) -> None:
        gateway.attach_charger(connection.send)
        async for raw in connection:
            await gateway.receive("charger", raw)

    async with serve(
        vendor_handler, "127.0.0.1", 0, ssl=server_context, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
    ) as vendor_server:
        vendor_port = vendor_server.sockets[0].getsockname()[1]
        base = f"wss://127.0.0.1:{vendor_port}/ocpp/charger"
        async with offline_server(charger_handler, admission, server_context) as charger_server:
            port = charger_server.sockets[0].getsockname()[1]
            async with connect(
                f"wss://127.0.0.1:{port}/charger/{admission.identity}",
                ssl=client_context,
                additional_headers=[("Authorization", admission.authorization)],
                subprotocols=["ocpp1.6"],
                logger=TRANSPORT_LOG,
                proxy=None,
            ) as charger:
                await charger.send(build_call("boot", "BootNotification", {"chargePointVendor": "SYNTHETIC"}))
                assert decode(await charger.recv())[2]["status"] == "Accepted"
                await charger.send(
                    build_call(
                        "start",
                        "StartTransaction",
                        {
                            "connectorId": 1,
                            "idTag": "LOCAL_TEST",
                            "meterStart": 1000,
                            "timestamp": "2026-01-01T00:00:00Z",
                        },
                    )
                )
                assert decode(await charger.recv())[2]["transactionId"] == 1001

                # Connect only to the fake cloud and keep its receiver free for correlated replies.
                async with offline_vendor(base, admission, client_context) as vendor:
                    gateway.attach_vendor(vendor.send)

                    # The independent read loop prevents bridge requests from blocking response handling.
                    async def receive_vendor() -> None:
                        async for raw in vendor:
                            await gateway.receive("vendor", raw)

                    reader = asyncio.create_task(receive_vendor())
                    await gateway.flush()
                    assert journal.vendor_id(1001) == 9001
                    limit = asyncio.create_task(gateway.set_limit(12))
                    command = decode(await charger.recv())
                    assert command[2] == "SetChargingProfile"
                    await charger.send(build_call_result(command[1], {"status": "Accepted"}))
                    assert (await limit)[2]["status"] == "Accepted"
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
    await gateway.close()
    journal.close()
    assert vendor_headers == [[admission.authorization]]
    assert vendor_paths == ["/ocpp/charger/OFFLINE001"]
    assert [frame[2] for frame in vendor_calls] == ["BootNotification", "StartTransaction"]
    assert admission.authorization not in caplog.text and "synthetic-secret-only" not in caplog.text
    assert admission.authorization not in repr(admission)
    assert admission.authorization.encode() not in (tmp_path / "journal.db").read_bytes()


# Intent: translate the observed iCS query over both real TLS legs for an open and then completed transaction.
# Why: nested JSON and historical mappings must survive actual websocket framing, not just memory transports.
async def test_prototype_tls_ics_transaction_query(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], admission: Admission
) -> None:
    server_context, client_context, _ = tls
    journal = Journal(tmp_path / "journal.db", admission.identity)
    gateway = Gateway(journal, ICS_POLICY, timeout=1)
    vendor_connection: asyncio.Future[ServerConnection] = asyncio.get_running_loop().create_future()
    cloud_replies: asyncio.Queue[list[Any]] = asyncio.Queue()

    # Reply as the simulated vendor; use the real capture's aliased cloud transaction id.
    async def vendor_handler(connection: ServerConnection) -> None:
        vendor_connection.set_result(connection)
        async for raw in connection:
            frame = decode(raw)
            if frame[0] != 2:
                await cloud_replies.put(frame)
                continue
            result: dict[str, Any] = {}
            if frame[2] == "BootNotification":
                result = {"status": "Accepted", "interval": 60}
            elif frame[2] == "StartTransaction":
                result = {"transactionId": 6000001, "idTagInfo": {"status": "Accepted"}}
            await connection.send(build_call_result(frame[1], result))

    # Attach only the already authenticated, loopback-only fake charger.
    async def charger_handler(connection: ServerConnection) -> None:
        gateway.attach_charger(connection.send)
        async for raw in connection:
            await gateway.receive("charger", raw)

    async with serve(
        vendor_handler, "127.0.0.1", 0, ssl=server_context, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
    ) as vendor_server:
        vendor_port = vendor_server.sockets[0].getsockname()[1]
        async with offline_server(charger_handler, admission, server_context) as charger_server:
            port = charger_server.sockets[0].getsockname()[1]
            async with connect(
                f"wss://127.0.0.1:{port}/charger/{admission.identity}",
                ssl=client_context,
                additional_headers=[("Authorization", admission.authorization)],
                subprotocols=["ocpp1.6"],
                logger=TRANSPORT_LOG,
                proxy=None,
            ) as charger:
                # Boot is synthetic because no actual boot frame was present in the exported logs.
                await charger.send(build_call("boot", "BootNotification", {"chargePointVendor": "SYNTHETIC"}))
                assert decode(await charger.recv())[2]["status"] == "Accepted"
                await charger.send(build_call("start", "StartTransaction", STARTS[0]))
                assert decode(await charger.recv())[2]["transactionId"] == 1001
                async with offline_vendor(
                    f"wss://127.0.0.1:{vendor_port}/ocpp/charger", admission, client_context
                ) as vendor:
                    gateway.attach_vendor(vendor.send)

                    # Keep cloud replies flowing while the gateway waits for the fake charger's response.
                    async def receive_vendor() -> None:
                        async for raw in vendor:
                            await gateway.receive("vendor", raw)

                    reader = asyncio.create_task(receive_vendor())
                    try:
                        await gateway.flush()
                        cloud = await vendor_connection
                        query = next(row["frame"][3] for row in CALLS if row["frame"][2] == "DataTransfer")
                        for completed in (False, True):
                            if completed:
                                stop = next(row["frame"][3] for row in CALLS if row["frame"][2] == "StopTransaction")
                                await charger.send(
                                    build_call("stop", "StopTransaction", {**stop, "transactionId": 1001})
                                )
                                assert decode(await charger.recv())[2] == {}
                                await gateway.flush()
                            token = f"query-{completed}"
                            await cloud.send(build_call(token, "DataTransfer", query))
                            async with asyncio.timeout(2):
                                command = decode(await charger.recv())
                                assert json.loads(command[3]["data"]) == {"transactionId": 1001}
                                await charger.send(
                                    build_call_result(
                                        command[1],
                                        {"status": "Accepted", "data": '{"transactionId":1001,"eco":"Fast"}'},
                                    )
                                )
                                answer = await cloud_replies.get()
                            assert answer[1] == token
                            assert json.loads(answer[2]["data"]) == {"transactionId": 6000001, "eco": "Fast"}
                    finally:
                        reader.cancel()
                        await asyncio.gather(reader, return_exceptions=True)
    assert not gateway.failures
    await gateway.close()
    journal.close()


# Intent: unauthenticated or wrong-identity clients never reach the handler or displace an admitted connection.
# Why: identity checks after session replacement are too late.
@pytest.mark.parametrize(
    "headers,identity",
    [
        ([], "OFFLINE001"),
        ([("Authorization", "Basic wrong")], "OFFLINE001"),
        ([("Authorization", "Basic wrong"), ("Authorization", "Basic other")], "OFFLINE001"),
        (None, "OTHER"),
    ],
)
async def test_prototype_tls_admission_before_attach(
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    admission: Admission,
    headers: list[tuple[str, str]] | None,
    identity: str,
) -> None:
    server, client, _ = tls
    admitted: list[ServerConnection] = []

    # Keep the admitted client alive while unauthorized replacements are attempted.
    async def handler(connection: ServerConnection) -> None:
        admitted.append(connection)
        async for text in connection:
            await connection.send(text)

    async with offline_server(handler, admission, server) as listener:
        port = listener.sockets[0].getsockname()[1]
        url = f"wss://127.0.0.1:{port}/charger/"
        async with connect(
            url + admission.identity,
            ssl=client,
            additional_headers=[("Authorization", admission.authorization)],
            subprotocols=["ocpp1.6"],
            logger=TRANSPORT_LOG,
            proxy=None,
        ) as first:
            chosen = headers if headers is not None else [("Authorization", admission.authorization)]
            with pytest.raises(InvalidStatus) as error:
                async with connect(
                    url + identity,
                    ssl=client,
                    additional_headers=chosen,
                    subprotocols=["ocpp1.6"],
                    logger=TRANSPORT_LOG,
                    proxy=None,
                ):
                    pytest.fail("unauthorized client was admitted")
            assert error.value.response.status_code == 401
            await first.send("still attached")
            assert await first.recv() == "still attached"
    assert len(admitted) == 1


# Intent: the fixed vendor connector never forwards authentication through a redirect.
# Why: a legitimate vendor endpoint must not redirect a charger credential elsewhere.
async def test_prototype_tls_redirect_does_not_receive_credentials(
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], admission: Admission
) -> None:
    server, client, _ = tls
    reached: list[bool] = []

    # This target must remain completely untouched by the redirecting connection.
    async def target(connection: ServerConnection) -> None:
        reached.append(True)

    async with serve(target, "127.0.0.1", 0, ssl=server, logger=TRANSPORT_LOG) as second:
        second_port = second.sockets[0].getsockname()[1]

        # Send a real HTTP redirect before websocket admission.
        def redirect(connection: ServerConnection, request: Any) -> Any:
            response = connection.respond(HTTPStatus.TEMPORARY_REDIRECT, "moved\n")
            response.headers["Location"] = f"wss://127.0.0.1:{second_port}/other"
            return response

        async with serve(target, "127.0.0.1", 0, ssl=server, process_request=redirect, logger=TRANSPORT_LOG) as first:
            port = first.sockets[0].getsockname()[1]
            with pytest.raises(InvalidStatus):
                async with offline_vendor(f"wss://127.0.0.1:{port}/ocpp", admission, client):
                    pytest.fail("redirect was followed")
    assert reached == []


# Intent: an untrusted certificate or a deliberately weakened SSL context is refused.
# Why: the prototype must preserve the charger's existing trust checks.
async def test_prototype_tls_requires_trust(
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], admission: Admission
) -> None:
    server, _, _ = tls

    # Hold an idle fake endpoint; certificate validation happens before its handler can run.
    async def endpoint(connection: ServerConnection) -> None:
        await connection.wait_closed()

    async with serve(endpoint, "127.0.0.1", 0, ssl=server, logger=TRANSPORT_LOG) as listener:
        port = listener.sockets[0].getsockname()[1]
        with pytest.raises(ssl.SSLCertVerificationError):
            async with offline_vendor(f"wss://127.0.0.1:{port}", admission, ssl.create_default_context()):
                pytest.fail("untrusted certificate accepted")
    unsafe = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    unsafe.check_hostname = False
    unsafe.verify_mode = ssl.CERT_NONE
    with pytest.raises(SecurityError):
        offline_vendor("wss://127.0.0.1:1234/ocpp", admission, unsafe)


# Intent: non-loopback destinations and wildcard listeners are rejected before opening a socket.
# Why: this branch is an offline prototype, not a deployment entry point.
def test_prototype_tls_is_offline_only(tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], admission: Admission) -> None:
    server, client, _ = tls
    for address in ["wss://example.invalid/ocpp", "ws://127.0.0.1:1234/ocpp", "wss://192.0.2.1:1234/ocpp"]:
        with pytest.raises(SecurityError):
            offline_vendor(address, admission, client)
    with pytest.raises(SecurityError):
        offline_server(lambda _: None, admission, server, host="0.0.0.0")


# Intent: malformed configured credentials are refused before any handshake can be accepted.
# Why: comparing two empty strings must not count as authentication.
@pytest.mark.parametrize(
    "identity,header",
    [
        ("OFFLINE001", ""),
        ("OFFLINE001", "Bearer token"),
        ("OFFLINE001", "Basic !!!"),
        ("OFFLINE001", basic_auth_header("OTHER", "secret")[1]),
        ("OFFLINE001", basic_auth_header("OFFLINE001", "")[1]),
        ("../OFFLINE001", basic_auth_header("../OFFLINE001", "secret")[1]),
    ],
)
def test_prototype_tls_rejects_bad_admission(identity: str, header: str) -> None:
    with pytest.raises(SecurityError):
        Admission(identity, header)


# Intent: a trusted certificate with the wrong hostname still fails verification.
# Why: a wrong-CA test alone does not prove hostname checking remains enabled.
async def test_prototype_tls_hostname_mismatch(tmp_path: Path, admission: Admission) -> None:
    key, certificate = tmp_path / "wrong-key.pem", tmp_path / "wrong-certificate.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=wrong.example.invalid",
            "-addext",
            "subjectAltName=DNS:wrong.example.invalid",
            "-keyout",
            str(key),
            "-out",
            str(certificate),
        ],
        capture_output=True,
        check=True,
        timeout=20,
    )
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(certificate, key)
    client = ssl.create_default_context(cafile=str(certificate))

    # A valid certificate chain is available, but its name must not match the loopback address.
    async def endpoint(connection: ServerConnection) -> None:
        await connection.wait_closed()

    async with serve(endpoint, "127.0.0.1", 0, ssl=server, logger=TRANSPORT_LOG) as listener:
        port = listener.sockets[0].getsockname()[1]
        with pytest.raises(ssl.SSLCertVerificationError) as error:
            async with offline_vendor(f"wss://127.0.0.1:{port}", admission, client):
                pytest.fail("wrong hostname accepted")
        assert "mismatch" in error.value.verify_message.lower()
