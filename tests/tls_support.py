"""Offline TLS contexts and helpers for answering actual charger RPCs."""

from __future__ import annotations

import ssl
import subprocess
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.frames import basic_auth_header, build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.security import Admission

from tests.prototype_firmware import Firmware


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


# A fake device answers server RPCs independently while waiting for its own correlated local reply.
async def charger_reply(charger: Any, token: str, firmware: Firmware) -> list[Any]:
    while True:
        frame = decode(await charger.recv())
        if frame[0] == 2:
            await charger.send(build_call_result(frame[1], firmware.answer(frame[2], frame[3])))
        elif frame[1] == token:
            if token == "start":
                firmware.active = frame[2]["transactionId"]
            elif token == "stop":
                firmware.active = None
            return frame


# Complete a real station-profile exchange and answer the following composite read from device state.
async def accept_cap(charger: Any, command: list[Any], firmware: Firmware) -> None:
    await charger.send(build_call_result(command[1], firmware.answer(command[2], command[3])))
    verification = decode(await charger.recv())
    assert verification[2] == "GetCompositeSchedule"
    await charger.send(build_call_result(verification[1], firmware.answer(verification[2], verification[3])))
