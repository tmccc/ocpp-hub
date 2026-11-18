"""Verified TLS material and per-worker ports for independent real-peer model cases."""

import os
import ssl
import subprocess
from dataclasses import dataclass

import pytest

from tests.worker_ports import listener_port_factory


# Intent: retain verified TLS while amortizing synthetic RSA generation across a worker's independent cases.
# Why: each device and journal must be fresh; repeated RSA generation adds no event-order coverage.
@pytest.fixture(scope="session")
def model_tls(tmp_path_factory):
    root = tmp_path_factory.mktemp("model-tls")
    key, certificate = root / "key.pem", root / "certificate.pem"
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
    return TLSMaterial(key.read_bytes(), certificate.read_bytes())


@dataclass(frozen=True, repr=False)
class TLSMaterial:
    """Synthetic certificate bytes are reusable, but should never clutter failure representations."""

    key: bytes
    certificate: bytes


# Reconstruct private per-case files because the real preflight owns and checks their permissions.
def tls_for_case(path, material):
    key, certificate = path / "key.pem", path / "certificate.pem"
    key.write_bytes(material.key)
    certificate.write_bytes(material.certificate)
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(certificate, key)
    return server, ssl.create_default_context(cafile=str(certificate)), certificate


# Allocate only inside this worker's checked non-ephemeral pool, then start its sole scenario listener immediately.
def listener_port():
    return listener_port_factory(int(os.environ.get("MODEL_WORKER_INDEX", "0")))()
