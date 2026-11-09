"""Offline preflight for one explicitly configured trial; no socket or DNS operations."""

from __future__ import annotations

import ipaddress
import json
import os
import re
import ssl
import stat
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from ..frames import basic_auth_header
from .ics import unique_object
from .security import SecurityError
from .station import StationChoice


# Read through one verified descriptor so symlinks, permissive secrets and oversized files fail before parsing.
def read_file(path: Path, *, secret: bool = True, maximum: int = 131072) -> bytes:
    if not path.is_absolute():
        raise SecurityError("trial files require absolute paths")
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as stream:
            info = os.fstat(stream.fileno())
            owners = {os.getuid()} if secret else {0, os.getuid()}
            if not stat.S_ISREG(info.st_mode) or info.st_uid not in owners:
                raise SecurityError("trial file is not an owned regular file")
            if secret and stat.S_IMODE(info.st_mode) & 0o077:
                raise SecurityError("trial secrets must be private to their owner")
            if not secret and stat.S_IMODE(info.st_mode) & 0o022:
                raise SecurityError("trial trust material must not be writable by other users")
            if info.st_size > maximum:
                raise SecurityError("trial file exceeds its size limit")
            data = stream.read(maximum + 1)
            if len(data) > maximum:
                raise SecurityError("trial file exceeds its size limit")
            return data
    except OSError:
        raise SecurityError("trial file cannot be read safely") from None


# Parse exact JSON objects without retaining parser errors that may quote sensitive source text.
def private_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(read_file(path), object_pairs_hook=unique_object)
        if not isinstance(value, dict):
            raise ValueError
        return value
    except ValueError, RecursionError, UnicodeError:
        raise SecurityError("trial file requires an unambiguous JSON object") from None


# A Basic secret is deliberately independent on each leg; Android app Bearer tokens are never accepted here.
def basic_secret(path: Path, identity: str | None) -> str:
    value = private_json(path)
    username = value.get("username")
    if (
        set(value) != {"username", "password"}
        or not isinstance(username, str)
        or re.fullmatch(r"[A-Za-z0-9_-]{1,64}", username) is None
        or (identity is not None and username != identity)
    ):
        raise SecurityError("trial Basic credential must match its configured identity")
    password = value.get("password")
    if not isinstance(password, str) or not 1 <= len(password.encode()) <= 512:
        raise SecurityError("trial password is missing or unbounded")
    if any(ord(c) < 32 or ord(c) == 127 for c in password) or password.lower().startswith("bearer "):
        raise SecurityError("trial requires a charger Basic secret, not an app token")
    return basic_auth_header(username, password)[1]


# Reject ambiguous URL syntax rather than allowing redirects, implicit userinfo or silent endpoint rewriting.
def exact_wss(value: Any) -> str:
    if not isinstance(value, str) or not value.isascii() or len(value) > 1024:
        raise SecurityError("trial requires one bounded WSS endpoint")
    try:
        parsed = urlsplit(value)
        if (
            parsed.scheme != "wss"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.port is None
            or not 1 <= parsed.port <= 65535
            or not parsed.path.startswith("/")
            or any(c.isspace() or ord(c) < 32 for c in value)
            or any(c in value for c in ("%", "\\", "?", "#"))
            or "//" in parsed.path
            or any(part in (".", "..") for part in parsed.path.split("/"))
        ):
            raise ValueError
        return value
    except ValueError:
        raise SecurityError("trial WSS endpoint is malformed or ambiguous") from None


@dataclass(frozen=True, repr=False)
class TrialConfig:
    """Private endpoints and identities are excluded from representations and public status."""

    identity: str
    hardware_identity: str
    listener_host: str
    listener_port: int
    listener_name: str
    charger_ip: str
    charger_path: str
    vendor_url: str
    incoming_auth: str
    upstream_auth: str
    station_profile_kind: str
    station_stack_level: int
    station_profile_start: str | None
    incoming_secret: Path | None
    upstream_secret: Path | None
    native_secret: Path
    certificate: Path
    private_key: Path
    ca_file: Path | None
    journal: Path
    baseline: Path
    evidence: Path
    max_current_a: float = 6
    local_tags: frozenset[str] = field(default_factory=frozenset)

    # Output files must never alias a credential, certificate, configuration input or another state file.
    def check_paths(self, configuration: Path | None = None) -> None:
        inputs = [self.native_secret, self.certificate, self.private_key]
        inputs += [
            path
            for path in (self.incoming_secret, self.upstream_secret, self.ca_file, configuration)
            if path is not None
        ]
        outputs = [
            self.journal,
            Path(str(self.journal) + ".lock"),
            Path(str(self.journal) + "-wal"),
            Path(str(self.journal) + "-shm"),
            self.journal.with_suffix(".status.json"),
            self.journal.with_suffix(".events.jsonl"),
            self.journal.with_suffix(".events.jsonl.1"),
            self.baseline,
            self.baseline.with_suffix(".original-config.json"),
            self.evidence,
        ]
        resolved = [path.resolve(strict=False) for path in outputs]
        input_paths = {path.resolve(strict=False) for path in inputs}
        if len(resolved) != len(set(resolved)) or input_paths.intersection(resolved):
            raise SecurityError("trial output paths must not alias inputs or other state files")
        credentials = [self.native_secret]
        credentials += [path for path in (self.incoming_secret, self.upstream_secret) if path is not None]
        if len({path.resolve(strict=False) for path in credentials}) != len(credentials):
            raise SecurityError("trial credential files must remain independent")

    # Enforce a single station, single source address, 6 A commissioning ceiling and explicit file locations.
    def __post_init__(self) -> None:
        if re.fullmatch(r"[A-Za-z0-9_-]{1,64}", self.identity) is None:
            raise SecurityError("trial charger identity is malformed")
        if re.fullmatch(r"[A-Za-z0-9_-]{1,64}", self.hardware_identity) is None:
            raise SecurityError("trial hardware identity is malformed")
        try:
            StationChoice(self.station_profile_kind, self.station_stack_level, self.station_profile_start)
        except ValueError:
            raise SecurityError("trial station profile selection is invalid") from None
        try:
            host, peer = ipaddress.ip_address(self.listener_host), ipaddress.ip_address(self.charger_ip)
            if (
                host.is_unspecified
                or host.is_multicast
                or peer.is_unspecified
                or peer.is_multicast
                or not (host.is_private or host.is_loopback)
                or not (peer.is_private or peer.is_loopback)
            ):
                raise ValueError
        except ValueError:
            raise SecurityError("trial requires explicit listener and charger IP addresses") from None
        if type(self.listener_port) is not int or not 1 <= self.listener_port <= 65535:
            raise SecurityError("trial requires an explicit nonzero listener port")
        if not isinstance(self.listener_name, str) or not self.listener_name.isascii() or len(self.listener_name) > 253:
            raise SecurityError("trial certificate name is malformed")
        if any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in self.listener_name):
            raise SecurityError("trial certificate name is malformed")
        if self.charger_path != "/charger/" + self.identity:
            raise SecurityError("trial charger path must match its exact identity")
        exact_wss(self.vendor_url)
        if not self.vendor_url.endswith("/" + self.identity):
            raise SecurityError("trial upstream path must include its exact charger identity")
        if self.incoming_auth not in ("basic", "none") or (self.incoming_auth == "basic") != (
            self.incoming_secret is not None
        ):
            raise SecurityError("incoming authentication must be explicitly Basic or explicitly absent")
        if self.upstream_auth not in ("basic", "none") or (self.upstream_auth == "basic") != (
            self.upstream_secret is not None
        ):
            raise SecurityError("upstream authentication must be explicitly Basic or explicitly absent")
        if type(self.max_current_a) not in (int, float) or self.max_current_a != 6:
            raise SecurityError("commissioning package is restricted to six amperes")
        for path in (
            self.native_secret,
            self.certificate,
            self.private_key,
            self.journal,
            self.baseline,
            self.evidence,
        ):
            if not path.is_absolute():
                raise SecurityError("trial requires absolute file paths")
        if self.ca_file is not None and not self.ca_file.is_absolute():
            raise SecurityError("trial requires an absolute CA file path")
        for credential_path in (self.incoming_secret, self.upstream_secret):
            if credential_path is not None and not credential_path.is_absolute():
                raise SecurityError("trial requires absolute credential paths")
        if len(self.local_tags) > 32 or any(
            re.fullmatch(r"[A-Za-z0-9_-]{1,20}", tag) is None for tag in self.local_tags
        ):
            raise SecurityError("trial local tags must be explicit and bounded")
        self.check_paths()

    # Load only an explicit private configuration; do not discover household files or infer credentials.
    @classmethod
    def load(cls, path: Path) -> TrialConfig:
        value = private_json(path)
        keys = set(cls.__dataclass_fields__)
        if set(value) != keys:
            raise SecurityError("trial configuration fields are incomplete or unknown")
        paths = {
            "native_secret",
            "certificate",
            "private_key",
            "journal",
            "baseline",
            "evidence",
        }
        try:
            for key in paths:
                value[key] = Path(value[key])
            value["incoming_secret"] = None if value["incoming_secret"] is None else Path(value["incoming_secret"])
            value["upstream_secret"] = None if value["upstream_secret"] is None else Path(value["upstream_secret"])
            value["ca_file"] = None if value["ca_file"] is None else Path(value["ca_file"])
            if not isinstance(value["local_tags"], list) or not all(
                isinstance(tag, str) for tag in value["local_tags"]
            ):
                raise ValueError
            value["local_tags"] = frozenset(value["local_tags"])
            result = cls(**value)
            result.check_paths(path)
            return result
        except TypeError, ValueError:
            raise SecurityError("trial configuration contains invalid field types") from None


@dataclass(frozen=True, repr=False)
class PreparedTLS:
    """Only in-memory contexts and separately admitted credentials leave preflight."""

    server: ssl.SSLContext
    client: ssl.SSLContext
    incoming: str | None
    upstream: str | None
    certificate_expires_at: float = 0


# OpenSSL checks the configured IP/DNS SAN; the public certificate dates must cover the whole bounded trial.
def verify_listener_certificate(descriptor: int, name: str) -> float:
    try:
        try:
            ipaddress.ip_address(name)
            option = "-checkip"
        except ValueError:
            if re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?", name) is None:
                raise SecurityError("trial certificate DNS name is malformed")
            option = "-checkhost"
        result = subprocess.run(
            ["/usr/bin/openssl", "x509", "-in", f"/proc/self/fd/{descriptor}", "-noout", "-dates", option, name],
            capture_output=True,
            text=True,
            timeout=5,
            pass_fds=(descriptor,),
        )
        if result.returncode:
            raise ValueError
        dates = dict(
            line.split("=", 1) for line in result.stdout.splitlines() if line.startswith(("notBefore=", "notAfter="))
        )
        if (
            not ssl.cert_time_to_seconds(dates["notBefore"])
            <= time.time()
            < ssl.cert_time_to_seconds(dates["notAfter"]) - 1800
        ):
            raise ValueError
        return ssl.cert_time_to_seconds(dates["notAfter"])
    except OSError, ValueError, KeyError, subprocess.TimeoutExpired:
        raise SecurityError("listener certificate name or validity does not cover the trial") from None


# Load TLS keys from verified bytes through anonymous descriptors, avoiding a second pathname open race.
def prepare_tls(config: TrialConfig) -> PreparedTLS:
    incoming = basic_secret(config.incoming_secret, config.identity) if config.incoming_secret is not None else None
    upstream = basic_secret(config.upstream_secret, config.identity) if config.upstream_secret is not None else None
    certificate = read_file(config.certificate, secret=False)
    private_key = read_file(config.private_key)
    descriptors: list[int] = []
    try:
        for payload in (certificate, private_key):
            descriptor = os.memfd_create("ocpp-trial-tls", os.MFD_CLOEXEC)
            descriptors.append(descriptor)
            with os.fdopen(os.dup(descriptor), "wb") as stream:
                stream.write(payload)
            os.lseek(descriptor, 0, os.SEEK_SET)
        server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        expires = verify_listener_certificate(descriptors[0], config.listener_name)
        os.lseek(descriptors[0], 0, os.SEEK_SET)
        server.minimum_version = ssl.TLSVersion.TLSv1_2
        server.load_cert_chain(
            f"/proc/self/fd/{descriptors[0]}", f"/proc/self/fd/{descriptors[1]}", password=lambda: ""
        )
        ca_data = None if config.ca_file is None else read_file(config.ca_file, secret=False).decode("ascii")
        client = ssl.create_default_context(cadata=ca_data)
        client.minimum_version = ssl.TLSVersion.TLSv1_2
        return PreparedTLS(server, client, incoming, upstream, expires)
    except OSError, ValueError, UnicodeError:
        raise SecurityError("trial TLS material could not be validated") from None
    finally:
        for descriptor in descriptors:
            os.close(descriptor)
