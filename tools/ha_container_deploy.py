"""Validated in-memory custom-component installation for the deployment helper's explicit Docker transport."""

from __future__ import annotations

import hashlib
import io
import json
import shutil
import sys
import tarfile
import time
from pathlib import Path, PurePosixPath
from typing import Any


# Validate the entire small archive and compile source before backing up or touching installed component files.
def validated_files(domain: str, archive: bytes, expected: dict[str, str]) -> dict[str, bytes]:
    if domain not in ("ocpp_hub", "ics_proxy") or len(archive) > 16 * 1024 * 1024:
        raise ValueError("unsupported component or oversized archive")
    result = {}
    with tarfile.open(fileobj=io.BytesIO(archive)) as package:
        for member in package:
            path = PurePosixPath(member.name)
            if (
                path.is_absolute()
                or ".." in path.parts
                or not member.isfile()
                or member.name in result
                or member.size > 2 * 1024 * 1024
                or member.name not in expected
            ):
                raise ValueError("archive contains unexpected paths or file types")
            stream = package.extractfile(member)
            if stream is None:
                raise ValueError("archive file is missing")
            data = stream.read()
            if hashlib.sha256(data).hexdigest() != expected[member.name]:
                raise ValueError("archive digest mismatch")
            if member.name.endswith(".py"):
                compile(data, member.name, "exec")
            result[member.name] = data
    if set(result) != set(expected) or json.loads(result["manifest.json"])["domain"] != domain:
        raise ValueError("archive component identity or inventory mismatch")
    return result


# Keep a verified previous copy outside custom_components and restore it if writing or verification fails.
def install(config: Path, domain: str, archive: bytes, expected: dict[str, str]) -> dict[str, Any]:
    files = validated_files(domain, archive, expected)
    parent = config / "custom_components"
    target, backups = parent / domain, config / ("." + domain + "-backup")
    if target.is_symlink():
        raise ValueError("component directory must not be a symlink")
    parent.mkdir(parents=True, exist_ok=True)
    backups.mkdir(mode=0o700, exist_ok=True)
    backup = None
    if target.exists():
        if any(p.is_symlink() for p in target.rglob("*")):
            raise ValueError("installed component contains a symlink")
        backup = backups / (domain + "-" + str(time.time_ns()) + ".tar.gz")
        with tarfile.open(backup, "w:gz") as package:
            package.add(target, arcname=domain)
    try:
        if target.exists():
            shutil.rmtree(target)
        target.mkdir()
        for name, data in files.items():
            path = target / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        actual = {
            str(path.relative_to(target)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in target.rglob("*")
            if path.is_file() and "__pycache__" not in path.parts
        }
        if actual != expected:
            raise ValueError("installed component verification failed")
    except Exception:
        # This installer boundary must undo partial disk changes before reporting a failure to the operator.
        if target.exists():
            shutil.rmtree(target)
        if backup is not None:
            with tarfile.open(backup) as package:
                package.extractall(parent, filter="data")
        raise
    for old in sorted(backups.glob(domain + "-*.tar.gz"), reverse=True)[3:]:
        old.unlink()
    return {"domain": domain, "verified_files": len(files), "backup_created": backup is not None}


# The Docker transport supplies only a fixed component choice, digests and tar bytes; no credential is accepted.
def main() -> None:
    result = install(Path("/config"), sys.argv[1], sys.stdin.buffer.read(), json.loads(sys.argv[2]))
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
