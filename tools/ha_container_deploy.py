"""Validated in-memory custom-component installation for the deployment helper's explicit Docker transport."""

from __future__ import annotations

import ctypes
import fcntl
import hashlib
import io
import json
import os
import platform
import sys
import tarfile
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from functools import partial
from pathlib import Path, PurePosixPath
from types import ModuleType
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


# Exchange two fully written directories in one Linux operation so the live path never disappears.
def exchange_directories(staged: Path, installed: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    args = (-100, os.fsencode(staged), -100, os.fsencode(installed), 2)  # AT_FDCWD and RENAME_EXCHANGE
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is not None:
        result = renameat2(*args)
    else:
        # The Alpine HA image uses musl, which may lack a libc wrapper for this Linux syscall.
        number = {"x86_64": 316, "aarch64": 276}.get(platform.machine())
        if number is None:
            raise OSError("atomic HA component replacement is unavailable on this architecture")
        result = libc.syscall(number, *args)
    if result:
        error = ctypes.get_errno()
        raise OSError(error, f"atomic HA component replacement failed: {os.strerror(error)}")


# Serialize container and SSH installs for the same component throughout activation.
@contextmanager
def installation_lock(backups: Path) -> Iterator[None]:
    """Share the installed component's lock with both deployment transports."""
    with (backups / ".install.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        shared = backups / ".install-lock"
        shared.mkdir()
        try:
            yield
        finally:
            shared.rmdir()


# Back up the previous directory and publish only a fully validated replacement.
def install(
    config: Path,
    domain: str,
    archive: bytes,
    expected: dict[str, str],
    *,
    prepare_restart: Callable[[], tuple[Callable[[], None], Callable[[], None]]] | None = None,
) -> dict[str, Any]:
    files = validated_files(domain, archive, expected)
    parent = config / "custom_components"
    target, backups = parent / domain, config / ("." + domain + "-backup")
    parent.mkdir(parents=True, exist_ok=True)
    backups.mkdir(mode=0o700, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix=f".{domain}-deploy-", dir=parent, ignore_cleanup_errors=True) as scratch:
        staged = Path(scratch) / domain
        for name, data in files.items():
            path = staged / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        actual = {
            str(path.relative_to(staged)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in staged.rglob("*")
            if path.is_file()
        }
        if actual != expected:
            raise ValueError("staged component verification failed")

        with installation_lock(backups):
            restart = prepare_restart() if prepare_restart is not None else None
            if target.is_symlink() or (target.exists() and not target.is_dir()):
                raise ValueError("component path must be a directory")
            replacing = target.exists()
            backup = None
            if replacing:
                if any(path.is_symlink() for path in target.rglob("*")):
                    raise ValueError("installed component contains a symlink")
                backup = backups / f"{domain}-{time.time_ns()}.tar.gz"
                pending = backup.with_suffix(".tmp")
                try:
                    with tarfile.open(pending, "w:gz") as package:
                        package.add(target, arcname=domain)
                    pending.rename(backup)
                finally:
                    pending.unlink(missing_ok=True)
            if replacing:
                exchange_directories(staged, target)
            else:
                staged.rename(target)
            if restart is not None:
                check, activate = restart
                try:
                    check()
                except (Exception, SystemExit) as error:
                    # Core has not been asked to restart. Restore the old files under the same lock.
                    try:
                        if replacing:
                            exchange_directories(staged, target)
                        else:
                            target.rename(staged)
                    except OSError:
                        raise SystemExit(f"{error} Restoring the previous installation files failed.") from None
                    if backup is not None:
                        with suppress(OSError):
                            backup.unlink(missing_ok=True)
                    raise SystemExit(f"{error} Restored the previous installation files.") from None
                try:
                    activate()
                except (Exception, SystemExit) as error:
                    # Core may have restarted using the candidate. Keep those files and the previous backup.
                    raise SystemExit(f"{error} The new component remains installed.") from None
            older = [path for path in backups.glob(domain + "-*.tar.gz") if path != backup]
            keep = 2 if backup is not None else 3
            for old in sorted(older, key=lambda path: path.stat().st_mtime_ns, reverse=True)[keep:]:
                old.unlink()
    return {"domain": domain, "verified_files": len(files), "backup_created": replacing}


# Receive validated code and the optional private restart input without writing either to the container.
def main() -> None:
    stream = sys.stdin.buffer
    if len(sys.argv) not in (3, 5):
        raise SystemExit("The Home Assistant installer received invalid deployment arguments.")
    prepare_restart = None
    if len(sys.argv) == 5:
        program = ModuleType("deployment_ha_restart")
        sys.modules[program.__name__] = program
        exec(compile(sys.argv[4], "ha_restart.py", "exec"), program.__dict__)
        port = int(sys.argv[3])
        token = stream.readline().decode().rstrip("\n")

        prepare_restart = partial(program.prepare_container_restart, port, token)

    result = install(
        Path("/config"), sys.argv[1], stream.read(), json.loads(sys.argv[2]), prepare_restart=prepare_restart
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
