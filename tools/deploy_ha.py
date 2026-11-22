#!/usr/bin/env python3
"""Deploy custom_components/ocpp_hub to the Home Assistant server over SSH, with a key and no password.

Steps: checks -> back up the current copy on the server -> stream the tracked files as a tar -> swap the directory ->
verify every file by sha256 -> optional restart of Home Assistant.

Authentication stays with a key: HA_HOST, HA_SSH_USER and the PATH of a key (HA_SSH_KEY) come from the git-ignored
``.env``; a password is never read. With ``--batch`` nothing is ever prompted. Standard library only.

    python tools/deploy_ha.py --dry-run     # show target and files, change nothing
    python tools/deploy_ha.py --batch       # checks, deploy, verify
    python tools/deploy_ha.py --batch --restart --yes
"""

from __future__ import annotations

import argparse
import hashlib
import http.client
import io
import json
import os
import re
import shlex
import subprocess
import sys
import tarfile
import tempfile
from dataclasses import dataclass, field, replace
from pathlib import Path
from urllib.parse import urlparse, urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools import ha_restart  # noqa: E402

INTEGRATION = Path("custom_components") / "ocpp_hub"
BUILD_FILE = "BUILD"
REMOTE_CONFIG = "/config"
KEEP_BACKUPS = 3


@dataclass(frozen=True)
class Target:
    """Where and how to reach the Home Assistant server."""

    host: str
    user: str
    config_dir: str = REMOTE_CONFIG
    key: str | None = None  # path to a private key; ssh reads it, this script never does
    sudo: bool = True  # the SSH user cannot write in /config and needs passwordless sudo

    # The folder the integration lives in on the server.
    # Deploy, backup and verification must agree on one path.
    @property
    def remote_dir(self) -> str:
        """Where the integration lives on the server."""
        return f"{self.config_dir}/custom_components/ocpp_hub"

    # The folder holding timestamped backups of the previous copy.
    # It lives outside custom_components so Home Assistant never loads a backup.
    @property
    def backup_dir(self) -> str:
        """Where timestamped backups of the previous copy are kept, outside custom_components so HA never loads them."""
        return f"{self.config_dir}/.ocpp_hub-backup"


# Read KEY=value pairs from a .env text; comments and blank lines are ignored and quotes are stripped.
# Deployment settings live in a git-ignored file and must never be exported to child processes.
def parse_env(text: str) -> dict[str, str]:
    env: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip().strip("\"'")
    return env


# Return the bare host name of a value that may be a host or a URL such as http://homeassistant.local:8123.
# .env often holds the API URL while SSH needs only the host.
def _bare_host(value: str) -> str:
    return (urlparse(value).hostname or value) if "//" in value else value


# Resolve host, SSH user and key path from options or .env, and refuse to guess.
# A missing user must stop the run with a readable message instead of connecting as the wrong account.
def target_from_env(
    env: dict[str, str], host: str | None = None, user: str | None = None, key: str | None = None
) -> Target:
    resolved_host = host or env.get("HA_HOST") or urlparse(env.get("HA_API_URL", "")).hostname
    resolved_user = user or env.get("HA_SSH_USER")
    if not resolved_host or not resolved_user:
        raise SystemExit("No SSH target: set HA_HOST and HA_SSH_USER in .env or pass --host and --user.")
    resolved_key = key or env.get("HA_SSH_KEY")
    key_path = str(Path(resolved_key).expanduser()) if resolved_key else None
    return Target(_bare_host(resolved_host), resolved_user, key=key_path)


# List the files to deploy relative to the integration folder: tracked and untracked-but-not-ignored, no caches.
def relative_files(root: Path = ROOT, integration: Path = INTEGRATION) -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "-c", "-o", "--exclude-standard", "-z", "--", str(integration)],
        cwd=root,
        capture_output=True,
        check=True,
    ).stdout.decode()
    prefix = integration.as_posix() + "/"
    return sorted(p[len(prefix) :] for p in out.split("\0") if p.startswith(prefix) and "__pycache__" not in p)


# Stamp the commit and mark local tracked or untracked component code shipped by an unchecked deploy.
def write_build_stamp(root: Path = ROOT, integration: Path = INTEGRATION) -> str:
    head = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"], cwd=root, capture_output=True, text=True, check=True
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=normal", "--", str(integration)],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    stamp = head + ("+dirty" if dirty else "")
    (root / integration / BUILD_FILE).write_text(stamp + "\n")
    return stamp


# Turn `sha256sum` output into a {relative path: digest} mapping; paths may start with './'.
# The remote listing is plain text and must be compared with the local digests.
def parse_sha256sum(output: str) -> dict[str, str]:
    digests: dict[str, str] = {}
    for line in output.splitlines():
        if line.strip():
            digest, path = line.split(None, 1)
            digests[path.strip().removeprefix("./")] = digest
    return digests


# Compare local and remote digests and return (missing on the server, unexpected on the server, different).
# A deploy is only done when all three lists are empty.
def compare(local: dict[str, str], remote: dict[str, str]) -> tuple[list[str], list[str], list[str]]:
    missing = sorted(set(local) - set(remote))
    extra = sorted(set(remote) - set(local))
    different = sorted(name for name in set(local) & set(remote) if local[name] != remote[name])
    return missing, extra, different


# Build the POSIX sh run on the server: back up the old copy, unpack stdin next to it, swap, prune old backups.
# Unpacking completely before replacing means a broken transfer leaves the running integration untouched;
# the script avoids GNU-only flags because the SSH add-on runs BusyBox.
def remote_install_script(target: Target, keep: int = KEEP_BACKUPS) -> str:
    d, b, parent = target.remote_dir, target.backup_dir, f"{target.config_dir}/custom_components"
    return " && ".join(
        [
            f"mkdir -p '{b}' '{parent}'",
            f"if [ -d '{d}' ]; then tar -C '{parent}' -czf '{b}/ocpp_hub-'$(date +%Y%m%d-%H%M%S)'.tar.gz' ocpp_hub; fi",
            f"rm -rf '{parent}/.ocpp_hub-new' && mkdir '{parent}/.ocpp_hub-new'",
            f"tar -C '{parent}/.ocpp_hub-new' -xf -",
            f"rm -rf '{d}' && mv '{parent}/.ocpp_hub-new' '{d}'",
            f"ls -1t '{b}' | tail -n +{keep + 1} | while read f; do rm -f '{b}'/\"$f\"; done",
        ]
    )


# Wrap a script in `sudo -n sh -c` when needed; stdin (the tar stream) passes straight through.
# `-n` makes sudo fail instead of asking for a password, so nothing here can prompt for one.
def privileged(script: str, sudo: bool) -> str:
    return f"sudo -n sh -c {shlex.quote(script)}" if sudo else script


# Build an ssh invocation that shares one authenticated connection (ControlMaster).
# A key is passed by path only and `batch` forbids any prompt, so an unauthorised key fails instead of asking.
def ssh_command(
    target: Target, control_path: str, *remote: str, master: bool = False, batch: bool = False
) -> list[str]:
    options = ["-o", f"ControlPath={control_path}", "-o", "ControlMaster=yes" if master else "ControlMaster=no"]
    if target.key:
        options += ["-i", target.key, "-o", "IdentitiesOnly=yes"]
    if batch:
        options += ["-o", "BatchMode=yes"]
    if master:
        options += ["-o", "ControlPersist=300"]
    return ["ssh", *options, f"{target.user}@{target.host}", *remote]


# Run the CI-equivalent checks and abort the deploy when they fail.
# A failing tree must never reach the server; one command gives the same answer as CI.
def run_checks(
    root: Path = ROOT,
    collection_executors: Path | None = None,
    *,
    snapshot: Path | None = None,
    domain: str = "ocpp_hub",
) -> None:
    command = [sys.executable, str(root / "tools" / "validate.py")]
    command += ["--deployment-target", "ics-client" if domain == "ics_proxy" else "legacy-ha"]
    if collection_executors is not None:
        command += ["--collection-executors", str(collection_executors)]
    if snapshot is not None:
        command += ["--validated-snapshot", str(snapshot)]
    result = subprocess.run(command, cwd=root)
    if result.returncode:
        raise SystemExit("Checks failed; nothing was deployed. Fix them (or use --skip-checks if you know why).")


# Both transports send the same archive and verify the same bytes that were packaged.
def make_archive(directory: Path, files: list[str], pinned: dict[str, str]) -> tuple[bytes, dict[str, str]]:
    data = io.BytesIO()
    digests = {}
    with tarfile.open(fileobj=data, mode="w") as package:
        for name in files:
            path = directory / name
            if path.is_symlink() or not path.is_file():
                raise SystemExit("Deployment accepts regular files only")
            content = path.read_bytes()
            digest = hashlib.sha256(content).hexdigest()
            if name in pinned and digest != pinned[name]:
                raise SystemExit("Validated integration bytes changed before packaging")
            info = tarfile.TarInfo(name)
            info.mode = 0o755 if path.stat().st_mode & 0o111 else 0o644
            info.size = len(content)
            package.addfile(info, io.BytesIO(content))
            digests[name] = digest
    return data.getvalue(), digests


# Select the exact validated inventory, or the explicitly requested unchecked working tree.
def prepare_archive(
    integration: Path, scratch: Path, *, skip_checks: bool, collection_executors: Path | None
) -> tuple[Path, bytes, dict[str, str]]:
    source = ROOT
    pinned: dict[str, str] = {}
    if skip_checks:
        files = relative_files(source, integration)
        stamp = write_build_stamp(source, integration)
    else:
        run_checks(source, collection_executors, snapshot=scratch, domain=integration.name)
        metadata = json.loads((scratch / "snapshot.json").read_text())
        source = scratch / "source"
        prefix = integration.as_posix() + "/"
        pinned = {
            row["path"][len(prefix) :]: row["sha256"] for row in metadata["files"] if row["path"].startswith(prefix)
        }
        files = sorted(pinned)
        stamp = metadata["commit"][:12]
        (source / integration / BUILD_FILE).write_text(stamp + "\n")
    if not files:
        raise SystemExit(f"No files found under {integration}.")
    print(f"Build {stamp}")
    data, digests = make_archive(source / integration, sorted({*files, BUILD_FILE}), pinned)
    return source, data, digests


# Open the shared connection, install the files, and verify every file by checksum.
# The swap is only reported as done when the server copy is byte-identical to the local files.
def deploy(target: Target, archive: bytes, expected: dict[str, str], control_path: str, batch: bool = False) -> None:
    # Build an ssh command line for this target on the shared connection.
    # Every remote step must use the same control path and batch setting.
    def ssh(*remote: str, master: bool = False) -> list[str]:
        return ssh_command(target, control_path, *remote, master=master, batch=batch)

    subprocess.run(ssh("true", master=True), check=True)
    try:
        subprocess.run(ssh(privileged(remote_install_script(target), target.sudo)), input=archive, check=True)
        listing = subprocess.run(
            ssh(f"cd '{target.remote_dir}' && find . -type f -exec sha256sum {{}} +"),
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        missing, extra, different = compare(expected, parse_sha256sum(listing))
        if missing or extra or different:
            raise SystemExit(f"Verification failed. missing={missing} unexpected={extra} different={different}")
        print(f"Deployed and verified {len(expected)} files in {target.remote_dir}")
    finally:
        subprocess.run(
            ["ssh", "-O", "exit", "-o", f"ControlPath={control_path}", f"{target.user}@{target.host}"],
            capture_output=True,
        )


@dataclass(frozen=True)
class RestartAccess:
    """Private API settings and the selected transport's optional binding."""

    url: str
    token: str = field(repr=False)
    ssh_host: str | None = None
    uptime_entity: str = "sensor.uptime"
    container_port: int = 8123


# Collect the existing API credentials without exposing the token to arguments or logs.
def restart_access(env: dict[str, str], path: Path | None) -> RestartAccess:
    try:
        if path is None:
            access = RestartAccess(
                env.get("HA_API_URL", ""),
                env.get("HA_API_TOKEN", ""),
                env.get("HA_API_SSH_HOST"),
                env.get("HA_UPTIME_ENTITY", "sensor.uptime"),
            )
        else:
            private = json.loads(path.read_text())
            access = RestartAccess(
                private["hass_url"],
                Path(private["token_file"]).read_text().strip(),
                private.get("ssh_host"),
                private.get("uptime_entity", "sensor.uptime"),
                private.get("container_port", 8123),
            )
    except KeyError, OSError, TypeError, ValueError:
        raise SystemExit("The private Home Assistant API settings could not be read.") from None
    if not isinstance(access.url, str) or not isinstance(access.ssh_host, (str, type(None))):
        raise SystemExit("The private Home Assistant API URL or SSH target is invalid.")
    if not access.token or "\n" in access.token or "\r" in access.token:
        raise SystemExit("A Home Assistant API token is needed for --restart.")
    return access


# Match the API configuration to the SSH deployment target before sending any request.
def ssh_restart_api(target: Target, access: RestartAccess) -> ha_restart.HomeAssistantAPI:
    url = urlsplit(access.url.rstrip("/"))
    host = url.hostname
    if url.scheme not in ("http", "https") or not host or url.username or url.password or url.query or url.fragment:
        raise SystemExit("A valid Home Assistant API URL is needed for the SSH restart.")
    if _bare_host(access.ssh_host or host).lower() != target.host.lower():
        raise SystemExit("The Home Assistant API configuration does not name the selected SSH target.")
    if not isinstance(access.uptime_entity, str) or re.fullmatch(r"sensor\.[a-z0-9_]+", access.uptime_entity) is None:
        raise SystemExit("The SSH restart requires a configured Home Assistant Uptime sensor.")
    prefix = url.path if url.path.endswith("/api") else url.path + "/api"
    connection = http.client.HTTPSConnection if url.scheme == "https" else http.client.HTTPConnection
    return ha_restart.HomeAssistantAPI(lambda: connection(host, url.port, timeout=10), access.token, prefix)


# Pin Docker operations to the inspected container rather than resolving its name again after installation.
def container_id(name: str) -> str:
    command = ["docker", "inspect", "--type", "container", "--format", "{{.Id}}", name]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    value = result.stdout.strip()
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise SystemExit("Cannot resolve the selected Home Assistant container.")
    return value


# Keep a container install and the optional restart in the same process and under the same lock.
def deploy_container(
    container: str, source: Path, domain: str, archive: bytes, expected: dict[str, str], access: RestartAccess | None
) -> None:
    installer = (source / "tools/ha_container_deploy.py").read_text()
    command = ["docker", "exec", "-i", container, "python", "-c", installer, domain, json.dumps(expected)]
    if access is not None:
        port = access.container_port
        if type(port) is not int or not 1 <= port <= 65535:
            raise SystemExit("The Home Assistant container API port is invalid.")
        command.extend((str(port), (source / "tools/ha_restart.py").read_text()))
        archive = access.token.encode() + b"\n" + archive
    try:
        subprocess.run(command, input=archive, check=True)
    except subprocess.CalledProcessError as error:
        raise SystemExit(error.returncode) from None


# Resolve the optional restart confirmation before modifying the selected installation.
def confirm_restart(assume_yes: bool, batch: bool) -> bool:
    if assume_yes:
        return True
    if batch:
        raise SystemExit("A batch restart also requires --yes.")
    question = "Restart Home Assistant now? Automations and chargers are offline for 1-5 minutes. [y/N] "
    return input(question).strip().lower() == "y"


# Command line entry point.
# One command covers checks, deploy, verification and the optional restart.
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", help="override HA_HOST")
    parser.add_argument("--user", help="override HA_SSH_USER")
    parser.add_argument("--key", help="path to an SSH private key (override HA_SSH_KEY)")
    parser.add_argument("--no-sudo", action="store_true", help="do not use sudo on the server")
    parser.add_argument("--batch", action="store_true", help="never prompt; fails instead of asking for a password")
    parser.add_argument("--dry-run", action="store_true", help="show the plan, change nothing")
    parser.add_argument("--skip-checks", action="store_true", help="do not run tools/validate.py first")
    parser.add_argument("--restart", action="store_true", help="validate the HA config and restart Home Assistant")
    parser.add_argument("--yes", action="store_true", help="with --restart: do not ask for confirmation")
    parser.add_argument("--domain", choices=("ocpp_hub", "ics_proxy"), default="ocpp_hub")
    parser.add_argument("--container", help="use an explicit local Docker container instead of SSH")
    parser.add_argument("--ha-api-config", type=Path, help="private existing hass_url/token_file configuration")
    parser.add_argument(
        "--collection-executors", type=Path, help="explicit trusted qualification collection executor map"
    )
    args = parser.parse_args()
    env = parse_env((ROOT / ".env").read_text()) if (ROOT / ".env").exists() else {}
    if args.container:
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", args.container) is None:
            raise SystemExit("Invalid container name")
        integration = Path("custom_components") / args.domain
        files = sorted({*relative_files(ROOT, integration), BUILD_FILE})
        if args.dry_run:
            print(f"Dry run: {args.domain}, container {args.container}, {len(files)} files; no changes")
            return 0
        selected = container_id(args.container)
        restart_requested = args.restart and confirm_restart(args.yes, args.batch)
        access = restart_access(env, args.ha_api_config) if restart_requested else None
        with tempfile.TemporaryDirectory() as tmp:
            source, archive, expected = prepare_archive(
                integration, Path(tmp), skip_checks=args.skip_checks, collection_executors=args.collection_executors
            )
            deploy_container(selected, source, args.domain, archive, expected, access)
        if access is not None:
            print("Verified Home Assistant is running in a new Core process.")
        return 0
    if args.domain != "ocpp_hub":
        raise SystemExit("The separate proxy client requires explicit container transport")
    target = replace(target_from_env(env, args.host, args.user, args.key), sudo=not args.no_sudo)
    files = relative_files(ROOT)
    if not files:
        raise SystemExit(f"No files found under {INTEGRATION}.")
    files = sorted({*files, BUILD_FILE})
    key_note = f"key {target.key}" if target.key else "no key: ssh will ask for the password"
    print(
        f"Target {target.user}@{target.host}:{target.remote_dir} "
        f"({len(files)} files, backups in {target.backup_dir}, {key_note})"
    )
    if args.dry_run:
        print("\n".join(f"  {name}" for name in files))
        print("Dry run: would run the checks, back up, deploy and verify. Nothing was changed.")
        return 0
    access = restart_access(env, args.ha_api_config) if args.restart and confirm_restart(args.yes, args.batch) else None
    if access is not None:
        api = ssh_restart_api(target, access)

        # Recheck the same API sensor before and after requesting restart.
        def lifetime():
            return ha_restart.uptime(api, access.uptime_entity, target.config_dir)

        ssh_before = lifetime()
    with tempfile.TemporaryDirectory() as tmp:
        _, archive, expected = prepare_archive(
            INTEGRATION, Path(tmp), skip_checks=args.skip_checks, collection_executors=args.collection_executors
        )
        deploy(target, archive, expected, os.path.join(tmp, "ssh-%C"), args.batch)
    if access is not None:
        ha_restart.restart(api, ssh_before, lifetime)
        print("Verified Home Assistant is running with a new Core start time.")
    else:
        print("Next: restart Home Assistant (a changed integration is loaded after a restart).")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ha_restart.RestartError as error:
        raise SystemExit(str(error)) from None
    except http.client.HTTPException, OSError, ValueError:
        raise SystemExit("Home Assistant access failed.") from None
