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
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, replace
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
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
# What git knows is what ships; a stray local file or a __pycache__ must never reach the server.
def relative_files(root: Path = ROOT, integration: Path = INTEGRATION) -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "-c", "-o", "--exclude-standard", "-z", "--", str(integration)],
        cwd=root,
        capture_output=True,
        check=True,
    ).stdout.decode()
    prefix = integration.as_posix() + "/"
    return sorted(p[len(prefix) :] for p in out.split("\0") if p.startswith(prefix) and "__pycache__" not in p)


# Write the commit the deployed code comes from into the integration's BUILD file and return it.
# A log line must say which code wrote it; without a stamp an old build and the current one look identical.
def write_build_stamp(root: Path = ROOT, integration: Path = INTEGRATION) -> str:
    head = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"], cwd=root, capture_output=True, text=True, check=True
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=no", "--", str(integration)],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    stamp = head + ("+dirty" if dirty else "")
    (root / integration / BUILD_FILE).write_text(stamp + "\n")
    return stamp


# Compute the sha256 of every local file to deploy.
# The server copy is verified against these digests after the swap.
def local_digests(files: list[str], root: Path = ROOT, integration: Path = INTEGRATION) -> dict[str, str]:
    return {name: hashlib.sha256((root / integration / name).read_bytes()).hexdigest() for name in files}


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
def run_checks(root: Path = ROOT) -> None:
    result = subprocess.run([sys.executable, str(root / "tools" / "validate.py")], cwd=root)
    if result.returncode:
        raise SystemExit("Checks failed; nothing was deployed. Fix them (or use --skip-checks if you know why).")


# Start tar writing the files, relative to the integration folder, to its stdout.
# Entries are root-owned so the server copy matches the other components regardless of local user ids.
def stream_tar(files: list[str], root: Path = ROOT) -> subprocess.Popen[bytes]:
    command = [
        "tar",
        "-C",
        str(root / INTEGRATION),
        "--owner=0",
        "--group=0",
        "--numeric-owner",
        "--null",
        "-T",
        "-",
        "-cf",
        "-",
    ]
    tar = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    assert tar.stdin is not None
    tar.stdin.write(("\0".join(files) + "\0").encode())
    tar.stdin.close()
    return tar


# Open the shared connection, install the files, and verify every file by checksum.
# The swap is only reported as done when the server copy is byte-identical to the local files.
def deploy(target: Target, files: list[str], control_path: str, batch: bool = False) -> None:
    # Build an ssh command line for this target on the shared connection.
    # Every remote step must use the same control path and batch setting.
    def ssh(*remote: str, master: bool = False) -> list[str]:
        return ssh_command(target, control_path, *remote, master=master, batch=batch)

    subprocess.run(ssh("true", master=True), check=True)
    try:
        tar = stream_tar(files)
        subprocess.run(ssh(privileged(remote_install_script(target), target.sudo)), stdin=tar.stdout, check=True)
        assert tar.stdout is not None
        tar.stdout.close()
        if tar.wait():
            raise SystemExit("tar failed locally; verify the server copy before restarting anything.")
        listing = subprocess.run(
            ssh(f"cd '{target.remote_dir}' && find . -type f -exec sha256sum {{}} +"),
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        missing, extra, different = compare(local_digests(files), parse_sha256sum(listing))
        if missing or extra or different:
            raise SystemExit(f"Verification failed. missing={missing} unexpected={extra} different={different}")
        print(f"Deployed and verified {len(files)} files in {target.remote_dir}")
    finally:
        subprocess.run(
            ["ssh", "-O", "exit", "-o", f"ControlPath={control_path}", f"{target.user}@{target.host}"],
            capture_output=True,
        )


# Poll the REST API until it answers 200 again after a restart.
# The API first goes away and then comes back; a short wait first avoids mistaking the old instance for the new.
def wait_for_home_assistant(base: str, token: str, timeout_s: int = 300, poll_s: int = 5) -> bool:
    request = urllib.request.Request(base + "/", headers={"Authorization": f"Bearer {token}"})
    time.sleep(poll_s)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                if response.status == 200:
                    return True
        except OSError, http.client.HTTPException:
            pass
        time.sleep(poll_s)
    return False


# Validate the Home Assistant configuration and restart it over the REST API, after a confirmation.
# A new or changed integration is only loaded after a restart; the token is read from .env and never printed.
def restart_home_assistant(env: dict[str, str], assume_yes: bool) -> None:
    base, token = env.get("HA_API_URL", "").rstrip("/"), env.get("HA_API_TOKEN", "")
    if not base or not token:
        raise SystemExit("HA_API_URL and HA_API_TOKEN are needed for --restart.")
    question = "Restart Home Assistant now? Automations and chargers are offline for 1-5 minutes. [y/N] "
    if not assume_yes and input(question).strip().lower() != "y":
        print("Not restarting. A new integration is only found after a restart.")
        return

    # POST to the Home Assistant API with the bearer token and return the JSON answer.
    # The config check and the restart share one authenticated call and the token is never printed.
    def call(path: str) -> dict[str, object]:
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        request = urllib.request.Request(base + path, data=b"{}", method="POST", headers=headers)
        with urllib.request.urlopen(request, timeout=60) as response:
            body: dict[str, object] = json.loads(response.read() or "{}")
            return body

    check = call("/config/core/check_config")
    if check.get("result") != "valid":
        raise SystemExit(f"HA reports an invalid configuration, not restarting: {check.get('errors')}")
    try:
        call("/services/homeassistant/restart")
    except TimeoutError, ConnectionError, http.client.HTTPException, urllib.error.URLError:
        pass  # HA drops the connection while it shuts down; that is the expected outcome
    print("Restart requested, waiting for Home Assistant...")
    print(
        "Home Assistant is back." if wait_for_home_assistant(base, token) else "No answer within 5 minutes; check it."
    )


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
    args = parser.parse_args()
    env = parse_env((ROOT / ".env").read_text()) if (ROOT / ".env").exists() else {}
    if args.container:
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", args.container) is None:
            raise SystemExit("Invalid container name")
        integration = Path("custom_components") / args.domain
        files = sorted({*relative_files(integration=integration), BUILD_FILE})
        if args.dry_run:
            print(f"Dry run: {args.domain}, container {args.container}, {len(files)} files; no changes")
            return 0
        if not args.skip_checks:
            run_checks()
        print(f"Build {write_build_stamp(integration=integration)}")
        data = io.BytesIO()
        with tarfile.open(fileobj=data, mode="w") as package:
            for name in files:
                package.add(ROOT / integration / name, arcname=name, recursive=False)
        expected = local_digests(files, integration=integration)
        installer = (ROOT / "tools/ha_container_deploy.py").read_text()
        subprocess.run(
            ["docker", "exec", "-i", args.container, "python", "-c", installer, args.domain, json.dumps(expected)],
            input=data.getvalue(),
            check=True,
        )
        if args.restart:
            if args.ha_api_config is not None:
                private = json.loads(args.ha_api_config.read_text())
                api_base = private["hass_url"].rstrip("/")
                if not api_base.endswith("/api"):
                    api_base += "/api"
                env.update(HA_API_URL=api_base, HA_API_TOKEN=Path(private["token_file"]).read_text().strip())
            restart_home_assistant(env, args.yes)
        return 0
    if args.domain != "ocpp_hub":
        raise SystemExit("The separate proxy client requires explicit container transport")
    target = replace(target_from_env(env, args.host, args.user, args.key), sudo=not args.no_sudo)
    files = relative_files()
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
    if not args.skip_checks:
        run_checks()
    print(f"Build {write_build_stamp()}")
    with tempfile.TemporaryDirectory() as tmp:
        deploy(target, files, os.path.join(tmp, "ssh-%C"), args.batch)
    if args.restart:
        restart_home_assistant(env, args.yes)
    else:
        print("Next: restart Home Assistant (a changed integration is loaded after a restart).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
