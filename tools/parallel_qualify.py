#!/usr/bin/env python3
"""Freeze a clean commit and qualify one complete suite in pinned, isolated Docker workers."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import shlex
import shutil
import subprocess
import sys
import tarfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.qualification_snapshot import freeze  # noqa: E402

HARNESS = ("parallel_worker.py", "qualification_evidence.py", "audit_parallel_qualification.py")


# Every mounted runtime path is explicit; no private host, credential, image or source default is embedded.
def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--worker-host", help="optional SSH alias; omitted means this machine")
    parser.add_argument("--worker-root", type=Path, help="absolute remote scratch root")
    parser.add_argument("--worker-python", help="supervisor interpreter; required for SSH execution")
    parser.add_argument("--venv", type=Path, required=True)
    parser.add_argument("--image", required=True, help="full immutable sha256 Docker image ID")
    parser.add_argument("--runtime-mode", choices=("host", "image"), default="host")
    parser.add_argument("--venv-target", type=Path)
    parser.add_argument("--container-python")
    parser.add_argument("--readonly-mount", action="append", default=[], metavar="HOST=CONTAINER")
    parser.add_argument("--expected-package", action="append", default=[], metavar="NAME==VERSION")
    parser.add_argument(
        "--suite", choices=("engine-model", "ha-proxy", "ha-legacy", "ha-proxy-legacy"), default="engine-model"
    )
    parser.add_argument("--workers", type=int, choices=range(2, 21), default=20)
    parser.add_argument("--timeout-seconds", type=int, default=10800)
    parser.add_argument("--label")
    parser.add_argument("--stage-only", action="store_true")
    args = parser.parse_args()
    if not re.fullmatch(r"sha256:[a-f0-9]{64}", args.image):
        parser.error("--image requires the full immutable image ID")
    if not args.output.is_absolute() or not args.venv.is_absolute() or not 1 <= args.timeout_seconds <= 10800:
        parser.error("scratch/runtime paths must be absolute and timeout must be between1and10800 seconds")
    if args.worker_host and (args.worker_root is None or not args.worker_root.is_absolute() or not args.worker_python):
        parser.error("SSH execution requires --worker-root and --worker-python")
    if os.environ.get("MODEL_SMOKE") or os.environ.get("MODEL_CASE_LIMIT"):
        parser.error("full qualification forbids model sampling switches")
    return args


# Capture only committed blobs and execute the matching frozen harness, retaining every failed attempt separately.
def main() -> int:
    args = arguments()
    label = args.label or f"{args.suite}-{time.strftime('%Y%m%dT%H%M%S', time.gmtime())}-{secrets.token_hex(4)}"
    if not re.fullmatch(r"[A-Za-z0-9_-]+", label):
        raise ValueError("invalid batch label")
    local = args.output / label
    local.mkdir(parents=True, exist_ok=False)
    metadata = freeze(args.source.resolve(), local / "source")
    specs = {
        "engine-model": ["tests", "tests_model"],
        "ha-proxy": ["tests_ha_proxy"],
        "ha-legacy": ["tests_ha"],
        "ha-proxy-legacy": ["tests_ha_proxy"],
    }
    venv_target = args.venv_target or (args.venv if args.runtime_mode == "host" else Path("/venv"))
    requirements = (
        "requirements-offline.txt"
        if args.suite == "engine-model"
        else "requirements-ha-proxy.txt"
        if args.suite == "ha-proxy"
        else "requirements-ha-legacy.txt"
    )
    expected = dict(
        line.strip().split("==", 1)
        for line in (local / "source" / requirements).read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )
    for item in args.expected_package:
        name, value = item.split("==", 1)
        if name in expected and expected[name] != value:
            raise ValueError("runtime overrides cannot replace committed package pins")
        expected[name] = value
    mounts = []
    for value in args.readonly_mount:
        source, target = value.split("=", 1)
        if not Path(source).is_absolute() or not Path(target).is_absolute() or "," in value:
            raise ValueError("read-only mounts require absolute HOST=CONTAINER paths without commas")
        mounts.append({"source": source, "target": target})
    config = {
        "mode": "full",
        "suite": args.suite,
        "workers": args.workers,
        "label": label,
        "commit": metadata["commit"],
        "source_digest": metadata["source_digest"],
        "specs": specs[args.suite],
        "image": args.image,
        "venv": str(args.venv),
        "venv_target": str(venv_target),
        "runtime_mode": args.runtime_mode,
        "container_python": args.container_python or str(venv_target / "bin/python"),
        "readonly_mounts": mounts,
        "expected_packages": expected,
        "runner": "tools/offline_validate.py" if args.suite == "engine-model" else "tools/offline_ha_validate.py",
        "runner_args": ["--legacy-ha"] if args.suite == "ha-legacy" else [],
        "timeout_seconds": args.timeout_seconds,
        "created_epoch": time.time(),
    }
    config["harness_hashes"] = {}
    for filename in HARNESS:
        source = local / "source/tools" / filename
        shutil.copy2(source, local / filename)
        config["harness_hashes"][filename] = hashlib.sha256(source.read_bytes()).hexdigest()
    (local / "snapshot.json").write_text(json.dumps(metadata, indent=2) + "\n")
    (local / "config.json").write_text(json.dumps(config, indent=2) + "\n")
    print(
        json.dumps(
            {
                "batch": str(local),
                "commit": metadata["commit"],
                "source_digest": metadata["source_digest"],
                "workers": args.workers,
                "suite": args.suite,
            }
        ),
        flush=True,
    )
    if args.stage_only:
        return 0
    python = args.worker_python or sys.executable
    if args.worker_host:
        remote = args.worker_root / "batches" / label
        ssh = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ServerAliveInterval=30",
            "-o",
            "ServerAliveCountMax=3",
            args.worker_host,
        ]
        with tarfile.open(local / "input.tar.gz", "w:gz") as archive:
            for name in ("source", "config.json", "snapshot.json", *HARNESS):
                archive.add(local / name, arcname=name)
        subprocess.run([*ssh, shlex.join(["mkdir", "-p", str(remote.parent)])], check=True)
        subprocess.run([*ssh, shlex.join(["mkdir", "-m", "700", str(remote)])], check=True)
        subprocess.run(["rsync", "-a", str(local / "input.tar.gz"), f"{args.worker_host}:{remote}/"], check=True)
        command = " && ".join(
            [
                "umask 077",
                shlex.join(["tar", "-xf", str(remote / "input.tar.gz"), "-C", str(remote)]),
                shlex.join(
                    [
                        "flock",
                        "-n",
                        "-E",
                        "75",
                        str(args.worker_root / "qualification.lock"),
                        "timeout",
                        "--signal=TERM",
                        "--kill-after=30s",
                        f"{args.timeout_seconds}s",
                        "nice",
                        "-n",
                        "10",
                        python,
                        "-B",
                        str(remote / "parallel_worker.py"),
                    ]
                ),
            ]
        )
        with (local / "remote.log").open("w") as output:
            result = subprocess.run([*ssh, command], stdout=output, stderr=subprocess.STDOUT)
        retrieval = subprocess.run(
            [
                "rsync",
                "-a",
                "--exclude=source",
                "--exclude=input.tar.gz",
                "--exclude=tmp",
                f"{args.worker_host}:{remote}/",
                str(local) + "/",
            ]
        )
        if retrieval.returncode:
            return retrieval.returncode
    else:
        with (local / "worker.log").open("w") as output:
            result = subprocess.run(
                [python, "-B", str(local / "parallel_worker.py")], stdout=output, stderr=subprocess.STDOUT
            )
    (local / "transport-result.json").write_text(json.dumps({"returncode": result.returncode}) + "\n")
    audit = subprocess.run(
        [
            sys.executable,
            "-B",
            str(local / "audit_parallel_qualification.py"),
            str(local),
            "--expected-commit",
            metadata["commit"],
            "--expected-source-digest",
            metadata["source_digest"],
        ]
    )
    return result.returncode or audit.returncode


if __name__ == "__main__":
    raise SystemExit(main())
