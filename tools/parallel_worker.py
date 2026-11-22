#!/usr/bin/env python3
"""Supervise pinned offline pytest processes under the launcher's shared timeout group."""

from __future__ import annotations

import hashlib
import json
import os
import resource
import signal
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "source"
ABORT = None


# Preserve signal-driven cancellation and allow the supervisor to save interrupted/unrun accounting.
def interrupted(signum, frame):
    global ABORT
    ABORT = ABORT or "signal_" + str(signum)


# Persist atomic metadata so an interrupted run never exposes half a JSON summary.
def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".new")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


class WorkQueue:
    """Assign the next case to the worker that asks for it."""

    # The supervisor is the only writer of the global dispatch ledger.
    def __init__(self, root, selected, ledger):
        self.root = root
        self.pending = iter(selected)
        self.ledger = ledger
        self.assigned = defaultdict(list)
        self.finished = set()

    # A worker asks for exactly one further grant at a time. Keep the grant even when its worker aborts.
    def dispatch(self, index):
        if index in self.finished:
            return
        directory = self.root / str(index)
        request = directory / "request.json"
        if not request.exists():
            return
        sequence = json.loads(request.read_text())
        assigned = self.assigned[index]
        if type(sequence) is not int or sequence not in (len(assigned) - 1, len(assigned)):
            raise RuntimeError(f"Worker {index} made an out-of-order queue request")
        if sequence != len(assigned):
            return
        identifier = next(self.pending, None)
        grant = {"sequence": sequence, "id": identifier}
        self.ledger.write(json.dumps({"worker": index, **grant}) + "\n")
        self.ledger.flush()
        if identifier is None:
            self.finished.add(index)
        else:
            assigned.append(identifier)
        write_json(directory / "grant.json", grant)


# Verify snapshot files before and after execution using the original path/digest convention.
def snapshot_digest(metadata):
    digest = hashlib.sha256()
    for row in sorted(metadata["files"], key=lambda item: os.fsencode(item["path"])):
        data = (SOURCE / row["path"]).read_bytes()
        if hashlib.sha256(data).hexdigest() != row["sha256"]:
            raise RuntimeError("Immutable source changed: " + row["path"])
        digest.update(os.fsencode(row["path"]) + b"\0" + hashlib.sha256(data).digest())
    return digest.hexdigest()


# Full qualification never substitutes a sample or a historical subset for the collected suite.
def select(ids, config):
    if config["mode"] != "full":
        raise RuntimeError("Only complete suite qualification is supported")
    return ids, {"all_collected": len(ids)}


# Build a network-isolated container with only its own scratch writable and explicit read-only runtime inputs.
def container_command(config, directory, cpu, name, env, start_recovery_ram=False):
    from tools.qualification_collection import runtime_mounts, validate_readonly_mounts

    validate_readonly_mounts(config, ROOT, directory)
    command = [
        "docker",
        "run",
        "--rm",
        "--pull=never",
        "--network",
        "none",
        "--read-only",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "--cpuset-cpus",
        str(cpu),
        "--name",
        name,
        "--label",
        "codex.qualification.owner=" + str(ROOT),
        "--mount",
        f"type=bind,src={ROOT},dst={ROOT},readonly",
        "--mount",
        f"type=bind,src={directory},dst={directory}",
        "--mount",
        f"type=bind,src={config['venv']},dst={config['venv_target']},readonly",
    ]
    for source, target in runtime_mounts(config):
        command += ["--mount", f"type=bind,src={source},dst={target},readonly"]
    if start_recovery_ram:
        from tools.qualification_scratch import RAM_DIRECTORY

        ram = directory / RAM_DIRECTORY
        if config["suite"] != "engine-model" or ":" in str(ram):
            raise ValueError("invalid engine Start recovery tmpfs destination")
        ram.mkdir(mode=0o700)
        command += [
            "--tmpfs",
            f"{ram}:rw,noexec,nosuid,nodev,size=128m,mode=0700,uid={os.getuid()},gid={os.getgid()}",
        ]
    for key, value in env.items():
        if key in (
            "PYTHONPATH",
            "TMPDIR",
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD",
            "PYTHONDONTWRITEBYTECODE",
            "SCENARIO_RESULTS",
            "MODEL_WORKER_INDEX",
            "QUALIFICATION_EXPECTED_PACKAGES",
            "XDG_CACHE_HOME",
            "PYTEST_ADDOPTS",
            "PYTEST_PLUGINS",
        ):
            command += ["--env", key + "=" + value]
    return command + [
        "--env",
        "PYTHONHOME=",
        "--env",
        "LD_LIBRARY_PATH=",
        "--workdir",
        str(SOURCE),
        "--entrypoint",
        config["container_python"],
        config["image"],
    ]


# Read process ancestry and identity without collecting command lines or environment secrets.
def processes():
    result = {}
    for path in Path("/proc").iterdir():
        if path.name.isdigit():
            try:
                fields = (path / "stat").read_text().rsplit(")", 1)[1].split()
                result[int(path.name)] = {
                    "ppid": int(fields[1]),
                    "start": fields[19],
                    "cpu_ticks": int(fields[11]) + int(fields[12]),
                    "rss_bytes": int(fields[21]) * os.sysconf("SC_PAGE_SIZE"),
                }
            except OSError, ValueError, IndexError:
                pass
    return result


# Signal only the verified descendant tree of a task-owned pytest process.
def stop_tree(process, signum):
    table = processes()
    owned = {process.pid} if process.pid in table else set()
    changed = True
    while changed:
        additions = {pid for pid, row in table.items() if row["ppid"] in owned} - owned
        changed = bool(additions)
        owned.update(additions)
    identities = {pid: table[pid]["start"] for pid in owned}
    for pid in sorted(owned, reverse=True):
        try:
            now = Path("/proc") / str(pid) / "stat"
            if now.read_text().rsplit(")", 1)[1].split()[19] == identities[pid]:
                os.kill(pid, signum)
        except OSError, ValueError, IndexError:
            pass
    return identities


# Kill surviving tracked children by immutable process start identity after a short abort grace.
def kill_survivors(identities):
    for pid, start in identities.items():
        try:
            if (Path("/proc") / str(pid) / "stat").read_text().rsplit(")", 1)[1].split()[19] == start:
                os.kill(pid, signal.SIGKILL)
        except OSError, ValueError, IndexError:
            pass


# Inspect only task-owned container identity fields; never retrieve environment or command arguments.
def container_identity(item, table):
    if item.get("container_id"):
        return
    template = (
        "[{{json .Id}},{{json .State.Pid}},{{json .HostConfig.NetworkMode}},{{json .HostConfig.CpusetCpus}},"
        '{{json (index .Config.Labels "codex.qualification.owner")}}]'
    )
    try:
        checked = subprocess.run(
            ["docker", "inspect", "--format", template, item["container_name"]],
            text=True,
            capture_output=True,
            timeout=3,
        )
    except subprocess.TimeoutExpired:
        return  # Startup telemetry may be retried; it must not cancel an otherwise healthy test.
    if checked.returncode:
        return
    fields = json.loads(checked.stdout)
    if len(fields) != 5:
        raise RuntimeError("Unexpected container identity response")
    identifier, pid, network, cpu, owner = fields
    if owner != str(ROOT) or network != "none" or cpu != str(item["cpu"]):
        raise RuntimeError("Container ownership/network/CPU identity mismatch")
    init_pid = int(pid)
    if init_pid <= 0:
        return
    # The configured Python is the direct entrypoint in both mounted-host and image-native runtime modes.
    pytest_pid = init_pid
    try:
        namespace = os.readlink(f"/proc/{pytest_pid}/ns/net")
    except OSError:
        return
    identity = {
        "container_name": item["container_name"],
        "container_id": identifier,
        "container_entry_pid": init_pid,
        "pytest_pid": pytest_pid,
        "network_namespace": namespace,
        "cpu": item["cpu"],
        "network_mode": network,
    }
    item.update(identity)
    write_json(ROOT / str(item["index"]) / "container-identity.json", identity)


# Remove only deterministic names whose explicit owner label matches this unique batch.
def cleanup_containers(workers):
    removed = []
    errors = []
    names = [item["container_name"] for item in workers]
    if names:
        try:
            checked = subprocess.run(
                [
                    "docker",
                    "inspect",
                    "--format",
                    '{{.Name}} {{index .Config.Labels "codex.qualification.owner"}}',
                    *names,
                ],
                text=True,
                capture_output=True,
                timeout=3,
            )
            # Nonexistent --rm containers make inspect nonzero; existing names still appear on stdout.
            notices = [line.lower() for line in checked.stderr.splitlines() if line.strip()]
            if checked.returncode and (
                not notices or any("no such object" not in line and "no such container" not in line for line in notices)
            ):
                errors.append("container ownership inspection failed")
            for line in checked.stdout.splitlines():
                if not line.strip():
                    continue  # Docker prints empty rows for already removed --rm containers.
                fields = line.split(maxsplit=1)
                if len(fields) != 2:
                    errors.append("refused malformed container ownership row")
                    continue
                name, owner = fields
                name = name.lstrip("/")
                if name not in names or owner != str(ROOT):
                    errors.append("refused foreign container name: " + name)
                    continue
                removed.append(name)
        except subprocess.TimeoutExpired:
            errors.append("container ownership inspection timed out")
    if removed:
        try:
            result = subprocess.run(["docker", "rm", "-f", *removed], text=True, capture_output=True, timeout=10)
            if result.returncode:
                errors.append("container removal returned " + str(result.returncode))
        except subprocess.TimeoutExpired:
            errors.append("container removal timed out")
    write_json(ROOT / "container-cleanup.json", {"removed_names": removed, "errors": errors})
    return errors


# Record host pressure and owned process CPU/RSS only, never command lines or environment.
def telemetry(workers):
    memory = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith(("MemAvailable:", "MemTotal:")):
            key, value, unit = line.split()
            memory[key.rstrip(":")] = int(value) * 1024
    table = processes()
    for item in workers:
        container_identity(item, table)
    pressure = {}
    for name in ("cpu", "memory", "io"):
        try:
            pressure[name] = (Path("/proc/pressure") / name).read_text().strip()
        except OSError:
            pressure[name] = None
    sockets = {}
    for name in ("sockstat", "sockstat6"):
        sockets[name] = (Path("/proc/net") / name).read_text().strip()
    tcp_states = defaultdict(int)
    for name in ("tcp", "tcp6"):
        for line in (Path("/proc/net") / name).read_text().splitlines()[1:]:
            fields = line.split()
            if len(fields) > 3:
                tcp_states[fields[3]] += 1
    row = {
        "epoch": time.time(),
        "load": os.getloadavg(),
        "memory": memory,
        "psi": pressure,
        "socket_counters": sockets,
        "tcp_state_counts": dict(tcp_states),
        "workers": [
            {
                "index": item["index"],
                "docker_cli_pid": item["process"].pid,
                "pytest_pid": item.get("pytest_pid"),
                "cpu": item["cpu"],
                "container_name": item["container_name"],
                "network_namespace": item.get("network_namespace"),
                **{key: table.get(item.get("pytest_pid"), {}).get(key) for key in ("cpu_ticks", "rss_bytes")},
            }
            for item in workers
        ],
    }
    if not (ROOT / "telemetry.jsonl").exists():
        row["initial_resources"] = {
            "ip_local_port_range": Path("/proc/sys/net/ipv4/ip_local_port_range").read_text().strip(),
            "rlimit_nofile": resource.getrlimit(resource.RLIMIT_NOFILE),
            "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
            "available_cpu_affinity": sorted(os.sched_getaffinity(0)),
        }
    with (ROOT / "telemetry.jsonl").open("a") as output:
        output.write(json.dumps(row) + "\n")


# Execute the entire frozen collection or explicit preflight under unchanged test/production time budgets.
def main():
    global ABORT
    started_monotonic = time.monotonic()
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    config = json.loads((ROOT / "config.json").read_text())
    metadata = json.loads((ROOT / "snapshot.json").read_text())
    started = time.time()
    deadline = started_monotonic + config["timeout_seconds"]

    # Collection, manifest preparation and dispatch all draw from one budget.
    def remaining():
        seconds = deadline - time.monotonic()
        if seconds <= 0:
            raise TimeoutError("qualification_timeout")
        if ABORT:
            raise RuntimeError(ABORT)
        return seconds

    workers = []
    preparation = []
    queue = None
    ledger = None
    summary = {
        "mode": config["mode"],
        "scheduler": config.get("scheduler"),
        "commit": config["commit"],
        "source_digest": config["source_digest"],
        "started_epoch": started,
        "workers": config["workers"],
        "groups": [],
        "complete": False,
        "remote_batch_directory": str(ROOT),
        "retained_worker_tmp_pattern": str(ROOT / "<worker-index>" / "tmp"),
    }
    summary["container_image"] = config["image"]
    summary["worker_network"] = "none, one namespace per worker"
    write_json(ROOT / "result.json", summary)
    try:
        if config.get("scheduler") != "pull-v1":
            raise RuntimeError("Unsupported qualification scheduler")
        if snapshot_digest(metadata) != config["source_digest"] or metadata["commit"] != config["commit"]:
            raise RuntimeError("Snapshot identity mismatch")
        for filename, expected in config["harness_hashes"].items():
            if (
                hashlib.sha256((ROOT / filename).read_bytes()).hexdigest() != expected
                or hashlib.sha256((SOURCE / "tools" / filename).read_bytes()).hexdigest() != expected
            ):
                raise RuntimeError("Frozen harness differs from committed source")
        if os.environ.get("PYTEST_ADDOPTS") or os.environ.get("PYTEST_PLUGINS"):
            raise RuntimeError("full qualification forbids inherited pytest filters/plugins")
        sys.path.insert(0, str(SOURCE))
        from tools.qualification_collection import runtime_digest

        summary["runtime_digest_before"] = runtime_digest(config)
        cpus = sorted(os.sched_getaffinity(0))
        if len(cpus) < config["workers"] + 4:
            raise RuntimeError(f"Need {config['workers']}+4 available CPUs; affinity has {len(cpus)}")
        cpus = cpus[: config["workers"]]
        summary["cpu_assignments"] = cpus
        telemetry(workers)
        env = {
            **os.environ,
            "TMPDIR": str(ROOT / "tmp"),
            "PYTHONPATH": str(ROOT),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
            "PYTEST_ADDOPTS": "",
            "PYTEST_PLUGINS": "",
        }
        env.pop("MODEL_SMOKE", None)
        env.pop("MODEL_CASE_LIMIT", None)
        (ROOT / "tmp").mkdir()
        command = ["-B", config["runner"], *config["runner_args"]]
        collection_dir = ROOT / "collection"
        collection_dir.mkdir()
        collection_name = "icsq-" + hashlib.sha256(config["label"].encode()).hexdigest()[:20] + "-collection"
        preparation.append({"container_name": collection_name})
        collection_env = {
            **env,
            "TMPDIR": str(collection_dir),
            "MODEL_WORKER_INDEX": "0",
            "SCENARIO_RESULTS": str(collection_dir / "results.jsonl"),
            "QUALIFICATION_EXPECTED_PACKAGES": json.dumps(config["expected_packages"]),
            "XDG_CACHE_HOME": str(collection_dir / "cache"),
        }
        with (ROOT / "collection.log").open("w") as output:
            collection = subprocess.run(
                [
                    *container_command(config, collection_dir, cpus[0], collection_name, collection_env),
                    *command,
                    "-p",
                    "qualification_evidence",
                    *config["specs"],
                    "--collect-only",
                    "--basetemp=" + str(collection_dir / "tmp"),
                ],
                cwd=SOURCE,
                env=env,
                stdout=output,
                stderr=subprocess.STDOUT,
                timeout=remaining(),
            )
        if collection.returncode or ABORT:
            raise RuntimeError("Collection failed or was interrupted")
        collection_rows = [json.loads(line) for line in (collection_dir / "results.jsonl").read_text().splitlines()]
        collections = [row["ids"] for row in collection_rows if row.get("event") == "collection"]
        if len(collections) != 1:
            raise RuntimeError("Missing exact machine-readable collection")
        ids = collections[0]
        if any(
            not any(identifier.startswith(root.rstrip("/") + "/") for root in config["specs"]) for identifier in ids
        ):
            raise RuntimeError("Collection has an unexpected suite root")
        if not ids or len(ids) != len(set(ids)):
            raise RuntimeError("Empty or duplicate full collection")
        write_json(ROOT / "collected-ids.json", ids)
        if (SOURCE / "tests_model/catalog.py").is_file():
            # Generate the exact manifest under the same loopback audit hook as tests.
            manifest_code = (
                "import runpy,sys; from tools.offline_validate import network_guard; "
                "sys.addaudithook(network_guard); "
                'sys.argv=["tools/schedule_sequence_manifest.py",sys.argv[1],"--suite",sys.argv[2]]; '
                'runpy.run_path("tools/schedule_sequence_manifest.py",run_name="__main__")'
            )
            with (ROOT / "manifest.log").open("w") as output:
                generated = subprocess.run(
                    [
                        "taskset",
                        "-c",
                        str(cpus[0]),
                        sys.executable,
                        "-B",
                        "-c",
                        manifest_code,
                        str(ROOT / "sequence-manifest.jsonl"),
                        config["suite"],
                    ],
                    cwd=SOURCE,
                    env=env,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    timeout=remaining(),
                )
            if generated.returncode or ABORT:
                raise RuntimeError("Manifest generation failed or interrupted")
        selected, reasons = select(ids, config)
        write_json(ROOT / "selected-ids.json", selected)
        write_json(ROOT / "selection.json", reasons)
        summary.update({"collected_cases": len(ids), "selected_cases": len(selected), "selection": reasons})
        ledger = (ROOT / "dispatch.jsonl").open("x")
        queue = WorkQueue(ROOT, selected, ledger)
        for index, cpu in enumerate(cpus):
            remaining()
            directory = ROOT / str(index)
            directory.mkdir()
            output = (directory / "pytest.log").open("w")
            worker_env = {
                **env,
                "TMPDIR": str(directory),
                "MODEL_WORKER_INDEX": str(index),
                "SCENARIO_RESULTS": str(directory / "results.jsonl"),
                "QUALIFICATION_EXPECTED_PACKAGES": json.dumps(config["expected_packages"]),
                "XDG_CACHE_HOME": str(directory / "cache"),
            }
            container_name = "icsq-" + hashlib.sha256(config["label"].encode()).hexdigest()[:20] + "-" + str(index)
            mixed_scratch = config["suite"] == "engine-model"
            docker = container_command(config, directory, cpu, container_name, worker_env, mixed_scratch)
            scratch_option = []
            if mixed_scratch:
                from tools.qualification_scratch import RAM_OPTION

                scratch_option = [RAM_OPTION]
            remaining()
            process = subprocess.Popen(
                [
                    *docker,
                    *command,
                    "-p",
                    "qualification_evidence",
                    "--qualification-work-queue",
                    *scratch_option,
                    *config["specs"],
                    "--tb=short",
                    "--basetemp=" + str(directory / "tmp"),
                ],
                cwd=SOURCE,
                env=worker_env,
                stdout=output,
                stderr=subprocess.STDOUT,
            )
            # Daemon-owned pytest/init/openssl processes are cleaned up by explicit task-owned container names.
            workers.append(
                {
                    "index": index,
                    "cpu": cpu,
                    "process": process,
                    "output": output,
                    "container_name": container_name,
                    "started_epoch": time.time(),
                    "cases": 0,
                }
            )
        next_telemetry = 0.0
        stopped = False
        pending_kills = {}
        stop_at = None
        while any(item["process"].poll() is None for item in workers):
            now = time.monotonic()
            if now >= next_telemetry:
                telemetry(workers)
                next_telemetry = now + 5
            if time.monotonic() >= deadline:
                ABORT = ABORT or "qualification_timeout"
            # Pytest's failure exit after the final grant still rejects the release, but peers can finish.
            failed = [
                item
                for item in workers
                if (code := item["process"].poll()) not in (None, 0)
                and (code != 1 or item["index"] not in queue.finished)
            ]
            if failed and ABORT is None:
                ABORT = "worker_" + str(failed[0]["index"]) + "_exit_" + str(failed[0]["process"].returncode)
            if not ABORT:
                for item in workers:
                    if item["process"].poll() is None:
                        queue.dispatch(item["index"])
            if ABORT and not stopped:
                cleanup_containers(workers)
                for item in workers:
                    if item["process"].poll() is None:
                        pending_kills.update(stop_tree(item["process"], signal.SIGTERM))
                stop_at = now + 3
                stopped = True
            if stopped and pending_kills and now >= stop_at:
                kill_survivors(pending_kills)
                pending_kills.clear()
            time.sleep(0.05)
        if pending_kills:
            # Wait only for the short abort grace, then remove any openssl child orphaned by pytest exit.
            time.sleep(max(0, stop_at - time.monotonic()))
            kill_survivors(pending_kills)
        telemetry(workers)
        for item in workers:
            item["output"].close()
            item["cases"] = len(queue.assigned[item["index"]])
            group = {key: item[key] for key in ("index", "cpu", "cases", "started_epoch")}
            group.update(
                {key: item.get(key) for key in ("container_name", "container_id", "pytest_pid", "network_namespace")}
            )
            group.update({"rc": item["process"].returncode, "wall_seconds": time.time() - item["started_epoch"]})
            summary["groups"].append(group)
        summary["runtime_digest_after"] = runtime_digest(config)
        if summary["runtime_digest_before"] != summary["runtime_digest_after"]:
            raise RuntimeError("Mounted test runtime changed during qualification")
        summary["source_digest_after"] = snapshot_digest(metadata)
        if time.monotonic() >= deadline:
            ABORT = ABORT or "qualification_timeout"
        summary["complete"] = ABORT is None and all(group["rc"] == 0 for group in summary["groups"])
    except Exception as error:
        # This is the outer process boundary: preserve failure evidence and stop only owned worker descendants.
        ABORT = ABORT or type(error).__name__ + ": " + str(error)
        cleanup_containers(workers)
        for item in workers:
            if item["process"].poll() is None:
                identities = stop_tree(item["process"], signal.SIGTERM)
                try:
                    item["process"].wait(timeout=3)
                except subprocess.TimeoutExpired:
                    pass
                kill_survivors(identities)
        summary["groups"] = [
            {
                "index": item["index"],
                "cpu": item["cpu"],
                "cases": len(queue.assigned[item["index"]]) if queue is not None else 0,
                "rc": item["process"].poll(),
            }
            for item in workers
        ]
    finally:
        cleanup_errors = cleanup_containers(workers + preparation)
        if queue is not None:
            for item in workers:
                index = item["index"]
                write_json(ROOT / str(index) / "assigned-ids.json", queue.assigned[index])
        if ledger is not None:
            ledger.close()
        if cleanup_errors:
            ABORT = ABORT or "task_container_cleanup_failed"
            summary["complete"] = False
        summary.update({"abort_reason": ABORT, "finished_epoch": time.time(), "wall_seconds": time.time() - started})
        write_json(ROOT / "result.json", summary)
    # The local auditor independently derives passed/failed/interrupted/unrun IDs from raw phases.
    audit = subprocess.run(
        [
            sys.executable,
            "-B",
            str(ROOT / "audit_parallel_qualification.py"),
            str(ROOT),
            "--expected-commit",
            config["commit"],
            "--expected-source-digest",
            config["source_digest"],
        ],
        check=False,
    )
    print(json.dumps(summary), flush=True)
    return 0 if summary["complete"] and audit.returncode == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
