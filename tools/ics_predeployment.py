"""Run the extra iCS release checks on committed source and retain their test results."""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import signal
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path
from xml.etree import ElementTree

from tools.qualification_snapshot import git

PROXY_INPUTS = (
    "custom_components/ocpp_hub/core",
    "tools/managed_proxy.py",
    "tools/managed_reporting.py",
    "tools/managed_incident.py",
    "tools/trial_proxy.py",
    "requirements-offline.txt",
    "requirements-trial.txt",
)


@dataclass(frozen=True)
class Check:
    """One offline command and the exact test it must complete."""

    name: str
    runner: str
    directory: str
    case: str
    seconds: int


DISCOVERY = Check(
    "recovery search",
    "offline_validate.py",
    "tests_discovery",
    "tests_discovery.test_recovery_discovery.test_recovery_discovery",
    300,
)
WEEK = Check(
    "installation week",
    "offline_ha_validate.py",
    "tests_installation",
    "tests_installation.test_installation_week.test_week_on_migrated_installation",
    120,
)


# Require the full installed identity, and schedule the week when shipped proxy bytes or pins differ.
def changed_proxy(repo: Path, candidate: str, installed: str | None) -> bool:
    if not installed or not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", installed):
        raise ValueError("provide the verified full installed proxy commit for standalone release validation")
    if git(repo, "rev-parse", "--verify", installed + "^{commit}").decode().strip() != installed:
        raise ValueError("installed proxy commit does not identify committed source in this repository")
    return bool(git(repo, "diff", "--name-only", installed, candidate, "--", *PROXY_INPUTS).strip())


# Require qualification to have exercised the packages that the standalone installer will use.
def require_standalone_pins(source: Path, audited_packages: dict[str, str]) -> None:
    expected = dict(
        line.strip().split("==", 1)
        for line in (source / "requirements-trial.txt").read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )
    if not expected or any(audited_packages.get(name) != version for name, version in expected.items()):
        raise ValueError("standalone runtime pins differ from the audited engine runtime")


# Export the exact installed commit without opening its running installation or checking out another branch.
def export_installed(repo: Path, commit: str, destination: Path) -> None:
    archive = git(repo, "archive", "--format=tar", commit)
    with tarfile.open(fileobj=io.BytesIO(archive)) as package:
        package.extractall(destination, filter="data")


# Use the selected virtualenv as invoked and reject leftover pytest filters and Python overrides.
def clean_env() -> dict[str, str]:
    return {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("PYTEST_", "PYTHON")) and k not in ("MODEL_WORKER_INDEX", "OCPP_WEEK_COMMISSIONING_ROOT")
    }


# Bind cached results to the interpreter and its installed package versions, including the HA pin.
def runtime_identity(python: str, source: Path) -> dict:
    code = (
        "import hashlib, json, sys, importlib.metadata as m; from pathlib import Path; "
        "print(json.dumps({'python': sys.version, 'executable': sys.executable, "
        "'executable_sha256': hashlib.sha256(Path(sys.executable).read_bytes()).hexdigest(), "
        "'packages': sorted((d.metadata['Name'].lower(), d.version) for d in m.distributions())}))"
    )
    return json.loads(
        subprocess.check_output([python, "-E", "-B", "-c", code], cwd=source, env=clean_env(), timeout=20)
    )


# Confirm the one expected test ran successfully, and that discovery actually ran all twenty histories.
def evidence(check: Check, attempt: Path) -> dict[str, str]:
    xml = attempt / "pytest.xml"
    cases = ElementTree.parse(xml).findall(".//testcase")
    if (
        len(cases) != 1
        or cases[0].get("classname", "") + "." + cases[0].get("name", "") != check.case
        or any(cases[0].find(name) is not None for name in ("skipped", "failure", "error"))
    ):
        raise ValueError(f"{check.name} did not pass the required test")
    artifacts = [xml]
    if check == DISCOVERY:
        path = attempt / "discovery" / "summary.json"
        summary = json.loads(path.read_text())
        if (
            summary["status"] != "passed"
            or summary["seed"] != 19
            or summary["planned"] != 20
            or len(summary["runs"]) != 20
            or any(row["purpose"] != "search" or row["status"] != "passed" for row in summary["runs"])
        ):
            raise ValueError("recovery search did not finish all twenty histories")
        artifacts.append(path)
    return {str(path.relative_to(attempt)): hashlib.sha256(path.read_bytes()).hexdigest() for path in artifacts}


# Retain failures as well as successes, and stop child processes if a check exhausts its time.
def run_check(check: Check, python: str, source: Path, attempt: Path, previous: Path | None) -> None:
    scratch = attempt / "scratch"
    scratch.mkdir()
    env = clean_env() | {"TMPDIR": str(scratch)}
    command = [
        python,
        "-E",
        "-B",
        str(source / "tools" / check.runner),
        check.directory,
        "--junitxml=" + str(attempt / "pytest.xml"),
    ]
    if check == DISCOVERY:
        command += ["--discovery-output=" + str(attempt / "discovery"), "--discovery-seed=19", "--discovery-cases=20"]
    else:
        assert previous is not None
        env["OCPP_WEEK_COMMISSIONING_ROOT"] = str(previous)
        command += [f"--week-current={source}", f"--week-previous={previous}"]
    log = attempt / "test.log"
    with (
        log.open("w") as output,
        subprocess.Popen(
            command, cwd=source, env=env, stdout=output, stderr=subprocess.STDOUT, start_new_session=True
        ) as process,
    ):
        try:
            code = process.wait(timeout=check.seconds)
        except subprocess.TimeoutExpired, KeyboardInterrupt:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise
    if code:
        raise ValueError(f"{check.name} failed. See {log}")


# A matching successful run can be reused only while its required test evidence still verifies.
def passed_attempt(check: Check, directory: Path, inputs: dict) -> Path | None:
    try:
        receipt = json.loads((directory / "passed.json").read_text())
        name = receipt["attempt"]
        if receipt["inputs"] != inputs or not re.fullmatch(r"attempt-[a-z0-9_]+", name):
            return None
        attempt = directory / name
        return attempt if receipt["evidence"] == evidence(check, attempt) else None
    except OSError, ValueError, TypeError, KeyError, ElementTree.ParseError:
        return None


# Run each check once for matching source, installed build and runtime within a qualification bundle.
def ensure_check(
    check: Check,
    source: Path,
    output: Path,
    metadata: dict,
    python: str,
    *,
    installed: tuple[str, Path] | None = None,
) -> None:
    import fcntl

    runtime = runtime_identity(python, source)
    if check == WEEK:
        pins = (
            "requirements-ha-proxy.txt",
            "requirements-ha-legacy.txt",
        )
        versions = {
            line.removeprefix("homeassistant==")
            for pin in pins
            for line in (source / pin).read_text().splitlines()
            if line.startswith("homeassistant==")
        }
        if dict(runtime["packages"]).get("homeassistant") not in versions:
            raise ValueError("installation week requires a pinned Home Assistant runtime")
    inputs = {
        "check": check.name,
        "commit": metadata["commit"],
        "source_digest": metadata["source_digest"],
        "runtime": runtime,
        "installed": installed[0] if installed is not None else None,
    }
    key = hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()
    directory = output / key
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "run.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if attempt := passed_attempt(check, directory, inputs):
            print(f"{check.name}: reused passing results at {attempt}", flush=True)
            return
        attempt = Path(tempfile.mkdtemp(dir=directory, prefix="attempt-"))
        print(f"{check.name}: running on {inputs['commit']} (log: {attempt / 'test.log'})", flush=True)
        run_check(check, python, source, attempt, installed[1] if installed is not None else None)
        receipt = {"inputs": inputs, "attempt": attempt.name, "evidence": evidence(check, attempt)}
        pending = directory / "passed.new"
        pending.write_text(json.dumps(receipt, indent=2) + "\n")
        pending.replace(directory / "passed.json")
