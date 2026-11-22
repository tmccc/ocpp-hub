#!/usr/bin/env python3
"""Run static checks and re-audit complete20-worker qualification, or run an explicit targeted test selection.

    python tools/validate.py --qualification-report /absolute/path/bundle.json
    python tools/validate.py --quick    # skip pytest
    python tools/validate.py --tests-only -k issue171   # targeted only, not full qualification

On Windows tests that fail for environment reasons only (for example CRLF checkout changes to fixture digests or
the bash-based search tool tests) are listed in ``tools/known_env_failures.txt`` and ignored there; any other
failure fails the run. The list is empty at the moment. On Linux/macOS the baseline is not applied.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from xml.etree import ElementTree

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools import ics_predeployment  # noqa: E402
from tools.qualification_snapshot import freeze  # noqa: E402

BASELINE_FILE = ROOT / "tools" / "known_env_failures.txt"
# Keep in sync with the "Format check" and "Type check" steps of .github/workflows/checks.yml.
FORMAT_TARGETS = ["custom_components"]
MYPY_TARGETS = ["custom_components/ocpp_hub/core", "custom_components/ics_proxy/api.py"]


@dataclass(frozen=True)
class Deployment:
    """Identify the rollout so that only the relevant iCS checks are required."""

    target: str
    installed: str | None
    ha_python: Path | None


# Pick the repository virtualenv interpreter for the current OS, falling back to the running Python.
# Tools must run the same ruff/pytest versions the project is developed with.
def venv_python() -> str:
    for candidate in (ROOT / ".venv" / "Scripts" / "python.exe", ROOT / ".venv" / "bin" / "python"):
        if candidate.exists():
            return str(candidate)
    return sys.executable


# Read the baseline of environment-only failing test ids (prefix match, comments allowed).
# Windows checkouts fail a handful of tests for reasons unrelated to the change under test.
def load_baseline(path: Path = BASELINE_FILE) -> list[str]:
    if not path.exists():
        return []
    lines = (line.strip() for line in path.read_text(encoding="utf-8").splitlines())
    return [line for line in lines if line and not line.startswith("#")]


# Split failing test ids into known-environment and new failures.
# Only new failures should fail a local run.
def classify_failures(failures: list[str], baseline: list[str]) -> tuple[list[str], list[str]]:
    known = [item for item in failures if any(item.startswith(prefix) for prefix in baseline)]
    return known, [item for item in failures if item not in known]


# Extract failing node ids from pytest's short summary.
# The baseline comparison needs stable ids, not the raw log.
def parse_failures(output: str) -> list[str]:
    return [match.group(1).strip() for match in re.finditer(r"^(?:FAILED|ERROR) (\S+)", output, re.MULTILINE)]


# A known assertion failure may be waived on Windows; collection, usage and internal errors never become success.
def pytest_succeeded(completed: subprocess.CompletedProcess[str], known: list[str], new: list[str]) -> bool:
    if completed.returncode == 0:
        return True
    output = completed.stdout + completed.stderr
    return completed.returncode == 1 and bool(known) and not new and not re.search(r"^ERROR ", output, re.MULTILINE)


# Run one named step and report pass or fail without aborting the remaining steps.
# A single run should list every problem, not only the first.
def run_step(name: str, command: list[str], results: dict[str, bool]) -> subprocess.CompletedProcess[str]:
    print(f"\n== {name}")
    env = dict(os.environ)
    if env.get("TMPDIR"):
        for variable, child in (
            ("MYPY_CACHE_DIR", "mypy"),
            ("RUFF_CACHE_DIR", "ruff"),
            ("PYTHONPYCACHEPREFIX", "compiled"),
        ):
            env.setdefault(variable, str(Path(env["TMPDIR"]) / child))
    completed = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, env=env)
    output = (completed.stdout + completed.stderr).strip()
    print(output[-2500:] if output else "(no output)")
    results[name] = completed.returncode == 0
    return completed


# Run the YAML syntax check CI applies to every *.yml/*.yaml file in the repository.
# A broken fixture or workflow should fail locally, not in CI.
def check_yaml(python: str, results: dict[str, bool]) -> None:
    code = (
        "import pathlib,yaml,sys\n"
        "bad=[]\n"
        "for pat in ('*.yml','*.yaml'):\n"
        "    for p in pathlib.Path('.').rglob(pat):\n"
        "        if {'.venv','.venv-ha','.git'} & set(p.parts): continue\n"
        "        try: yaml.safe_load(p.read_text(encoding='utf-8'))\n"
        "        except Exception as e: bad.append(f'{p}: {str(e)[:80]}')\n"
        "print('\\n'.join(bad)); sys.exit(1 if bad else 0)\n"
    )
    run_step("yaml", [python, "-c", code], results)


# Check all four raw qualification phases against the exact clean source.
def audit_qualification(path, source, metadata, python, results, collection_executors):
    bundle = json.loads(path.read_text())
    required = {"engine-model", "ha-proxy", "ha-legacy", "ha-proxy-legacy"}
    if (
        bundle.get("commit") != metadata["commit"]
        or bundle.get("source_digest") != metadata["source_digest"]
        or set(bundle.get("phases", {})) != required
    ):
        raise ValueError("qualification bundle does not cover this exact source and every required suite")
    engine_packages = {}
    for suite in sorted(required):
        batch = Path(bundle["phases"][suite])
        config = json.loads((batch / "config.json").read_text())
        if config.get("suite") != suite or config.get("workers") != 20 or config.get("mode") != "full":
            raise ValueError("qualification phase requires its entire suite in full mode with20 workers")
        completed = run_step(
            f"qualification ({suite})",
            [
                python,
                "-B",
                str(source / "tools/audit_parallel_qualification.py"),
                str(batch),
                "--expected-commit",
                metadata["commit"],
                "--expected-source-digest",
                metadata["source_digest"],
                *(["--collection-executors", str(collection_executors)] if collection_executors else []),
            ],
            results,
        )
        audited = json.loads(completed.stdout)
        if (
            completed.returncode != 0
            or audited.get("full_qualification_passed") is not True
            or audited.get("mode") != "full"
            or audited.get("suite") != suite
            or audited.get("workers") != 20
            or audited.get("commit") != metadata["commit"]
            or audited.get("source_digest") != metadata["source_digest"]
        ):
            results[f"qualification ({suite})"] = False
            raise ValueError("fresh raw audit did not establish full qualification for this exact suite/source")
        if suite == "engine-model":
            engine_packages = config["expected_packages"]
    return engine_packages


# Finish the normal gate and the focused iCS checks before publishing a deployable snapshot.
def run_full_qualification(
    path: Path | None,
    python: str,
    results: dict[str, bool],
    collection_executors: Path | None = None,
    *,
    validated_snapshot: Path | None = None,
    deployment: Deployment | None = None,
) -> None:
    if path is None:
        print("Full qualification requires --qualification-report from tools/parallel_qualify.py (20 workers).")
        results["full qualification"] = False
        return
    with tempfile.TemporaryDirectory(prefix="ocpp-release-") as tmp:
        scratch = Path(tmp)
        try:
            target = deployment.target if deployment is not None else "qualification-only"
            ics_release = target in ("ics-proxy", "ics-client")
            source = (validated_snapshot or scratch).resolve() / "source" if ics_release or validated_snapshot else None
            metadata = freeze(ROOT, source)
            week = target == "ics-proxy" and ics_predeployment.changed_proxy(
                ROOT, metadata["commit"], deployment.installed
            )
            if week and (deployment.ha_python is None or not deployment.ha_python.is_file()):
                raise ValueError(
                    "provide --ics-ha-python from a pinned Home Assistant runtime for the installation week"
                )
            engine_packages = audit_qualification(path, source or ROOT, metadata, python, results, collection_executors)
            if target == "ics-proxy":
                ics_predeployment.require_standalone_pins(source, engine_packages)
            if ics_release and all(results.values()):
                output = path.resolve().parent / "ics-predeployment"
                ics_predeployment.ensure_check(ics_predeployment.DISCOVERY, source, output, metadata, python)
                results["recovery search"] = True
                if week:
                    previous = scratch / "installed"
                    ics_predeployment.export_installed(ROOT, deployment.installed, previous)
                    ics_predeployment.ensure_check(
                        ics_predeployment.WEEK,
                        source,
                        output,
                        metadata,
                        str(deployment.ha_python.absolute()),
                        installed=(deployment.installed, previous),
                    )
                    results["installation week"] = True
            if validated_snapshot is not None and all(results.values()):
                (validated_snapshot / "snapshot.json").write_text(json.dumps(metadata) + "\n")
        except (
            OSError,
            ValueError,
            KeyError,
            RuntimeError,
            ElementTree.ParseError,
            subprocess.SubprocessError,
        ) as error:
            print(f"Full qualification is invalid: {type(error).__name__}: {error}")
            results["full qualification"] = False


# Orchestrate the CI-equivalent checks.
# Release candidates need CI parity before a tag is pushed (a failing hassfest once shipped in a tag).
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--quick", action="store_true", help="skip pytest")
    mode.add_argument("--tests-only", action="store_true", help="run only pytest")
    parser.add_argument("-k", dest="expression", default="", help="engine pytest -k expression; not full qualification")
    parser.add_argument("--qualification-report", type=Path, default=os.environ.get("OCPP_QUALIFICATION_REPORT"))
    parser.add_argument(
        "--collection-executors", type=Path, help="explicit trusted source-pinned collection executor map"
    )
    parser.add_argument("--validated-snapshot", type=Path, help="retain the source that passed full qualification")
    parser.add_argument(
        "--deployment-target",
        choices=("ics-proxy", "ics-client", "legacy-ha", "qualification-only"),
        default="ics-proxy",
        help="release target, or qualification-only for an evidence audit",
    )
    parser.add_argument(
        "--installed-proxy-commit",
        default=os.environ.get("OCPP_INSTALLED_PROXY_COMMIT"),
        help="verified full commit currently installed on this standalone proxy target",
    )
    parser.add_argument(
        "--ics-ha-python",
        type=Path,
        default=os.environ.get("OCPP_ICS_HA_PYTHON"),
        help="pinned Home Assistant interpreter used for a changed standalone proxy",
    )
    args = parser.parse_args()
    if args.validated_snapshot and (args.quick or args.tests_only or args.expression):
        parser.error("--validated-snapshot requires full validation")
    if args.validated_snapshot and args.deployment_target == "qualification-only":
        parser.error("an evidence audit cannot produce a deployment snapshot")
    python = venv_python()
    results: dict[str, bool] = {}

    if not args.tests_only:
        run_step("ruff check", [python, "-m", "ruff", "check", "."], results)
        run_step("ruff format", [python, "-m", "ruff", "format", "--check", *FORMAT_TARGETS], results)
        run_step("mypy --strict", [python, "-m", "mypy", "--strict", "--follow-imports=skip", *MYPY_TARGETS], results)
        run_step("version", [python, "tools/version.py", "check"], results)
        run_step("translations (hassfest rules)", [python, "tools/check_translations.py"], results)
        check_yaml(python, results)
        run_step("compileall", [python, "-m", "compileall", "-q", "-f", "custom_components/"], results)
        run_step("git diff --check", ["git", "diff", "--check", "HEAD"], results)

    if not args.quick and args.expression:
        command = [python, "-m", "pytest", "-q", "-rf", "-p", "no:cacheprovider", "tests/", "-k", args.expression]
        completed = run_step("pytest", command, results)
        failures = parse_failures(completed.stdout + completed.stderr)
        baseline = load_baseline() if sys.platform == "win32" else []
        known, new = classify_failures(failures, baseline)
        results["pytest"] = pytest_succeeded(completed, known, new)
        print(f"pytest: {len(failures)} failed, {len(known)} known environment failures, {len(new)} new")
        for item in new:
            print(f"  NEW FAILURE: {item}")
        print("Targeted engine tests only; full qualification requires all four20-worker suites.")
    elif not args.quick:
        run_full_qualification(
            args.qualification_report,
            python,
            results,
            validated_snapshot=args.validated_snapshot,
            collection_executors=args.collection_executors,
            deployment=Deployment(args.deployment_target, args.installed_proxy_commit, args.ics_ha_python),
        )

    print("\nSummary")
    for name, ok in results.items():
        print(f"  {'ok  ' if ok else 'FAIL'} {name}")
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
