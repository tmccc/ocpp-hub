"""Check when the normal release gate runs and reuses the additional iCS tests."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from tools import ics_predeployment as checks
from tools import validate


# Commit a tiny source tree so the release gate must select real immutable candidate and predecessor bytes.
def commit(repo, name, content):
    path = repo / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    subprocess.run(["git", "add", name], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-qm", name], cwd=repo, check=True)
    return checks.git(repo, "rev-parse", "HEAD").decode().strip()


# Supply successive commits where only the final one changes the proxy.
@pytest.fixture
def releases(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    for key, value in (("user.name", "Example"), ("user.email", "example@example.invalid")):
        subprocess.run(["git", "config", key, value], cwd=repo, check=True)
    installed = commit(repo, "tools/managed_proxy.py", "# installed\n")
    client = commit(repo, "custom_components/ics_proxy/sensor.py", "# client change\n")
    candidate = commit(repo, "tools/managed_proxy.py", "# candidate\n")
    return repo, installed, client, candidate


# Intent: decide on the week from shipped code, with a real older Git commit as the source for the simulator.
# Why: an HA-only commit does not change the standalone build, while a new proxy must use its actual predecessor.
def test_proxy_change_detection_and_export(tmp_path, releases):
    repo, installed, client, candidate = releases
    assert checks.changed_proxy(repo, client, installed) is False
    assert checks.changed_proxy(repo, candidate, installed) is True
    with pytest.raises(ValueError, match="full installed proxy commit"):
        checks.changed_proxy(repo, candidate, installed[:12])
    exported = tmp_path / "installed"
    checks.export_installed(repo, installed, exported)
    assert (exported / "tools/managed_proxy.py").read_text() == "# installed\n"


# Intent: exact scoped release checks precede publication of an approved source snapshot.
# Why: a failed week must stop the actual packaging path, even when the normal four suites have already passed.
@pytest.mark.parametrize(
    ("target", "same_proxy", "week_fails", "expected"),
    [
        ("legacy-ha", False, False, ()),
        ("ics-client", False, False, (checks.DISCOVERY,)),
        ("ics-proxy", True, False, (checks.DISCOVERY,)),
        ("ics-proxy", False, False, (checks.DISCOVERY, checks.WEEK)),
        ("ics-proxy", False, True, (checks.DISCOVERY, checks.WEEK)),
    ],
)
def test_release_snapshot_waits_for_required_checks(
    tmp_path, monkeypatch, releases, target, same_proxy, week_fails, expected
):
    repo, installed, _, candidate = releases
    monkeypatch.setattr(validate, "ROOT", repo)
    snapshot = tmp_path / "validated"
    snapshot.mkdir()
    calls = []

    # Only stand in for the four raw suite audits. The release function still freezes the real commit.
    def audit(path, source, metadata, python, results, executors):
        assert metadata["commit"] == candidate
        results["four suites"] = True

    # Capture the command boundary before the release process can mark a snapshot as approved.
    def invoke(check, source, output, metadata, python, *, installed=None):
        assert not (snapshot / "snapshot.json").exists()
        assert source == snapshot / "source" and metadata["commit"] == candidate
        assert (source / "tools/managed_proxy.py").read_text() == "# candidate\n"
        if installed is not None:
            assert installed[0] == releases[1]
            assert (installed[1] / "tools/managed_proxy.py").read_text() == "# installed\n"
        calls.append(check)
        if week_fails and check == checks.WEEK:
            raise ValueError("synthetic failed week")

    monkeypatch.setattr(validate, "audit_qualification", audit)
    monkeypatch.setattr(checks, "ensure_check", invoke)
    results = {}
    validate.run_full_qualification(
        tmp_path / "report.json",
        sys.executable,
        results,
        validated_snapshot=snapshot,
        deployment=validate.Deployment(target, candidate if same_proxy else installed, Path(sys.executable)),
    )
    assert tuple(calls) == expected
    assert (snapshot / "snapshot.json").exists() is not week_fails
    assert all(results.values()) is not week_fails


# Intent: a later validation reuses real test evidence while a changed result must be refreshed.
# Why: one passing flag would mask skipped or missing tests and repeated deploy preparation would waste time.
def test_reusing_and_rechecking_discovery_results(tmp_path, monkeypatch):
    attempts = []

    # Write the exact raw artifacts in miniature so the cache treats them the same way as actual child output.
    def run(check, python, source, attempt, previous):
        attempts.append(attempt)
        path = attempt / "discovery"
        path.mkdir()
        (attempt / "pytest.xml").write_text(
            '<testsuite><testcase classname="tests_discovery.test_recovery_discovery" '
            'name="test_recovery_discovery"/></testsuite>'
        )
        (path / "summary.json").write_text(
            json.dumps(
                {
                    "status": "passed",
                    "seed": 19,
                    "planned": 20,
                    "runs": [{"purpose": "search", "status": "passed"}] * 20,
                }
            )
        )

    monkeypatch.setattr(checks, "runtime_identity", lambda python, source: {"python": "fixture"})
    monkeypatch.setattr(checks, "run_check", run)
    args = (checks.DISCOVERY, tmp_path, tmp_path, {"commit": "a", "source_digest": "b"}, sys.executable)
    checks.ensure_check(*args)
    checks.ensure_check(*args)
    assert len(attempts) == 1
    (attempts[0] / "discovery/summary.json").write_text('{"status":"passed","seed":19,"planned":20,"runs":[]}')
    checks.ensure_check(*args)
    assert len(attempts) == 2
