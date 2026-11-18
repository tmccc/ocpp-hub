"""The release version must agree in pyproject.toml, manifest.json and a numeric CHANGELOG heading."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from tools import version

ROOT = Path(__file__).resolve().parents[1]


# Intent: run the version check tool and require that it reports no drift.
# Why: CI fails on a mismatch, so a release can never carry two different versions.
def test_version_is_in_sync() -> None:
    completed = subprocess.run(
        [sys.executable, "tools/version.py", "check"], cwd=ROOT, capture_output=True, text=True, check=False
    )

    assert completed.returncode == 0, completed.stderr


# Intent: the publishing gate accepts the current release tag and refuses a different version.
# Why: a historical changelog heading must not let a new archive publish under its old tag.
def test_release_tag_must_match_the_checked_out_version(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(version, "read_pyproject", lambda: "1.10.0rc2")
    monkeypatch.setattr(version, "read_manifest", lambda path: "1.10.0-rc2")
    monkeypatch.setattr(version, "read_changelog_headings", lambda: ["Unreleased", "1.10.0-rc2", "1.9.0"])

    assert version.main(["version.py", "check", "v1.10.0-rc2"]) == 0
    assert version.main(["version.py", "check", "v1.9.0"]) == 1


# Intent: distinct numeric version components must cause actual release-version drift to fail.
# Why: stripping dots made two different releases compare equal and let CI miss a mismatch.
@pytest.mark.parametrize("package,manifest", [("0.2.3", "0.23"), ("1.10.0", "11.0.0")])
def test_numeric_components_do_not_collapse(monkeypatch: pytest.MonkeyPatch, package: str, manifest: str) -> None:
    monkeypatch.setattr(version, "read_pyproject", lambda: package)
    monkeypatch.setattr(version, "read_manifest", lambda path: manifest)
    monkeypatch.setattr(version, "read_changelog_headings", lambda: [manifest])

    assert version.check() == 1


# Intent: the supported display prerelease and its PEP 440 spelling still compare as one version.
# Why: retaining component boundaries must not break the existing manifest/package convention.
@pytest.mark.parametrize("display", ["1.8.0-rc2", " 1.8.0-RC2 "])
def test_prerelease_display_spelling_remains_equivalent(display: str) -> None:
    assert version.canonical(display) == version.canonical("1.8.0rc2")


# Intent: both manifest versions and the latest release heading must agree with the package.
# Why: one stale client manifest or an older matching heading used to escape the release check.
@pytest.mark.parametrize("client,latest", [("0.2.2", "0.2.3"), ("0.2.3", "0.2.4")])
def test_client_and_latest_changelog_are_checked(monkeypatch, client, latest):
    monkeypatch.setattr(version, "read_pyproject", lambda: "0.2.3")
    monkeypatch.setattr(version, "read_manifest", lambda path: client if path.parent.name == "ics_proxy" else "0.2.3")
    monkeypatch.setattr(version, "read_changelog_headings", lambda: ["Unreleased", latest, "0.2.3"])
    assert version.check() == 1


# Intent: bump writes an equivalent package version and the same display version to both manifests.
# Why: manual edits to the second manifest can drift from the shipped package.
def test_bump_updates_both_manifests(tmp_path, monkeypatch):
    package, legacy, thin, changelog = (tmp_path / file for file in ("pyproject.toml", "old.json", "thin.json", "log"))
    package.write_text('version = "0.2.3"\n')
    for manifest in (legacy, thin):
        manifest.write_text('{"version":"0.2.3"}\n')
    changelog.write_text("## Unreleased\n\n## 0.3.0-rc1\n")
    monkeypatch.setattr(version, "PYPROJECT", package)
    monkeypatch.setattr(version, "MANIFESTS", (legacy, thin))
    monkeypatch.setattr(version, "CHANGELOG", changelog)
    assert version.bump("0.3.0-rc1") == 0
    assert version.read_pyproject() == "0.3.0rc1"
    assert [version.read_manifest(path) for path in (legacy, thin)] == ["0.3.0-rc1"] * 2
