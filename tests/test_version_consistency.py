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


# Intent: distinct numeric version components must cause actual release-version drift to fail.
# Why: stripping dots made two different releases compare equal and let CI miss a mismatch.
@pytest.mark.parametrize("package,manifest", [("0.2.3", "0.23"), ("1.10.0", "11.0.0")])
def test_numeric_components_do_not_collapse(monkeypatch: pytest.MonkeyPatch, package: str, manifest: str) -> None:
    monkeypatch.setattr(version, "read_pyproject", lambda: package)
    monkeypatch.setattr(version, "read_manifest", lambda: manifest)
    monkeypatch.setattr(version, "read_changelog_headings", lambda: [manifest])

    assert version.check() == 1


# Intent: the supported display prerelease and its PEP 440 spelling still compare as one version.
# Why: retaining component boundaries must not break the existing manifest/package convention.
@pytest.mark.parametrize("display", ["1.8.0-rc2", " 1.8.0-RC2 "])
def test_prerelease_display_spelling_remains_equivalent(display: str) -> None:
    assert version.canonical(display) == version.canonical("1.8.0rc2")
