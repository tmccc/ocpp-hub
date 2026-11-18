#!/usr/bin/env python3
"""Keep the release version identical across the places that carry it.

The version lives in four files:

- ``pyproject.toml`` (PEP 440: ``1.8.0rc2``)
- ``both integration manifests`` (display: ``1.8.0-rc2``)
- the newest numeric ``## <version>`` heading in ``CHANGELOG.md``

``bump`` converts the input ``X.Y.Z-rcN`` form to the PEP 440 ``X.Y.ZrcN``
for ``pyproject.toml`` and keeps the display form (hyphen) elsewhere::

    python tools/version.py check
    python tools/version.py bump 1.8.0
    python tools/version.py bump 1.8.0-rc3  # pyproject gets 1.8.0rc3

``check`` exits non-zero when the files do not agree. It is enforced by
``tests/test_version_consistency.py`` so CI fails on drift. ``bump`` updates
``pyproject.toml`` and both manifests; add the matching CHANGELOG heading
yourself because release notes are human-written.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"
MANIFESTS = tuple(ROOT / "custom_components" / domain / "manifest.json" for domain in ("ocpp_hub", "ics_proxy"))
CHANGELOG = ROOT / "CHANGELOG.md"

# Pre-release suffixes that must lose their hyphen to be valid PEP 440.
_PRE_RELEASE = r"(rc|alpha|beta|a|b|post|dev)"


# Normalise a version string for comparison.
# 1.8.0-rc2 and 1.8.0rc2 name the same release.
def canonical(version: str) -> str:
    """Normalize ``1.8.0-rc2`` / ``1.8.0rc2`` / ``1.8.0`` for comparison."""
    return pyproject_version(version.strip().lower())


# Convert a display version to PEP 440 form.
# Pyproject.toml needs 1.8.0rc2 while the manifest keeps the hyphen.
def pyproject_version(version: str) -> str:
    """PEP 440 form for pyproject: ``1.8.0-rc2`` → ``1.8.0rc2``."""
    return re.sub(rf"-{_PRE_RELEASE}", r"\1", version, count=1, flags=re.IGNORECASE)


# Read the version from pyproject.toml.
def read_pyproject() -> str:
    match = re.search(r'^version\s*=\s*"([^"]+)"', PYPROJECT.read_text(), re.MULTILINE)
    if match is None:
        raise SystemExit("version not found in pyproject.toml")
    return match.group(1)


# Read the version from the integration manifest.
# Home Assistant and HACS show this version.
def read_manifest(path: Path) -> str:
    return str(json.loads(path.read_text())["version"])


# List the version headings of the changelog.
# The top heading must match the released version.
def read_changelog_headings() -> list[str]:
    return re.findall(r"^##\s+(\S+)", CHANGELOG.read_text(), re.MULTILINE)


# Compare the package with both manifests and the newest numeric changelog heading.
# CI must fail when they disagree.
def check() -> int:
    pyproject = read_pyproject()
    problems: list[str] = []
    for path in MANIFESTS:
        manifest = read_manifest(path)
        if canonical(pyproject) != canonical(manifest):
            problems.append(f"pyproject.toml ({pyproject}) differs from {path.parent.name} manifest ({manifest})")
    latest = next((heading for heading in read_changelog_headings() if re.match(r"\d", heading)), "")
    if canonical(latest) != canonical(pyproject):
        problems.append(f"CHANGELOG.md latest numeric heading is {latest or 'missing'}, expected {pyproject}")
    if problems:
        for problem in problems:
            print(f"VERSION MISMATCH: {problem}", file=sys.stderr)
        return 1
    print(f"version in sync: {pyproject}")
    return 0


# Prepare each version change before writing the package and both manifests.
# Release notes stay human-written, but the numbers should not be.
def bump(new: str) -> int:
    updates = [(PYPROJECT, r'(^version\s*=\s*")([^"]+)(")', pyproject_version(new))]
    updates.extend((path, r'("version"\s*:\s*")([^"]+)(")', new) for path in MANIFESTS)
    staged = {}
    for path, pattern, value in updates:
        updated, count = re.subn(
            pattern, lambda m: f"{m.group(1)}{value}{m.group(3)}", path.read_text(), count=1, flags=re.MULTILINE
        )
        if not count:
            print(f"version not found in {path}", file=sys.stderr)
            return 1
        staged[path] = updated
    for path, updated in staged.items():
        path.write_text(updated)
    message = (
        f"bumped to {new} (pyproject uses {pyproject_version(new)}); "
        f"add the matching '## {new}' heading to CHANGELOG.md"
    )
    print(message)
    return check()


# Dispatch the check and bump commands.
# One entry point serves CI and the release steps.
def main(argv: list[str]) -> int:
    if len(argv) < 2 or argv[1] not in ("check", "bump"):
        print(__doc__, file=sys.stderr)
        return 2
    if argv[1] == "check":
        return check()
    if len(argv) < 3:
        print("usage: python tools/version.py bump X.Y.Z", file=sys.stderr)
        return 2
    return bump(argv[2])


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
