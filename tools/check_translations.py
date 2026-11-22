#!/usr/bin/env python3
"""Check the Home Assistant translation rules that hassfest enforces and local tests used to miss.

    python tools/check_translations.py

Rules checked for ``strings.json`` and every ``translations/*.json``:

- the file is valid JSON;
- no string contains HTML-like text such as ``<port>``;
- an entry under ``issues`` has a ``title`` and either a ``description`` (not fixable) or a ``fix_flow`` (fixable),
  never both (hassfest: "two or more values in the same group of exclusion 'fixable'").
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


# Return both integrations' source strings and language files, or those of the requested integration.
def translation_files(component: Path | None = None) -> list[Path]:
    root = Path(__file__).resolve().parents[1] / "custom_components"
    components = (component,) if component is not None else (root / "ocpp_hub", root / "ics_proxy")
    files = []
    for directory in components:
        files.extend([directory / "strings.json", *sorted((directory / "translations").glob("*.json"))])
    return files


# Validate the `issues` section of one parsed translation file.
# A fixable repair must carry its text in fix_flow; a top-level description makes hassfest fail.
def issue_problems(data: dict[str, object]) -> list[str]:
    problems: list[str] = []
    issues = data.get("issues", {})
    if not isinstance(issues, dict):
        return ["issues must be an object"]
    for key, issue in issues.items():
        if not isinstance(issue, dict):
            problems.append(f"issues.{key} must be an object")
            continue
        if "title" not in issue:
            problems.append(f"issues.{key} has no title")
        if "description" in issue and "fix_flow" in issue:
            problems.append(f"issues.{key} has both description and fix_flow (fixable exclusion group)")
        if "description" not in issue and "fix_flow" not in issue:
            problems.append(f"issues.{key} needs a description or a fix_flow")
    return problems


# Find strings that look like HTML, such as ``<port>`` or ``<b>``, anywhere in a parsed translation file.
# Hassfest rejects them ("the string should not contain HTML"), and angle-bracket placeholders look like tags.
def html_problems(node: object, path: str = "") -> list[str]:
    problems: list[str] = []
    if isinstance(node, dict):
        for key, value in node.items():
            problems += html_problems(value, f"{path}.{key}" if path else str(key))
    elif isinstance(node, str) and re.search(r"<[A-Za-z/!][^>]*>", node):
        problems.append(f"{path} contains HTML-like text: {node[:60]!r}")
    return problems


# Check every translation file and print the problems found.
# One command gives CI parity for the translation rules.
def main() -> int:
    failed = False
    for path in translation_files():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            print(f"{path}: invalid JSON: {error}")
            failed = True
            continue
        except OSError, UnicodeError:
            print(f"{path}: could not read translation file")
            failed = True
            continue
        if not isinstance(data, dict):
            print(f"{path}: translation data must be an object")
            failed = True
            continue
        for problem in [*issue_problems(data), *html_problems(data)]:
            print(f"{path}: {problem}")
            failed = True
    print("translations: FAILED" if failed else "translations: ok")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
