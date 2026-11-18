#!/usr/bin/env python3
"""Select the changelog body for the tagged version without publishing unreleased work."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

from tools.version import canonical


# Match the requested version rather than relying on the position of an Unreleased heading.
def extract(changelog: str, tag: str) -> str:
    version = canonical(tag.removeprefix("v"))
    if not re.match(r"\d", version):
        raise ValueError(f"Not a release version: {tag}")
    headings = list(re.finditer(r"^##[ \t]+(\S+)[^\n]*$", changelog, re.MULTILINE))
    for index, heading in enumerate(headings):
        if canonical(heading[1]) != version:
            continue
        end = headings[index + 1].start() if index + 1 < len(headings) else len(changelog)
        notes = changelog[heading.end() : end].strip()
        if not notes:
            raise ValueError(f"Changelog entry for {tag} is empty")
        return notes
    raise ValueError(f"Changelog has no entry for {tag}")


# A missing or empty tagged entry fails the release step before GitHub can publish misleading notes.
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag")
    parser.add_argument("--changelog", type=Path, default=Path("CHANGELOG.md"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        notes = extract(args.changelog.read_text(), args.tag)
    except ValueError as error:
        parser.error(str(error))
    args.output.write_text(notes + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
