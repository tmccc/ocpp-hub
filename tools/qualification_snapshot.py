"""Capture and verify only ordinary committed Git blobs for an immutable test run."""

from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path


# Run Git against an explicit repository without changing the process directory.
def git(source: Path, *args: str) -> bytes:
    return subprocess.check_output(["git", "-C", str(source), *args])


# Verify every committed byte and preserve its mode in a read-only snapshot.
def freeze(source: Path, destination: Path | None = None) -> dict:
    if git(source, "status", "--porcelain", "--untracked-files=all").strip():
        raise RuntimeError("Commit all source changes before qualification; working tree is not clean.")
    commit = git(source, "rev-parse", "HEAD").decode().strip()
    algorithm = git(source, "rev-parse", "--show-object-format").decode().strip()
    tree = git(source, "ls-tree", "-rz", "--full-tree", commit)
    entries = []
    digest = hashlib.sha256()
    if destination is not None:
        destination.mkdir()
    for record in sorted(item for item in tree.split(b"\0") if item):
        header, name = record.split(b"\t", 1)
        mode, kind, blob = header.split()
        if kind != b"blob" or mode not in (b"100644", b"100755"):
            raise RuntimeError("Qualification accepts ordinary tracked files only: " + os.fsdecode(name))
        relative = Path(os.fsdecode(name))
        if relative.is_absolute() or ".." in relative.parts:
            raise RuntimeError("Unsafe source path")
        original = source / relative
        if original.is_symlink():
            raise RuntimeError("Tracked path became a symlink: " + str(relative))
        data = original.read_bytes()
        object_hash = hashlib.new(algorithm, b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
        if object_hash != blob.decode():
            raise RuntimeError("Working bytes differ from committed blob: " + str(relative))
        if destination is not None:
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            target.chmod(0o555 if mode == b"100755" else 0o444)
        checksum = hashlib.sha256(data).hexdigest()
        entries.append(
            {"path": os.fsdecode(name), "sha256": checksum, "git_blob": blob.decode(), "mode": mode.decode()}
        )
    # Match the original batch digest convention: sorted path bytes, NUL, then binary SHA256.
    for item in sorted(entries, key=lambda row: os.fsencode(row["path"])):
        digest.update(os.fsencode(item["path"]) + b"\0" + bytes.fromhex(item["sha256"]))
    if (
        git(source, "rev-parse", "HEAD").decode().strip() != commit
        or git(source, "status", "--porcelain", "--untracked-files=all").strip()
    ):
        raise RuntimeError("Source changed while being frozen")
    return {"commit": commit, "source_digest": digest.hexdigest(), "files": entries}
