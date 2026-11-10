"""Real archive installation and rollback into an isolated configuration directory."""

from __future__ import annotations

import hashlib
import io
import json
import tarfile
from pathlib import Path

import pytest

from tools.ha_container_deploy import install


# Build only in-memory synthetic component files, with an exact independent digest inventory.
def archive(files: dict[str, bytes]) -> tuple[bytes, dict[str, str]]:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as package:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            package.addfile(info, io.BytesIO(data))
    return buffer.getvalue(), {name: hashlib.sha256(data).hexdigest() for name, data in files.items()}


# Intent: initial install and replacement verify all bytes and retain an outside-component backup.
# Why: the new HA client must never overwrite the older OCPP integration or leave an unverified copy deployed.
def test_install_and_backup_are_domain_scoped(tmp_path: Path) -> None:
    first = {"manifest.json": json.dumps({"domain": "ics_proxy"}).encode(), "__init__.py": b"VALUE = 1\n"}
    data, expected = archive(first)
    assert install(tmp_path, "ics_proxy", data, expected)["verified_files"] == 2
    assert not (tmp_path / "custom_components/ocpp_hub").exists()
    second = first | {"__init__.py": b"VALUE = 2\n"}
    data, expected = archive(second)
    assert install(tmp_path, "ics_proxy", data, expected)["backup_created"]
    assert (tmp_path / "custom_components/ics_proxy/__init__.py").read_bytes() == second["__init__.py"]
    assert len(list((tmp_path / ".ics_proxy-backup").glob("*.tar.gz"))) == 1


# Intent: unsafe names, incorrect hashes and syntax errors fail before an installed component is touched.
# Why: a malformed transfer cannot become a partially imported HA integration.
@pytest.mark.parametrize("failure", ["path", "hash", "syntax", "domain"])
def test_archive_validation_precedes_all_disk_mutation(tmp_path: Path, failure: str) -> None:
    files = {"manifest.json": b'{"domain":"ics_proxy"}', "__init__.py": b"VALUE = 1\n"}
    if failure == "path":
        files["../escape"] = b"bad"
    if failure == "syntax":
        files["__init__.py"] = b"def invalid(:\n"
    if failure == "domain":
        files["manifest.json"] = b'{"domain":"other"}'
    data, expected = archive(files)
    if failure == "hash":
        expected["__init__.py"] = "0" * 64
    with pytest.raises((ValueError, SyntaxError)):
        install(tmp_path, "ics_proxy", data, expected)
    assert list(tmp_path.iterdir()) == []


# Intent: a failure while publishing files restores the byte-for-byte previous copy from its validated backup.
# Why: permission or disk errors must leave the existing HA integration recoverable.
def test_partial_write_failure_restores_previous_component(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    old = {"manifest.json": b'{"domain":"ics_proxy"}', "__init__.py": b"VALUE = 1\n"}
    data, expected = archive(old)
    install(tmp_path, "ics_proxy", data, expected)
    actual = Path.write_bytes
    failed = False

    # Fail one publication write, preserving actual archive parsing, backup creation and rollback extraction.
    def write(path: Path, data: bytes) -> int:
        nonlocal failed
        if path.name == "__init__.py" and not failed:
            failed = True
            raise OSError("synthetic publication failure")
        return actual(path, data)

    monkeypatch.setattr(Path, "write_bytes", write)
    data, expected = archive(old | {"__init__.py": b"VALUE = 2\n"})
    with pytest.raises(OSError):
        install(tmp_path, "ics_proxy", data, expected)
    assert (tmp_path / "custom_components/ics_proxy/__init__.py").read_bytes() == old["__init__.py"]
