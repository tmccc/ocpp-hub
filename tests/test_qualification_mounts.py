"""Configured auxiliary runtime assets cannot overlay the frozen collection source or evidence."""

import json
import sys
from pathlib import Path

import pytest

from tools import parallel_qualify, parallel_worker
from tools import qualification_collection as collection


# Keep each role concrete so overlap checks cover actual container destinations rather than host-source filenames.
def configuration(tmp_path, target=None, mode="image"):
    asset = tmp_path / "openssl"
    asset.write_text("read-only synthetic executable asset\n")
    return {
        "venv": str(tmp_path / "host-venv"),
        "venv_target": "/venv",
        "container_python": "/venv/bin/python",
        "runtime_mode": mode,
        "readonly_mounts": [] if target is None else [{"source": str(asset), "target": target}],
        "image": "sha256:" + "a" * 64,
    }


# Intent: an auxiliary mount cannot replace source, its ancestors, evidence or either Python runtime code location.
# Why: host-side SHA checks cannot observe bytes hidden by a later Docker mount in both actual collection and replay.
@pytest.mark.parametrize(
    "target",
    [
        "/work/batch",
        "/work",
        "/work/batch/source/conftest.py",
        "/work/batch/source/tests/test_case.py",
        "/work/batch/qualification_evidence.py",
        "/work/batch/3",
        "/work/batch/3/results.jsonl",
        "/venv",
        "/venv/lib/python3.14/site-packages/_pytest/python.py",
        "/venv/bin/python",
        "/usr",
        "/usr/local/lib/python3.14/site-packages/_pytest/python.py",
        "/usr/src/homeassistant",
        "/lib",
        "/opt/python-system/typing_extensions.py",
        "/",
        "/work/batch/source/../source/conftest.py",
        "//work/batch/source/conftest.py",
        "/work//batch/source/conftest.py",
    ],
)
def test_auxiliary_mount_rejects_protected_or_traversing_destinations(tmp_path, target):
    config = configuration(tmp_path, target)
    with pytest.raises(ValueError):
        collection.validate_readonly_mounts(config, Path("/work/batch"), Path("/work/batch/3"))


# Intent: a configurable primary venv mount cannot itself hide source or evidence even with no auxiliary mounts.
# Why: protecting only the auxiliary list would leave the same overlay available through --venv-target.
@pytest.mark.parametrize("target", ["/work", "/work/batch/source", "/work/batch/3", "/"])
def test_runtime_mount_cannot_overlay_data(tmp_path, target):
    config = configuration(tmp_path)
    config["venv_target"] = target
    with pytest.raises(ValueError, match="runtime mount overlaps"):
        collection.validate_readonly_mounts(config, Path("/work/batch"), Path("/work/batch/3"))


# Intent: the provisioned image-mode OpenSSL executable and isolated runtime directory remain valid unchanged.
# Why: the protection must preserve actual HA11 TLS-fixture dependencies without permitting arbitrary code overlays.
def test_existing_image_openssl_assets_are_admitted(tmp_path):
    config = configuration(tmp_path, "/usr/bin/openssl")
    runtime = tmp_path / "openssl-runtime"
    runtime.mkdir()
    config["readonly_mounts"].append({"source": str(runtime), "target": "/task/openssl-runtime"})
    collection.validate_readonly_mounts(config, Path("/work/batch"), Path("/work/batch/collection"))


# Intent: the exact system-file exception is image-only and cannot become a symlink/directory or repeated override.
# Why: an ancestor directory, another runtime mode or a second mount must not expand the narrow OpenSSL exception.
@pytest.mark.parametrize("problem", ["host", "symlink", "directory", "duplicate", "other-home-code"])
def test_openssl_exception_is_exact(tmp_path, problem):
    config = configuration(tmp_path, "/usr/bin/openssl")
    if problem == "host":
        config["runtime_mode"] = "host"
    elif problem == "symlink":
        path = tmp_path / "openssl-alias"
        path.symlink_to(tmp_path / "openssl")
        config["readonly_mounts"][0]["source"] = str(path)
    elif problem == "directory":
        path = tmp_path / "not-a-file"
        path.mkdir()
        config["readonly_mounts"][0]["source"] = str(path)
    elif problem == "duplicate":
        config["readonly_mounts"].append(dict(config["readonly_mounts"][0]))
    else:
        config["readonly_mounts"][0]["target"] = "/root/.local/lib/python3.14/site-packages/sitecustomize.py"
    with pytest.raises(ValueError):
        collection.validate_readonly_mounts(config, Path("/work/batch"), Path("/work/batch/collection"))


# Intent: the actual worker command builder rejects a frozen-source overlay before launching Docker.
# Why: validation only in a launcher would be bypassed by replaying an edited raw configuration directly.
def test_worker_builder_uses_shared_mount_guard(tmp_path, monkeypatch):
    batch = tmp_path / "batch"
    monkeypatch.setattr(parallel_worker, "ROOT", batch)
    monkeypatch.setattr(parallel_worker, "SOURCE", batch / "source")
    config = configuration(tmp_path, str(batch / "source/tests/conftest.py"))
    with pytest.raises(ValueError, match="overlaps"):
        parallel_worker.container_command(config, batch / "3", 0, "unused", {})


# Intent: fresh audit replay rejects the same unsafe raw configuration before touching runtime or starting a container.
# Why: independently recollecting through an identical source overlay would otherwise agree with the incomplete run.
def test_fresh_replay_uses_shared_mount_guard(tmp_path, monkeypatch):
    batch = tmp_path / "batch"
    batch.mkdir()
    config = configuration(tmp_path, str(batch / "source/tests/conftest.py"))
    (batch / "config.json").write_text(json.dumps(config))

    # A protected target must fail before any remaining replay input, runtime scan or Docker operation is needed.
    def forbidden(*args, **kwargs):
        raise AssertionError("unsafe replay reached runtime or Docker")

    monkeypatch.setattr(collection, "runtime_digest", forbidden)
    monkeypatch.setattr(collection.subprocess, "run", forbidden)
    with pytest.raises(ValueError, match="overlaps"):
        collection.replay(batch, "a" * 40, "b" * 64)


# Intent: the actual stage-only launcher refuses the final concrete batch/source destination before dispatch.
# Why: source-preserving argument validation must happen before a remote worker can interpret an overlay config.
def test_launcher_rejects_concrete_source_overlay(tmp_path, monkeypatch):
    output = tmp_path / "output"
    source = tmp_path / "repo"
    source.mkdir()
    asset = tmp_path / "overlay"
    asset.write_text("not a source file\n")

    # Supply a tiny source snapshot for this argument-boundary unit; no test body, Docker container or worker executes.
    def minimal_freeze(repo, destination):
        destination.mkdir()
        (destination / "requirements-offline.txt").write_text("pytest==9.2.0\n")
        return {"commit": "a" * 40, "source_digest": "b" * 64}

    monkeypatch.setattr(parallel_qualify, "freeze", minimal_freeze)
    monkeypatch.setattr(parallel_qualify, "HARNESS", ())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "parallel_qualify",
            "--source",
            str(source),
            "--output",
            str(output),
            "--venv",
            str(tmp_path / "venv"),
            "--image",
            "sha256:" + "a" * 64,
            "--stage-only",
            "--label",
            "guard",
            "--readonly-mount",
            str(asset) + "=" + str(output / "guard/source/tests/conftest.py"),
        ],
    )
    with pytest.raises(ValueError, match="overlaps"):
        parallel_qualify.main()
    assert not (output / "guard/config.json").exists()


# Intent: alternate slash spellings cannot hide a primary venv's overlap with the source directory.
# Why: Docker normalizes destinations, so lexical validation must reject aliases before comparing protected roles.
@pytest.mark.parametrize("target", ["//work/batch/source", "/work//batch/source"])
def test_primary_runtime_destination_requires_canonical_spelling(tmp_path, target):
    config = configuration(tmp_path)
    config["venv_target"] = target
    with pytest.raises(ValueError, match="canonical"):
        collection.validate_readonly_mounts(config, Path("/work/batch"), Path("/work/batch/3"))
