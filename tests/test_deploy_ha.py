"""The pure parts of tools/deploy_ha.py: settings, digests, the remote script and the ssh command line."""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import sys
import tarfile

import pytest

from tests.test_qualification_evidence import project
from tests.tool_loader import load_tool
from tools.qualification_snapshot import freeze

deploy = load_tool("deploy_ha")


# Intent: .env parsing ignores comments and blank lines and strips quotes.
# Why: the real .env mixes all three, and a quote left on a value breaks the host name.
def test_parse_env_handles_comments_and_quotes() -> None:
    env = deploy.parse_env("# note\n\nHA_HOST=\"ha.local\"\nHA_SSH_USER = admin \nHA_SSH_KEY='~/.ssh/key'\n")

    assert env == {"HA_HOST": "ha.local", "HA_SSH_USER": "admin", "HA_SSH_KEY": "~/.ssh/key"}


# Intent: the target falls back to the host of HA_API_URL and never reads a password.
# Why: most .env files only carry the API URL; a missing user must stop the run.
def test_target_resolution_uses_api_url_host_and_requires_a_user() -> None:
    target = deploy.target_from_env({"HA_API_URL": "http://ha.local:8123/api", "HA_SSH_USER": "admin"})

    assert (target.host, target.user, target.key) == ("ha.local", "admin", None)
    with pytest.raises(SystemExit):
        deploy.target_from_env({"HA_HOST": "ha.local"})


# Intent: the comparison reports missing, unexpected and different files separately.
# Why: a verified deploy needs all three lists empty, and each kind points to a different problem.
def test_compare_separates_missing_extra_and_different() -> None:
    local = {"a.py": "1", "b.py": "2", "c.py": "3"}
    remote = deploy.parse_sha256sum("1  ./a.py\n9  ./b.py\n4  ./d.py\n")

    assert deploy.compare(local, remote) == (["c.py"], ["d.py"], ["b.py"])


# Intent: the install script unpacks the new copy completely before it replaces the old one, and backs up first.
# Why: a broken transfer must leave the running integration untouched.
def test_remote_script_backs_up_unpacks_then_swaps() -> None:
    script = deploy.remote_install_script(deploy.Target("h", "u"))

    assert script.index("tar -C '/config/custom_components' -czf") < script.index("-xf -") < script.index("mv '/config")
    assert "ocpp_hub" in script and "tail -n +4" in script


# Intent: privileged commands use `sudo -n`, which fails rather than prompting for a password.
# Why: nothing in this tool may ever wait on or type a password.
def test_privileged_never_prompts() -> None:
    assert deploy.privileged("echo hi", True).startswith("sudo -n sh -c ")
    assert deploy.privileged("echo hi", False) == "echo hi"


# Intent: batch mode adds BatchMode=yes and a key is passed by path with IdentitiesOnly.
# Why: an unauthorised key must fail at once, and only the named key may be offered.
def test_ssh_command_is_batch_and_key_only() -> None:
    command = deploy.ssh_command(deploy.Target("h", "u", key="/k"), "/tmp/cp", "true", batch=True)

    assert "BatchMode=yes" in command and "IdentitiesOnly=yes" in command
    assert command[command.index("-i") + 1] == "/k"
    assert command[-2:] == ["u@h", "true"]


# Intent: both checked HA transports install and verify bytes from the validated snapshot after checkout edits.
# Why: qualification pins the release candidate before an install can consume it.
@pytest.mark.parametrize("container", [False, True])
def test_checked_deployment_packages_only_the_pinned_snapshot(tmp_path, monkeypatch, container):
    domain = "ics_proxy" if container else "ocpp_hub"
    component = f"custom_components/{domain}/main.py"
    installer = "tools/ha_container_deploy.py"
    source = project(tmp_path / "checkout", {component: "# pinned\n", installer: "# pinned installer\n"})
    monkeypatch.setattr(deploy, "ROOT", source)
    capture = []

    # Change the live checkout only after the validation boundary has returned its own pinned source.
    def checked(root, collection_executors, *, snapshot):
        metadata = freeze(root, snapshot / "source")
        (snapshot / "snapshot.json").write_text(json.dumps(metadata))
        (root / component).write_text("# changed\n")
        (root / installer).write_text("# changed installer\n")
        capture.append(metadata["commit"][:12])

    # Inspect the outbound archive and its byte-for-byte digest map without opening a connection.
    def receive(data, expected):
        with tarfile.open(fileobj=io.BytesIO(data), mode="r") as archive:
            assert sorted(archive.getnames()) == ["BUILD", "main.py"]
            assert archive.extractfile("main.py").read() == b"# pinned\n"
            assert archive.extractfile("BUILD").read().decode() == capture[0] + "\n"
            assert expected == {
                member.name: hashlib.sha256(archive.extractfile(member).read()).hexdigest() for member in archive
            }
        capture.append("sent")

    monkeypatch.setattr(deploy, "run_checks", checked)
    if container:
        original_run = subprocess.run

        # Pass Git through, replacing only the outward install.
        def transport(command, **kwargs):
            if command[0] != "docker":
                return original_run(command, **kwargs)
            assert command[-3] == "# pinned installer\n" and command[-2] == domain
            receive(kwargs["input"], json.loads(command[-1]))
            return subprocess.CompletedProcess(command, 0)

        monkeypatch.setattr(deploy.subprocess, "run", transport)
        args = ["--domain", domain, "--container", "unit-home-assistant"]
    else:

        # Capture the SSH package at the same boundary as the container package.
        def transport(target, archive, expected, control_path, batch):
            receive(archive, expected)

        monkeypatch.setattr(deploy, "deploy", transport)
        args = ["--host", "ha.example", "--user", "unit"]
    monkeypatch.setattr(sys, "argv", ["deploy_ha.py", *args])
    assert deploy.main() == 0 and capture[-1] == "sent"
