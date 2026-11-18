"""The pure parts of tools/deploy_ha.py: settings, digests, the remote script and the ssh command line."""

from __future__ import annotations

import hashlib
import io
import json
import os
import socket
import subprocess
import sys
import tarfile
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from tests.test_qualification_evidence import project
from tests.tool_loader import load_tool
from tools import ha_restart
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


# Intent: a different host must be deliberately named in the private SSH API configuration.
# Why: changing the filesystem target must never restart the old default Home Assistant instance.
def test_restart_api_is_bound_to_the_selected_ssh_target() -> None:
    target = deploy.Target("new-ssh.example", "unit")
    access = deploy.RestartAccess("http://old.example:8123", "synthetic-token")
    with pytest.raises(SystemExit, match="selected SSH target"):
        deploy.ssh_restart_api(target, access)
    selected = deploy.RestartAccess("http://new-ha.example:8123", "synthetic-token", ssh_host=target.host)
    assert deploy.ssh_restart_api(target, selected).prefix == "/api"


# Intent: restart completion needs a new Core lifetime after the request.
# Why: refusal or an answering old process must not look like a successful deployment.
@pytest.mark.parametrize("outcome", ["rejected", "unchanged", "replaced"])
def test_restart_requires_a_new_lifetime(monkeypatch, outcome) -> None:
    lifetime = [1]
    elapsed = [0.0]
    requests = []

    # Advance the test clock without waiting for the production deadline.
    def sleep(seconds: float) -> None:
        elapsed[0] += seconds

    # Report a successful config check, followed by the selected restart response.
    def request(path: str, *, post: bool) -> dict[str, str]:
        assert post
        requests.append(path)
        if path.endswith("check_config"):
            return {"result": "valid"}
        if outcome == "rejected":
            raise ha_restart.ResponseError("HTTP 500")
        if outcome == "replaced":
            lifetime[0] = 2
        return {}

    api = SimpleNamespace(request=request)
    monkeypatch.setattr(ha_restart, "time", SimpleNamespace(monotonic=lambda: elapsed[0], sleep=sleep))
    if outcome == "rejected":
        with pytest.raises(ha_restart.ResponseError, match="HTTP 500"):
            ha_restart.restart(api, 1, lambda: lifetime[0], timeout=1)
        assert elapsed == [0.0]
    elif outcome == "unchanged":
        with pytest.raises(ha_restart.RestartError, match="verified new Core lifetime"):
            ha_restart.restart(api, 1, lambda: lifetime[0], timeout=1)
    else:
        ha_restart.restart(api, 1, lambda: lifetime[0], timeout=1)
    assert requests == ["/config/core/check_config", "/services/homeassistant/restart"]


# Intent: connecting to a Linux loopback port proves the accepted socket belongs to the chosen process.
# Why: host-network Docker containers can reach services owned by another container.
def test_container_proof_uses_the_accepted_socket_owner(monkeypatch) -> None:
    lifetime = (os.getpid(), "synthetic-start")
    monkeypatch.setattr(ha_restart, "core_lifetime", lambda: lifetime)
    with socket.socket() as listener, ThreadPoolExecutor(max_workers=1) as pool:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(3)
        accepted = pool.submit(listener.accept)
        connection = ha_restart.connect_to_core(listener.getsockname()[1], lifetime)
        try:
            server, _ = accepted.result(timeout=3)
            with server:
                assert connection.sock is not None and ha_restart.owns_connection(lifetime[0], connection.sock)
        finally:
            connection.close()


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
            if command[1] == "inspect":
                assert command[-1] == "unit-home-assistant"
                return subprocess.CompletedProcess(command, 0, stdout="a" * 64 + "\n")
            assert command[3] == "a" * 64
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
