"""Dormant managed CLI, private health, durable mode and actual TLS shutdown boundaries."""

from __future__ import annotations

import asyncio
import json
import signal
import socket
import sqlite3
import ssl
import threading
import time
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_native_repair import REVOCATIONS, repair_pending, repair_revoked
from ocpp_hub_core.prototype.native_bounded import NativeBoundedSpec
from ocpp_hub_core.prototype.policy import DeniedError
from ocpp_hub_core.prototype.recovery_contracts import Prerequisite, RestoreState
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError
from ocpp_hub_core.prototype.trial_session import TrialSession, save_private
from websockets.asyncio.client import connect

from tests.connected_rig import connected_bench, strategy_file
from tests.management_peer import TOKEN, call
from tests.prototype_rig import BOOT
from tests.runtime_rig import FAST, until
from tests.test_native_repair_storage import refused_write
from tests.test_ordinary_legacy_transport_recovery import legacy_fault
from tests.tls_support import tls as tls
from tests.trial_helpers import config_files
from tools import managed_proxy, trial_proxy
from tools.managed_reporting import ReportWriter


# Intent: old reports cannot replace a new process's status when the old disk writer is briefly stalled.
# Why: the existing process lock should allow replacement only once no previous writer can publish.
async def test_delayed_final_report_owns_shared_output_until_finished(tmp_path):
    journal = Journal(tmp_path / "state.db", "REPORT001")
    entered, release = threading.Event(), threading.Event()
    reports = []

    # Let the worker take its last snapshot, then delay that write until after the async shutdown timeout.
    def stalled(status):
        entered.set()
        if release.wait(5):
            reports.append(status)

    writer = ReportWriter(stalled, ownership=journal.retain_ownership())
    try:
        journal.close()
        final = {"running": False}
        await writer.close(final)
        assert entered.is_set() and writer.thread.is_alive()
        with pytest.raises(RecoveryRequiredError, match="active owner"):
            Journal(journal.path, journal.identity)
    finally:
        release.set()
        await until(lambda: not writer.thread.is_alive())
    assert reports == [final]
    with closing(Journal(journal.path, journal.identity)):
        pass


# A complete measured healthy status includes physical freshness and the service's local-Start restriction.
def healthy_status(now: float) -> dict[str, Any]:
    return {
        "checked_at": now - 1,
        "native_observed_at": now - 2,
        "state": "healthy",
        "running": True,
        "mode": "app_schedule",
        "local_start_disabled": True,
        "owner_control_enabled": True,
        "charger_connected": True,
        "cloud_connected": True,
        "boot_verified": True,
        "phase": "active",
        "preservation_failed": False,
        "native_limit_a": 6,
        "station_ceiling_a": 6,
        "certificate_remaining_seconds": 86400,
    }


# Intent: fresh measured health is accepted without constructing a socket or opening journal ownership.
# Why: independent service monitoring must remain a read-only file operation.
def test_health_is_fresh_and_dormant(tmp_path: Path, monkeypatch: Any) -> None:
    path = tmp_path / "status.json"
    save_private(path, healthy_status(10000))

    # A health command has no reason to resolve an endpoint or create a transport.
    def deny(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("health attempted networking")

    monkeypatch.setattr(socket, "socket", deny)
    monkeypatch.setattr(socket, "getaddrinfo", deny)
    result = managed_proxy.health(path, 10000)
    assert result["healthy"]
    assert result["status_age_seconds"] == 1 and result["native_age_seconds"] == 2
    assert sorted(p.name for p in tmp_path.iterdir()) == ["status.json"]


# Intent: old, future, faulted or contradictory reports cannot produce a successful independent health check.
# Why: a new status timestamp must not turn stale native data or unavailable owner control into readiness.
@pytest.mark.parametrize(
    "change",
    [
        {"checked_at": 9954},
        {"checked_at": 10001},
        {"native_observed_at": 9984},
        {"native_observed_at": 10001},
        {"running": False},
        {"state": "attention"},
        {"local_start_disabled": False},
        {"owner_control_enabled": False},
        {"charger_connected": False},
        {"cloud_connected": False},
        {"boot_verified": False},
        {"phase": "recovering"},
        {"preservation_failed": True},
        {"native_limit_a": 32},
        {"station_ceiling_a": 32},
        {"certificate_remaining_seconds": 1800},
        {"certificate_remaining_seconds": float("nan")},
        {"checked_at": "invalid"},
        {"native_observed_at": None},
    ],
)
def test_health_rejects_stale_or_incomplete_report(tmp_path: Path, change: dict[str, Any]) -> None:
    path = tmp_path / "status.json"
    save_private(path, healthy_status(10000) | change)
    assert not managed_proxy.health(path, 10000)["healthy"]


# Intent: health failures leave a nonzero exit status and never quote malformed private JSON/parser content.
# Why: a monitoring process must receive a useful classification without accidentally printing credential-like data.
def test_health_cli_classifies_private_parse_failure(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], capsys: Any
) -> None:
    config, path = config_files(tmp_path, 19876, 19877)
    status = config.journal.with_suffix(".status.json")
    status.write_text('{"checked_at":"SYNTHETIC_PRIVATE_TOKEN", invalid')
    status.chmod(0o600)
    assert managed_proxy.main(["health", "--config", str(path)]) == 2
    output = capsys.readouterr().out
    assert json.loads(output) == {"managed_action_failed": "SecurityError"}
    assert "SYNTHETIC_PRIVATE_TOKEN" not in output and not config.journal.exists()


# Intent: ordinary degraded health exits1 without opening a journal or attempting charger commands.
# Why: systemd/independent monitors need to distinguish an unhealthy report from a successful cached status read.
def test_health_cli_stale_report_returns_failure(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], capsys: Any
) -> None:
    config, path = config_files(tmp_path, 19876, 19877)
    save_private(config.journal.with_suffix(".status.json"), healthy_status(time.time() - 60))
    assert managed_proxy.main(["health", "--config", str(path)]) == 1
    assert not json.loads(capsys.readouterr().out)["healthy"]
    assert not config.journal.exists()


# Intent: the native-mode specification cannot be overwritten by a status, event, rotation or journal output.
# Why: corruption after the first valid parse could otherwise break the next automatic service restart.
@pytest.mark.parametrize(
    "output", ["journal", "status", "events", "rotation", "baseline", "original", "evidence", "wal"]
)
def test_managed_spec_rejects_output_aliases(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], output: str
) -> None:
    config, _ = config_files(tmp_path, 19876, 19877)
    paths = {
        "journal": config.journal,
        "status": config.journal.with_suffix(".status.json"),
        "events": config.journal.with_suffix(".events.jsonl"),
        "rotation": config.journal.with_suffix(".events.jsonl.1"),
        "baseline": config.baseline,
        "original": config.baseline.with_suffix(".original-config.json"),
        "evidence": config.evidence,
        "wal": Path(str(config.journal) + "-wal"),
    }
    with pytest.raises(SecurityError):
        managed_proxy.check_spec_path(config, paths[output])
    alias = tmp_path / "alias.json"
    alias.symlink_to(paths[output])
    with pytest.raises(SecurityError):
        managed_proxy.check_spec_path(config, alias)


# Intent: derived managed event files cannot alias existing input credentials or configuration.
# Why: event rotation is a write just as status replacement is, and must preserve all private inputs.
@pytest.mark.parametrize("rotation", [False, True])
def test_event_outputs_cannot_alias_config_inputs(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], rotation: bool
) -> None:
    config, _ = config_files(tmp_path, 19876, 19877)
    target = config.journal.with_suffix(".events.jsonl.1" if rotation else ".events.jsonl")
    with pytest.raises(SecurityError):
        replace(config, native_secret=target)


# Intent: bounded events rotate without following a replacement symlink into another private file.
# Why: persistent diagnostics must not grow indefinitely or overwrite a credential through a path alias.
def test_private_events_rotate_and_refuse_symlink_target(tmp_path: Path) -> None:
    path = tmp_path / "proxy.events.jsonl"
    previous = b"x" * (2 * 1024 * 1024 + 1)
    path.write_bytes(previous)
    path.chmod(0o600)
    managed_proxy.event(path, {"state": "healthy"})
    assert path.with_suffix(path.suffix + ".1").read_bytes() == previous
    assert json.loads(path.read_text()) == {"state": "healthy"}
    assert path.stat().st_mode & 0o077 == 0
    private = tmp_path / "private.json"
    private.write_bytes(b"preserved")
    link = tmp_path / "linked.events.jsonl"
    link.symlink_to(private)
    with pytest.raises(OSError):
        managed_proxy.event(link, {"state": "attention"})
    assert private.read_bytes() == b"preserved"


# Intent: managed check validates the exact listener, mode and loaded certificate without touching device state.
# Why: installing the service must not make its check action an implicit charger switch or Start.
async def test_managed_check_is_dormant(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: Any,
    capsys: Any,
) -> None:
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        value = rig.spec.value()
        value["trial_route"].update(sURL="/charger/", sDomain=rig.prepared.config.listener_name)
        path = tmp_path / "native-spec.json"
        save_private(path, value)
        requests = list(rig.peer.requests)

        # TLS parsing and certificate validation need no networking after the synthetic fixture is prepared.
        def deny(*args: Any, **kwargs: Any) -> None:
            raise AssertionError("managed preflight attempted networking")

        with monkeypatch.context() as patch:
            patch.setattr(socket, "socket", deny)
            patch.setattr(socket, "getaddrinfo", deny)
            assert (
                managed_proxy.main(
                    ["check", "--config", str(tmp_path / "trial.json"), "--native-bounded-spec", str(path)]
                )
                == 0
            )
        report = json.loads(capsys.readouterr().out)
        assert report["managed_mode"] == "app_schedule" and not report["network_contacted"]
        assert report["certificate_remaining_seconds"] > 1800
        assert rig.peer.requests == requests and not rig.prepared.config.journal.exists()
        assert not rig.bench.device_calls and not rig.bench.calls


# Intent: the configured charger and management ports cannot claim the same local listener.
# Why: a permanent configuration conflict must be reported before either service starts.
async def test_managed_check_and_serve_refuse_matching_loopback_ports(tmp_path, tls, unused_tcp_port, capsys):
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        value = rig.spec.value()
        value["trial_route"].update(sURL="/charger/", sDomain=rig.prepared.config.listener_name)
        config, spec = tmp_path / "trial.json", tmp_path / "native-spec.json"
        save_private(spec, value)
        management, token = tmp_path / "management.json", tmp_path / "management-token.json"
        save_private(token, {"token": TOKEN})
        assert rig.prepared.config.listener_host == "127.0.0.1"
        save_private(
            management, {"host": "127.0.0.1", "port": rig.prepared.config.listener_port, "token_file": str(token)}
        )

        assert (
            managed_proxy.main(
                [
                    "check",
                    "--config",
                    str(config),
                    "--native-bounded-spec",
                    str(spec),
                    "--management-config",
                    str(management),
                ]
            )
            == 2
        )
        assert json.loads(capsys.readouterr().out) == {"managed_action_failed": "SecurityError"}
        with pytest.raises(SecurityError, match="separate ports"):
            await managed_proxy.serve(config, spec, management)
        assert not rig.prepared.config.journal.exists() and not rig.bench.device_calls


# Intent: stopped inspection sees committed WAL data while leaving the original database and sidecars untouched.
# Why: a crash can leave the exact current evidence in the WAL, and inspection must not change that evidence.
async def test_operator_inspection_reads_a_stopped_wal_without_writing_it(tmp_path, tls, unused_tcp_port):
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        config = rig.prepared.config
        with closing(Journal(config.journal, config.identity)):
            with pytest.raises(RecoveryRequiredError, match="active owner"):
                with trial_proxy.operator_journal(config, inspect=True):
                    pytest.fail("inspection bypassed the original lock")
        original = config.journal
        with closing(sqlite3.connect(original, isolation_level=None)) as writer:
            writer.execute("PRAGMA wal_autocheckpoint=0")
            writer.execute("INSERT INTO meta VALUES('test_stopped_wal','true')")
            files = tuple(Path(str(original) + suffix) for suffix in ("", ".lock", "-wal", "-shm"))
            before = [path.read_bytes() for path in files]
            with trial_proxy.operator_journal(config, inspect=True) as snapshot:
                assert snapshot.get("test_stopped_wal") is True
                snapshot.put("test_stopped_wal", False)
            assert [path.read_bytes() for path in files] == before
            assert writer.execute("SELECT value FROM meta WHERE key='test_stopped_wal'").fetchone()[0] == "true"


# Intent: commissioning refuses a valid native strategy stored at its own status output path.
# Why: starting from that alias would replace the owner's spec with a status report.
async def test_commissioning_refuses_to_overwrite_its_native_spec(tmp_path, tls, unused_tcp_port):
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        strategy = strategy_file(rig, tmp_path)
        status = rig.prepared.config.journal.with_suffix(".status.json")
        save_private(status, json.loads(strategy.read_text()))
        before = status.read_bytes()
        with pytest.raises(SecurityError, match="must not alias"):
            await trial_proxy.run(tmp_path / "trial.json", 1, False, status)
        assert status.read_bytes() == before
        assert not rig.prepared.config.journal.exists() and not rig.bench.device_calls


# Intent: a saved managed journal can never reopen with assistant-local Start enabled by an ordinary constructor.
# Why: automatic restart must preserve the local-Start restriction even if the caller omits its explicit flag.
async def test_saved_managed_mode_requires_local_start_restriction(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        first = TrialSession(rig.prepared, FAST, native_bounded=rig.spec, allow_local_start=False)
        await first.start()
        ManagedController(first)
        await first.runtime.close()
        first.journal.close()
        first.closed = True
        with pytest.raises(SecurityError):
            TrialSession(rig.prepared, FAST, native_bounded=rig.spec)
        second = TrialSession(rig.prepared, FAST, native_bounded=rig.spec, allow_local_start=False)
        assert not second.gateway.policy.allow_local_start
        assert second.journal.get("managed_mode")["local_start"] is False
        await second.close()


# Intent: a managed restart on an already cleared rollback journal must refuse readiness before opening management.
# Why: commissioning deliberately leaves the charger listener closed in this phase while preserving the recovery data.
async def test_managed_runner_rejects_rollback_without_charger_listener(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    unused_tcp_port_factory: Any,
    monkeypatch: Any,
) -> None:
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        spec_path = strategy_file(rig, tmp_path)
        previous = rig.session()
        await previous.start()
        previous.journal.phase.begin_cleanup()
        await previous.close()
        rig.route_to_trial()

        token_path = tmp_path / "management-token.json"
        management_path = tmp_path / "management.json"
        save_private(token_path, {"token": TOKEN})
        save_private(
            management_path, {"host": "127.0.0.1", "port": unused_tcp_port_factory(), "token_file": str(token_path)}
        )
        notifications = []

        # Fail immediately if startup crosses the management listener boundary before detecting the missing charger.
        async def unexpected_management_start(_server: Any) -> None:
            raise AssertionError("management opened without a charger listener")

        monkeypatch.setattr(managed_proxy, "preflight", lambda path: rig.prepared)
        monkeypatch.setattr(managed_proxy, "notify", notifications.append)
        monkeypatch.setattr(managed_proxy.ManagementServer, "start", unexpected_management_start)
        with pytest.raises(RecoveryRequiredError, match="charger listener"):
            await managed_proxy.serve(tmp_path / "trial.json", spec_path, management_path)

        assert not any(value.startswith("READY=1") for value in notifications)
        assert not rig.bench.device_calls and not rig.bench.calls
        config = rig.prepared.config
        with closing(Journal(config.journal, config.identity)) as journal:
            assert journal.phase.read().value == "rollback"
            assert journal.get("managed_mode") is None


# Intent: a stop requested inside the actual positive-send native audit invalidates the release ticket.
# Why: cancelling only the polling coroutine would not stop shielded control work from raising the ceiling later.
async def test_stop_request_fences_inflight_release_before_positive_send(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        session = TrialSession(rig.prepared, FAST, native_bounded=rig.spec, allow_local_start=False)
        rig.sessions.append(session)
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await rig.bench.call(charger, "stop-race-boot", "BootNotification", BOOT)
            await until(lambda: session.gateway.controls.ready and session.gateway.vendor_registration == "Accepted")
            await until(lambda: session.journal.next_delivery() is None and not session.gateway.vendor._pending)
            observed = asyncio.Event()
            release_read = asyncio.Event()
            original = session.native_bounded.observe
            reads = 0

            # Delay a real native response during positive Set preparation, after its local ownership claim exists.
            async def delay_positive_read() -> Any:
                nonlocal reads
                reads += 1
                native = await original()
                if reads == 3:
                    observed.set()
                    await release_read.wait()
                return native

            session.native_bounded.observe = delay_positive_read
            pending = asyncio.create_task(controller.recovery.restore())
            try:
                await asyncio.wait_for(observed.wait(), 5)
                controller.request_stop()
                release_read.set()
                with pytest.raises((DeniedError, UnavailableError, SecurityError)):
                    await pending
                assert controller.stopping and session.journal.get("control")["inhibit"]
                assert not session.journal.get("managed_control_fault")
                assert rig.bench.received_limits and set(rig.bench.received_limits) == {0}
                assert not [frame for frame in rig.bench.device_calls if frame[2] == "RemoteStartTransaction"]
                assert (await controller.tick())["state"] == "stopping"
            finally:
                release_read.set()
                if not pending.done():
                    pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)


# Intent: automatic release remains at zero if its registered cloud connection ends during native send validation.
# Why: the cloud accepted before starting the release does not establish authority to send a later positive profile.
async def test_automatic_release_waits_at_zero_after_cloud_loss(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int, monkeypatch: Any
) -> None:
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        session = TrialSession(rig.prepared, FAST, native_bounded=rig.spec, allow_local_start=False)
        rig.sessions.append(session)
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            assert (await rig.bench.call(charger, "cloud-loss-boot", "BootNotification", BOOT))[0] == 3
            g, j = session.gateway, session.journal
            await until(lambda: g.controls.ready and g.vendor_registration == "Accepted")
            await until(lambda: j.next_delivery() is None and not g.vendor._pending)
            scope = controller.requests.intent()
            assert scope.mode == "app" and g.controls.desired() == rig.bench.firmware.effective_limit() == 0
            vendor_epoch = g._vendor_epoch
            positive_audit, release_audit = asyncio.Event(), asyncio.Event()
            observe = session.native_bounded.observe

            # Hold only a genuine, completed native sample while the release owns the positive Set.
            async def delay_positive_audit() -> Any:
                native = await observe()
                attempt = g.controls.release_attempt
                if attempt is not None and attempt.ticket is not None and not positive_audit.is_set():
                    assert attempt.purpose == "automatic" and not attempt.positive_may_have_dispatched
                    positive_audit.set()
                    await release_audit.wait()
                return native

            monkeypatch.setattr(session.native_bounded, "observe", delay_positive_audit)
            pending = asyncio.create_task(controller.recovery.restore())
            try:
                await asyncio.wait_for(positive_audit.wait(), 5)
                assert g.vendor_registration == "Accepted" and g._vendor_epoch == vendor_epoch
                await session.runtime.stop_vendor()
                assert g._vendor_epoch != vendor_epoch and not session.runtime.status.vendor_connected
                release_audit.set()
                progress = await asyncio.wait_for(pending, 5)
                assert progress.state is RestoreState.WAITING and progress.pending.reason is Prerequisite.CLOUD
                assert progress.operation is not None and progress.operation.effects.zero_verified
                assert not progress.operation.effects.positive_dispatched
                assert g.controls.ready and rig.bench.firmware.effective_limit() == 0
                assert set(rig.bench.received_limits) == {0}
                assert controller.requests.intent() == scope and controller.released_epoch is None
                assert controller.requests.pending_return() is None and not j.fault.latched()
            finally:
                release_audit.set()
                await asyncio.gather(pending, return_exceptions=True)


# Intent: the actual managed runner handles a stop signal by retaining zero and writing a stopped report.
# Why: systemd shutdown must retire both connections and the journal without clearing profiles or restoring32.
@pytest.mark.parametrize(
    "management_enabled,failed_output,repair,management_bind_busy",
    [
        (False, None, False, False),
        (True, None, False, False),
        (True, "status", False, False),
        (True, "events", False, False),
        (True, "writer", False, False),
        (True, None, True, False),
        pytest.param(False, "cloud_worker", False, False, id="unexpected-cloud-worker-exit"),
        pytest.param(True, None, False, True, id="occupied-management-port"),
    ],
)
async def test_managed_runner_signal_shutdown_retains_zero_and_records_stop(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: Any,
    unused_tcp_port_factory: Any,
    management_enabled: bool,
    failed_output: str | None,
    repair: bool,
    management_bind_busy: bool,
) -> None:
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        value = rig.spec.value()
        value["trial_route"].update(sURL="/charger/", sDomain=rig.prepared.config.listener_name)
        rig.spec = NativeBoundedSpec.parse(value)
        spec_path = tmp_path / "native-spec.json"
        save_private(spec_path, value)
        sessions = []
        controllers = []
        callbacks: dict[Any, Any] = {}
        removed = []
        notifications = []
        loop = asyncio.get_running_loop()

        # Inject only prepared synthetic transport addresses; the session and both TLS protocol legs remain real.
        def create_session(prepared: Any, **kwargs: Any) -> TrialSession:
            session = TrialSession(prepared, FAST, **kwargs)
            sessions.append(session)
            rig.sessions.append(session)
            return session

        # Observe the runner's real controller without substituting its work or its poll cadence.
        def create_controller(session: TrialSession) -> ManagedController:
            controller = ManagedController(session)
            controllers.append(controller)
            return controller

        # Signal registration is the outer runner boundary, separate from the real engine under test.
        def register(signum: Any, callback: Any) -> None:
            callbacks[signum] = callback

        # Confirm that the runner releases both installed handlers when shutdown finishes.
        def remove(signum: Any) -> bool:
            removed.append(signum)
            return True

        monkeypatch.setattr(managed_proxy, "preflight", lambda path: rig.prepared)
        monkeypatch.setattr(managed_proxy, "TrialSession", create_session)
        monkeypatch.setattr(managed_proxy, "ManagedController", create_controller)
        monkeypatch.setattr(managed_proxy, "notify", notifications.append)
        monkeypatch.setattr(loop, "add_signal_handler", register)
        monkeypatch.setattr(loop, "remove_signal_handler", remove)
        management_path = None
        management_blocker = None
        if management_enabled:
            port = unused_tcp_port_factory()
            secret = tmp_path / "management-token.json"
            management_path = tmp_path / "management.json"
            save_private(secret, {"token": TOKEN})
            save_private(management_path, {"host": "127.0.0.1", "port": port, "token_file": str(secret)})
            if management_bind_busy:
                management_blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                management_blocker.bind(("127.0.0.1", port))
                management_blocker.listen()
        blocked = None
        if failed_output == "writer":
            # Optional output can lose its thread before the service starts.
            def refuse_writer(*args: Any, **kwargs: Any) -> Any:
                raise RuntimeError("synthetic output-worker creation refusal")

            monkeypatch.setattr(managed_proxy, "ReportWriter", refuse_writer)
        elif failed_output is not None:
            blocked = rig.prepared.config.journal.with_suffix(
                ".status.json" if failed_output == "status" else ".events.jsonl"
            )
            blocked.mkdir()
        running = asyncio.create_task(managed_proxy.serve(tmp_path / "trial.json", spec_path, management_path))
        try:
            await until(lambda: bool(sessions) and sessions[0].started)
            session = sessions[0]
            rig.route_to_trial()
            async with rig.charger(session) as charger:
                await rig.bench.call(charger, "managed-signal-boot", "BootNotification", BOOT)
                await until(
                    lambda: session.gateway.controls.ready and session.gateway.vendor_registration == "Accepted"
                )
                if management_blocker is not None:
                    status_path = rig.prepared.config.journal.with_suffix(".status.json")

                    # The service itself must keep polling the same charger owner while the socket is occupied.
                    def reports_management(available: bool) -> bool:
                        assert not running.done()
                        if not status_path.exists():
                            return False
                        status = json.loads(status_path.read_text())
                        return status["state"] == "healthy" and status.get("management_available") is available

                    await until(lambda: reports_management(False), timeout=12)
                    boot = session.gateway.boot_observation
                    assert rig.bench.firmware.effective_limit() == 6
                    management_blocker.close()
                    await until(lambda: reports_management(True), timeout=12)
                    assert sessions == [session] and session.gateway.boot_observation == boot
                if management_enabled:
                    async with connect(
                        f"ws://127.0.0.1:{port}/management",
                        additional_headers={"Authorization": "Bearer " + TOKEN},
                        subprotocols=["ics-proxy.v1"],
                        proxy=None,
                    ) as client:
                        status = (await call(client, "status"))["result"]
                        assert status["api_version"] == 1 and status["can_pause"] is True
                        paused = await call(client, "pause", revision=status["control_revision"])
                        assert paused["ok"] and paused["result"]["state"] == "accepted"
                        async with asyncio.timeout(12):
                            while True:
                                receipt = (await call(client, "receipt", request_id=paused["result"]["id"]))["result"]
                                if receipt["state"] == "completed":
                                    break
                                await asyncio.sleep(0.1)
                        assert rig.bench.firmware.effective_limit() == 0
                        if repair:
                            controller, j = controllers[0], session.journal
                            b = SimpleNamespace(c=controller, rig=rig, session=session)
                            fault = await legacy_fault(b, "NativeTransportUnavailableError")
                            before = dict(rig.peer.configuration)
                            with refused_write(j, REVOCATIONS, "statement"):
                                rig.peer.configuration["untouched"] = "independent native contradiction"
                                paused = await call(client, "pause", revision=controller.requests.intent().revision)
                                assert paused["ok"]
                                async with asyncio.timeout(12):
                                    while True:
                                        assert not running.done()
                                        status = (await call(client, "status"))["result"]
                                        if status.get("fault") == "native_repair_publication_pending":
                                            break
                                        await asyncio.sleep(0.05)
                                assert repair_pending(j, fault) and not status["can_return"]
                                rig.peer.configuration = before
                            await until(lambda: repair_revoked(j, fault), timeout=10)
                            assert not running.done() and sessions == [session] and not repair_pending(j, fault)
                            assert rig.bench.firmware.effective_limit() == 0
                        if failed_output is not None:
                            before = (await call(client, "status"))["result"]
                            async with asyncio.timeout(12):
                                while True:
                                    latest = (await call(client, "status"))["result"]
                                    if latest["checked_at"] > before["checked_at"]:
                                        break
                                    await asyncio.sleep(0.1)
                            assert latest["diagnostics"]["reporting_errors"] > 0
                            assert latest["healthy"] and latest["local_hold_verified"]
                            assert not running.done() and rig.bench.firmware.effective_limit() == 0
                if failed_output == "cloud_worker":
                    assert session.runtime._runner is not None and not session.runtime._runner.done()
                    session.runtime._runner.cancel()
                    with pytest.raises(RuntimeError, match="cloud worker stopped unexpectedly"):
                        await asyncio.wait_for(running, 8)
                else:
                    callbacks[signal.SIGTERM]()
                    assert await asyncio.wait_for(running, 8) == 0
            if failed_output == "status":
                assert blocked.is_dir()
            elif failed_output == "writer":
                assert not rig.prepared.config.journal.with_suffix(".status.json").exists()
            else:
                report = json.loads(rig.prepared.config.journal.with_suffix(".status.json").read_text())
                assert report["running"] is False and report["state"] == "stopped"
                assert report["shutdown_uncertain"] is False
            assert set(removed) == {signal.SIGTERM, signal.SIGINT}
            assert "WATCHDOG=1" in notifications and any(n.startswith("STOPPING=1") for n in notifications)
            assert session.closed and rig.bench.firmware.effective_limit() == 0
            assert rig.peer.runtime_user == 60 and rig.peer.configuration == rig.spec.configuration("trial")
            assert not [
                frame
                for frame in rig.bench.device_calls
                if frame[2] in ("RemoteStartTransaction", "ClearChargingProfile")
            ]
        finally:
            if management_blocker is not None:
                management_blocker.close()
            if not running.done():
                if signal.SIGTERM in callbacks:
                    callbacks[signal.SIGTERM]()
                running.cancel()
                await asyncio.gather(running, return_exceptions=True)
