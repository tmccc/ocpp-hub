"""Dormant managed CLI, private health, durable mode and actual TLS shutdown boundaries."""

from __future__ import annotations

import asyncio
import json
import signal
import socket
import ssl
import time
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.native_bounded import NativeBoundedSpec
from ocpp_hub_core.prototype.policy import DeniedError
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.trial_session import TrialSession, save_private

from tests.handover_helpers import crash
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_native_bounded_session import connected_bench
from tests.test_prototype import BOOT
from tests.test_prototype_runtime import FAST, until
from tests.test_prototype_tls import tls as tls
from tests.trial_helpers import config_files
from tools import managed_proxy


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
            pending = asyncio.create_task(controller.restore_owner_control())
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


# Intent: the actual managed runner handles a stop signal by retaining zero and writing a stopped report.
# Why: systemd shutdown must retire both connections and the journal without clearing profiles or restoring32.
async def test_managed_runner_signal_shutdown_retains_zero_and_records_stop(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int, monkeypatch: Any
) -> None:
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        value = rig.spec.value()
        value["trial_route"].update(sURL="/charger/", sDomain=rig.prepared.config.listener_name)
        rig.spec = NativeBoundedSpec.parse(value)
        spec_path = tmp_path / "native-spec.json"
        save_private(spec_path, value)
        sessions = []
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

        # Signal registration is the outer runner boundary, separate from the real engine under test.
        def register(signum: Any, callback: Any) -> None:
            callbacks[signum] = callback

        # Confirm that the runner releases both installed handlers when shutdown finishes.
        def remove(signum: Any) -> bool:
            removed.append(signum)
            return True

        monkeypatch.setattr(managed_proxy, "preflight", lambda path: rig.prepared)
        monkeypatch.setattr(managed_proxy, "TrialSession", create_session)
        monkeypatch.setattr(managed_proxy, "notify", notifications.append)
        monkeypatch.setattr(loop, "add_signal_handler", register)
        monkeypatch.setattr(loop, "remove_signal_handler", remove)
        running = asyncio.create_task(managed_proxy.serve(tmp_path / "trial.json", spec_path))
        try:
            await until(lambda: bool(sessions) and sessions[0].started)
            session = sessions[0]
            rig.route_to_trial()
            async with rig.charger(session) as charger:
                await rig.bench.call(charger, "managed-signal-boot", "BootNotification", BOOT)
                await until(
                    lambda: session.gateway.controls.ready and session.gateway.vendor_registration == "Accepted"
                )
                callbacks[signal.SIGTERM]()
                assert await asyncio.wait_for(running, 8) == 0
            report = json.loads(rig.prepared.config.journal.with_suffix(".status.json").read_text())
            assert report["running"] is False and report["state"] == "stopped"
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
            if not running.done():
                if signal.SIGTERM in callbacks:
                    callbacks[signal.SIGTERM]()
                running.cancel()
                await asyncio.gather(running, return_exceptions=True)


# Intent: the production service's independent deadline task protects a full open car while its main poll is stalled.
# Why: invoking the controller in a test cannot detect a missing, prematurely cancelled, or unwired service task.
async def test_running_service_enforces_timer_with_stalled_main_poll(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        await crash(b.session)
        callbacks = {}
        sessions = []
        loop = asyncio.get_running_loop()
        rig = b.rig

        # Keep the pre-commissioned synthetic installation, both protocol peers and the real service loop.
        def simulated_session(prepared, **kwargs):
            session = TrialSession(prepared, FAST, **kwargs)
            sessions.append(session)
            rig.sessions.append(session)
            return session

        monkeypatch.setattr(managed_proxy, "preflight", lambda _: rig.prepared)
        monkeypatch.setattr(managed_proxy, "native_spec", lambda *_: rig.spec)
        monkeypatch.setattr(managed_proxy, "TrialSession", simulated_session)
        monkeypatch.setattr(managed_proxy, "notify", lambda _: None)
        monkeypatch.setattr(loop, "add_signal_handler", lambda sig, callback: callbacks.update({sig: callback}))
        monkeypatch.setattr(loop, "remove_signal_handler", lambda _: True)
        spec_path = tmp_path / "native-spec.json"
        save_private(spec_path, rig.spec.value())
        status_path = rig.prepared.config.journal.with_suffix(".status.json")
        running = asyncio.create_task(managed_proxy.serve(tmp_path / "service.json", spec_path))
        entered, release = asyncio.Event(), asyncio.Event()

        # Observe only the health report published by the actual process loop.
        async def wait_healthy(open_session):
            async with asyncio.timeout(22):
                while True:
                    if status_path.exists():
                        status = json.loads(status_path.read_text())
                        if status.get("state") == "healthy" and status.get("charging_session_open") is open_session:
                            return status
                    await asyncio.sleep(0.05)

        try:
            await until(lambda: bool(sessions) and sessions[0].started)
            b.session = sessions[0]
            async with rig.charger(b.session) as b.sock:
                assert (await rig.bench.call(b.sock, "running-service-boot", "BootNotification", BOOT))[0] == 3
                assert (await wait_healthy(False))["station_ceiling_a"] == 6
                b.clock.now = b.start
                local = await b.fire(b.timer, demand=False)
                assert (await wait_healthy(True))["power_w"] == 0
                read = ManagedController.settled_native

                # Suspend the foreground poll at its native observation. Only the service's deadline task can progress.
                async def stalled(controller):
                    entered.set()
                    await release.wait()
                    return await read(controller)

                monkeypatch.setattr(ManagedController, "settled_native", stalled)
                await asyncio.wait_for(entered.wait(), 8)
                b.clock.now = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
                async with asyncio.timeout(35):
                    while not b.stops():
                        await asyncio.sleep(0.03)
                assert b.stops() == [local] and b.device.effective_limit() == 0
                assert b.session.journal.active() == local and b.timer in b.device.plans
                release.set()
                await b.close_transaction(local)
                assert (await wait_healthy(False))["station_ceiling_a"] == 6 and set(b.device.plans) == {9}
                daily = await b.advance_idle_to(int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp()))
                assert daily != local and (await wait_healthy(True))["power_w"] > 0 and b.stops() == [local]
                await b.close_transaction(daily)
                callbacks[signal.SIGTERM]()
                assert await asyncio.wait_for(running, 8) == 0
        finally:
            release.set()
            if not running.done():
                if signal.SIGTERM in callbacks:
                    callbacks[signal.SIGTERM]()
                await asyncio.wait_for(running, 10)
