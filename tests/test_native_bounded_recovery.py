"""Independent native HTTP and actual TLS exercise bounded recovery acknowledgement and real Stop replay."""

from __future__ import annotations

import copy
import ssl
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError
from ocpp_hub_core.prototype.trial_session import save_private

from tests.test_native_bounded_session import ConnectedBench, connected_bench
from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tools import trial_proxy


# Terminate transport and journal ownership without the normal shutdown Stop, preserving real durable facts.
async def crash(session: Any) -> None:
    await session.runtime.close()
    session.journal.close()
    session.closed = True


# Replace one introduced controller while the native peer, TLS backend and journal survive independently.
@asynccontextmanager
async def recovered(
    directory: Path, contexts: tuple[ssl.SSLContext, ssl.SSLContext, Path], port: int, before_boot: bool = False
) -> AsyncIterator[tuple[ConnectedBench, Any, Any]]:
    async with connected_bench(directory, contexts, port) as rig:
        first = rig.session()
        await first.start()
        rig.route_to_trial()
        if not before_boot:
            async with rig.charger(first) as charger:
                await rig.bench.call(charger, "first-boot", "BootNotification", BOOT)
                await until(lambda: first.gateway.controls.ready and first.gateway.vendor_registration == "Accepted")
                await crash(first)
        else:
            await crash(first)
        second = rig.session()
        await second.start()
        async with rig.charger(second) as charger:
            await rig.bench.call(charger, "second-boot", "BootNotification", BOOT)
            await until(lambda: second.gateway.controls.ready and second.gateway.vendor_registration == "Accepted")
            await until(lambda: second.journal.next_delivery() is None and not second.gateway.vendor._pending)
            yield rig, second, charger


# Intent: recovery acknowledgement succeeds after either an introduced restart or a crash before the first Boot.
# Why: rearming controls is a separate explicit operation that must leave the independently confirmed zero hold intact.
@pytest.mark.parametrize("before_boot", [False, True])
async def test_native_resume_retains_zero_and_requires_separate_release(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int, before_boot: bool
) -> None:
    async with recovered(tmp_path, tls, unused_tcp_port, before_boot) as (rig, session, _):
        assert session.journal.get("trial_phase") == "recovering"
        for command in ("start", "release"):
            with pytest.raises((RecoveryRequiredError, SecurityError, ValueError)):
                await session.command({"command": command})
        count = len(rig.bench.device_calls)
        result = await session.command({"command": "native-bounded-resume"})
        assert result == {"recovery_acknowledged": True, "zero_hold_retained": True, "charging_resumed": False}
        assert len(rig.bench.device_calls) == count
        assert session.journal.get("trial_phase") == "active"
        assert session.gateway.controls.ready and session.gateway.controls.desired() == 0
        assert rig.bench.firmware.effective_limit() == 0 and rig.peer.runtime_user == 60
        assert session.journal.active() is None
        await session.command({"command": "release"})
        assert session.gateway.controls.ready and rig.bench.firmware.effective_limit() == 6


# Intent: stale limits, altered resources, pending work and retired introductions cannot acknowledge recovery.
# Why: clearing the software uncertainty must never substitute for current native protection and actual history ACKs.
@pytest.mark.parametrize(
    "fault",
    ["runtime", "saved", "phase", "plan", "unknown-tx", "missing-profile", "receipt", "grant", "boot", "stage"],
)
async def test_native_resume_rejects_incomplete_or_changed_protection(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int, fault: str
) -> None:
    async with recovered(tmp_path, tls, unused_tcp_port) as (rig, session, _):
        if fault == "runtime":
            rig.peer.runtime_user = 320
        elif fault == "saved":
            rig.peer.configuration["AmpLmt"] = "32"
        elif fault == "phase":
            rig.peer.phase_fault = True
        elif fault == "plan":
            rig.bench.firmware.plans[9]["limitCurrent"] = 6
        elif fault == "unknown-tx":
            rig.bench.firmware.active = 777777
        elif fault == "missing-profile":
            del rig.bench.firmware.profiles[7001]
        elif fault == "receipt":
            session.journal.db.execute("UPDATE trial_resources SET state='uncertain'")
        elif fault == "grant":
            session.journal.put("vendor_start_grant", {"local": None, "token": "synthetic-unresolved"})
        elif fault == "boot":
            session.journal.put("boot", BOOT | {"firmwareVersion": "different"})
        else:
            session.native_bounded.save("closing")
        count = len(rig.bench.device_calls)
        with pytest.raises((RecoveryRequiredError, SecurityError, UnavailableError)):
            await session.command({"command": "native-bounded-resume"})
        assert session.journal.get("trial_phase") != "active"
        assert len(rig.bench.device_calls) == count


# Intent: an undelivered real device observation prevents acknowledgement until the backend has acknowledged it.
# Why: recovery cannot delete or silently bypass queued cloud history just to make local controls available.
async def test_native_resume_retains_real_pending_outbox(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    async with recovered(tmp_path, tls, unused_tcp_port) as (rig, session, charger):
        await session.runtime.stop_vendor()
        await rig.bench.call(
            charger,
            "offline-status",
            "StatusNotification",
            {"connectorId": 1, "errorCode": "NoError", "status": "Preparing"},
        )
        queued = [tuple(row) for row in session.journal.db.execute("SELECT * FROM outbox")]
        assert queued
        with pytest.raises(RecoveryRequiredError):
            await session.command({"command": "native-bounded-resume"})
        assert queued == [tuple(row) for row in session.journal.db.execute("SELECT * FROM outbox")]
        assert session.journal.get("trial_phase") == "recovering"


# Intent: a new local generation during the native read makes an otherwise complete zero observation obsolete.
# Why: a delayed acknowledgement must never reactivate controls belonging to a retired charger introduction.
async def test_native_resume_discards_read_overtaken_by_revision(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int
) -> None:
    async with recovered(tmp_path, tls, unused_tcp_port) as (_, session, _):
        original = session.native_bounded.observe

        # Return a real native response only after the current controller generation was deliberately invalidated.
        async def overtaken() -> Any:
            native = await original()
            session.gateway.controls.invalidate()
            return native

        session.native_bounded.observe = overtaken
        with pytest.raises(UnavailableError):
            await session.command({"command": "native-bounded-resume"})
        assert session.journal.get("trial_phase") == "recovering"
        assert session.journal.get("control")["inhibit"]


# Intent: a restarted known open transaction gets zero then its actual Stop, and remains debt until its real replay.
# Why: Accepted RemoteStop alone cannot be promoted into native closure or acknowledged history.
@pytest.mark.parametrize("finished_while_absent", [False, True])
async def test_native_restart_reconciles_real_known_stop(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int, finished_while_absent: bool
) -> None:
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        first = rig.session()
        await first.start()
        rig.route_to_trial()
        async with rig.charger(first) as charger:
            await rig.bench.call(charger, "known-first-boot", "BootNotification", BOOT)
            await until(lambda: first.gateway.controls.ready and first.gateway.vendor_registration == "Accepted")
            await first.command({"command": "release"})
            started = await rig.bench.call(charger, "genuine-start", "StartTransaction", START)
            known = started[2]["transactionId"]
            await until(lambda: first.journal.vendor_id(known) is not None and first.journal.next_delivery() is None)
            await crash(first)
        stop = {"transactionId": known, "timestamp": START["timestamp"], "meterStop": 1001}
        if finished_while_absent:
            rig.bench.firmware.active = None
            rig.bench.history[-1]["stop"] = copy.deepcopy(stop)
        second = rig.session()
        await second.start()
        checkpoint = len(rig.bench.device_calls)
        async with rig.charger(second) as charger:
            await rig.bench.call(charger, "known-second-boot", "BootNotification", BOOT)
            await until(lambda: second.gateway.controls.ready and second.gateway.vendor_registration == "Accepted")
            stops = [frame for frame in rig.bench.device_calls[checkpoint:] if frame[2] == "RemoteStopTransaction"]
            assert bool(stops) is not finished_while_absent
            if stops:
                assert stops[0][3] == {"transactionId": known}
                frames = rig.bench.device_calls[checkpoint:]
                position = next(i for i, frame in enumerate(frames) if frame[2] == "RemoteStopTransaction")
                assert any(frame[2] == "SetChargingProfile" for frame in frames[:position])
            assert second.journal.active() == known
            with pytest.raises((RecoveryRequiredError, SecurityError)):
                await second.command({"command": "native-bounded-resume"})
            assert (await rig.bench.call(charger, "actual-stop-replay", "StopTransaction", stop))[0] == 3
            await until(lambda: second.journal.active() is None and second.journal.next_delivery() is None)
            await until(lambda: not second.gateway.vendor._pending)
            assert (await second.command({"command": "native-bounded-resume"}))["zero_hold_retained"]
            assert second.journal.get("trial_phase") == "active" and second.gateway.controls.desired() == 0
            assert len([row for row in rig.bench.calls if row[2] == "StartTransaction"]) == 1
            assert len([row for row in rig.bench.calls if row[2] == "StopTransaction"]) == 1


# Intent: the repeatable native-mode CLI validates its private strategy without creating a journal or contacting peers.
# Why: selecting the connected strategy must be explicit and its offline check must remain genuinely dormant.
async def test_native_cli_check_is_dormant(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int, capsys: Any
) -> None:
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        value = rig.spec.value()
        value["trial_route"]["sURL"] = "/charger/"
        value["trial_route"]["sDomain"] = rig.prepared.config.listener_name
        path = tmp_path / "native-spec.json"
        save_private(path, value)
        requests = list(rig.peer.requests)
        assert (
            trial_proxy.main(["check", "--config", str(tmp_path / "trial.json"), "--native-bounded-spec", str(path)])
            == 0
        )
        assert '"native_bounded_strategy_configured": true' in capsys.readouterr().out
        assert requests == rig.peer.requests and rig.bench.calls == [] and rig.bench.device_calls == []
        assert not rig.prepared.config.journal.exists()


# Intent: an otherwise valid private native strategy cannot select a different endpoint or baseline through the CLI.
# Why: the actual listener, charger identity and original restoration configuration must describe the same trial.
@pytest.mark.parametrize("fault", ["domain", "port", "path", "identity", "original", "hardware"])
async def test_native_cli_rejects_mismatched_strategy(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], unused_tcp_port: int, fault: str
) -> None:
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        value = rig.spec.value()
        value["trial_route"]["sURL"] = "/charger/"
        value["trial_route"]["sDomain"] = rig.prepared.config.listener_name
        if fault == "domain":
            value["trial_route"]["sDomain"] = "different.invalid"
        elif fault == "port":
            value["trial_route"]["sPort"] = str(unused_tcp_port + 1 if unused_tcp_port < 65535 else 65534)
        elif fault == "path":
            value["trial_route"]["sURL"] = "/different/"
        elif fault == "identity":
            value["original_configuration"]["Id"] = "OTHER"
        elif fault == "hardware":
            value["hardware_identity"] = value["original_configuration"]["bPSN"] = "OTHER"
        else:
            value["original_configuration"]["untouched"] = "changed"
        path = tmp_path / "native-spec.json"
        save_private(path, value)
        with pytest.raises(SecurityError):
            trial_proxy.native_spec(path, rig.prepared)
        assert not rig.prepared.config.journal.exists() and rig.bench.device_calls == []
