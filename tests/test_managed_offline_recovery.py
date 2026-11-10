"""Ordinary managed process replacement recovers local zero authority through real TLS and native HTTP peers."""

from __future__ import annotations

import asyncio
import json
import socket
import ssl
import uuid
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests.test_managed_current import adjustable, locally_ready, select
from tests.test_managed_proxy import OwnerBench, healthy, managed_session, starts
from tests.test_managed_test_charge import ChargeRig
from tests.test_native_bounded_recovery import crash
from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


@dataclass
class OfflineRecovery:
    """The replacement has a real refused vendor endpoint while its local native and charger peers remain alive."""

    test: ChargeRig
    original_vendor_url: str
    prior_mode: str
    prior_revision: int
    prior_transaction_count: int
    first_recovery_profile: int


# Real previous Start/Stop facts at zero watts create historical delivery debt without ever drawing test current.
async def prior_lifecycle_debt(test: ChargeRig) -> None:
    await test.session.runtime.stop_vendor()
    answer = await test.rig.bench.call(test.charger, "prior-native-authorize", "Authorize", {"idTag": "NATIVE_TIMER"})
    assert answer[2]["idTagInfo"]["status"] == "Accepted"
    reply = await test.rig.bench.call(
        test.charger, "prior-native-start", "StartTransaction", START | {"idTag": "NATIVE_TIMER"}
    )
    local = reply[2]["transactionId"]
    assert reply[2]["idTagInfo"]["status"] == "Accepted"
    await test.rig.bench.call(
        test.charger,
        "prior-native-stop",
        "StopTransaction",
        {"transactionId": local, "timestamp": START["timestamp"], "meterStop": START["meterStart"]},
    )
    assert test.session.journal.active() is None and test.rig.bench.firmware.current == 0


# Replace the ordinary real session; only its vendor destination is changed to a reserved non-listening loopback port.
@asynccontextmanager
async def replacement_offline(
    directory: Path,
    contexts: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    port: int,
    monkeypatch: pytest.MonkeyPatch,
    *,
    initial: str = "app",
    selected: int = 16,
    debt: str | None = None,
    refuse_vendor: bool = True,
) -> AsyncIterator[OfflineRecovery]:
    async with adjustable(directory, contexts, port, monkeypatch) as first:
        await select(first, selected)
        await healthy(first.controller)
        if initial == "paused":
            first.controller.command(uuid.uuid4().hex, "pause", first.controller.requests.intent().revision)
            await healthy(first.controller)
        await until(lambda: first.session.journal.next_delivery() is None and not first.session.gateway.vendor._pending)
        if debt == "lifecycle":
            await prior_lifecycle_debt(first)
        elif debt == "mutation":
            first.session.journal.mutation_intent(
                "vendor", "ChangeConfiguration", {"key": "HeartbeatInterval", "value": "30"}
            )
        previous = first.controller.requests.intent()
        transactions = first.session.journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        await first.controller.current.close()
        await first.controller.tests.close()
        profile_index = len(first.rig.bench.received_limits)
        await crash(first.session)

        with socket.socket() as unavailable:
            unavailable.bind(("127.0.0.1", 0))
            replacement = managed_session(first.rig)
            transport = replacement.runtime.transport
            vendor = transport.config.vendor_url
            url = urlsplit(vendor)
            if refuse_vendor:
                transport.config = replace(
                    transport.config,
                    vendor_url=urlunsplit(
                        (
                            url.scheme,
                            "127.0.0.1:" + str(unavailable.getsockname()[1]),
                            url.path,
                            url.query,
                            url.fragment,
                        )
                    ),
                )
            await replacement.start()
            controller = ManagedController(replacement)
            try:
                async with first.rig.charger(replacement) as charger:
                    yield OfflineRecovery(
                        ChargeRig(first.rig, replacement, controller, charger),
                        vendor,
                        previous.mode,
                        previous.revision,
                        transactions,
                        profile_index,
                    )
            finally:
                controller.request_stop()
                await controller.current.close()
                await controller.tests.close()
                if controller.pause_task is not None:
                    await asyncio.gather(controller.pause_task, return_exceptions=True)
                await replacement.close()


# A genuine new charger Boot must complete its native introduction while the vendor connection really fails.
async def local_boot(recovery: OfflineRecovery, token: str = "offline-replacement-boot") -> None:
    test = recovery.test
    answer = await test.rig.bench.call(test.charger, token, "BootNotification", BOOT)
    assert answer[0] == 3
    await until(
        lambda: (
            test.session.gateway.controls.ready
            and test.controller.native.introduced_epoch == test.session.gateway._charger_epoch
        )
    )
    await until(lambda: test.session.runtime.status.vendor_failed_charger_epoch == test.session.gateway._charger_epoch)
    assert not test.session.runtime.status.vendor_connected
    assert test.session.journal.get("trial_phase") == "recovering"


# Queue genuine nonmutating observations through the independent device's actual TLS connection.
async def observations(test: ChargeRig) -> None:
    await test.rig.bench.call(
        test.charger,
        "offline-real-status",
        "StatusNotification",
        {"connectorId": 1, "errorCode": "NoError", "status": "Preparing"},
    )
    await test.rig.bench.call(
        test.charger,
        "offline-real-meter",
        "MeterValues",
        {
            "connectorId": 1,
            "meterValue": [
                {"timestamp": START["timestamp"], "sampledValue": [{"measurand": "Power.Active.Import", "value": "0"}]}
            ],
        },
    )


# Intent: idle app and paused services recover fresh local zero authority with selected16/32 and never issue Start.
# Why: an unavailable vendor must not strand a safe local receiver or automatically raise its retained current.
@pytest.mark.parametrize("initial", ["app", "paused"])
@pytest.mark.parametrize("selected", [16, 32])
async def test_offline_replacement_recovers_verified_pause_without_start_or_new_transaction(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    initial: str,
    selected: int,
) -> None:
    async with replacement_offline(
        tmp_path, tls, unused_tcp_port, monkeypatch, initial=initial, selected=selected
    ) as recovery:
        test = recovery.test
        assert (await test.controller.tick())["state"] == "starting"
        assert test.session.journal.get("trial_phase") == "recovering"
        await local_boot(recovery)
        status = await locally_ready(test.controller)
        assert status["phase"] == "active" and status["local_hold_verified"]
        assert status["desired_control"] == "paused" and not status["cloud_connected"]
        assert status["requested_current_a"] == selected and status["native_limit_a"] == 32
        assert status["normal_start_ready"] and status["power_w"] == status["station_ceiling_a"] == 0
        assert test.controller.requests.intent().revision == recovery.prior_revision + (initial == "app")
        assert all(limit == 0 for limit in test.rig.bench.received_limits[recovery.first_recovery_profile :])
        assert not starts(test.rig)
        assert test.session.journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0


# Intent: a remembered Boot body or a newly connected socket cannot substitute for actual current introduction.
# Why: offline mode must still fence every persisted setting until its authenticated charger has freshly registered.
async def test_offline_recovery_requires_actual_new_boot(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with replacement_offline(tmp_path, tls, unused_tcp_port, monkeypatch) as recovery:
        test = recovery.test
        assert test.session.journal.get("boot")
        for _ in range(2):
            status = await test.controller.tick()
            assert status["state"] == "starting" and not status.get("local_healthy", False)
        assert test.session.journal.get("trial_phase") == "recovering"
        assert not test.session.gateway.current_boot_seen(test.session.gateway._charger_epoch)
        assert not starts(test.rig)


# Intent: a real connection awaiting its first Boot acknowledgement does not count as an offline failure.
# Why: the ordinary registration window must preserve prior app intent until actual connection failure is observed.
@pytest.mark.parametrize("previous_failure", [False, True])
async def test_delayed_online_boot_acknowledgement_does_not_force_offline_pause(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    previous_failure: bool,
) -> None:
    enabled = [False]
    boot_waiting, release = asyncio.Event(), asyncio.Event()
    actual_vendor = OwnerBench.vendor

    # Delay only the independent backend's genuine Boot reply on its real TLS socket.
    async def delayed_vendor(bench: OwnerBench, connection: Any) -> None:
        actual_send = connection.send

        # The backend still records and processes every real request; its selected Boot ACK waits at this boundary.
        async def send(message: Any) -> None:
            frame = json.loads(message)
            boot = frame[0] == 3 and any(
                call[0] == 2 and call[1] == frame[1] and call[2] == "BootNotification" for call in bench.calls
            )
            if enabled[0] and boot:
                boot_waiting.set()
                await release.wait()
            await actual_send(message)

        monkeypatch.setattr(connection, "send", send)
        await actual_vendor(bench, connection)

    monkeypatch.setattr(OwnerBench, "vendor", delayed_vendor)
    async with (
        replacement_offline(tmp_path, tls, unused_tcp_port, monkeypatch, refuse_vendor=previous_failure) as recovery,
        AsyncExitStack() as stack,
    ):
        test = recovery.test
        previous_failed_epoch = None
        if previous_failure:
            await local_boot(recovery, "previous-failed-generation-boot")
            previous_failed_epoch = test.session.runtime.status.vendor_failed_charger_epoch
            await test.session.runtime.stop_vendor()
            await test.charger.close()
            test.charger = await stack.enter_async_context(test.rig.charger(test.session))
            transport = test.session.runtime.transport
            transport.config = replace(transport.config, vendor_url=recovery.original_vendor_url)
        enabled[0] = True
        try:
            reply = await test.rig.bench.call(test.charger, "delayed-online-device-boot", "BootNotification", BOOT)
            assert reply[0] == 3
            if previous_failure:
                assert previous_failed_epoch != test.session.gateway._charger_epoch
                test.session.runtime._runner = asyncio.create_task(test.session.runtime._vendor_runner())
            await asyncio.wait_for(boot_waiting.wait(), 3)
            await until(lambda: test.session.gateway.controls.ready)
            assert test.session.runtime.status.vendor_connected
            assert test.session.runtime.status.vendor_failed_charger_epoch == previous_failed_epoch
            assert test.session.gateway.vendor_registration != "Accepted"
            status = await test.controller.tick()
            assert status["state"] == "waiting_for_cloud"
            assert test.controller.requests.intent().mode == "app"
            assert test.controller.requests.intent().revision == recovery.prior_revision
            assert test.rig.bench.firmware.effective_limit() == 0 and not starts(test.rig)

            release.set()
            await until(lambda: test.session.gateway.vendor_registration == "Accepted")
            status = await healthy(test.controller)
            assert status["desired_control"] == "app" and status["station_ceiling_a"] == 16
            assert test.controller.requests.intent().revision == recovery.prior_revision
            assert not starts(test.rig) and test.session.journal.active() is None
        finally:
            release.set()


# Intent: queued Status/Meter survives local recovery and later real ACK delivery while preserving the pause.
# Why: readiness cannot erase history, invent acknowledgement, or let cloud reconnection release the pause.
async def test_observations_remain_queued_then_flush_on_reconnect_while_pause_remains(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with replacement_offline(tmp_path, tls, unused_tcp_port, monkeypatch) as recovery:
        test = recovery.test
        await local_boot(recovery)
        await observations(test)
        queue = [tuple(row) for row in test.session.journal.db.execute("SELECT * FROM outbox ORDER BY seq")]
        assert {row[0] for row in test.session.journal.db.execute("SELECT action FROM outbox")} == {
            "StatusNotification",
            "MeterValues",
        }
        status = await locally_ready(test.controller)
        assert status["normal_start_ready"] and status["local_hold_verified"]
        assert queue == [tuple(row) for row in test.session.journal.db.execute("SELECT * FROM outbox ORDER BY seq")]
        intent = test.controller.requests.intent()
        transport = test.session.runtime.transport
        transport.config = replace(transport.config, vendor_url=recovery.original_vendor_url)
        await until(
            lambda: (
                test.session.gateway.vendor_registration == "Accepted" and test.session.journal.next_delivery() is None
            )
        )
        status = await healthy(test.controller)
        assert status["local_hold_verified"] and test.controller.requests.intent() == intent
        assert test.rig.bench.firmware.effective_limit() == 0 and not starts(test.rig)
        request = uuid.uuid4().hex
        test.controller.command(request, "app_control", intent.revision)
        status = await healthy(test.controller)
        assert status["desired_control"] == "app" and status["station_ceiling_a"] == 16
        assert test.controller.requests.receipt(request)["state"] == "completed" and not starts(test.rig)


# Intent: the strict legacy acknowledgement still refuses queued data while the explicit managed local path retains it.
# Why: enabling offline managed recovery must not silently broaden unrelated commissioning/recovery callers.
async def test_default_resume_stays_strict_and_local_only_retains_real_observation_queue(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with replacement_offline(tmp_path, tls, unused_tcp_port, monkeypatch) as recovery:
        test = recovery.test
        await local_boot(recovery)
        await observations(test)
        test.controller.prepare_offline_pause()
        queue = [tuple(row) for row in test.session.journal.db.execute("SELECT * FROM outbox")]
        with pytest.raises(RecoveryRequiredError):
            await test.controller.native.resume()
        assert test.session.journal.get("trial_phase") == "recovering"
        result = await test.controller.native.resume(local_only=True)
        assert result == {"recovery_acknowledged": True, "zero_hold_retained": True, "charging_resumed": False}
        assert queue == [tuple(row) for row in test.session.journal.db.execute("SELECT * FROM outbox")]
        assert test.session.journal.get("trial_phase") == "active" and not starts(test.rig)


# Intent: genuine lifecycle debt or an unresolved mutation prevents offline acknowledgement at actual zero.
# Why: the new path permits observations only, never unresolved charging or mutating work from an earlier process.
@pytest.mark.parametrize("debt", ["lifecycle", "mutation"])
async def test_prior_lifecycle_or_mutation_debt_blocks_offline_readiness(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    debt: str,
) -> None:
    async with replacement_offline(tmp_path, tls, unused_tcp_port, monkeypatch, debt=debt) as recovery:
        test = recovery.test
        await local_boot(recovery)
        status = await test.controller.tick()
        assert not status.get("local_healthy", False)
        assert test.session.journal.get("trial_phase") == "recovering"
        assert test.rig.bench.firmware.effective_limit() == 0 and not starts(test.rig)
        assert test.session.journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == (
            recovery.prior_transaction_count
        )


# Intent: unknown native activity cannot be adopted or receive a guessed Stop merely to make offline recovery available.
# Why: owned zero and a remembered session counter do not authorize control of an external charging transaction.
async def test_unknown_native_session_blocks_local_recovery_without_guessed_stop(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with replacement_offline(tmp_path, tls, unused_tcp_port, monkeypatch) as recovery:
        test = recovery.test
        await local_boot(recovery)
        test.rig.bench.firmware.active = 9999999
        test.rig.bench.cp, test.rig.bench.connection = "C", "Charging"
        test.rig.bench.history.append({"transactionId": 9999999, "origin": "unknown-native-session"})
        status = await test.controller.tick()
        assert not status.get("local_healthy", False) and test.session.journal.get("trial_phase") == "recovering"
        assert not any(frame[2] == "RemoteStopTransaction" for frame in test.rig.bench.device_calls)
        assert not starts(test.rig) and test.rig.bench.firmware.effective_limit() == 0


# Intent: a newer Pause, process shutdown or actual new Boot overtakes a delayed local-only native acknowledgement.
# Why: complete native evidence from an older generation cannot reactivate a retired recovery request.
@pytest.mark.parametrize("boundary", ["pause", "shutdown", "boot"])
async def test_local_recovery_rechecks_authority_after_awaited_native_read(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    async with replacement_offline(tmp_path, tls, unused_tcp_port, monkeypatch) as recovery, AsyncExitStack() as stack:
        test = recovery.test
        await local_boot(recovery)
        test.controller.prepare_offline_pause()
        actual = test.controller.native.observe
        entered, proceed = asyncio.Event(), asyncio.Event()
        delayed_once = False

        # Preserve the real native HTTP result while delaying only the first acknowledgement's completion.
        async def delayed() -> Any:
            nonlocal delayed_once
            native = await actual()
            if not delayed_once:
                delayed_once = True
                entered.set()
                await proceed.wait()
            return native

        monkeypatch.setattr(test.controller.native, "observe", delayed)
        task = asyncio.create_task(test.controller.native.resume(local_only=True))
        try:
            await asyncio.wait_for(entered.wait(), 3)
            if boundary == "pause":
                test.controller.command(uuid.uuid4().hex, "pause", test.controller.requests.intent().revision)
            elif boundary == "shutdown":
                test.controller.request_stop()
            else:
                new_charger = await stack.enter_async_context(test.rig.charger(test.session))
                answer = await test.rig.bench.call(new_charger, "overtaking-real-boot", "BootNotification", BOOT)
                assert answer[0] == 3
            proceed.set()
            with pytest.raises((RecoveryRequiredError, UnavailableError, SecurityError)):
                await task
            assert test.session.journal.get("trial_phase") == "recovering"
            assert test.rig.bench.firmware.effective_limit() == 0 and not starts(test.rig)
        finally:
            proceed.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
