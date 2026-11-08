"""Real TLS process loss and independent native state exercise the restricted restart path."""

from __future__ import annotations

import asyncio
import copy
import time
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.frames import build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.security import TRANSPORT_LOG, SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError, fingerprint
from ocpp_hub_core.prototype.trial_evidence import Evidence
from ocpp_hub_core.prototype.trial_recovery import require_recovery_state
from ocpp_hub_core.prototype.trial_session import TrialSession, preflight
from websockets.asyncio.server import serve

from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import FAST, until
from tests.test_prototype_tls import tls as tls
from tests.test_trial_session import Bench
from tests.trial_helpers import config_files


# Drive actual encrypted peer connections while retaining the fake device independently across controller replacement.
@asynccontextmanager
async def introduced(bench: Bench, session: TrialSession, token: str):
    async with bench.connection_to(session) as charger:
        reader = asyncio.create_task(bench.read_charger(charger))
        try:
            await bench.call(charger, token, "BootNotification", BOOT)
            await until(lambda: session.gateway.controls.ready and session.gateway.vendor_registration == "Accepted")
            yield charger, reader
        finally:
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)


# Simulate abrupt process loss without executing the normal shutdown Stop or discarding any durable event.
async def crash(session: TrialSession) -> None:
    await session.runtime.close()
    session.journal.close()
    session.closed = True


# Intent: only consistent physical state and an exact known session can authorize connected recovery.
# Why: a boolean native-open flag cannot distinguish the proxy's session from another native transaction.
@pytest.mark.parametrize(
    "change",
    [
        {"native_transaction_id": 8},
        {"native_transaction_id": True},
        {"native_transaction_id": None},
        {"transaction_open": False},
        {"cp": "D"},
        {"connection": "Plug"},
        {"current_a": float("nan")},
        {"power_w": float("inf")},
        {"observed_at": 1},
        {"identity_digest": fingerprint("OTHER")},
    ],
)
def test_recovery_rejects_unknown_or_contradictory_native_state(change: dict[str, Any]) -> None:
    evidence = Evidence(fingerprint("OFFLINE001"), time.time(), "C", "Charging", 6, 1440, True, "config", (), True, 7)
    with pytest.raises((SecurityError, ValueError)):
        require_recovery_state(replace(evidence, **change), "OFFLINE001", 7)


# Intent: an idle plugged restart installs only zero, creates no lifecycle fact and requires explicit owner resume.
# Why: recovering a listener must not silently release a previous hold or authorize autonomous timer charging.
@pytest.mark.asyncio
async def test_plugged_idle_restart_keeps_zero_until_explicit_resume(tmp_path: Path, tls, unused_tcp_port: int) -> None:
    bench = Bench()
    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        prepared = preflight(path)
        first = TrialSession(prepared, FAST, bench.observe)
        await first.start()
        async with introduced(bench, first, "boot"):
            await first.command({"command": "stop"})
            bench.cp, bench.connection = "B", "Plug"
            await crash(first)
        second = TrialSession(prepared, FAST, bench.observe)
        checkpoint = len(bench.received_limits)
        try:
            await second.start()
            async with introduced(bench, second, "boot2") as (charger, _):
                assert second.journal.get("trial_phase") == "recovering"
                assert bench.received_limits[checkpoint:] and set(bench.received_limits[checkpoint:]) == {0}
                assert second.journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0
                assert not bench.remote_stop.is_set()
                for command in ("start", "release"):
                    with pytest.raises((RecoveryRequiredError, ValueError)):
                        await second.command({"command": command})
                bench.firmware.requested_current = 32  # Independent native timer demand still cannot defeat zero.
                assert bench.firmware.current == 0
                result = await second.command({"command": "resume"})
                assert result["zero_hold_retained"] and not result["charging_resumed"]
                assert second.journal.get("trial_phase") == "active" and bench.firmware.current == 0
                assert second.gateway.controls.desired() == 0 and second.gateway.controls.ready
                denied = await bench.call(charger, "after-resume-auth", "Authorize", {"idTag": START["idTag"]})
                assert denied[2]["idTagInfo"]["status"] == "Invalid" and bench.firmware.current == 0
                bench.cp, bench.connection = "A", "Unplug"
                bench.firmware.requested_current = 0  # Removing the actual cable removes the vehicle's current demand.
                await second.command({"command": "rollback"})
        finally:
            await second.close()


# Intent: unknown or mismatched native sessions never get adopted or receive a guessed automatic Stop.
# Why: an existing owned station profile does not prove that every transaction running on the charger is ours.
@pytest.mark.asyncio
@pytest.mark.parametrize("known_session", [False, True])
async def test_unknown_native_session_refuses_recovery_before_bind(
    tmp_path: Path, tls, unused_tcp_port: int, known_session: bool
) -> None:
    bench = Bench()
    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        prepared = preflight(path)
        first = TrialSession(prepared, FAST, bench.observe)
        await first.start()
        async with introduced(bench, first, "boot") as (charger, _):
            if known_session:
                await bench.call(charger, "start", "StartTransaction", START)
                await until(lambda: first.journal.vendor_id(1001) is not None)
            bench.firmware.active = 8888
            bench.firmware.requested_current = 6
            bench.cp, bench.connection = "C", "Charging"
            saved_facts = [tuple(row) for row in first.journal.db.execute("SELECT * FROM transactions")]
            await crash(first)
        second = TrialSession(prepared, FAST, bench.observe)
        checkpoint = len(bench.received_limits)
        try:
            with pytest.raises(SecurityError):
                await second.start()
            assert second.runtime._server is None and second.gateway.charger.sender is None
            assert len(bench.received_limits) == checkpoint and not bench.remote_stop.is_set()
            assert [tuple(row) for row in second.journal.db.execute("SELECT * FROM transactions")] == saved_facts
        finally:
            await second.close()


class LostStartBench(Bench):
    """The independent backend receives Start but deliberately loses its response."""

    # Track actual requests rather than inferring successful cloud delivery from the proxy's local queue.
    def __init__(self) -> None:
        super().__init__()
        self.starts = 0
        self.start_seen = asyncio.Event()

    # A closed connection after receiving Start leaves the caller unable to know whether the backend accepted it.
    async def vendor(self, connection: Any) -> None:
        async for raw in connection:
            frame = decode(raw)
            self.calls.append(frame)
            if frame[0] != 2:
                continue
            if frame[2] == "StartTransaction":
                self.starts += 1
                self.start_seen.set()
                await connection.close()
                return
            answer = (
                {"status": "Accepted", "interval": 1, "currentTime": "2030-01-01T00:00:00Z"}
                if frame[2] == "BootNotification"
                else {"currentTime": "2030-01-01T00:00:00Z"}
                if frame[2] == "Heartbeat"
                else {}
            )
            await connection.send(build_call_result(frame[1], answer))


# Intent: a lost cloud Start response survives charging recovery without a duplicate Start or fabricated vendor ID.
# Why: transport reconnection must not trade charge history integrity for apparent cloud availability.
@pytest.mark.asyncio
async def test_charging_restart_preserves_ambiguous_cloud_start(tmp_path: Path, tls, unused_tcp_port: int) -> None:
    bench = LostStartBench()
    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        prepared = preflight(path)
        first = TrialSession(prepared, FAST, bench.observe)
        await first.start()
        async with introduced(bench, first, "boot") as (charger, _):
            await bench.call(charger, "start", "StartTransaction", START)
            await asyncio.wait_for(bench.start_seen.wait(), 2)
            await until(lambda: first.journal.next_delivery().state == "ambiguous")
            original = dict(first.journal.db.execute("SELECT * FROM transactions").fetchone())
            bench.firmware.requested_current = 6
            bench.cp, bench.connection = "C", "Charging"
            await crash(first)
        second = TrialSession(prepared, FAST, bench.observe)
        try:
            await second.start()
            async with introduced(bench, second, "boot2") as (charger, _):
                await asyncio.wait_for(bench.remote_stop.wait(), 2)
                assert bench.firmware.current == 0
                assert second.journal.active() == 1001
                await bench.call(
                    charger,
                    "stop",
                    "StopTransaction",
                    {"transactionId": 1001, "timestamp": START["timestamp"], "meterStop": 1001},
                )
                await until(lambda: second.runtime.status.delivery_blocked)
                await until(lambda: second.runtime.status.heartbeats > 0)
                assert bench.starts == 1 and second.journal.vendor_id(1001) is None
                assert second.journal.next_delivery().state == "ambiguous"
                row = dict(second.journal.db.execute("SELECT * FROM transactions").fetchone())
                assert row["start"] == original["start"] and row["start_info"] == original["start_info"]
                assert row["stop"] is not None
                with pytest.raises(RecoveryRequiredError):
                    await second.command({"command": "resume"})
        finally:
            await second.close()


# Intent: a live socket replacement blocks authorization before Boot and establishes zero before the known Stop.
# Why: waiting for a later native audit leaves a window where an early Authorize could reinstall a positive ceiling.
@pytest.mark.asyncio
async def test_transport_replacement_claims_hold_before_first_message(
    tmp_path: Path, tls, unused_tcp_port: int
) -> None:
    bench = Bench()
    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        session = TrialSession(preflight(path), FAST, bench.observe)
        try:
            await session.start()
            async with introduced(bench, session, "boot") as (charger, _):
                await bench.call(charger, "start", "StartTransaction", START)
                await until(lambda: session.journal.vendor_id(1001) is not None)
                bench.cp, bench.connection = "C", "Charging"
                bench.firmware.requested_current = 6
            await until(lambda: not session.runtime.status.charger_connected)
            checkpoint = len(bench.device_calls)
            async with bench.connection_to(session) as charger:
                reader = asyncio.create_task(bench.read_charger(charger))
                try:
                    await until(lambda: session.runtime.status.charger_connected)
                    assert session.journal.get("trial_phase") == "recovering"
                    assert session.gateway.controls.desired() == 0
                    denied = await bench.call(charger, "early-auth", "Authorize", {"idTag": START["idTag"]})
                    assert denied[0] == 3 and denied[2]["idTagInfo"]["status"] == "Invalid"
                    assert len(bench.device_calls) == checkpoint
                    await session.command({"command": "stop"})
                    assert session.gateway.controls.ready and not session.gateway.current_boot_seen(
                        session.gateway._charger_epoch
                    )
                    with pytest.raises(RecoveryRequiredError):
                        await session.command({"command": "resume"})
                    bench.remote_stop.clear()
                    checkpoint = len(bench.device_calls)
                    await bench.call(charger, "boot2", "BootNotification", BOOT)
                    await asyncio.wait_for(bench.remote_stop.wait(), 2)
                    actions = [frame[2] for frame in bench.device_calls[checkpoint:]]
                    assert actions.index("SetChargingProfile") < actions.index("RemoteStopTransaction")
                    assert all(
                        frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
                        for frame in bench.device_calls[checkpoint:]
                        if frame[2] == "SetChargingProfile"
                    )
                    await bench.call(
                        charger,
                        "stop",
                        "StopTransaction",
                        {"transactionId": 1001, "timestamp": START["timestamp"], "meterStop": 1001},
                    )
                    await until(lambda: session.journal.next_delivery() is None)
                    assert bench.firmware.current == 0 and session.journal.get("trial_phase") == "recovering"
                finally:
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            await session.close()


# Intent: a missing native Stop remains unresolved until the genuine replay arrives, with its original Start receipt.
# Why: native-closed is not authority to fabricate a Stop or change the originally acknowledged local transaction.
@pytest.mark.asyncio
async def test_native_closed_restart_waits_for_actual_stop_replay(tmp_path: Path, tls, unused_tcp_port: int) -> None:
    bench = Bench()
    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        prepared = preflight(path)
        first = TrialSession(prepared, FAST, bench.observe)
        await first.start()
        async with introduced(bench, first, "boot") as (charger, _):
            answer = await bench.call(charger, "start", "StartTransaction", START)
            await until(lambda: first.journal.vendor_id(1001) is not None)
            original = dict(first.journal.db.execute("SELECT * FROM transactions").fetchone())
            bench.firmware.active = None  # The device closed locally while its real Stop message was lost.
            bench.cp, bench.connection = "B", "Plug"
            await crash(first)
        second = TrialSession(prepared, FAST, bench.observe)
        try:
            await second.start()
            async with introduced(bench, second, "boot2") as (charger, _):
                assert not bench.remote_stop.is_set()
                assert second.journal.active() == 1001
                replay = await bench.call(charger, "start-retry", "StartTransaction", START)
                assert replay[2] == answer[2]
                with pytest.raises(RecoveryRequiredError):
                    await second.command({"command": "resume"})
                row = dict(second.journal.db.execute("SELECT * FROM transactions").fetchone())
                assert row["start"] == original["start"] and row["stop"] is None
                await bench.call(
                    charger,
                    "actual-stop-replay",
                    "StopTransaction",
                    {"transactionId": 1001, "timestamp": START["timestamp"], "meterStop": 1001},
                )
                await until(lambda: second.journal.next_delivery() is None)
                await second.command({"command": "resume"})
                assert second.gateway.controls.desired() == 0
        finally:
            await second.close()


# Intent: a refused zero cap still attempts Stop for the exact known session without accepting unsafe readiness.
# Why: one failed protective operation must not disable the other available safety action during restart.
@pytest.mark.asyncio
async def test_recovery_zero_rejection_still_stops_known_session(tmp_path: Path, tls, unused_tcp_port: int) -> None:
    bench = Bench()
    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        prepared = preflight(path)
        first = TrialSession(prepared, FAST, bench.observe)
        await first.start()
        async with introduced(bench, first, "boot") as (charger, _):
            await bench.call(charger, "start", "StartTransaction", START)
            await until(lambda: first.journal.vendor_id(1001) is not None)
            bench.cp, bench.connection = "C", "Charging"
            bench.firmware.requested_current = 6
            await crash(first)
        original_answer = bench.firmware.answer

        # The independent device rejects this actual wire command while retaining its previous installed cap.
        def reject_zero(action: str, payload: dict[str, Any]) -> dict[str, Any]:
            if (
                action == "SetChargingProfile"
                and payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
            ):
                return {"status": "Rejected"}
            return original_answer(action, payload)

        bench.firmware.answer = reject_zero
        second = TrialSession(prepared, FAST, bench.observe)
        try:
            await second.start()
            async with bench.connection_to(second) as charger:
                reader = asyncio.create_task(bench.read_charger(charger))
                try:
                    await bench.call(charger, "boot2", "BootNotification", BOOT)
                    await asyncio.wait_for(bench.remote_stop.wait(), 2)
                    await until(lambda: second.runtime.status.last_failure == "BootOrCeilingUnavailable")
                    assert not second.gateway.controls.ready
                    assert second.gateway.controls.desired() == 0
                    await bench.call(
                        charger,
                        "actual-stop",
                        "StopTransaction",
                        {"transactionId": 1001, "timestamp": START["timestamp"], "meterStop": 1001},
                    )
                    assert bench.firmware.current == 0 and second.journal.active() is None
                    with pytest.raises(SecurityError, match="ownership remains uncertain"):
                        await second.command({"command": "resume"})
                finally:
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            bench.firmware.answer = original_answer
            await second.close()


# Intent: successive fresh journals allocate above retained device history and preserve every earlier native body.
# Why: testing only the allocator formula misses integration errors between admission, TLS Start and a new ledger.
@pytest.mark.asyncio
async def test_two_fresh_journals_charge_above_retained_native_ids(tmp_path: Path, tls, unused_tcp_port: int) -> None:
    import json

    from ocpp_hub_core.prototype.trial_session import save_private

    bench = Bench()
    bench.history = [{"transactionId": 9000000, "old": "incomplete"}, {"transactionId": 1001, "old": "closed"}]
    originals = copy.deepcopy(bench.history)
    assigned = []
    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        for index in range(2):
            value = json.loads(path.read_text())
            value["journal"] = str(tmp_path / f"state-{index}.db")
            save_private(path, value)
            prepared = preflight(path)
            session = TrialSession(prepared, FAST, bench.observe)
            try:
                bench.configuration = "original"
                await session.start()
                bench.configuration = "trial"
                async with introduced(bench, session, f"boot-{index}") as (charger, _):
                    before = max(row["transactionId"] for row in bench.history)
                    await session.command({"command": "start"})
                    payload = {**START, "meterStart": START["meterStart"] + index}
                    result = await bench.call(charger, f"start-{index}", "StartTransaction", payload)
                    local = result[2]["transactionId"]
                    assigned.append(local)
                    assert local > before
                    bench.cp, bench.connection = "C", "Charging"
                    bench.firmware.requested_current = 6
                    assert (await bench.observe()).current_a == 6
                    await session.command({"command": "stop"})
                    await bench.call(
                        charger,
                        f"stop-{index}",
                        "StopTransaction",
                        {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1005 + index},
                    )
                    await until(lambda: session.journal.next_delivery() is None)
                    assert all(row in bench.history for row in originals)
                    bench.cp, bench.connection = "A", "Unplug"
                    await session.command({"command": "rollback"})
                await until(lambda: session.gateway.charger.sender is None)
                bench.configuration = "original"
                await session.command({"command": "confirm-original-backend"})
            finally:
                await session.close()
        assert assigned == [9000001, 9000002]
        assert all(row in bench.history for row in originals) and len(bench.history) == 4


# Intent: native idle checks inspect every phase and every power channel, including malformed extra/missing channels.
# Why: a zero first phase and stale zero total must not hide current flowing elsewhere during recovery or cleanup.
@pytest.mark.parametrize(
    "field,values",
    [
        ("cur", [0, 60, 0]),
        ("cur", [0, 0, 60]),
        ("power", [1440, 0, 0, 0]),
        ("power", [0, 1440, 0, 0]),
        ("power", [0, 0, 1440, 0]),
        ("cur", [0]),
        ("cur", [0, 0, 0, 0]),
        ("power", [0, 0, 0]),
        ("cur", [0, True, 0]),
        ("power", [0, float("nan"), 0, 0]),
    ],
)
def test_native_idle_requires_all_meter_channels(field: str, values: list[Any]) -> None:
    from ocpp_hub_core.prototype.trial_evidence import native_evidence

    from tests.test_trial_transport import native_values

    data = native_values()
    data["METER"][0]["meterData"][field] = values
    with pytest.raises(SecurityError):
        evidence = native_evidence(data["config"], data, time.time())
        evidence.require_unplugged("OFFLINE001")


# Intent: a pre-Boot emergency Stop cannot substitute for the missing native/Boot recovery audit, even when idle.
# Why: an old saved Boot payload and a fresh zero composite otherwise satisfy the remaining idle resume checks.
@pytest.mark.asyncio
async def test_idle_preboot_safety_stop_cannot_unlock_recovery(tmp_path: Path, tls, unused_tcp_port: int) -> None:
    bench = Bench()
    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        prepared = preflight(path)
        first = TrialSession(prepared, FAST, bench.observe)
        await first.start()
        async with introduced(bench, first, "boot"):
            bench.cp, bench.connection = "B", "Plug"
            await crash(first)
        second = TrialSession(prepared, FAST, bench.observe)
        try:
            await second.start()
            async with bench.connection_to(second) as charger:
                reader = asyncio.create_task(bench.read_charger(charger))
                try:
                    await until(lambda: second.runtime.status.charger_connected)
                    await second.command({"command": "stop"})
                    assert second.gateway.controls.ready and second.journal.active() is None
                    assert second.journal.next_delivery() is None and not second.journal.get("control")["uncertain"]
                    assert not second.gateway.current_boot_seen(second.gateway._charger_epoch)
                    with pytest.raises(RecoveryRequiredError):
                        await second.command({"command": "resume"})
                    assert second.journal.get("trial_phase") == "recovering"
                    await bench.call(charger, "actual-boot", "BootNotification", BOOT)
                    await until(
                        lambda: second.runtime.status.boot_observed and second.gateway.vendor_registration == "Accepted"
                    )
                    await until(lambda: second.recovery.introduced_epoch == second.gateway._charger_epoch)
                    await second.command({"command": "resume"})
                    assert second.gateway.controls.desired() == 0
                finally:
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            await second.close()


# Intent: new native history during listener handoff blocks first admission instead of reusing its reserved number.
# Why: a completed original-backend session can appear after startup sampled history but before the proxy sees Boot.
@pytest.mark.asyncio
async def test_first_boot_rechecks_history_created_during_handoff(tmp_path: Path, tls, unused_tcp_port: int) -> None:
    bench = Bench()
    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        session = TrialSession(preflight(path), FAST, bench.observe)
        try:
            await session.start()
            assert session.journal.get("next_transaction") == 1000
            bench.history.append({"transactionId": 1001, "old": "completed during handoff"})
            async with bench.connection_to(session) as charger:
                reader = asyncio.create_task(bench.read_charger(charger))
                try:
                    await bench.call(charger, "boot", "BootNotification", BOOT)
                    await until(lambda: session.runtime.status.charger_fault.count > 0)
                    assert session.journal.get("trial_phase") == "blocked" and not session.gateway.controls.ready
                    assert bench.received_limits == []
                    assert session.journal.get("next_transaction") == 1000
                    denied = await bench.call(charger, "new-start", "StartTransaction", START)
                    assert denied[0] == 4
                    assert session.journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0
                    assert bench.history == [{"transactionId": 1001, "old": "completed during handoff"}]
                finally:
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            await session.close()


# Intent: the cloud and owner cannot cause positive writes while the first native/history introduction audit waits.
# Why: phaseactive alone previously let a cloud command install station/profile writes before the native audit finished.
@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["SetChargingProfile", "RemoteStartTransaction"])
async def test_initial_audit_blocks_cloud_and_owner_positive_writes(
    tmp_path: Path, tls, unused_tcp_port: int, action: str
) -> None:
    from ocpp_hub_core import commands
    from ocpp_hub_core.frames import build_call

    from tests.test_trial_auth import info

    bench = Bench()
    entered, release, answered = asyncio.Event(), asyncio.Event(), asyncio.Event()
    answers = []
    calls = 0

    # Hold the second physical observation, which is the first post-Boot introduction rather than pre-bind admission.
    async def observe():
        nonlocal calls
        calls += 1
        if calls == 2:
            entered.set()
            await release.wait()
        return await bench.observe()

    # A real backend sends a valid charging-capable request immediately after registration while the audit is held.
    async def eager_vendor(connection):
        async for raw in connection:
            frame = decode(raw)
            if frame[0] != 2:
                if frame[1] == "early-command":
                    answers.append(frame)
                    answered.set()
                continue
            if frame[2] == "BootNotification":
                body = {"status": "Accepted", "interval": 1, "currentTime": "2030-01-01T00:00:00Z"}
            elif frame[2] == "Authorize":
                body = {"idTagInfo": info()}
            else:
                body = {}
            await connection.send(build_call_result(frame[1], body))
            if frame[2] == "BootNotification":
                await entered.wait()
                payload = (
                    commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 42, stack_level=1)
                    if action == "SetChargingProfile"
                    else {"connectorId": 1, "idTag": "LOCAL_TEST"}
                )
                await connection.send(build_call("early-command", action, payload))

    async with serve(eager_vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        session = TrialSession(preflight(path), FAST, observe)
        try:
            await session.start()
            async with bench.connection_to(session) as charger:
                reader = asyncio.create_task(bench.read_charger(charger))
                try:
                    await bench.call(charger, "boot", "BootNotification", BOOT)
                    await asyncio.wait_for(answered.wait(), 2)
                    assert answers[0][0] == 4 and answers[0][2] == "SecurityError"
                    for operation in (
                        session.command({"command": "release"}),
                        session.gateway.set_limit(6),
                        session.gateway.start("LOCAL_TEST"),
                    ):
                        with pytest.raises((ValueError, RecoveryRequiredError)):
                            await operation
                    assert bench.device_calls == [] and bench.firmware.profiles == {}
                    assert session.journal.get("trial_starting")
                    assert session.journal.db.execute("SELECT COUNT(*) FROM mutations").fetchone()[0] == 0
                    release.set()
                    await until(lambda: session.gateway.controls.ready)
                    assert not session.journal.get("trial_starting")
                    await session.command({"command": "rollback"})
                finally:
                    release.set()
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            await session.close()


# Intent: legitimate Stop or rollback supersedes a held native snapshot without poisoning inventory or cleanup state.
# Why: old6A evidence must not be compared against the newly acknowledged0A intent or overwrite frozen cleanup.
@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["stop", "rollback"])
async def test_late_native_snapshot_respects_newer_control_and_cleanup(
    tmp_path: Path, tls, unused_tcp_port: int, operation: str
) -> None:
    bench = Bench()
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0

    # Preserve the actual earlier device bytes while a legitimate newer wire operation completes independently.
    async def observe():
        nonlocal calls
        calls += 1
        evidence = await bench.observe()
        if calls == 3:
            entered.set()
            await release.wait()
        return evidence

    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        session = TrialSession(preflight(path), FAST, observe)
        try:
            await session.start()
            async with introduced(bench, session, "boot"):
                if operation == "stop":
                    bench.cp, bench.connection = "B", "Plug"
            await until(lambda: not session.runtime.status.charger_connected)
            async with bench.connection_to(session) as charger:
                reader = asyncio.create_task(bench.read_charger(charger))
                try:
                    await bench.call(charger, "boot2", "BootNotification", BOOT)
                    await asyncio.wait_for(entered.wait(), 2)
                    await session.command({"command": operation})
                    if operation == "rollback":
                        assert session.journal.get("trial_phase") == "backend_restore_pending"
                        assert not bench.firmware.profiles
                    else:
                        assert session.gateway.controls.ready and session.gateway.controls.desired() == 0
                    release.set()
                    if operation == "rollback":
                        await until(lambda: session.runtime.status.charger_fault.count > 0)
                        assert session.journal.get("trial_phase") == "backend_restore_pending"
                        row = session.journal.db.execute("SELECT state FROM trial_resources").fetchone()
                        assert row["state"] == "absent_verified" and not bench.firmware.profiles
                    else:
                        await until(lambda: session.recovery.introduced_epoch == session.gateway._charger_epoch)
                        assert calls >= 4 and session.gateway.controls.ready
                        assert session.gateway.controls.desired() == 0 and bench.firmware.current == 0
                    assert not session.journal.get("trial_inventory_fault")
                    assert not session.journal.get("trial_preservation_fault")
                finally:
                    release.set()
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            await session.close()


# Intent: silent connected recovery requests a genuine device Boot and stays at zero after the actual reply arrives.
# Why: reusing a saved Boot or waiting indefinitely leaves a restarted controller unable to establish current authority.
@pytest.mark.asyncio
async def test_silent_reconnect_uses_actual_read_only_boot_probe(tmp_path: Path, tls, unused_tcp_port: int) -> None:
    bench = Bench()
    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        prepared = preflight(path)
        first = TrialSession(prepared, FAST, bench.observe)
        await first.start()
        async with introduced(bench, first, "boot"):
            bench.cp, bench.connection = "B", "Plug"
            await crash(first)
        bench.boot_on_probe = True
        second = TrialSession(prepared, replace(FAST, boot_probe_after=0.05), bench.observe)
        checkpoint = len(bench.device_calls)
        try:
            await second.start()
            async with bench.connection_to(second) as charger:
                reader = asyncio.create_task(bench.read_charger(charger))
                try:
                    await until(
                        lambda: second.gateway.controls.ready and second.gateway.vendor_registration == "Accepted"
                    )
                    assert second.runtime.status.boot_observed and bench.auto_boots == 1
                    probes = [frame for frame in bench.device_calls[checkpoint:] if frame[2] == "TriggerMessage"]
                    assert len(probes) == 1 and probes[0][3] == {"requestedMessage": "BootNotification"}
                    assert second.journal.get("trial_phase") == "recovering"
                    assert second.gateway.controls.desired() == 0 and bench.firmware.current == 0
                    assert second.journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0
                finally:
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            await second.close()


# Intent: continuous legitimate control changes exhaust the audit budget without becoming an inventory fault.
# Why: a bounded observer must remain safe and diagnosable when it cannot obtain a stable resource/session snapshot.
@pytest.mark.asyncio
async def test_unstable_native_audit_is_bounded_without_poisoning_inventory(
    tmp_path: Path, tls, unused_tcp_port: int
) -> None:
    bench = Bench()
    race = False
    attempts = 0
    session = None

    # Each sampled snapshot is overtaken by an actual ordinary zero Stop on the real synthetic charger connection.
    async def observe():
        nonlocal attempts
        evidence = await bench.observe()
        if race:
            attempts += 1
            await session.command({"command": "stop"})
        return evidence

    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        session = TrialSession(preflight(path), FAST, observe)
        try:
            await session.start()
            async with introduced(bench, session, "boot"):
                bench.cp, bench.connection = "B", "Plug"
            await until(lambda: not session.runtime.status.charger_connected)
            race = True
            async with bench.connection_to(session) as charger:
                reader = asyncio.create_task(bench.read_charger(charger))
                try:
                    await bench.call(charger, "boot2", "BootNotification", BOOT)
                    await until(lambda: session.runtime.status.charger_fault.count > 0)
                    assert attempts == 3 and not session.journal.get("trial_inventory_fault")
                    assert not session.journal.get("trial_preservation_fault")
                    assert session.gateway.controls.desired() == 0 and bench.firmware.current == 0
                    assert session.journal.get("trial_phase") == "recovering"
                    with pytest.raises(RecoveryRequiredError):
                        await session.command({"command": "resume"})
                finally:
                    race = False
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            await session.close()


# Intent: an already pending Set0 response is resolved before native ownership classification begins.
# Why: unchanged pending receipts do not mean the old native profile is a stable confirmation of the new intent.
@pytest.mark.asyncio
async def test_native_audit_waits_for_already_pending_profile_ack(tmp_path: Path, tls, unused_tcp_port: int) -> None:
    bench = Bench()
    ack_held, release_ack, observed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = 0

    # Track the real post-reconnect native audit without adding or fabricating any charger state.
    async def observe():
        nonlocal calls
        calls += 1
        if calls >= 3:
            observed.set()
        return await bench.observe()

    # Execute the independent firmware update but withhold its actual Accepted wire reply until explicitly released.
    async def hold_reply(action, payload):
        if (
            action == "SetChargingProfile"
            and payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
        ):
            ack_held.set()
            await release_ack.wait()

    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        session = TrialSession(preflight(path), FAST, observe)
        try:
            await session.start()
            async with introduced(bench, session, "boot"):
                bench.cp, bench.connection = "B", "Plug"
            await until(lambda: not session.runtime.status.charger_connected)
            bench.before_reply = hold_reply
            async with bench.connection_to(session) as charger:
                reader = asyncio.create_task(bench.read_charger(charger))
                stop_task = None
                boot_task = None
                try:
                    await until(lambda: session.runtime.status.charger_connected)
                    stop_task = asyncio.create_task(session.command({"command": "stop"}))
                    await asyncio.wait_for(ack_held.wait(), 2)
                    assert session._resource_mutation_inflight()
                    boot_task = asyncio.create_task(bench.call(charger, "boot2", "BootNotification", BOOT))
                    await until(lambda: session.gateway.current_boot_seen(session.gateway._charger_epoch))
                    with pytest.raises(TimeoutError):
                        await asyncio.wait_for(observed.wait(), 0.1)
                    release_ack.set()
                    await stop_task
                    await boot_task
                    await until(lambda: session.recovery.introduced_epoch == session.gateway._charger_epoch)
                    assert observed.is_set() and session.gateway.controls.ready
                    assert session.gateway.controls.desired() == 0 and not session.journal.get("trial_inventory_fault")
                    assert session.journal.get("trial_phase") == "recovering"
                finally:
                    release_ack.set()
                    for task in (stop_task, boot_task):
                        if task is not None and not task.done():
                            task.cancel()
                    await asyncio.gather(
                        *(task for task in (stop_task, boot_task) if task is not None), return_exceptions=True
                    )
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            bench.before_reply = None
            await session.close()
