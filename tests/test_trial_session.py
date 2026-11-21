"""The complete standalone commissioning lifecycle over both real encrypted loopback connections."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import uuid
from contextlib import closing

import pytest
from ocpp_hub_core.frames import build_call
from ocpp_hub_core.prototype.native_http import NativeTransportUnavailableError
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError
from ocpp_hub_core.prototype.trial_session import TrialSession

from tests.charge_rig import owner_rig, positive_flow, ready
from tests.managed_rig import healthy, managed_bench, starts
from tests.prototype_rig import BOOT, START
from tests.runtime_rig import until
from tests.test_rpc_validation_deadlines import NativeAuditPeer
from tests.tls_support import tls as tls


# Intent: keep receiving the charger's actual Stop report when shutdown cannot confirm its protective write.
# Why: a failed write or a changed native session cannot establish that the ledger's transaction has ended.
@pytest.mark.parametrize("failure", ["write_intent", "write_reply", "different_native_session"])
async def test_managed_shutdown_waits_for_genuine_stop_after_station_write_failure(
    tmp_path, tls, unused_tcp_port, monkeypatch, failure
):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        j, controls = b.session.journal, b.session.gateway.controls
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        await healthy(b.controller)
        condition = (
            "OLD.state='pending' AND NEW.state='confirmed'" if failure == "write_reply" else "NEW.state='pending'"
        )
        j.db.execute(
            "CREATE TEMP TRIGGER refuse_shutdown_zero BEFORE UPDATE ON trial_resources "
            "WHEN NEW.key='profile:7001' AND json_extract(NEW.desired,'$.limit')=0 AND "
            + condition
            + " BEGIN SELECT RAISE(ABORT,'shutdown station zero refused'); END"
        )
        if failure == "different_native_session":
            native = b.rig.peer.native

            # Report a different physical transaction while retaining the original journal identity.
            def unrelated_session(key):
                report = native(key)
                if key == "SESSION" and b.rig.bench.firmware.active == local:
                    report["saveData"]["sessionList"][0]["_transactionId"] = local + 1
                return report

            monkeypatch.setattr(b.rig.peer, "native", unrelated_session)
        b.controller.request_stop()
        mark = len(b.rig.bench.device_calls)
        shutdown = asyncio.create_task(b.session.close())
        if failure == "different_native_session":
            await until(lambda: controls._stopping is not None and controls._stopping.done())
        else:
            await until(lambda: controls.stop_acknowledged(local))
        # Acceptance must leave the receiver up while the charger still owes the actual report.
        await asyncio.sleep(0.1)
        assert not shutdown.done() and j.active() == local
        stop = {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100}
        actual = await b.rig.bench.call(b.charger, "delayed-shutdown-stop", "StopTransaction", stop)
        assert actual[0] == 3
        b.rig.peer.permission_enabled = False
        b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
        await shutdown

        commands = b.rig.bench.device_calls[mark:]
        assert [call[3] for call in commands if call[2] == "RemoteStopTransaction"] == (
            [] if failure == "different_native_session" else [{"transactionId": local}]
        )
        assert any(call[2] == "SetChargingProfile" for call in commands) == (failure == "write_reply")
        assert b.session.final_status["shutdown_attempt_uncertain"] is True
        with closing(sqlite3.connect(j.path.as_uri() + "?mode=ro", uri=True)) as reader:
            assert reader.execute("SELECT stop FROM transactions WHERE local=?", (local,)).fetchone()[0] is not None


# Intent: a fresh Boot invalidates a pending shutdown Stop before it can reach the charger.
# Why: the same socket and identical Boot payload can describe a new charger session.
async def test_managed_shutdown_does_not_stop_across_a_repeat_boot(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        await healthy(b.controller)
        original_send = b.charger.send
        g = b.session.gateway
        boot = g.boot_observation
        token = "repeat-boot-during-zero"
        boot_reply = asyncio.get_running_loop().create_future()
        b.rig.bench.replies[token] = boot_reply
        reported = False

        # A real Boot with the same metadata is a distinct observation on the same socket.
        async def boot_before_composite_reply(raw):
            nonlocal reported
            reply = json.loads(raw)
            if (
                not reported
                and reply[0] == 3
                and any(
                    frame[1] == reply[1] and frame[2] == "GetCompositeSchedule" for frame in b.rig.bench.device_calls
                )
            ):
                reported = True
                await original_send(build_call(token, "BootNotification", BOOT))
            await original_send(raw)

        b.controller.request_stop()
        monkeypatch.setattr(b.charger, "send", boot_before_composite_reply)
        mark = len(b.rig.bench.device_calls)
        shutdown = asyncio.create_task(b.session.close())
        assert (await asyncio.wait_for(boot_reply, 2))[0] == 3 and g.boot_observation not in (boot, None)
        controls = g.controls
        await until(lambda: controls._stopping is not None and controls._stopping.done())
        stop = {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100}
        assert (await b.rig.bench.call(b.charger, "stop-after-repeat-boot", "StopTransaction", stop))[0] == 3
        b.rig.peer.permission_enabled = False
        b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
        await shutdown
        assert not [call for call in b.rig.bench.device_calls[mark:] if call[2] == "RemoteStopTransaction"]
        assert b.session.final_status["shutdown_attempt_uncertain"] is True


# Intent: a commissioning Stop cannot target the old session after a new Boot on the same socket.
# Why: an unconfirmed zero write may still permit Stop only while its original Boot remains current.
async def test_commissioning_stop_does_not_cross_a_repeat_boot(tmp_path, tls, unused_tcp_port, monkeypatch):
    from tests.test_managed_boot_supersession import delay_reply
    from tests.test_native_bounded_handover import native_ready

    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, charger):
        await session.command({"command": "release"})
        started = await rig.bench.call(charger, "commissioning-start", "StartTransaction", START)
        assert started[0] == 3 and started[2]["idTagInfo"]["status"] == "Accepted"
        local = started[2]["transactionId"]
        boot = session.gateway.boot_observation
        send, held, entered = delay_reply(monkeypatch, rig, charger, "GetCompositeSchedule")
        mark = len(rig.bench.device_calls)
        task = asyncio.create_task(session.command({"command": "stop"}))
        await asyncio.wait_for(entered.wait(), 3)
        assert (await rig.bench.call(charger, "new-commissioning-boot", "BootNotification", BOOT))[0] == 3
        assert session.gateway.boot_observation not in (None, boot)
        await send(held[0])
        with pytest.raises(UnavailableError):
            await task
        stop = {"transactionId": local, "timestamp": "2026-01-01T00:01:00Z", "meterStop": 1100}
        assert (await rig.bench.call(charger, "actual-commissioning-stop", "StopTransaction", stop))[0] == 3
        assert not any(frame[2] == "RemoteStopTransaction" for frame in rig.bench.device_calls[mark:])


# Intent: close a transaction that reports a previously issued owner Start during shutdown.
# Why: denying new authorizations must still leave enough time to settle a Start already accepted by the charger.
async def test_managed_shutdown_stops_late_transaction_for_already_issued_owner_start(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j, controls = b.controller, b.session.journal, b.session.gateway.controls
        c.command(uuid.uuid4().hex, "start", c.requests.intent().revision)
        await c.charging.settle()
        tag = starts(b.rig)[0][3]["idTag"]
        assert j.get("local_start_receipt")["stage"] == "accepted" and j.active() is None

        c.request_stop()
        shutdown = asyncio.create_task(b.session.close())
        await until(lambda: controls.ready and controls.desired() == 0)
        fresh = await b.rig.bench.call(b.charger, "fresh-authorize-during-shutdown", "Authorize", {"idTag": tag})
        assert fresh[2]["idTagInfo"]["status"] == "Invalid"
        start = await b.rig.bench.call(b.charger, "late-issued-owner-start", "StartTransaction", START | {"idTag": tag})
        assert start[0] == 3 and start[2]["idTagInfo"]["status"] == "Accepted"
        local = start[2]["transactionId"]
        assert j.active() == local and not j.get("control")["start_pending"]
        await until(lambda: controls.stop_acknowledged(local))
        stop = {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100}
        assert (await b.rig.bench.call(b.charger, "stop-late-issued-owner-start", "StopTransaction", stop))[0] == 3
        await shutdown

        assert [call[3] for call in b.rig.bench.device_calls if call[2] == "RemoteStopTransaction"] == [
            {"transactionId": local}
        ]
        assert len(starts(b.rig)) == 1 and b.rig.bench.firmware.effective_limit() == 0
        assert b.session.final_status["shutdown_attempt_uncertain"] is False


# Intent: confirm shutdown from an actual Stop report and independent zero after a lost RPC reply.
# Why: a missing reply leaves the command ambiguous, but the later physical evidence can settle the outcome.
async def test_shutdown_verifies_real_closure_when_only_the_stop_reply_is_lost(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        await healthy(b.controller)
        original_send = b.charger.send
        b.session.gateway.charger.timeout = 0.8

        # Drop only the reply to RemoteStop; keep the charger's independently reported Stop on the wire.
        async def omit_stop_reply(raw):
            reply = json.loads(raw)
            if reply[0] == 3 and any(
                call[1] == reply[1] and call[2] == "RemoteStopTransaction" for call in b.rig.bench.device_calls
            ):
                return
            await original_send(raw)

        monkeypatch.setattr(b.charger, "send", omit_stop_reply)
        b.controller.request_stop()
        shutdown = asyncio.create_task(b.session.close())
        await asyncio.wait_for(b.rig.bench.remote_stop.wait(), 3)
        stop = {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100}
        assert (await b.rig.bench.call(b.charger, "shutdown-reported-stop", "StopTransaction", stop))[0] == 3
        b.rig.peer.permission_enabled = False
        b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
        await shutdown
        assert b.rig.bench.firmware.effective_limit() == 0
        assert b.session.final_status["shutdown_attempt_uncertain"] is False
        with closing(sqlite3.connect(b.session.journal.path.as_uri() + "?mode=ro", uri=True)) as reader:
            row = reader.execute("SELECT value FROM meta WHERE key='trial_shutdown_attempt'").fetchone()
            saved = json.loads(row[0])
            assert saved["local"] == local and saved["state"] == "verified" and saved["boot"]


# Intent: a saved Stop is not enough to verify a final hold while the actual native session still reports open.
# Why: the two device reports can arrive in either order after a genuine charge.
async def test_managed_shutdown_requires_native_closure(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex)
        assert (await healthy(b.controller))["charging_session_open"]
        peer = b.rig.peer
        native = peer.native
        session_report = native("SESSION")
        b.controller.request_stop()
        replay = await b.rig.bench.call(
            b.charger, "replay-during-shutdown", "StartTransaction", START | {"idTag": "NATIVE_TIMER"}
        )
        assert replay[0] == 3 and replay[2]["transactionId"] == local
        stop = {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100}
        assert (await b.rig.bench.call(b.charger, "managed-stop-first", "StopTransaction", stop))[0] == 3
        assert b.session.journal.active() is None

        # Leave this independent channel open past the real shutdown deadline.
        def delayed_closure(key):
            return session_report if key == "SESSION" else native(key)

        monkeypatch.setattr(peer, "native", delayed_closure)
        b.rig.bench.cp, b.rig.bench.connection = "C", "Charging"
        await b.session.close()
        assert b.rig.bench.firmware.effective_limit() == 0
        assert b.session.final_status["shutdown_attempt_uncertain"] is True


# Intent: whole-second device timestamps and a delayed real TLS reply still confirm a presently effective ceiling.
# Why: a one-second request can expire in transit even when the charger accepted and applied the requested limit.
async def test_rounded_composite_survives_reply_delay(tmp_path, tls, unused_tcp_port):
    from tests.test_native_bounded_handover import native_ready

    async with native_ready(tmp_path, tls, unused_tcp_port, composite_reply_delay=1.05) as (rig, session, _):
        await session.command({"command": "release"})
        assert rig.bench.firmware.effective_limit() == 6
        assert (await session.command({"command": "native-bounded-handover"}))["profiles_restored"]


# Intent: shutdown after a preservation failure and verified profile absence sends no new profile or clear.
# Why: the zero-hold recovery exception must not resurrect a trial limit after cleanup has removed it.
async def test_fault_shutdown_does_not_recreate_absent_profile(tmp_path, tls, unused_tcp_port):
    from tests.test_native_bounded_handover import native_ready

    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, _):
        rig.bench.firmware.profiles.clear()
        rig.bench.firmware.plans.clear()
        with pytest.raises(SecurityError):
            session.book.owned_present((await rig.prepared.observer.observe_handover()).physical)
        received = list(rig.bench.received_limits)
        await session.close()
        assert rig.bench.received_limits == received and not rig.bench.firmware.profiles
        assert session.final_status is not None and session.final_status["preservation_failed"]
        assert session.final_status["shutdown_uncertain"]


# Intent: a failed native cleanup audit still permits shutdown zero and the exact known transaction's Stop.
# Why: freezing cleanup before validation must not bypass the session's normal safety shutdown path.
@pytest.mark.parametrize("failure", ["native-guard", "observation"])
async def test_failed_rollback_keeps_safety_shutdown(tmp_path, tls, unused_tcp_port, unused_tcp_port_factory, failure):
    from tests.test_native_bounded_handover import native_ready

    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, charger):
        await session.command({"command": "release"})
        started = await rig.bench.call(charger, "start", "StartTransaction", START)
        local = started[2]["transactionId"]
        rig.bench.firmware.requested_current = 6
        native_port = rig.prepared.observer.port
        if failure == "native-guard":
            rig.peer.phase_fault = True
        else:
            rig.prepared.observer.port = unused_tcp_port_factory()
        try:
            with pytest.raises(SecurityError if failure == "native-guard" else NativeTransportUnavailableError):
                await session.command({"command": "native-bounded-handover"})
        finally:
            rig.peer.phase_fault = False
            rig.prepared.observer.port = native_port
        assert session.journal.get("trial_phase") == "blocked"
        closing = asyncio.create_task(session.close())
        await asyncio.wait_for(rig.bench.remote_stop.wait(), 2)
        assert rig.bench.firmware.current == 0
        await rig.bench.call(
            charger,
            "stop",
            "StopTransaction",
            {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1000},
        )
        await closing
        assert session.final_status is not None and not session.final_status["shutdown_uncertain"]


# Intent: native commissioning keeps actual authorization, Start, Stop and cleanup through both TLS legs.
# Why: retiring a session mode must preserve authentication and station-kind coverage of the shared transport.
@pytest.mark.parametrize("incoming_mode", ["basic", "none"])
@pytest.mark.parametrize("profile_kind", ["Relative", "Absolute"])
async def test_trial_session_full_lifecycle_and_verified_rollback(
    tmp_path, tls, unused_tcp_port, incoming_mode, profile_kind
):
    from ocpp_hub_core.prototype.station import StationChoice

    from tests.test_native_bounded_handover import native_ready, restore_original_route, verify_original_backend

    choice = StationChoice(profile_kind, 1, "2020-01-01T00:00:00Z" if profile_kind == "Absolute" else None)
    async with native_ready(tmp_path, tls, unused_tcp_port, station=choice, incoming_mode=incoming_mode) as (
        rig,
        session,
        charger,
    ):
        bench = rig.bench
        status = await session.command({"command": "status"})
        assert status["effective_ceiling_confirmed"] and status["boot_observed"]
        await session.command({"command": "start"})
        assert (await bench.call(charger, "auth", "Authorize", {"idTag": "LOCAL_TEST"}))[2]["idTagInfo"][
            "status"
        ] == "Accepted"
        local = (await bench.call(charger, "start", "StartTransaction", START))[2]["transactionId"]
        await until(lambda: session.journal.vendor_id(local) == 9001)
        bench.cp, bench.connection = "C", "Charging"
        bench.firmware.requested_current = 32
        rig.peer.paused = False
        assert bench.firmware.current == 6
        stopped = await session.command({"command": "stop"})
        assert stopped["transaction_still_open"] and bench.firmware.current == 0
        await bench.call(
            charger,
            "stop",
            "StopTransaction",
            {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1000},
        )
        await until(lambda: session.journal.next_delivery() is None)
        assert not (await session.command({"command": "native-bounded-handover"}))["complete"]
        assert 7001 not in bench.firmware.profiles
        with pytest.raises(RecoveryRequiredError, match="original route"):
            await session.native_bounded.observe_backend()
        await restore_original_route(rig, session, charger)
        await verify_original_backend(rig, session)

        # Apply only the explicit synthetic native patch; verification still comes through real HTTP.
        async def patch(values):
            rig.peer.configuration.update(values)
            rig.peer.runtime_user = int(rig.peer.configuration["AmpLmt"]) * 10

        assert (await session.native_bounded.restore_settings(patch))["complete"]
        assert not session.gateway.failures


# Intent: an older baseline containing an inactive original profile blocks native admission before any listener.
# Why: parsing retained evidence cannot grant permission for unproven selective cleanup beside original profiles.
async def test_original_profile_blocks_fresh_trial_before_binding(tmp_path, tls, unused_tcp_port):
    from dataclasses import replace

    from ocpp_hub_core import commands

    from tests.connected_rig import connected_bench

    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        rig.bench.firmware.answer(
            "SetChargingProfile", commands.set_charging_profile(1, commands.PURPOSE_TX, 32, 42, transaction_id=77)
        )
        native = await rig.prepared.observer.observe_handover()
        # Model an existing saved baseline without weakening the new profile-free capture function.
        rig.prepared.baseline = replace(rig.prepared.baseline, resources=native.physical.resources)
        assert rig.prepared.summary()["new_profile_writes_blocked"]
        session = rig.session()
        with pytest.raises(SecurityError):
            await session.start()
        assert not session.started and not session.runtime.status.charger_connected
        assert not session.runtime.status.vendor_connected and not rig.bench.received_profiles
        assert session.journal.get("trial_phase") == "prepared"


# Intent: refusing a changed commissioning specification leaves the previous delivery evidence untouched.
# Why: the original installation owns restart decisions about a report whose reply was not recorded.
async def test_different_commissioning_spec_does_not_recover_the_existing_journal(tmp_path, tls, unused_tcp_port):
    async with managed_bench(tmp_path, tls, unused_tcp_port) as rig:
        path, identity = rig.prepared.config.journal, rig.prepared.config.identity
        with closing(Journal(path, identity)) as j:
            j.put("native_bounded_handover", {"stage": "admitted", "spec": rig.spec.value()})
            token = j.enqueue("StartTransaction", START)
            j.db.execute("UPDATE outbox SET state='inflight' WHERE token=?", (token,))
        other_boot = "1" * 64 if rig.spec.boot_digest != "1" * 64 else "2" * 64
        changed = rig.spec.parse(rig.spec.value() | {"boot_digest": other_boot})
        with pytest.raises(SecurityError, match="specification changed"):
            TrialSession(rig.prepared, native_bounded=changed)
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as stored:
            assert stored.execute("SELECT state FROM outbox WHERE token=?", (token,)).fetchone()[0] == "inflight"


# Intent: shutdown can verify a genuine closed zero after one unavailable native response.
# Why: retrying the read inside the existing deadline can complete the result without another charger command.
async def test_shutdown_retries_one_unavailable_native_confirmation(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch, native_peer=NativeAuditPeer) as b:
        c, peer = b.controller, b.rig.peer
        assert (await healthy(c))["charging_session_open"] is False
        assert not b.session.journal.active()
        first = len(b.rig.bench.device_calls)
        peer.arm = "503"
        await b.session.close()
        assert peer.selected_count == 1 and peer.selected_request["action"] == "getConfig"
        assert b.session.final_status["shutdown_attempt_uncertain"] is False
        assert b.rig.bench.firmware.effective_limit() == 0
        wire = b.rig.bench.device_calls[first:]
        assert [frame[2] for frame in wire if frame[2] in ("SetChargingProfile", "RemoteStopTransaction")] == [
            "SetChargingProfile"
        ]
