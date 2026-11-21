"""Vendor dispatch and confirmed profile ownership against independent TLS peers."""

import asyncio

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.frames import build_call, build_call_error, build_call_result
from ocpp_hub_core.prototype.connection_scope import current_control_scope
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.managed_diagnostics import snapshot
from ocpp_hub_core.prototype.managed_view import poll_base
from ocpp_hub_core.prototype.policy import DeniedError
from ocpp_hub_core.prototype.recovery_contracts import GateDecision
from ocpp_hub_core.prototype.security import TRANSPORT_LOG, SecurityError
from websockets.asyncio.server import serve

from tests.charge_rig import positive_flow, ready
from tests.managed_rig import healthy
from tests.prototype_rig import BOOT, START
from tests.runtime_rig import Bench, until
from tests.tls_support import admission as admission
from tests.tls_support import tls as tls
from tests.trial_peer import Bench as TrialBench


# Intent: retain the real charger rejection through repeated refusals of its journal publication.
# Why: the issued grant and its result must settle together, without another physical command.
@pytest.mark.recovery_replay
@pytest.mark.parametrize("failures,newer_pause,slow_send", [(1, False, True), (2, False, False), (2, True, False)])
async def test_cloud_start_rejection_survives_failed_outcome_writes(
    tmp_path, tls, unused_tcp_port, monkeypatch, failures, newer_pause, slow_send
):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        journal, bench = b.session.journal, b.rig.bench
        sender = b.charger.send
        reached, release = asyncio.Event(), asyncio.Event()
        drained = asyncio.Event()
        writes = 0

        # Change only the synthetic charger's actual reply after its independent receiver records the request.
        async def reject_start(raw):
            frame = decode(raw)
            if frame[0] == 3 and any(
                call[1] == frame[1] and call[2] == "RemoteStartTransaction" for call in bench.device_calls
            ):
                reached.set()
                if newer_pause or slow_send:
                    await release.wait()
                raw = build_call_result(frame[1], {"status": "Rejected"})
            await sender(raw)

        charger = b.session.gateway.charger
        wire_send = charger.sender
        assert wire_send is not None

        # The real receiver can see the rejection before the outgoing send finishes draining.
        async def drain_start(raw):
            await wire_send(raw)
            frame = decode(raw)
            if frame[0] == 2 and frame[2] == "RemoteStartTransaction":
                await drained.wait()

        # Refuse the specified writes of this definite outcome in the real transaction.
        def refuse_write():
            nonlocal writes
            writes += 1
            return writes <= failures

        monkeypatch.setattr(b.charger, "send", reject_start)
        if slow_send:
            monkeypatch.setattr(charger, "sender", drain_start)
        journal.db.create_function("refuse_start_result", 0, refuse_write)
        journal.db.execute(
            "CREATE TEMP TRIGGER fail_start_result BEFORE UPDATE ON vendor_command_scope "
            "WHEN NEW.wire_result='Rejected' AND refuse_start_result() "
            "BEGIN SELECT RAISE(ABORT,'synthetic journal refusal'); END"
        )
        if newer_pause:
            journal.db.execute(
                "CREATE TEMP TRIGGER fail_start_reply_cache BEFORE UPDATE OF response ON inbound_receipts "
                "WHEN NEW.side='vendor' AND NEW.token='record-rejected-start' "
                "BEGIN SELECT RAISE(ABORT,'synthetic response cache refusal'); END"
            )
        request = asyncio.create_task(
            bench.cloud_call("record-rejected-start", "RemoteStartTransaction", {"idTag": "CLOUD_OWNER"})
        )
        try:
            await asyncio.wait_for(reached.wait(), 2)
            if newer_pause:
                b.controller.command("a" * 32, "pause", b.controller.requests.intent().revision)
            if slow_send:
                auth = b.session.gateway.authorisation
                assert auth.allows("CLOUD_OWNER")
                release.set()
                await until(lambda: not auth.allows("CLOUD_OWNER"))
                assert not request.done()
        finally:
            release.set()
            drained.set()
        reply = await request
        assert reply == [3, "record-rejected-start", {"status": "Rejected"}]
        await b.controller.tick()
        assert writes == failures + 1
        assert not journal.get("vendor_start_grant") and not journal.get("uncertain_vendor_command")
        assert journal.db.execute("SELECT wire_result FROM vendor_command_scope").fetchone()[0] == "Rejected"
        assert sum(call[2] == "RemoteStartTransaction" for call in bench.device_calls) == 1
        if newer_pause:
            assert (
                journal.db.execute(
                    "SELECT response FROM inbound_receipts WHERE side='vendor' AND token=?", (reply[1],)
                ).fetchone()[0]
                is None
            )
            await bench.cloud.send(build_call(reply[1], "RemoteStartTransaction", {"idTag": "CLOUD_OWNER"}))
            await until(lambda: sum(frame == reply for frame in bench.calls) == 2)
            assert sum(call[2] == "RemoteStartTransaction" for call in bench.device_calls) == 1
            assert b.controller.requests.intent().mode == "paused"
            assert bench.firmware.effective_limit() == 0
            journal.db.execute("DROP TRIGGER fail_start_reply_cache")
            await b.controller.tick()
            saved = journal.db.execute(
                "SELECT response FROM inbound_receipts WHERE side='vendor' AND token=?", (reply[1],)
            ).fetchone()[0]
            assert decode(saved) == reply


# Intent: the accepted cloud Stop remains known even when its final mutation update repeatedly fails.
# Why: the actual closure report and the accepted command are different facts, and each owns one record.
@pytest.mark.recovery_replay
async def test_cloud_stop_keeps_reply_while_waiting_for_storage_and_closure(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        journal, bench = b.session.journal, b.rig.bench
        local = await positive_flow(b, "daily")
        cloud_id = journal.vendor_id(local)
        assert cloud_id is not None
        refused = 0

        # The saved answer has its own transaction. Refuse only the later mutation update.
        def attempt():
            nonlocal refused
            refused += 1
            return 1

        journal.db.create_function("refuse_stop_completion", 0, attempt)
        journal.db.execute(
            "CREATE TEMP TRIGGER refuse_stop_completion BEFORE UPDATE ON mutations "
            "WHEN NEW.action='RemoteStopTransaction' AND NEW.state='confirmed' AND refuse_stop_completion() "
            "BEGIN SELECT RAISE(ABORT,'synthetic journal refusal'); END"
        )
        try:
            reply = await bench.cloud_call(
                "durable-accepted-stop", "RemoteStopTransaction", {"transactionId": cloud_id}
            )
            assert reply == [3, "durable-accepted-stop", {"status": "Accepted"}] and refused >= 2
            row = journal.db.execute(
                "SELECT state,wire_result,local FROM mutations JOIN vendor_command_scope ON mutation=token"
            ).fetchone()
            assert tuple(row) == ("pending", "Accepted", local)
            assert not journal.get("uncertain_vendor_command") and journal.active() == local
            mutation = journal.db.execute("SELECT token FROM mutations").fetchone()[0]
            outcome = journal.outcomes.vendor(mutation)
            assert outcome.status == "Accepted" and not outcome.settled and outcome.saving_reply
            assert outcome.session.local == local and not outcome.session.closed
            status = poll_base(b.controller)
            assert status["vendor_reply_storage_pending"] == 1 and status["vendor_write_uncertain"] == 0
            assert status["operator_reconciliation_required"] is False
            blockers = snapshot(b.controller, status)["blockers"]
            assert "vendor_reply_storage_pending" in blockers and "command_outcome_unknown" not in blockers
        finally:
            journal.db.execute("DROP TRIGGER refuse_stop_completion")
        await b.controller.tick()
        assert journal.db.execute("SELECT state FROM mutations").fetchone()[0] == "confirmed"
        assert journal.active() == local
        stopped = await bench.call(
            b.charger,
            "closure-after-ack",
            "StopTransaction",
            {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100},
        )
        assert stopped[0] == 3 and journal.active() is None
        outcome = journal.outcomes.vendor(mutation)
        assert outcome.settled and outcome.status == "Accepted" and outcome.session.closed
        b.rig.peer.permission_enabled = False
        await until(lambda: journal.next_delivery() is None)
        assert sum(call[2] == "RemoteStopTransaction" for call in bench.device_calls) == 1


# Intent: the same live charger can settle its exact reply after the request deadline.
# Why: normal waiting, a missing answer, and a received answer still being saved have different outcomes.
@pytest.mark.recovery_replay
@pytest.mark.parametrize(
    "action,poll_first",
    [("RemoteStartTransaction", False), ("RemoteStartTransaction", True), ("RemoteStopTransaction", False)],
)
async def test_late_cloud_command_reply_settles_exactly_once(
    tmp_path, tls, unused_tcp_port, monkeypatch, action, poll_first
):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        g, j, bench = b.session.gateway, b.session.journal, b.rig.bench
        g.charger.timeout = 0.25
        entered = asyncio.Event()
        original_answer = bench.firmware.answer
        send = b.charger.send
        delayed = None
        local = None
        status = "Rejected"
        payload = {"idTag": "CLOUD_OWNER"}
        if action == "RemoteStopTransaction":
            local = await positive_flow(b, "daily")
            payload = {"transactionId": j.vendor_id(local)}
            status = "Accepted"

        # Delay one answer while leaving the actual charger reader free to apply protective zero.
        async def delay_reply(raw):
            nonlocal delayed
            frame = decode(raw)
            if (
                frame[0] == 3
                and any(call[1] == frame[1] and call[2] == action for call in bench.device_calls)
                and delayed is None
            ):
                delayed = raw
                entered.set()
                return
            await send(raw)

        # A rejected Start releases only its grant. An accepted Stop still awaits closure.
        def answer(name, command):
            actual = original_answer(name, command)
            return {"status": status} if name == action else actual

        monkeypatch.setattr(b.charger, "send", delay_reply)
        monkeypatch.setattr(bench.firmware, "answer", answer)
        j.db.execute(
            "CREATE TEMP TRIGGER hold_late_reply BEFORE UPDATE ON vendor_command_scope "
            "WHEN NEW.wire_result IN ('Accepted','Rejected') BEGIN SELECT RAISE(ABORT,'synthetic reply refusal'); END"
        )
        if poll_first:
            j.db.execute(
                "CREATE TEMP TRIGGER hold_timeout_write BEFORE UPDATE ON mutations "
                "WHEN NEW.state='uncertain' BEGIN SELECT RAISE(ABORT,'synthetic timeout refusal'); END"
            )
        task = asyncio.create_task(bench.cloud_call("delayed-result", action, payload))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            initial = poll_base(b.controller)
            assert initial["vendor_write_uncertain"] == 0 and not initial["operator_reconciliation_required"]
            expired = await task
            assert expired[:3] == [4, "delayed-result", "InternalError"]
            assert poll_base(b.controller)["vendor_write_uncertain"] == 1
            if poll_first:
                assert j.db.execute("SELECT state FROM mutations").fetchone()[0] == "pending"
                await b.controller.tick()
                assert j.fault.read({}).get("forwarded_timeout")
                await b.controller.tick()
                assert bench.firmware.effective_limit() == 0
            assert delayed is not None
            await send(delayed)
            await until(lambda: poll_base(b.controller)["vendor_reply_storage_pending"] == 1)
            evidence = poll_base(b.controller)
            assert not evidence["operator_reconciliation_required"] and not evidence["cloud_start_pending"]
            blockers = snapshot(b.controller, evidence)["blockers"]
            assert "vendor_reply_storage_pending" in blockers
            assert "cloud_start_pending" not in blockers and "command_outcome_unknown" not in blockers
            if not poll_first:
                await b.controller.tick()
                assert not j.fault.latched()
        finally:
            if poll_first:
                j.db.execute("DROP TRIGGER hold_timeout_write")
            j.db.execute("DROP TRIGGER hold_late_reply")
            await asyncio.gather(task, return_exceptions=True)
        await b.controller.tick()
        row = j.db.execute("SELECT state,wire_result FROM mutations JOIN vendor_command_scope ON token=mutation")
        assert tuple(row.fetchone()) == ("confirmed", status)
        assert not j.get("uncertain_vendor_command") and not j.get("vendor_start_grant")
        assert j.active() == local and sum(frame[2] == action for frame in bench.device_calls) == 1
        if poll_first:
            assert not j.fault.latched()
            restored = await healthy(b.controller)
            assert restored["desired_control"] == "app" and restored["station_ceiling_a"] == 6


# Intent: an Accepted cloud Start can bind its real report under a newer verified Pause.
# Why: the late lifecycle belongs in history even while storage cannot finish the command receipt.
async def test_cloud_start_report_preserves_history_under_pause_while_reply_saves(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        j, g, c, bench = b.session.journal, b.session.gateway, b.controller, b.rig.bench
        j.db.execute(
            "CREATE TEMP TRIGGER defer_accepted_start BEFORE UPDATE ON mutations "
            "WHEN NEW.action='RemoteStartTransaction' AND NEW.state='confirmed' "
            "BEGIN SELECT RAISE(ABORT,'synthetic start settlement refusal'); END"
        )
        try:
            reply = await bench.cloud_call("accepted-before-pause", "RemoteStartTransaction", {"idTag": "CLOUD_OWNER"})
            assert reply == [3, "accepted-before-pause", {"status": "Accepted"}]
            c.command("b" * 32, "pause", c.requests.intent().revision)
            await until(lambda: g.controls.ready and bench.firmware.effective_limit() == 0)
            paused = c.requests.intent()
            report = await bench.call(b.charger, "start-at-zero", "StartTransaction", START | {"idTag": "CLOUD_OWNER"})
            assert report[0] == 3 and report[2]["idTagInfo"]["status"] == "Accepted"
            assert j.active() == report[2]["transactionId"]
            mutation = j.get("vendor_start_grant", {})["mutation"]
            outcome = j.outcomes.vendor(mutation)
            assert outcome.status == "Accepted" and outcome.session.local == j.active() and not outcome.session.closed
            assert c.requests.intent() == paused and paused.mode == "paused"
            assert bench.firmware.effective_limit() == 0
        finally:
            j.db.execute("DROP TRIGGER defer_accepted_start")
        await c.tick()
        assert sum(frame[2] == "RemoteStartTransaction" for frame in bench.device_calls) == 1


# Intent: a fresh charger report can wait for the exact Boot response that preceded it.
# Why: the charger can read the genuine Boot response before the sending coroutine finishes.
async def test_start_report_waits_for_its_boot_send_to_finish(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        g, j, bench = b.session.gateway, b.session.journal, b.rig.bench
        sender = g.charger.sender
        assert sender is not None
        finish_boot = asyncio.Event()

        # Deliver the Boot response on the real socket while deliberately holding only its normal return.
        async def drain_boot(raw):
            await sender(raw)
            if decode(raw)[:2] == [3, "boot-send-finishing"]:
                await finish_boot.wait()

        monkeypatch.setattr(g.charger, "sender", drain_boot)
        report = None
        try:
            boot = await bench.call(b.charger, "boot-send-finishing", "BootNotification", BOOT)
            assert boot[0] == 3 and boot[2]["status"] == "Accepted"
            assert g.boot_observation is None
            report = asyncio.create_task(
                bench.call(b.charger, "fresh-after-boot", "StartTransaction", START | {"idTag": "NATIVE_TIMER"})
            )
            await until(lambda: len(g._tasks) >= 2)
            assert j.active() is None and not report.done()
            finish_boot.set()
            answer = await report
            assert answer[0] == 3 and answer[2]["idTagInfo"]["status"] == "Accepted"
            assert j.active() == answer[2]["transactionId"]
        finally:
            finish_boot.set()
            if report is not None:
                await asyncio.gather(report, return_exceptions=True)


# Intent: repeated local write failures retain the original Set and Clear answers in the managed resource path.
# Why: the proxy can finish ownership after storage returns, with no repeat transmission and no invented clear proof.
async def test_cloud_profile_answers_survive_refused_resource_writes(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        j, bench = b.session.journal, b.rig.bench
        profile = commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 111, stack_level=2)
        for action, expected, payload in (
            ("SetChargingProfile", "confirmed", profile),
            ("ClearChargingProfile", "clear_acknowledged", {"id": 111}),
        ):
            attempts = []

            # Refuse publication of the actual resource acknowledgement as often as the proxy tries.
            def refuse():
                attempts.append(True)
                return 1

            j.db.create_function("refuse_profile_ack", 0, refuse)
            j.db.execute(
                f"CREATE TEMP TRIGGER refuse_profile_ack BEFORE UPDATE ON trial_resources "
                f"WHEN NEW.key!='profile:7001' AND NEW.state='{expected}' AND refuse_profile_ack() "
                "BEGIN SELECT RAISE(ABORT,'synthetic profile acknowledgement refusal'); END"
            )
            try:
                answer = await bench.cloud_call(action, action, payload)
                assert answer == [3, action, {"status": "Accepted"}] and attempts
                initial_attempts = len(attempts)
                g = b.session.gateway
                g.decide(g.retry_reply_writes)
                assert len(attempts) == initial_attempts + 1
                counts = poll_base(b.controller)
                assert counts["vendor_reply_storage_pending"] == 1 and counts["vendor_write_uncertain"] == 0
            finally:
                j.db.execute("DROP TRIGGER refuse_profile_ack")
            await b.controller.tick()
            row = j.db.execute(
                "SELECT state,wire_result,wire_request,profile FROM mutations "
                "JOIN vendor_command_scope ON token=mutation WHERE action=?",
                (action,),
            ).fetchone()
            assert tuple(row)[:2] == ("confirmed", "Accepted")
            assert sum(call[1] == row["wire_request"] for call in bench.device_calls) == 1
            assert (row["profile"] in bench.firmware.profiles) is (action == "SetChargingProfile")


# Intent: an accepted profile replacement without a transaction remains clearable after the old session ends.
# Why: removing the old transaction association must not discard the mapping for the live replacement.
@pytest.mark.parametrize("tx_to_default", [True, False])
@pytest.mark.parametrize("replacement_status", ["Accepted", "Rejected", "malformed", "CALLERROR"])
@pytest.mark.parametrize("stop_before_reply", [False, True])
async def test_replaced_transaction_profile_keeps_mapping(
    tmp_path, tls, admission, tx_to_default, replacement_status, stop_before_reply
):
    bench = Bench(tmp_path / "journal.db", tls, admission)
    replies = {}
    replacement_seen, release_replacement = asyncio.Event(), asyncio.Event()
    profile_calls = 0
    pending_replies = []

    async with serve(
        bench.vendor, "127.0.0.1", 0, ssl=bench.server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
    ) as cloud:
        await bench.start(cloud.sockets[0].getsockname()[1])
        try:
            async with bench.charger() as charger:
                await bench.boot(charger)
                await until(lambda: bench.gateway.vendor_registration == "Accepted")

                # Delay only the acknowledgement; independent lifecycle replies must keep flowing.
                async def replacement_reply(frame, actual):
                    await release_replacement.wait()
                    if replacement_status == "CALLERROR":
                        await charger.send(build_call_error(frame[1], "InternalError", "unknown outcome"))
                    else:
                        body = {"unexpected": "reply"} if replacement_status == "malformed" else actual
                        await charger.send(build_call_result(frame[1], body))

                # Apply accepted commands at receipt, independently from their delayed acknowledgements.
                async def device_reader():
                    nonlocal profile_calls
                    async for raw in charger:
                        frame = decode(raw)
                        if frame[0] == 2:
                            if (
                                frame[2] == "SetChargingProfile"
                                and frame[3]["csChargingProfiles"]["chargingProfileId"] != 7001
                            ):
                                profile_calls += 1
                                if profile_calls == 2:
                                    actual = (
                                        {"status": "Rejected"}
                                        if replacement_status == "Rejected"
                                        else (bench.firmware.answer(frame[2], frame[3]))
                                    )
                                    pending_replies.append(asyncio.create_task(replacement_reply(frame, actual)))
                                    replacement_seen.set()
                                    continue
                            await charger.send(build_call_result(frame[1], bench.firmware.answer(frame[2], frame[3])))
                        else:
                            replies[frame[1]] = frame

                # Wait for a real correlated reply on the charger socket.
                async def local_call(token, action, body):
                    await charger.send(build_call(token, action, body))
                    await until(lambda: token in replies)
                    return replies[token]

                # Wait for a real correlated reply on the cloud socket.
                async def vendor_call(token, action, body):
                    await bench.connections[-1].send(build_call(token, action, body))
                    await until(lambda: any(f[0] != 2 and f[1] == token for f in bench.calls))
                    return next(f for f in bench.calls if f[0] != 2 and f[1] == token)

                reader = asyncio.create_task(device_reader())
                try:
                    opened = await local_call("open", "StartTransaction", START)
                    local = opened[2]["transactionId"]
                    bench.firmware.active = local
                    await until(lambda: bench.store.vendor_id(local) is not None)
                    vendor_id = bench.store.vendor_id(local)
                    initial_purpose = commands.PURPOSE_TX if tx_to_default else commands.PURPOSE_TX_DEFAULT
                    new_purpose = commands.PURPOSE_TX_DEFAULT if tx_to_default else commands.PURPOSE_TX
                    original = commands.set_charging_profile(
                        1,
                        initial_purpose,
                        6,
                        101,
                        **({"transaction_id": vendor_id} if tx_to_default else {}),
                    )
                    assert (await vendor_call("first-profile", "SetChargingProfile", original))[2] == {
                        "status": "Accepted"
                    }
                    mapped = bench.store.get("vendor_profiles")["101"]
                    replacement = commands.set_charging_profile(
                        1,
                        new_purpose,
                        0,
                        101,
                        **({"transaction_id": vendor_id} if not tx_to_default else {}),
                    )
                    replacing = asyncio.create_task(
                        vendor_call("replacement-profile", "SetChargingProfile", replacement)
                    )
                    await asyncio.wait_for(replacement_seen.wait(), 1)

                    # A real lifecycle may arrive while the command's real reply remains in flight.
                    async def stop():
                        stopped = await local_call(
                            "closed",
                            "StopTransaction",
                            {"transactionId": local, "meterStop": 1000, "timestamp": START["timestamp"]},
                        )
                        assert stopped[0] == 3
                        bench.firmware.active = None

                    if stop_before_reply:
                        await stop()
                    release_replacement.set()
                    result = await replacing
                    if replacement_status in ("Accepted", "Rejected"):
                        assert result[0] == 3 and result[2] == {"status": replacement_status}
                    else:
                        assert result[0] == 4
                    if not stop_before_reply:
                        if replacement_status != "Accepted":
                            assert bench.store.get("vendor_profile_transactions", {}).get("101") == (
                                local if tx_to_default else None
                            )
                        await stop()
                    live_default = (
                        replacement_status == "Accepted"
                        and tx_to_default
                        or replacement_status == "Rejected"
                        and not tx_to_default
                    )
                    if replacement_status in ("malformed", "CALLERROR"):
                        assert bench.store.db.execute("SELECT 1 FROM mutations WHERE state='uncertain'").fetchone()
                    elif live_default:
                        assert bench.store.get("vendor_profiles")["101"] == mapped
                        assert "101" not in bench.store.get("vendor_profile_transactions", {})
                        assert bench.firmware.profiles[mapped]["chargingProfilePurpose"] == "TxDefaultProfile"
                        cleared = await vendor_call("clear-replacement", "ClearChargingProfile", {"id": 101})
                        assert cleared[0] == 3 and cleared[2]["status"] == "Accepted"
                        assert mapped not in bench.firmware.profiles
                    else:
                        assert "101" not in bench.store.get("vendor_profiles", {})
                finally:
                    release_replacement.set()
                    await asyncio.gather(*pending_replies, return_exceptions=True)
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            await bench.close()


# Intent: refusal at the final resource gate does not quarantine a command never dispatched.
# Why: an unsupported profile must not permanently disable every later valid cloud write.
async def test_unsent_resource_refusal_does_not_poison_vendor_writes(tmp_path, tls, unused_tcp_port, monkeypatch):
    from tests.test_native_bounded_handover import native_ready

    connections = []
    original_vendor = TrialBench.vendor

    # Expose the independent real cloud socket without replacing its protocol answers.
    async def vendor(self, connection):
        connections.append(connection)
        await original_vendor(self, connection)

    monkeypatch.setattr(TrialBench, "vendor", vendor)
    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, charger):
        bench = rig.bench
        await session.command({"command": "release"})

        # Submit real cloud requests and observe their correlated results on the wire.
        async def vendor_call(token, body):
            await connections[-1].send(build_call(token, "SetChargingProfile", body))
            await until(lambda: any(f[0] != 2 and f[1] == token for f in bench.calls))
            return next(f for f in bench.calls if f[0] != 2 and f[1] == token)

        before = len(bench.device_calls)
        unsupported = commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 111)
        unsupported["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"].append(
            {"startPeriod": 60, "limit": 6}
        )
        denied = await vendor_call("unsupported-profile", unsupported)
        assert denied[0] == 4 and denied[2] == "SecurityError"
        assert len(bench.device_calls) == before, "the resource rejection happened before physical dispatch"
        rows = [dict(row) for row in session.journal.db.execute("SELECT action,state FROM mutations")]
        result = await vendor_call(
            "supported-profile", commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 112)
        )
        print("UNSENT_MUTATIONS", rows)
        print("LATER_SUPPORTED_RESULT", result)
        print("DEVICE_CALLS_DURING_BOTH_REQUESTS", bench.device_calls[before:])
        assert result[0] == 3 and result[2]["status"] == "Accepted"


# Intent: interrupted validation retires prepared-only grants; attempted transport retains its real debt.
# Why: neither validation cancellation nor a lost acknowledgement proves that Start reached the device.
@pytest.mark.parametrize("after_dispatch", [False, True], ids=["before-dispatch", "after-dispatch"])
@pytest.mark.parametrize("failure", ["cancel", "denied", "native-security"])
async def test_cancelled_vendor_start_retains_only_possible_dispatch(
    tmp_path, tls, unused_tcp_port, after_dispatch, failure
):
    from tests.test_native_bounded_handover import native_ready

    async with native_ready(tmp_path, tls, unused_tcp_port) as (rig, session, charger):
        await session.command({"command": "release"})
        entered, release = asyncio.Event(), asyncio.Event()
        reserved_grants = []
        task = None
        try:
            gateway = session.gateway
            if after_dispatch:
                sender = gateway.charger.sender
                assert sender is not None

                # The actual socket send occurs; only its coroutine's return is delayed.
                async def blocked_sender(raw):
                    await sender(raw)
                    reserved_grants.append(bool(session.journal.get("vendor_start_grant")))
                    entered.set()
                    if failure != "cancel":
                        error = DeniedError if failure == "denied" else SecurityError
                        raise error("injected validation or transport failure")
                    await release.wait()

                gateway.charger.sender = blocked_sender
            else:
                validate = gateway.charger.validate_send

                # Keep the real native validation result, delaying its return before physical dispatch.
                async def blocked_validation(action, payload) -> GateDecision | None:
                    decision = None
                    if validate is not None:
                        decision = await validate(action, payload)
                    reserved_grants.append(bool(session.journal.get("vendor_start_grant")))
                    entered.set()
                    if failure != "cancel":
                        error = DeniedError if failure == "denied" else SecurityError
                        raise error("injected validation or transport failure")
                    await release.wait()
                    return decision

                gateway.charger.validate_send = blocked_validation
            frame = [2, "cancelled-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": "LOCAL_TEST"}]
            task = asyncio.create_task(
                gateway._vendor_call(frame, gateway._vendor_epoch, current_control_scope(gateway))
            )
            await asyncio.wait_for(entered.wait(), 2)
            assert reserved_grants == [True]
            assert gateway.authorisation.allows("LOCAL_TEST") is after_dispatch
            if failure == "cancel":
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            elif failure == "native-security":
                answer = decode(await task)
                assert answer[0] == 4 and answer[2] == "InternalError"
            else:
                assert decode(await task)[0] == 4
            rows = session.journal.db.execute("SELECT state FROM mutations WHERE origin='vendor'").fetchall()
            assert [row[0] for row in rows] == (["uncertain"] if after_dispatch else [])
            assert bool(session.journal.get("vendor_start_grant")) is after_dispatch
            assert bool(session.journal.get("uncertain_vendor_command")) is after_dispatch
        finally:
            release.set()
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
