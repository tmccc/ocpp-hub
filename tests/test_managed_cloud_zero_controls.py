"""Real managed TLS commands translate the current cloud mapping, including sequential reuse of zero."""

import asyncio
import json
import uuid

import pytest
from ocpp_hub_core.commands import PURPOSE_TX, set_charging_profile

from tests import managed_rig as mp
from tests.charge_rig import ChargeRig, positive_flow
from tests.cloud_start_helpers import StartReplyBench
from tests.native_peer_failures import InterruptedNativePeer
from tests.runtime_rig import until
from tests.test_managed_boot_supersession import delay_reply
from tests.test_managed_stop_reply_recovery import running
from tests.tls_support import tls as tls
from tests_model.test_start_recovery_sequences import close_daily

COMMANDS = (("setTransaction", {"eco": "Fast"}), ("delayStop", {"stopTime": "2030-01-01T00:00:00Z"}), ("delayStop", {}))


# Exercise the real cloud socket and preserve only the independently observed proprietary charger writes.
async def commands(rig, vendor, expected_local):
    for message, extra in COMMANDS:
        before = [f for f in rig.bench.device_calls if f[2] == "DataTransfer"]
        payload = {
            "vendorId": "IoCharger",
            "messageId": message,
            "data": json.dumps({"transactionId": vendor, **extra}),
        }
        result = await rig.bench.cloud_call(uuid.uuid4().hex, "DataTransfer", payload)
        after = [f for f in rig.bench.device_calls if f[2] == "DataTransfer"]
        if expected_local is None:
            assert result[0] == 4 and after == before
        else:
            assert result[0] == 3 and result[2] == {"status": "Accepted", "data": ""}
            assert len(after) == len(before) + 1
            assert after[-1][3]["messageId"] == message
            assert json.loads(after[-1][3]["data"]) == {"transactionId": expected_local, **extra}


# Intent: Fast and delayed-stop set/cancel use only the current open mapping, including cloud ID zero and reuse.
# Why: validating the cloud namespace as a positive native ID rejects supported app controls before translation.
@pytest.mark.parametrize("vendor", [0, 1])
async def test_managed_cloud_controls_translate_zero_and_reused_mapping(
    tmp_path, tls, unused_tcp_port, monkeypatch, vendor
):
    async with mp.managed_bench(tmp_path, tls, unused_tcp_port, bench_type=StartReplyBench) as rig:
        rig.bench.fixed_vendor = vendor
        async with running(rig, "cloud-mapping-controls") as (session, controller, charger):
            await mp.healthy(controller)
            b = ChargeRig(rig, session, controller, charger)
            await commands(rig, vendor, None)
            locals_seen = []
            for number in range(2):
                local = await positive_flow(
                    b,
                    "daily",
                    call_id=uuid.uuid4().hex,
                    meter_start=1000 + 200 * number,
                    timestamp=f"2026-01-01T00:{number * 2:02d}:00Z",
                )
                locals_seen.append(local)
                assert session.journal.vendor_id(local) == vendor and local > 0
                await mp.healthy(controller)
                await commands(rig, vendor, local)
                request = uuid.uuid4().hex
                stopped = await rig.bench.cloud_call(request, "RemoteStopTransaction", {"transactionId": vendor})
                assert stopped[0] == 3 and stopped[2]["status"] == "Accepted"
                scope = session.journal.db.execute(
                    "SELECT local,wire_request,wire_result FROM vendor_command_scope WHERE request=?", (request,)
                ).fetchone()
                wire = next(f for f in reversed(rig.bench.device_calls) if f[2] == "RemoteStopTransaction")
                assert tuple(scope) == (local, wire[1], "Accepted")
                await close_daily(
                    b,
                    local,
                    "native",
                    timestamp=f"2026-01-01T00:{number * 2 + 1:02d}:00Z",
                    meter_stop=1100 + 200 * number,
                )
                await mp.healthy(controller)
                assert session.journal.local_id(vendor) is None
                await commands(rig, vendor, None)
            assert locals_seen[0] != locals_seen[1]
            assert not rig.bench.backend_errors and all(row["closed"] for row in rig.bench.backend_starts)
            assert session.journal.next_delivery() is None and not mp.starts(rig)


# Intent: a queued cloud command cannot change targets while its original cloud ID moves to the next session.
# Why: all transaction-scoped cloud writes share the charger's outgoing gate, while lifecycle reports stay independent.
@pytest.mark.parametrize(
    ("action", "payload"),
    [
        pytest.param("RemoteStopTransaction", {"transactionId": 0}, id="stop"),
        pytest.param(
            "SetChargingProfile", set_charging_profile(1, PURPOSE_TX, 6, 1001, transaction_id=0), id="profile"
        ),
        *(
            pytest.param(
                "DataTransfer",
                {"vendorId": "IoCharger", "messageId": message, "data": json.dumps({"transactionId": 0, **extra})},
                id=message + ("-scheduled" if "stopTime" in extra else ""),
            )
            for message, extra in COMMANDS
        ),
    ],
)
async def test_queued_cloud_command_cannot_follow_reused_mapping(
    tmp_path, tls, unused_tcp_port, monkeypatch, action, payload
):
    async with mp.managed_bench(tmp_path, tls, unused_tcp_port, bench_type=StartReplyBench) as rig:
        rig.bench.fixed_vendor = 0
        async with running(rig, "queued-cloud-command-boot") as (session, controller, charger):
            await mp.healthy(controller)
            b = ChargeRig(rig, session, controller, charger)
            first = await positive_flow(b, "daily")
            assert session.journal.local_id(0) == first
            actual_send, held_reply = charger.send, None

            # Hold one read reply while the fake charger continues to receive its real lifecycle responses.
            async def hold_read_reply(raw):
                nonlocal held_reply
                frame = json.loads(raw)
                if frame[0] == 3 and any(
                    row[1] == frame[1] and row[2] == "GetConfiguration" for row in rig.bench.device_calls
                ):
                    held_reply = raw
                    return
                await actual_send(raw)

            monkeypatch.setattr(charger, "send", hold_read_reply)
            tasks = [asyncio.create_task(rig.bench.cloud_call("held-read", "GetConfiguration", {}))]
            try:
                await until(lambda: held_reply is not None)
                tasks.append(asyncio.create_task(rig.bench.cloud_call("queued-owner-command", action, payload)))
                await until(lambda: bool(session.gateway.charger.gate._waiting))
                assert session.journal.local_id(0) == first
                sent = len(rig.bench.device_calls)
                assert (
                    await rig.bench.call(
                        charger,
                        "first-daily-completed",
                        "StopTransaction",
                        {"transactionId": first, "meterStop": 1100, "timestamp": "2026-01-01T00:01:00Z"},
                    )
                )[0] == 3
                second = await positive_flow(
                    b, "daily", call_id="next-daily", meter_start=1200, timestamp="2026-01-01T00:02:00Z"
                )
                assert second != first and session.journal.local_id(0) == second
            finally:
                if held_reply is not None:
                    await actual_send(held_reply)
                await asyncio.gather(*tasks, return_exceptions=True)
            assert tasks[0].result()[0] == 3
            assert tasks[1].result()[:3] == [4, "queued-owner-command", "SecurityError"]
            assert rig.bench.device_calls[sent:] == []
            assert session.journal.active() == second
            assert session.journal.db.execute("SELECT COUNT(*) FROM mutations").fetchone()[0] == 0


# Intent: a real Stop retains queue access when ordinary requests have filled the service.
# Why: a charger closure must still commit, and an app Stop must reach the wire before waiting reads.
@pytest.mark.parametrize("source", ["charger", "vendor"])
async def test_stop_survives_full_ordinary_request_queue(tmp_path, tls, unused_tcp_port, monkeypatch, source):
    async with mp.managed_bench(tmp_path, tls, unused_tcp_port, bench_type=StartReplyBench) as rig:
        async with running(rig, "stop-capacity-boot") as (session, controller, charger):
            await mp.healthy(controller)
            local = await positive_flow(ChargeRig(rig, session, controller, charger), "daily")
            journal, gateway = session.journal, session.gateway
            vendor = journal.vendor_id(local)
            assert vendor is not None
            original_send = charger.send
            held_reply = None

            # Hold the first real charger read reply while the two full-duplex links remain active.
            async def hold_read(raw):
                nonlocal held_reply
                frame = json.loads(raw)
                if (
                    held_reply is None
                    and frame[0] == 3
                    and any(call[1] == frame[1] and call[2] == "GetConfiguration" for call in rig.bench.device_calls)
                ):
                    held_reply = raw
                    return
                await original_send(raw)

            monkeypatch.setattr(charger, "send", hold_read)
            readers = [
                asyncio.create_task(rig.bench.cloud_call(f"held-read-{index}", "GetConfiguration", {}))
                for index in range(32)
            ]
            stop = None
            sent = len(rig.bench.device_calls)
            try:
                await until(lambda: held_reply is not None and len(gateway._tasks) == 32)
                if source == "charger":
                    body = {"transactionId": local, "meterStop": 1100, "timestamp": "2026-01-01T00:01:00Z"}
                    result = await rig.bench.call(charger, "saturated-real-stop", "StopTransaction", body)
                    assert result[0] == 3 and journal.active() is None
                    await until(lambda: journal.next_delivery() is None)
                    assert len([frame for frame in rig.bench.calls if frame[2] == "StopTransaction"]) == 1
                else:
                    stop = asyncio.create_task(
                        rig.bench.cloud_call("saturated-app-stop", "RemoteStopTransaction", {"transactionId": vendor})
                    )
                    await until(
                        lambda: "vendor" in gateway._reserved_stops and len(gateway.charger.gate._waiting) == 32
                    )
            finally:
                if held_reply is not None:
                    await original_send(held_reply)
                await asyncio.gather(*readers, *([stop] if stop is not None else []), return_exceptions=True)
            assert all(reader.result()[0] == 3 for reader in readers)
            assert not journal.db.execute("SELECT 1 FROM inbound_receipts WHERE token LIKE 'held-read-%'").fetchone()
            if stop is not None:
                assert stop.result()[2] == {"status": "Accepted"}
                first, second = rig.bench.device_calls[sent : sent + 2]
                assert first[2] == "GetConfiguration"
                assert second[2:] == ["RemoteStopTransaction", {"transactionId": local}]


# Intent: local Pause and vendor Stop share one charger Stop when they overlap for the same session.
# Why: the second request has to check the first acceptance after it acquires the charger gate.
@pytest.mark.recovery_replay
@pytest.mark.parametrize("first", ["local", "vendor"])
async def test_local_pause_and_cloud_stop_share_one_charger_stop(tmp_path, tls, unused_tcp_port, monkeypatch, first):
    async with mp.managed_bench(tmp_path, tls, unused_tcp_port, bench_type=StartReplyBench) as rig:
        async with running(rig, "shared-local-vendor-stop") as (session, c, charger):
            await mp.healthy(c)
            b = ChargeRig(rig, session, c, charger)
            local = await positive_flow(b, "daily")
            j, g = session.journal, session.gateway
            vendor = j.vendor_id(local)
            assert vendor is not None
            mark = len(rig.bench.device_calls)
            pause, request = uuid.uuid4().hex, uuid.uuid4().hex
            send, held, entered = delay_reply(monkeypatch, rig, charger, "RemoteStopTransaction")

            # Start the real management Pause and retain its independently scheduled worker.
            def pause_worker():
                assert c.command(pause, "pause", c.requests.intent().revision)["state"] == "accepted"
                assert c.pause_task is not None
                return c.pause_task

            # Send a new correlated vendor request on the already registered cloud connection.
            def cloud_stop():
                return asyncio.create_task(
                    rig.bench.cloud_call(request, "RemoteStopTransaction", {"transactionId": vendor})
                )

            first_task = pause_worker() if first == "local" else cloud_stop()
            second_task = None
            try:
                await asyncio.wait_for(entered.wait(), 3)
                assert rig.bench.firmware.effective_limit() == (0 if first == "local" else 6)
                second_task = cloud_stop() if first == "local" else pause_worker()
                await asyncio.wait_for(g.charger.gate.restrictive_waiting.wait(), 3)
            finally:
                if held:
                    await send(held[0])
                await asyncio.gather(first_task, *([second_task] if second_task is not None else []))
            cloud = second_task if first == "local" else first_task
            assert cloud is not None and cloud.result() == [3, request, {"status": "Accepted"}]
            stops = [f for f in rig.bench.device_calls[mark:] if f[2] == "RemoteStopTransaction"]
            assert len(stops) == 1 and stops[0][3] == {"transactionId": local}
            assert j.active() == local and j.outcomes.accepted_stop(local) and not j.outcomes.stop_report_closed(local)
            assert rig.bench.firmware.effective_limit() == 0 and c.requests.receipt(pause)["state"] == "accepted"
            owner = j.outcomes.owner(pause, include_wires=True)
            assert owner is not None
            owner_stops = [
                (wire.wire, wire.local, wire.reply) for wire in owner.wires if wire.action == "RemoteStopTransaction"
            ]
            forwarded = j.db.execute(
                "SELECT wire_request,local,wire_result FROM vendor_command_scope WHERE request=?", (request,)
            ).fetchone()
            exact_reply = (stops[0][1], local, "Accepted")
            if first == "local":
                assert owner_stops == [exact_reply] and forwarded is None
            else:
                assert owner_stops == [] and forwarded is not None and tuple(forwarded) == exact_reply

            await close_daily(b, local, "native")
            assert j.outcomes.stop_report_closed(local)
            assert (await mp.healthy(c))["local_hold_verified"] and c.requests.receipt(pause)["state"] == "completed"
            assert sum(f[2] == "StopTransaction" for f in rig.bench.lifecycle_wires) == 1


# Intent: a late local Stop acknowledgement can cover a vendor Stop that has prepared but has not sent.
# Why: the final decision must use the latest real reply and zero proof, and remove any unsent reservation.
@pytest.mark.recovery_replay
async def test_cloud_stop_reuses_late_local_acceptance_at_send_boundary(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with mp.managed_bench(tmp_path, tls, unused_tcp_port, bench_type=StartReplyBench) as rig:
        async with running(rig, "cloud-stop-after-late-local-reply") as (session, c, charger):
            await mp.healthy(c)
            b = ChargeRig(rig, session, c, charger)
            local = await positive_flow(b, "daily")
            j, g = session.journal, session.gateway
            vendor = j.vendor_id(local)
            assert vendor is not None
            mark = len(rig.bench.device_calls)
            send, held, entered = delay_reply(monkeypatch, rig, charger, "RemoteStopTransaction")
            with monkeypatch.context() as patch:
                patch.setattr(g.charger, "timeout", 0.2)
                pause = uuid.uuid4().hex
                assert c.command(pause, "pause", c.requests.intent().revision)["state"] == "accepted"
                assert c.pause_task is not None
                await asyncio.wait_for(entered.wait(), 3)
                await asyncio.wait_for(c.pause_task, 3)
            assert not j.outcomes.accepted_stop(local)
            await c.native.read(closing_local=local, stop_only=True)
            assert c.native.stop_zero_verified(local)

            validating, resume = asyncio.Event(), asyncio.Event()
            validate = g.charger.validate_send
            assert validate is not None

            # Hold only the vendor's actual outgoing validation after its mutation has been prepared.
            async def hold_stop(action, payload):
                if action == "RemoteStopTransaction":
                    validating.set()
                    await resume.wait()
                return await validate(action, payload)

            monkeypatch.setattr(g.charger, "validate_send", hold_stop)
            request = uuid.uuid4().hex
            task = asyncio.create_task(
                rig.bench.cloud_call(request, "RemoteStopTransaction", {"transactionId": vendor})
            )
            try:
                await asyncio.wait_for(validating.wait(), 3)
                assert j.db.execute("SELECT 1 FROM vendor_command_scope WHERE request=?", (request,)).fetchone()
                await send(held[0])
                await until(lambda: j.outcomes.accepted_stop(local))
            finally:
                resume.set()
                await task
            assert task.result() == [3, request, {"status": "Accepted"}]
            assert not j.db.execute("SELECT 1 FROM vendor_command_scope WHERE request=?", (request,)).fetchone()
            assert [f[3] for f in rig.bench.device_calls[mark:] if f[2] == "RemoteStopTransaction"] == [
                {"transactionId": local}
            ]
            assert not j.outcomes.stop_report_closed(local) and c.requests.receipt(pause)["state"] == "accepted"
            await close_daily(b, local, "native")
            assert j.outcomes.stop_report_closed(local)
            assert (await mp.healthy(c))["local_hold_verified"] and c.requests.receipt(pause)["state"] == "completed"
            assert sum(f[2] == "StopTransaction" for f in rig.bench.lifecycle_wires) == 1


# Intent: a late vendor reply can cover a local Stop before its prepared send.
# Why: the decision at the local wire boundary must see the same exact acceptance as the vendor path.
@pytest.mark.recovery_replay
async def test_local_pause_reuses_late_vendor_acceptance_at_send_boundary(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with mp.managed_bench(tmp_path, tls, unused_tcp_port, bench_type=StartReplyBench) as rig:
        async with running(rig, "local-stop-after-late-cloud-reply") as (session, c, charger):
            await mp.healthy(c)
            b = ChargeRig(rig, session, c, charger)
            local = await positive_flow(b, "daily")
            j, g = session.journal, session.gateway
            vendor = j.vendor_id(local)
            assert vendor is not None
            mark = len(rig.bench.device_calls)
            send, held, entered = delay_reply(monkeypatch, rig, charger, "RemoteStopTransaction")
            request = uuid.uuid4().hex
            with monkeypatch.context() as patch:
                patch.setattr(g.charger, "timeout", 0.2)
                vendor_task = asyncio.create_task(
                    rig.bench.cloud_call(request, "RemoteStopTransaction", {"transactionId": vendor})
                )
                await asyncio.wait_for(entered.wait(), 3)
                assert (await vendor_task)[:3] == [4, request, "InternalError"]
            assert not j.outcomes.accepted_stop(local)

            validating, resume = asyncio.Event(), asyncio.Event()
            validate = g.charger.validate_send
            assert validate is not None

            # Hold the local Stop after it has established zero and before its final queued authorization.
            async def hold_stop(action, payload):
                if action == "RemoteStopTransaction":
                    validating.set()
                    await resume.wait()
                return await validate(action, payload)

            monkeypatch.setattr(g.charger, "validate_send", hold_stop)
            pause = uuid.uuid4().hex
            assert c.command(pause, "pause", c.requests.intent().revision)["state"] == "accepted"
            assert c.pause_task is not None
            try:
                await asyncio.wait_for(validating.wait(), 3)
                assert rig.bench.firmware.effective_limit() == 0
                await send(held[0])
                await until(lambda: j.outcomes.accepted_stop(local))
            finally:
                resume.set()
                await c.pause_task

            stops = [f for f in rig.bench.device_calls[mark:] if f[2] == "RemoteStopTransaction"]
            assert len(stops) == 1 and stops[0][3] == {"transactionId": local}
            forwarded = j.db.execute(
                "SELECT wire_request,local,wire_result FROM vendor_command_scope WHERE request=?", (request,)
            ).fetchone()
            assert forwarded is not None and tuple(forwarded) == (stops[0][1], local, "Accepted")
            owner = j.outcomes.owner(pause, include_wires=True)
            assert owner is not None and all(wire.action != "RemoteStopTransaction" for wire in owner.wires)
            assert not j.outcomes.stop_report_closed(local) and c.requests.receipt(pause)["state"] == "accepted"
            await close_daily(b, local, "native")
            assert j.outcomes.stop_report_closed(local)
            assert (await mp.healthy(c))["local_hold_verified"] and c.requests.receipt(pause)["state"] == "completed"
            assert sum(f[2] == "StopTransaction" for f in rig.bench.lifecycle_wires) == 1


# Intent: a new vendor Stop can still reach the charger when the earlier Stop or present zero cannot cover it.
# Why: only a real prior acceptance with fresh physical zero can suppress another physical Stop.
@pytest.mark.recovery_replay
@pytest.mark.parametrize("evidence", ["renewed-draw", "native-unavailable", "prior-rejection"])
async def test_cloud_stop_requires_real_acceptance_and_fresh_zero(
    tmp_path, tls, unused_tcp_port, monkeypatch, evidence
):
    async with mp.managed_bench(
        tmp_path, tls, unused_tcp_port, bench_type=StartReplyBench, native_peer=InterruptedNativePeer
    ) as rig:
        async with running(rig, "cloud-stop-without-cover") as (session, c, charger):
            await mp.healthy(c)
            b = ChargeRig(rig, session, c, charger)
            local = await positive_flow(b, "daily")
            vendor = session.journal.vendor_id(local)
            assert vendor is not None
            mark = len(rig.bench.device_calls)
            actual_reply, actual_native = rig.bench.firmware.answer, rig.peer.native

            # Simulate a genuine charger refusal to the first origin, with no physical Stop effect.
            def refuse_stop(action, payload):
                return {"status": "Rejected"} if action == "RemoteStopTransaction" else actual_reply(action, payload)

            # Keep the exact session and profile but vary the physical reading returned by the independent peer.
            def renewed_draw(key):
                result = actual_native(key)
                if key == "METER":
                    result[0]["meterData"].update(cur=[15, 0, 0], power=[360, 0, 0, 360])
                return result

            pause, request = uuid.uuid4().hex, uuid.uuid4().hex
            with monkeypatch.context() as patch:
                if evidence == "prior-rejection":
                    patch.setattr(rig.bench.firmware, "answer", refuse_stop)
                c.command(pause, "pause", c.requests.intent().revision)
                assert c.pause_task is not None
                await asyncio.wait_for(c.pause_task, 3)
            assert session.journal.outcomes.accepted_stop(local) is (evidence != "prior-rejection")
            assert rig.bench.firmware.effective_limit() == 0

            with monkeypatch.context() as patch:
                if evidence == "renewed-draw":
                    patch.setattr(rig.peer, "native", renewed_draw)
                if evidence == "native-unavailable":
                    patch.setattr(rig.peer, "unavailable", True)
                answer = await rig.bench.cloud_call(request, "RemoteStopTransaction", {"transactionId": vendor})
            assert answer == [3, request, {"status": "Accepted"}]
            stops = [frame for frame in rig.bench.device_calls[mark:] if frame[2] == "RemoteStopTransaction"]
            assert [frame[3] for frame in stops] == [{"transactionId": local}] * 2
            sent = session.journal.db.execute(
                "SELECT wire_request,wire_result FROM vendor_command_scope WHERE request=?", (request,)
            ).fetchone()
            assert tuple(sent) == (stops[-1][1], "Accepted")
            await close_daily(b, local, "native")
            assert (await mp.healthy(c))["local_hold_verified"]
            assert sum(f[2] == "StopTransaction" for f in rig.bench.lifecycle_wires) == 1


# Intent: an invalid result to a real cloud Stop retains the dispatch as uncertain.
# Why: a correctly correlated frame cannot prove that the charger accepted or rejected the physical request.
@pytest.mark.parametrize("invalid", [{}, {"status": "Unknown"}], ids=["missing-status", "unknown-status"])
async def test_cloud_remote_stop_requires_valid_charger_result(tmp_path, tls, unused_tcp_port, monkeypatch, invalid):
    async with mp.managed_bench(tmp_path, tls, unused_tcp_port, bench_type=StartReplyBench) as rig:
        async with running(rig, "malformed-remote-stop-boot") as (session, controller, charger):
            await mp.healthy(controller)
            local = await positive_flow(ChargeRig(rig, session, controller, charger), "daily")
            actual_answer = rig.bench.firmware.answer

            # Preserve the fake charger's normal Stop effect and replace only its actual correlated reply body.
            def answer(action, payload):
                result = actual_answer(action, payload)
                return invalid if action == "RemoteStopTransaction" else result

            monkeypatch.setattr(rig.bench.firmware, "answer", answer)
            response = await rig.bench.cloud_call(
                "remote-stop", "RemoteStopTransaction", {"transactionId": session.journal.vendor_id(local)}
            )
            assert response[:3] == [4, "remote-stop", "InternalError"]
            assert rig.bench.device_calls[-1][2:] == ["RemoteStopTransaction", {"transactionId": local}]
            marker = session.journal.get("uncertain_vendor_command")
            assert marker["action"] == "RemoteStopTransaction"
            row = session.journal.db.execute("SELECT action,state FROM mutations WHERE token=?", (marker["mutation"],))
            assert tuple(row.fetchone()) == ("RemoteStopTransaction", "uncertain")
            assert session.journal.active() == local
