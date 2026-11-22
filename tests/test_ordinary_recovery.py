"""Known ordinary sessions and temporary native outages recover through real TLS and native HTTP."""

import asyncio
import socket
import sqlite3
import uuid
from contextlib import AsyncExitStack, closing
from dataclasses import replace
from urllib.parse import urlsplit, urlunsplit

import pytest
from ocpp_hub_core.prototype.connection_scope import current_scope
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.store import encoded

from tests.charge_rig import owner_start, positive_flow
from tests.current_rig import selected_ready
from tests.handover_helpers import crash
from tests.managed_rig import healthy, introduced, managed_session, starts
from tests.native_peer_failures import InterruptedNativePeer
from tests.prototype_rig import START
from tests.recovery_barriers import require_zero_held, returned_to_app
from tests.runtime_rig import until
from tests.tls_support import tls as tls


# Intent: an ordinary same-connection zero hold during a genuine cloud outage preserves the owner's app revision.
# Why: recovering phase alone does not mean a replacement introduction retired the owner's authority.
@pytest.mark.parametrize("completion_refused", [False, True])
async def test_same_connection_outage_hold_preserves_app_authority(
    tmp_path, tls, unused_tcp_port, monkeypatch, completion_refused
):
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, 32, native_peer=InterruptedNativePeer) as b:
        c, g, j = b.controller, b.session.gateway, b.session.journal
        intent = c.requests.intent()
        epoch, boot = g._charger_epoch, g.boot_observation
        transport = b.session.runtime.transport
        original = transport.config
        faults = b.session.runtime.status.vendor_fault.count
        with socket.socket() as unavailable:
            unavailable.bind(("127.0.0.1", 0))
            url = urlsplit(original.vendor_url)
            transport.config = replace(
                original,
                vendor_url=urlunsplit(url._replace(netloc="127.0.0.1:" + str(unavailable.getsockname()[1]))),
            )
            try:
                await b.rig.bench.cloud.close()
                await until(
                    lambda: (
                        b.session.runtime.status.vendor_fault.count > faults
                        and b.session.runtime.status.vendor_failed_scope == current_scope(g)
                        and not b.session.runtime.status.vendor_connected
                    )
                )
                b.rig.peer.unavailable = True
                for _ in range(3):
                    await c.tick()
                requests = []
                while not b.rig.peer.failed_reads.empty():
                    requests.append(b.rig.peer.failed_reads.get_nowait())
                assert requests and all(request["action"] == "getConfig" for request in requests)
                assert j.get("trial_phase") == "recovering"
                assert b.rig.bench.firmware.effective_limit() == 0
                b.rig.peer.unavailable = False
                await c.tick()
                assert g._charger_epoch == epoch and g.boot_observation == boot
                assert c.requests.intent() == intent
                assert not j.get("managed_control_fault")
            finally:
                b.rig.peer.unavailable = False
                transport.config = original
        await until(lambda: g.vendor_registration == "Accepted")
        if completion_refused:
            j.db.execute(
                "CREATE TEMP TRIGGER refuse_recovery_completion BEFORE INSERT ON meta "
                "WHEN NEW.key='managed_ordinary_recovery' AND json_extract(NEW.value,'$.state')='complete' "
                "BEGIN SELECT RAISE(ABORT,'completion storage temporarily busy'); END"
            )
            try:
                async with asyncio.timeout(5):
                    while c.ordinary.completion is None:
                        await c.tick()
                        await asyncio.sleep(0.02)
                assert g.controls.ready and g.controls.desired() == 32
                hold = j.get("managed_ordinary_recovery")
                mark = list(b.rig.bench.received_limits)
                b.rig.peer.unavailable = True
                status = await c.tick()
                assert status["state"] == "degraded" and status["fault"] == "native_observation_unavailable"
                assert not b.rig.peer.failed_reads.empty() and c.native_unavailable == 1
                assert c.ordinary.completion is not None
                assert j.get("managed_ordinary_recovery") == hold and b.rig.bench.received_limits == mark
            finally:
                b.rig.peer.unavailable = False
                j.db.execute("DROP TRIGGER refuse_recovery_completion")
        status = await healthy(c)
        assert c.requests.intent() == intent and status["station_ceiling_a"] == 32
        if completion_refused:
            assert j.get("managed_ordinary_recovery")["state"] == "complete"
            assert b.rig.bench.received_limits == mark
        assert not starts(b.rig) and not any(f[2] == "RemoteStopTransaction" for f in b.rig.bench.device_calls)


# Close only the fake device's real reported session after the behavior under test has been observed.
async def close_session(rig, session, charger, local, *, delivered=True):
    rig.peer.permission_enabled = False
    rig.bench.firmware.requested_current = 0
    reply = await rig.bench.call(
        charger,
        uuid.uuid4().hex,
        "StopTransaction",
        {
            "transactionId": local,
            "timestamp": START["timestamp"],
            "meterStop": 1100,
        },
    )
    assert reply[0] == 3
    await until(lambda: session.journal.active() is None and (not delivered or session.journal.next_delivery() is None))


# Intent: a genuine OCPP Stop followed by late native closure holds an ordinary mapped session at zero.
# Why: a matched closure gap must not become a persistent authority fault or grant positive current early.
@pytest.mark.parametrize("connection", ["plugged", "unplugged"])
@pytest.mark.parametrize("current", [6, 32])
async def test_ocpp_first_mapped_closure_recovers_only_after_native_closure(
    tmp_path, tls, unused_tcp_port, monkeypatch, connection, current
):
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        c, j, rig = b.controller, b.session.journal, b.rig
        intent = c.requests.intent()
        local = await positive_flow(b, "daily")
        assert (await healthy(c))["current_a"] == current
        open_session = rig.peer.native("SESSION")
        command_count = len(rig.bench.device_calls)
        limit_count = len(rig.bench.received_limits)
        await close_session(rig, b.session, b.charger, local)
        assert j.vendor_id(local) is not None
        rig.bench.cp, rig.bench.connection = ("B", "Plug") if connection == "plugged" else ("A", "Unplug")
        native = rig.peer.native

        with monkeypatch.context() as patch:
            patch.setattr(rig.peer, "native", lambda key: open_session if key == "SESSION" else native(key))
            for _ in range(4):
                status = await c.tick()
                assert status["state"] == "reconciling"
                assert not status.get("owner_control_enabled") and not j.fault.latched()
                assert rig.bench.firmware.effective_limit() == 0
                assert c.requests.intent() == intent
            assert rig.bench.received_limits[limit_count:] == [0]

        status = await healthy(c)
        assert status["owner_control_enabled"] and status["station_ceiling_a"] == current
        assert status["charging_session_open"] is False and c.requests.intent() == intent
        assert rig.bench.received_limits[limit_count:] == [0, current]
        assert not any(
            f[2] in ("RemoteStartTransaction", "RemoteStopTransaction") for f in rig.bench.device_calls[command_count:]
        )


# Intent: replacement during an exact, mapped Stop waits for the report and native closure.
# Why: the receiver must stay open at zero while either source still shows the session waiting to close.
@pytest.mark.parametrize("connection", [("B", "Plug"), ("A", "Unplug")])
@pytest.mark.parametrize(
    "order", ["stop-report", "accepted-shutdown-stop", "missing-shutdown-reply", "forwarded-stop", "forwarded-unsaved"]
)
async def test_replacement_during_ocpp_first_closure(tmp_path, tls, unused_tcp_port, monkeypatch, connection, order):
    from tests.test_managed_boot_supersession import delay_reply

    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, 32) as b:
        rig = b.rig
        intent = b.controller.requests.intent()
        local = await positive_flow(b, "daily")
        open_session = rig.peer.native("SESSION")
        if order == "stop-report":
            await close_session(rig, b.session, b.charger, local)
        elif order in ("forwarded-stop", "forwarded-unsaved"):
            journal = b.session.journal
            vendor = journal.vendor_id(local)
            assert vendor is not None
            request = uuid.uuid4().hex
            if order == "forwarded-unsaved":
                journal.db.execute(
                    "CREATE TEMP TRIGGER refuse_forwarded_stop_reply "
                    "BEFORE UPDATE OF wire_result ON vendor_command_scope "
                    "WHEN NEW.local IS NOT NULL AND OLD.wire_result='dispatched' AND NEW.wire_result='Accepted' "
                    "BEGIN SELECT RAISE(ABORT,'forwarded Stop reply storage temporarily refused'); END"
                )
            reply = await rig.bench.cloud_call(request, "RemoteStopTransaction", {"transactionId": vendor})
            assert reply[0] == 3 and reply[2] == {"status": "Accepted"}
            assert journal.active() == local
            capacity, journal.capacity = journal.capacity, 3
            try:
                for index in range(5):
                    with journal.atomic():
                        token = journal.mutation_intent("local", "UnrelatedWrite", {"index": index})
                        journal.mutation_finished(token, True)
                assert journal.outcomes.accepted_stop(local)
            finally:
                journal.capacity = capacity
            if order == "forwarded-unsaved":
                assert (
                    journal.db.execute(
                        "SELECT wire_result FROM vendor_command_scope WHERE request=?", (request,)
                    ).fetchone()[0]
                    == "dispatched"
                )
        else:
            if order == "missing-shutdown-reply":
                _, held, _ = delay_reply(monkeypatch, rig, b.charger, "RemoteStopTransaction")
                b.session.gateway.charger.timeout = 0.2
            b.controller.request_stop()
            await b.session._request_shutdown_stop(uuid.uuid4().hex, local)
            assert b.session.journal.active() == local
            if order == "missing-shutdown-reply":
                assert held and not b.session.journal.outcomes.accepted_stop(local)
            else:
                assert b.session.journal.outcomes.accepted_stop(local)
        rig.bench.cp, rig.bench.connection = connection
        if rig.bench.cp == "A":
            rig.peer.permission_enabled = False
        device_calls = len(rig.bench.device_calls)
        write_count = len(rig.bench.received_limits)
        native = rig.peer.native
        await crash(b.session)
        session = managed_session(rig)

        with monkeypatch.context() as patch:
            patch.setattr(rig.peer, "native", lambda key: open_session if key == "SESSION" else native(key))
            await session.start()
            if order == "forwarded-unsaved":
                assert session.journal.outcomes.accepted_stop(local)
                assert tuple(
                    session.journal.db.execute(
                        "SELECT state,wire_result FROM vendor_command_scope JOIN mutations ON mutation=token "
                        "WHERE request=?",
                        (request,),
                    ).fetchone()
                ) == ("confirmed", "Accepted")
            controller = ManagedController(session)
            async with rig.charger(session) as charger:
                await introduced(rig, session, charger, "mapped-closure-replacement")
                if order != "stop-report":
                    for _ in range(4):
                        status = await controller.tick()
                        assert not status["owner_control_enabled"] and not session.journal.fault.latched()
                        assert rig.bench.firmware.effective_limit() == 0 and session.journal.active() == local
                if order == "missing-shutdown-reply":
                    await until(lambda: session.gateway.vendor_registration == "Accepted")
                    request = uuid.uuid4().hex
                    vendor = session.journal.vendor_id(local)
                    reply = await rig.bench.cloud_call(request, "RemoteStopTransaction", {"transactionId": vendor})
                    assert reply[:3] == [4, request, "InternalError"]
                    assert not session.journal.outcomes.accepted_stop(local)
                await close_session(rig, session, charger, local)
                for _ in range(4):
                    status = await controller.tick()
                    assert not status["owner_control_enabled"] and not session.journal.fault.latched()
                    assert rig.bench.firmware.effective_limit() == 0
                assert set(rig.bench.received_limits[write_count:]) == {0}
                assert session.journal.active() is None and session.journal.latest_mapped_stop(local)
                patch.undo()
                status = await healthy(controller)
                assert status["owner_control_enabled"] and status["station_ceiling_a"] == 32
                assert controller.requests.intent() == intent
        assert not any(
            f[2] in ("RemoteStartTransaction", "RemoteStopTransaction") for f in rig.bench.device_calls[device_calls:]
        )


# Intent: the exact shutdown Stop can be unsent or still pending even when its optional wire record is absent.
# Why: replacement must resume a proven untouched session and hold an unanswered session through another shutdown.
@pytest.mark.parametrize("first_stop", ["proven-unsent", "unanswered-without-wire-record"])
async def test_shutdown_replacement_preserves_possible_dispatch(
    tmp_path, tls, unused_tcp_port, monkeypatch, first_stop
):
    from tests.test_managed_boot_supersession import delay_reply

    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, 32) as b:
        rig, j = b.rig, b.session.journal
        local = await positive_flow(b, "daily")
        await healthy(b.controller)
        g = b.session.gateway
        count = len(rig.bench.device_calls)
        saved_validate = g.charger.validate_send
        assert saved_validate is not None
        attempt = uuid.uuid4().hex

        # Refuse the outgoing Stop before the independent device can receive it.
        async def refuse_stop(action, payload):
            if action == "RemoteStopTransaction":
                raise UnavailableError("the outgoing Stop was never sent")
            return await saved_validate(action, payload)

        with monkeypatch.context() as patch:
            if first_stop == "proven-unsent":
                patch.setattr(g.charger, "validate_send", refuse_stop)
            else:
                delay_reply(patch, rig, b.charger, "RemoteStopTransaction")
                g.charger.timeout = 0.2
                j.db.execute(
                    "CREATE TEMP TRIGGER refuse_stop_wire BEFORE INSERT ON owner_command_wires "
                    "WHEN NEW.action='RemoteStopTransaction' "
                    "BEGIN SELECT RAISE(ABORT,'Stop wire storage unavailable'); END"
                )
            b.controller.request_stop()
            await b.session._request_shutdown_stop(attempt, local)
            b.session._record_shutdown_outcome(attempt, "uncertain")

        sent = first_stop != "proven-unsent"
        assert j.active() == local and j.outcomes.awaits_stop_report(local) is sent
        commands = [f[3] for f in rig.bench.device_calls[count:] if f[2] == "RemoteStopTransaction"]
        assert commands == ([{"transactionId": local}] if sent else [])
        assert rig.bench.firmware.effective_limit() == 0
        assert not j.db.execute("SELECT 1 FROM owner_command_wires WHERE action='RemoteStopTransaction'").fetchone()
        await crash(b.session)

        async with AsyncExitStack() as stack:
            session = managed_session(rig)
            stack.push_async_callback(crash, session)
            await session.start()
            controller = ManagedController(session)
            charger = await stack.enter_async_context(rig.charger(session))
            await introduced(rig, session, charger, "shutdown-dispatch-replacement")
            assert session.journal.active() == local
            if not sent:
                status = await healthy(controller)
                assert status["owner_control_enabled"] and status["station_ceiling_a"] == 32
                return
            assert not (await controller.tick())["owner_control_enabled"]
            controller.request_stop()
            second_attempt = uuid.uuid4().hex
            await session._request_shutdown_stop(second_attempt, local)
            session._record_shutdown_outcome(second_attempt, "uncertain")
            assert session.journal.outcomes.awaits_stop_report(local)
            assert [f[3] for f in rig.bench.device_calls[count:] if f[2] == "RemoteStopTransaction"] == commands

        third = managed_session(rig)
        async with AsyncExitStack() as stack:
            stack.push_async_callback(crash, third)
            await third.start()
            controller = ManagedController(third)
            charger = await stack.enter_async_context(rig.charger(third))
            await introduced(rig, third, charger, "shutdown-dispatch-second-replacement")
            assert third.journal.active() == local and third.journal.outcomes.awaits_stop_report(local)
            assert not (await controller.tick())["owner_control_enabled"]
            assert rig.bench.firmware.effective_limit() == 0
            await close_session(rig, third, charger, local)
            status = await healthy(controller)
            assert status["owner_control_enabled"] and status["station_ceiling_a"] == 32


# Intent: one Pause retains its exact session as the charger, native source, cloud and journal settle independently.
# Why: any order can leave a verified zero or accepted Stop without enough evidence to release or to send another Stop.
@pytest.mark.parametrize(
    "order",
    [
        "native-first",
        "report-first",
        "cloud-before-native",
        "return-before-report",
        "restart-open",
        "restart-reply-unsaved",
        "saved-cloud-reply",
        "shutdown",
    ],
)
async def test_pause_waits_for_unplugged_native_closure(tmp_path, tls, unused_tcp_port, monkeypatch, order):
    from tests.test_cloud_ack_storage import HeldReply, SqlFault, StorageBench, assert_retained
    from tests.test_managed_boot_supersession import delay_reply

    async with (
        selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, 6, bench_type=StorageBench) as b,
        AsyncExitStack() as replacements,
    ):
        session, charger, c, rig = b.session, b.charger, b.controller, b.rig
        j, bench = session.journal, rig.bench
        local = await positive_flow(b, "daily")
        assert (await healthy(c))["charging_session_open"]
        vendor = j.vendor_id(local)
        assert vendor is not None
        original_start = j.db.execute("SELECT start FROM transactions WHERE local=?", (local,)).fetchone()[0]
        open_session = rig.peer.native("SESSION")
        mark = len(bench.device_calls)
        identifier = uuid.uuid4().hex
        if order == "report-first":
            send, replies, stop_sent = delay_reply(monkeypatch, rig, charger, "RemoteStopTransaction")
        if order in ("restart-open", "restart-reply-unsaved"):
            j.db.execute(
                "CREATE TEMP TRIGGER refuse_stop_reply BEFORE UPDATE OF reply ON owner_command_wires "
                "WHEN OLD.action='RemoteStopTransaction' AND NEW.reply='Accepted' "
                "BEGIN SELECT RAISE(ABORT,'charger Stop reply storage temporarily refused'); END"
            )
        c.command(identifier, "pause", c.requests.intent().revision)
        if order == "report-first":
            await asyncio.wait_for(stop_sent.wait(), 3)
        else:
            await asyncio.wait_for(asyncio.shield(c.pause_task), 3)
            assert j.outcomes.accepted_stop(local)
        require_zero_held(rig, mark)

        # Keep peers alive, and reopen the same journal under a replacement proxy and charger socket.
        async def restart():
            new_session = managed_session(rig)
            await new_session.start()
            new_controller = ManagedController(new_session)
            if order == "restart-reply-unsaved":
                original = new_session.journal.outcomes.owner(identifier, include_wires=True)
                assert original is not None
                stop_wires = [wire for wire in original.wires if wire.action == "RemoteStopTransaction"]
                assert [(wire.local, wire.reply) for wire in stop_wires] == [(local, "Accepted")]
            new_charger = await replacements.enter_async_context(rig.charger(new_session))
            await introduced(rig, new_session, new_charger, "pause-replacement")
            return new_session, new_charger, new_controller, new_session.journal

        if order in ("restart-open", "restart-reply-unsaved"):
            query = (
                "SELECT reply FROM owner_command_wires WHERE request=? AND local=? AND action='RemoteStopTransaction'"
            )
            assert [tuple(r) for r in j.db.execute(query, (identifier, local))] == [(None,)]
            await c.tick()
            assert [tuple(r) for r in j.db.execute(query, (identifier, local))] == [(None,)]
            if order == "restart-open":
                j.db.execute("DROP TRIGGER refuse_stop_reply")
                await c.tick()
                assert [tuple(r) for r in j.db.execute(query, (identifier, local))] == [("Accepted",)]
            await crash(session)
            session, charger, c, j = await restart()
            assert j.active() == local and j.outcomes.accepted_stop(local)
            assert c.requests.receipt(identifier)["state"] != "completed"
            await c.tick()
            require_zero_held(rig, mark)

        cloud = HeldReply("StopTransaction", asyncio.Event(), asyncio.Event())
        bench.held_reply = cloud
        native = rig.peer.native
        shutdown = None
        returned = None
        defer_native = order in ("report-first", "cloud-before-native", "saved-cloud-reply", "shutdown")
        try:
            if order == "return-before-report":
                returned = uuid.uuid4().hex
                assert j.active() == local and j.outcomes.accepted_stop(local)
                assert c.requests.stop_owns_ticket(j.control.read().values.get("ticket"))
                assert not j.control.read().values.get("uncertain")
                c.command(returned, "app_control", c.requests.intent().revision)
                await c.tick()
                assert c.requests.receipt(returned)["state"] == "accepted"
                require_zero_held(rig, mark)

            if order == "native-first":
                bench.firmware.active = None
                bench.cp, bench.connection = "A", "Unplug"
                assert not (await c.tick()).get("local_hold_verified")
                assert j.active() == local and j.vendor_id(local) == vendor
                assert c.requests.receipt(identifier)["state"] != "completed"

            if order == "shutdown":
                c.request_stop()
                shutdown = asyncio.create_task(session.close())
                await until(lambda: j.get("trial_shutdown_attempt", {}).get("local") == local)
                await until(lambda: session.gateway.controls._stopping.done())
                require_zero_held(rig, mark)

            with monkeypatch.context() as patch:
                if defer_native:
                    patch.setattr(rig.peer, "native", lambda key: open_session if key == "SESSION" else native(key))
                await close_session(rig, session, charger, local, delivered=False)
                if order == "report-first":
                    await send(replies[0])
                    await asyncio.wait_for(asyncio.shield(c.pause_task), 3)
                bench.cp, bench.connection = "A", "Unplug"
                await asyncio.wait_for(cloud.arrived.wait(), 3)
                if order == "cloud-before-native":
                    cloud.release.set()
                    await until(lambda: j.next_delivery() is None)
                if defer_native and shutdown is None:
                    status = await c.tick()
                    assert status["state"] == "reconciling" and not j.fault.latched()
                    assert not status.get("local_hold_verified")
                    assert c.requests.receipt(identifier)["state"] != "completed"
                if order in ("saved-cloud-reply", "shutdown"):
                    failure = SqlFault("delete", cloud.token)
                    failure.install(j.db)
                    cloud.release.set()
                    await until(lambda: session.runtime.status.delivery_blocked)
                    assert_retained(j, cloud, saved=True)
                    require_zero_held(rig, mark)
                    if shutdown is None:
                        # With native closure still pending, only the restricted replacement receiver may start.
                        await crash(session)
                        with closing(sqlite3.connect(j.path)) as database, database:
                            failure.remove(database)
                        session, charger, c, j = await restart()
            if shutdown is not None:
                await shutdown
                assert session.final_status["shutdown_attempt_uncertain"] is False
                with closing(sqlite3.connect(j.path)) as database, database:
                    failure.remove(database)
                session, charger, c, j = await restart()
            if returned is None:
                returned = uuid.uuid4().hex
                c.command(returned, "app_control", c.requests.intent().revision)
            if not cloud.release.is_set():
                for _ in range(3):
                    await c.tick()
                assert c.requests.receipt(identifier)["state"] == "completed"
                assert c.requests.receipt(returned)["state"] == "accepted"
                assert j.next_delivery().token == cloud.token
            require_zero_held(rig, mark)
        finally:
            cloud.release.set()
            if shutdown is not None:
                await shutdown

        await returned_to_app(replace(b, session=session, controller=c, charger=charger), 6)
        assert c.requests.receipt(identifier)["state"] == c.requests.receipt(returned)["state"] == "completed"
        row = j.db.execute("SELECT local,vendor,start,stop,live FROM transactions").fetchall()
        stop = {"transactionId": local, "timestamp": START["timestamp"], "meterStop": 1100}
        assert [tuple(r) for r in row] == [(local, vendor, original_start, encoded(stop), 0)]
        assert [f[3] for f in bench.device_calls[mark:] if f[2] == "RemoteStopTransaction"] == [
            {"transactionId": local}
        ]
        assert [f for f in bench.lifecycle_wires if f[2] == "StopTransaction"] == [
            [2, cloud.token, "StopTransaction", stop | {"transactionId": vendor}]
        ]


# Intent: unrelated, unmapped and nonzero native reports remain strict refusals after OCPP closure.
# Why: only zero flow from the latest exact mapped lifecycle can enter the ordinary closure wait.
@pytest.mark.parametrize("sample", ["unknown", "older", "unmapped", "nonzero"])
async def test_ocpp_first_closure_does_not_adopt_other_native_sessions(
    tmp_path, tls, unused_tcp_port, monkeypatch, sample
):
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c, j, rig = b.controller, b.session.journal, b.rig
        older = None
        if sample == "older":
            older = await positive_flow(b, "daily")
            await close_session(rig, b.session, b.charger, older)
            rig.bench.cp, rig.bench.connection = "B", "Plug"
        if sample == "unmapped":
            monkeypatch.setattr(b.session.runtime, "vendor_enabled", lambda: False)
            await rig.bench.cloud.close()
            await until(lambda: not b.session.runtime.status.vendor_connected)
            start = await rig.bench.call(
                b.charger, uuid.uuid4().hex, "StartTransaction", START | {"idTag": "NATIVE_TIMER"}
            )
            assert start[0] == 3 and start[2]["idTagInfo"]["status"] == "Accepted"
            local = start[2]["transactionId"]
            assert j.vendor_id(local) is None
        else:
            local = await positive_flow(b, "daily", call_id=uuid.uuid4().hex, meter_start=1001)
            assert j.vendor_id(local) is not None
        await close_session(rig, b.session, b.charger, local, delivered=sample != "unmapped")
        rig.bench.cp, rig.bench.connection = "C", "Charging"
        native = rig.peer.native
        reported_local = older if older is not None else local
        if sample == "unknown":
            reported_local += 1

        # Keep independent native observations coherent while varying only the session identity or actual draw.
        def open_native(key):
            value = native(key)
            if key == "SESSION":
                value["saveData"]["sessionList"][0]["_transactionId"] = reported_local
            elif key == "METER" and sample == "nonzero":
                value[0]["meterData"].update(cur=[2, 0, 0], power=[48, 0, 0, 48])
            return value

        with monkeypatch.context() as patch:
            patch.setattr(rig.peer, "native", open_native)
            status = await c.tick()
            assert status["state"] == "degraded" and status["fault"] == "native_authority_or_preservation_failed"
            assert not status.get("owner_control_enabled") and j.active() is None
            assert j.get("managed_ordinary_recovery", {}).get("state") != "holding"
        rig.bench.cp, rig.bench.connection = "B", "Plug"


# Intent: ordinary app/daily known sessions continue at their original6/32 cap after reconnect/replacement.
# Why: current Boot/native source and zero proof permit continuation, without replaying Start or guessing Stop.
@pytest.mark.parametrize("current", [6, 32])
@pytest.mark.parametrize("source", ["daily", "cloud"])
@pytest.mark.parametrize("restart", [False, True])
async def test_known_ordinary_session_recovers_without_temporary_lease(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, source, restart
):
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        if source == "cloud":
            await owner_start(b)
        local = await positive_flow(b, source)
        if source == "cloud":
            b.rig.peer.plan_session = None
        await healthy(b.controller)
        starts_before = len(starts(b.rig))
        profiles = len(b.rig.bench.received_limits)
        assert b.session.journal.get("managed_temporary_schedule") is None
        if restart:
            await crash(b.session)
            session = managed_session(b.rig)
            await session.start()
            c = ManagedController(session)
        else:
            await b.charger.close()
            await until(lambda: not b.session.runtime.status.charger_connected)
            session, c = b.session, b.controller
        async with b.rig.charger(session) as charger:
            await introduced(b.rig, session, charger, "known-ordinary-recovery")
            assert b.rig.bench.firmware.effective_limit() == 0
            status = await healthy(c)
            assert status["current_a"] == current and status["station_ceiling_a"] == current
            assert session.journal.active() == local and session.journal.vendor_id(local) is not None
            assert len(starts(b.rig)) == starts_before
            assert not any(f[2] == "RemoteStopTransaction" for f in b.rig.bench.device_calls)
            assert b.rig.bench.received_limits[profiles:][0] == 0
            assert session.journal.get("managed_temporary_schedule") is None
            await close_session(b.rig, session, charger, local)
            await healthy(c)


# Intent: a bounded native outage holds ordinary idle/open operation at zero without a permanent authority latch.
# Why: missing/stale observation is not positive configuration/history drift; fresh proof restores exact owner intent.
@pytest.mark.parametrize("current", [6, 32])
@pytest.mark.parametrize("open_session", [False, True])
async def test_ordinary_http_outage_retries_zero_and_recovers(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, open_session
):
    async with selected_ready(
        tmp_path, tls, unused_tcp_port, monkeypatch, current, native_peer=InterruptedNativePeer
    ) as b:
        local = await positive_flow(b, "daily") if open_session else None
        await healthy(b.controller)
        revision = b.controller.requests.intent().revision
        before_starts = len(starts(b.rig))
        b.rig.peer.unavailable = True
        for _ in range(5):
            status = await b.controller.tick()
            assert status["state"] != "attention"
        assert b.rig.bench.firmware.effective_limit() == 0
        assert not b.session.journal.get("managed_control_fault")
        assert b.controller.requests.intent().mode == "app"
        assert b.controller.requests.intent().revision == revision
        b.rig.peer.unavailable = False
        status = await healthy(b.controller)
        assert status["station_ceiling_a"] == current
        assert b.controller.requests.intent().revision == revision
        assert b.session.journal.active() == local
        assert len(starts(b.rig)) == before_starts
        assert not any(f[2] == "RemoteStopTransaction" for f in b.rig.bench.device_calls)
        if local is not None:
            assert status["current_a"] == current
            await close_session(b.rig, b.session, b.charger, local)
            await healthy(b.controller)


class SlowOrTruncatedPeer(InterruptedNativePeer):
    """Independent HTTP delay and premature EOF supply missing or genuinely stale complete observations."""

    # Keep the endpoint behavior separate from engine clocks and native values.
    def __init__(self, bench):
        super().__init__(bench)
        self.transport_mode = None

    # Delay complete real replies or close a bounded partial response, without altering their physical content.
    async def handle(self, reader, writer):
        if self.transport_mode == "eof":
            await reader.readuntil(b"\r\n\r\n")
            writer.write(b"HTTP/1.0 200 OK\r\nContent-Length: 900\r\n\r\n{")
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return
        if self.transport_mode == "stale":
            await asyncio.sleep(0.55)
        await super().handle(reader, writer)


# Intent: interrupted HTTP bodies and actually elapsed stale complete audits take the same bounded zero recovery.
# Why: native availability failures must never become permanent authority drift merely because they repeat three times.
@pytest.mark.parametrize("current,mode", [(6, "eof"), (32, "eof"), (6, "stale")])
async def test_ordinary_native_eof_and_real_stale_audit_recover(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, mode
):
    async with selected_ready(
        tmp_path, tls, unused_tcp_port, monkeypatch, current, native_peer=SlowOrTruncatedPeer
    ) as b:
        revision = b.controller.requests.intent().revision
        b.rig.peer.transport_mode = mode
        for _ in range(3):
            status = await b.controller.tick()
            assert status["state"] != "attention"
        assert b.rig.bench.firmware.effective_limit() == 0
        assert not b.session.journal.get("managed_control_fault")
        expected = "StaleEvidenceError" if mode == "stale" else "NativeTransportUnavailableError"
        assert b.session.journal.get("managed_native_failure")["type"] == expected
        b.rig.peer.transport_mode = None
        status = await healthy(b.controller)
        assert status["station_ceiling_a"] == current
        assert b.controller.requests.intent().revision == revision
        assert b.session.journal.active() is None and not starts(b.rig)
