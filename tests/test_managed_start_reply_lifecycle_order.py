"""A real Start acknowledgement followed immediately by its lifecycle survives the owner-intent handoff."""

import asyncio
import copy
import json
import sys
import uuid

from ocpp_hub_core.frames import build_call, build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.local_control import LocalControl
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings
from ocpp_hub_core.prototype.managed_charging import ManagedCharging
from websockets.asyncio.client import connect

from tests.charge_rig import owner_rig, positive_flow
from tests.managed_rig import healthy, starts
from tests.management_peer import TOKEN, call
from tests.native_fault_support import coroutine_locals
from tests.prototype_rig import START
from tests.runtime_rig import until
from tests.test_managed_normal_start import natural_stop
from tests.tls_support import tls as tls


# Intent: a definite local Start rejection is recorded while its outgoing send still waits.
# Why: a send timeout cannot turn a known rejection into an issued Start or defeat the owner's zero hold.
async def test_local_start_saves_rejection_before_send_finishes(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, g, j, bench = b.controller, b.session.gateway, b.session.journal, b.rig.bench
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        sender, answer = g.charger.sender, bench.firmware.answer
        assert sender is not None
        draining, release = asyncio.Event(), asyncio.Event()
        g.charger.timeout = 0.5

        # Allow the actual synthetic charger to answer this one Start while its sender is still waiting.
        async def drain(raw):
            await sender(raw)
            frame = decode(raw)
            if frame[0] == 2 and frame[2] == "RemoteStartTransaction":
                draining.set()
                await release.wait()

        # Keep ordinary profile verification while giving the real Start a definite rejection.
        def reject(action, payload):
            return {"status": "Rejected"} if action == "RemoteStartTransaction" else answer(action, payload)

        monkeypatch.setattr(g.charger, "sender", drain)
        monkeypatch.setattr(bench.firmware, "answer", reject)
        identifier = uuid.uuid4().hex
        assert c.command(identifier, "start", c.requests.intent().revision)["state"] == "accepted"
        try:
            await asyncio.wait_for(draining.wait(), 5)
            await until(lambda: j.starts.receipt().get("stage") == "rejected", timeout=0.45)
            assert c.charging.worker is not None and not c.charging.worker.done()
            await c.charging.settle()
            assert j.starts.receipt().get("stage") == "rejected" and len(starts(b.rig)) == 1
            assert c.requests.intent().mode == "paused" and bench.firmware.effective_limit() == 0
        finally:
            release.set()


# Intent: a late local rejection retires its exact issued Start after the ordinary reply deadline.
# Why: true uncertainty can hold zero while a valid late answer is still allowed to release its pending receipt.
async def test_local_start_late_rejection_clears_only_its_receipt(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, g, j, bench = b.controller, b.session.gateway, b.session.journal, b.rig.bench
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        original_send = b.charger.send
        saved = []
        g.charger.timeout = 0.25

        # Save the actual correlated wire identifier and delay its rejection without blocking the charger reader.
        async def delay(raw):
            answer = decode(raw)
            if answer[0] == 3 and any(
                call[1] == answer[1] and call[2] == "RemoteStartTransaction" for call in bench.device_calls
            ):
                saved.append(answer[1])
                return
            await original_send(raw)

        monkeypatch.setattr(b.charger, "send", delay)
        identifier = uuid.uuid4().hex
        assert c.command(identifier, "start", c.requests.intent().revision)["state"] == "accepted"
        await c.charging.settle()
        assert len(saved) == 1 and j.starts.receipt()["stage"] == "issued"
        await original_send(build_call_result(saved[0], {"status": "Rejected"}))
        await until(lambda: j.starts.receipt().get("stage") == "rejected")
        outcome = j.outcomes.owner(identifier, include_wires=True)
        assert outcome.reply_known and not outcome.sessions
        assert [(wire.wire, wire.reply) for wire in outcome.wires if wire.action == "RemoteStartTransaction"] == [
            (saved[0], "Rejected")
        ]
        assert c.requests.intent().mode == "paused" and bench.firmware.effective_limit() == 0
        assert len(starts(b.rig)) == 1


# Intent: a temporarily refused wire evidence write cannot affect a genuine Start report.
# Why: the charging session has to remain linked to the original owner even when its reply saves later.
async def test_start_session_binds_while_its_wire_evidence_cannot_save(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        j.db.execute(
            "CREATE TEMP TRIGGER refuse_start_wire BEFORE INSERT ON owner_command_wires "
            "WHEN NEW.action='RemoteStartTransaction' BEGIN SELECT RAISE(ABORT,'wire write unavailable'); END"
        )
        identifier = uuid.uuid4().hex
        assert c.command(identifier, "start", c.requests.intent().revision)["state"] == "accepted"
        await c.charging.settle()
        local = await positive_flow(b)
        assert (
            j.db.execute(
                "SELECT 1 FROM owner_command_wires WHERE request=? AND action='RemoteStartTransaction'", (identifier,)
            ).fetchone()
            is None
        )
        outcome = j.outcomes.owner(identifier, include_wires=True)
        assert outcome.reply_known and [(item.local, item.closed) for item in outcome.sessions] == [(local, False)]
        assert [wire.local for wire in outcome.wires if wire.action == "RemoteStartTransaction"] == [local]

        j.db.execute("DROP TRIGGER refuse_start_wire")
        await c.tick()
        assert (
            j.db.execute(
                "SELECT reply FROM owner_command_wires WHERE request=? AND action='RemoteStartTransaction'",
                (identifier,),
            ).fetchone()[0]
            == "Accepted"
        )
        await natural_stop(b, local)
        outcome = j.outcomes.owner(identifier, include_wires=True)
        assert outcome.reply_known and [(item.local, item.closed) for item in outcome.sessions] == [(local, True)]


# Exercise one explicit real TLS frame ordering while observing the actual atomic publication boundary.
async def lifecycle_handoff(tmp_path, tls, unused_tcp_port_factory, monkeypatch, *, report_first):
    async with owner_rig(tmp_path, tls, unused_tcp_port_factory(), monkeypatch, startup=10) as b:
        c, g, j, rig = b.controller, b.session.gateway, b.session.journal, b.rig
        published = {**await healthy(c), "running": True, "certificate_remaining_seconds": 86400}
        port = unused_tcp_port_factory()
        api = ManagementServer(ManagementSettings(port, tmp_path / "unused-secret", TOKEN), c, lambda: published)
        await api.start()
        original_send, previous_profile = b.charger.send, sys.getprofile()
        lifecycle = asyncio.get_running_loop().create_future()
        lifecycle_id = uuid.uuid4().hex
        burst, entries, observer_errors = {}, [], []
        decisions = {}
        owner = g.controls.decisions
        report_code = owner.record_start_report.__code__
        stage_code = j.starts.record.__code__
        assert c.commands is g.command_processor is g.charger.command_processor is g.vendor.command_processor

        # Send two ordinary adjacent TLS frames, preserving the actual independent charger's Accepted reply.
        async def send_reply_then_lifecycle(raw):
            reply = json.loads(raw)
            command = next((item for item in rig.bench.device_calls if item[1] == reply[1]), None)
            if not burst and reply[0] == 3 and command is not None and command[2] == "RemoteStartTransaction":
                assert reply[2]["status"] == "Accepted"
                payload = START | {"idTag": command[3]["idTag"]}
                burst.update(reply=copy.deepcopy(reply), payload=payload)
                rig.bench.replies[lifecycle_id] = lifecycle
                report = build_call(lifecycle_id, "StartTransaction", payload)
                burst["forwarded"] = []
                if report_first:
                    await original_send(report)
                    burst["forwarded"].append("report")
                await original_send(raw)
                burst["forwarded"].append("reply")
                if not report_first:
                    await original_send(report)
                    burst["forwarded"].append("report")
                return
            await original_send(raw)

        # Observe real function entry without adding an await or changing any production method or verdict.
        def observe_entry(frame, event, arg):
            try:
                if event == "call" and frame.f_code is report_code and frame.f_locals.get("self") is owner:
                    decisions["report"] = c.commands._deciding
                if (
                    event == "call"
                    and frame.f_code is stage_code
                    and frame.f_locals.get("self") is j.starts
                    and frame.f_locals.get("stage") == "accepted"
                ):
                    decisions["accepted reply"] = c.commands._deciding
                if (
                    event == "call"
                    and frame.f_code is ManagedCharging.prepare_owner_report.__code__
                    and frame.f_locals.get("self") is c.charging
                    and burst
                    and frame.f_locals.get("payload") == burst["payload"]
                    and not entries
                ):
                    worker = c.charging.worker
                    entries.append(
                        {
                            "intent": c.requests.intent(),
                            "control": copy.deepcopy(j.get("control")),
                            "metadata": copy.deepcopy(c.charging.state()),
                            "lease": None
                            if c.charging.lease is None
                            else (c.charging.lease.mode, c.charging.lease.revision),
                            "positive": rig.bench.firmware.effective_limit() == 6,
                            "pending_worker": worker is not None and not worker.done(),
                            "shield_wait": worker is not None
                            and coroutine_locals(worker, LocalControl._owned.__code__).get("self") is g.controls,
                            "owned_children": len(g.controls._operations),
                            "receipt_stage": j.get("local_start_receipt", {}).get("stage"),
                        }
                    )
            except Exception as error:
                observer_errors.append(type(error).__name__)  # This observer cannot alter the engine being measured.
            if previous_profile is not None:
                previous_profile(frame, event, arg)

        try:
            async with connect(
                f"ws://127.0.0.1:{port}/management",
                additional_headers={"Authorization": "Bearer " + TOKEN},
                subprotocols=["ics-proxy.v1"],
                proxy=None,
            ) as socket:
                assert (await call(socket, "pause", revision=c.requests.intent().revision))["ok"]
                published = {**await healthy(c), "running": True, "certificate_remaining_seconds": 86400}
                identifier = uuid.uuid4().hex
                monkeypatch.setattr(b.charger, "send", send_reply_then_lifecycle)
                sys.setprofile(observe_entry)
                requested_revision = c.requests.intent().revision
                accepted = await call(socket, "start", id=identifier, revision=requested_revision)
                assert accepted["ok"] and accepted["result"]["state"] == "accepted"
                answer = await asyncio.wait_for(asyncio.shield(lifecycle), 5)
                sys.setprofile(previous_profile)
                assert not observer_errors and len(entries) == 1, (entries, observer_errors)
                assert decisions == {"report": True, "accepted reply": True}
                entry = entries[0]
                assert entry["positive"] and entry["pending_worker"], entry
                assert burst["forwarded"] == (["report", "reply"] if report_first else ["reply", "report"])
                if report_first:
                    assert entry["intent"].mode == "paused" and entry["intent"].revision == requested_revision
                    assert entry["metadata"]["state"] == "starting" and not entry["metadata"].get("start_rpc_accepted")
                    assert entry["control"]["owner"] == "local" and entry["control"]["uncertain"]
                else:
                    assert entry["intent"].mode == "app" and entry["intent"].revision == requested_revision + 1
                    assert entry["metadata"]["state"] == "awaiting_power" and entry["metadata"]["start_rpc_accepted"]
                    assert entry["control"]["owner"] == "shared" and not entry["control"]["uncertain"]
                    assert entry["lease"] == ("app", requested_revision + 1)
                assert entry["shield_wait"] and entry["owned_children"] == 1, entry
                assert entry["receipt_stage"] in ("issued", "accepted"), entry
                assert burst["reply"][2]["status"] == "Accepted" and len(starts(rig)) == 1
                assert answer[0] == 3 and answer[2]["idTagInfo"]["status"] == "Accepted", answer
                await c.charging.settle()
                assert c.requests.intent().mode == "app" and c.requests.intent().revision == requested_revision + 1
                assert j.get("control")["owner"] == "shared" and c.charging.state()["start_rpc_accepted"]
                # Apply only this genuine accepted lifecycle to the independent physical peer, as Bench.call does.
                local = answer[2]["transactionId"]
                rig.bench.firmware.active = local
                rig.bench.history.append({"transactionId": local, "start": copy.deepcopy(burst["payload"])})
                rig.peer.plan_session = (local, 987654)
                rig.peer.paused, rig.peer.permission_enabled = False, True
                rig.bench.cp, rig.bench.connection = "C", "Charging"
                rig.bench.firmware.fire_plan(9)
                assert (await healthy(c))["current_a"] == 6
                assert j.active() == local
                await until(lambda: j.vendor_id(local) is not None)
                assert c.requests.receipt(identifier)["state"] == "completed"
                c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
                await c.pause_task
                await natural_stop(b, local)
                assert (await healthy(c))["local_hold_verified"]
        finally:
            sys.setprofile(previous_profile)
            monkeypatch.setattr(b.charger, "send", original_send)
            await api.close()


# Intent: actual ACK-first frames enter through completely committed app authority before the parent resumes.
# Why: reply-first delivery must observe atomic child publication and retain the original genuine lifecycle.
async def test_reply_first_start_lifecycle_crosses_real_intent_handoff(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch
):
    await lifecycle_handoff(tmp_path, tls, unused_tcp_port_factory, monkeypatch, report_first=False)


# Intent: actual report-first frames enter while paused and reclassify after joining the owned Start publication.
# Why: the earlier paused handler cannot keep its old branch after the actual accepted child commits app authority.
async def test_report_first_start_lifecycle_waits_for_child_publication(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch
):
    await lifecycle_handoff(tmp_path, tls, unused_tcp_port_factory, monkeypatch, report_first=True)
