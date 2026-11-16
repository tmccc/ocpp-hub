"""A real Start acknowledgement followed immediately by its lifecycle survives the owner-intent handoff."""

import asyncio
import copy
import json
import sys
import uuid

from ocpp_hub_core.frames import build_call
from ocpp_hub_core.prototype.local_control import LocalControl
from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings
from ocpp_hub_core.prototype.managed_schedule import ManagedSchedule
from websockets.asyncio.client import connect

from tests.test_managed_api import TOKEN, call
from tests.test_managed_closed_timer_release_retry import coroutine_locals
from tests.test_managed_normal_start import natural_stop, owner_rig
from tests.test_managed_proxy import healthy, starts
from tests.test_prototype import START
from tests.test_prototype_tls import tls as tls


# Intent: a genuine reply-first Start/lifecycle pair remains accepted across the shielded worker's intent handoff.
# Why: the handler must reclassify current app authority after waiting, rather than retain the earlier paused branch.
async def test_reply_first_start_lifecycle_crosses_real_intent_handoff(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch
):
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

        # Send two ordinary adjacent TLS frames, preserving the actual independent charger's Accepted reply.
        async def send_reply_then_lifecycle(raw):
            reply = json.loads(raw)
            command = next((item for item in rig.bench.device_calls if item[1] == reply[1]), None)
            if not burst and reply[0] == 3 and command is not None and command[2] == "RemoteStartTransaction":
                assert reply[2]["status"] == "Accepted"
                payload = START | {"idTag": command[3]["idTag"]}
                burst.update(reply=copy.deepcopy(reply), payload=payload)
                rig.bench.replies[lifecycle_id] = lifecycle
                await original_send(raw)
                await original_send(build_call(lifecycle_id, "StartTransaction", payload))
                return
            await original_send(raw)

        # Observe real function entry without adding an await or changing any production method or verdict.
        def observe_entry(frame, event, arg):
            try:
                if (
                    event == "call"
                    and frame.f_code is ManagedSchedule.prepare_report_hold.__code__
                    and frame.f_locals.get("self") is c.schedule
                    and burst
                    and frame.f_locals.get("payload") == burst["payload"]
                    and not entries
                ):
                    worker = c.tests.worker
                    entries.append(
                        {
                            "paused": c.requests.intent().mode == "paused",
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
                accepted = await call(socket, "start", id=identifier, revision=c.requests.intent().revision)
                assert accepted["ok"] and accepted["result"]["state"] == "accepted"
                answer = await asyncio.wait_for(asyncio.shield(lifecycle), 5)
                sys.setprofile(previous_profile)
                assert not observer_errors and len(entries) == 1, (entries, observer_errors)
                entry = entries[0]
                assert entry["paused"] and entry["positive"] and entry["pending_worker"], entry
                assert entry["shield_wait"] and entry["owned_children"] == 1, entry
                assert entry["receipt_stage"] in ("issued", "accepted"), entry
                assert burst["reply"][2]["status"] == "Accepted" and len(starts(rig)) == 1
                assert answer[0] == 3 and answer[2]["idTagInfo"]["status"] == "Accepted", answer
                # Apply only this genuine accepted lifecycle to the independent physical peer, as Bench.call does.
                local = answer[2]["transactionId"]
                rig.bench.firmware.active = local
                rig.bench.history.append({"transactionId": local, "start": copy.deepcopy(burst["payload"])})
                rig.peer.plan_session = (local, 987654)
                rig.peer.paused, rig.peer.permission_enabled = False, True
                rig.bench.cp, rig.bench.connection = "C", "Charging"
                rig.bench.firmware.fire_plan(9)
                assert (await healthy(c))["current_a"] == 6
                assert j.active() == local and j.vendor_id(local) is not None
                assert c.requests.receipt(identifier)["state"] == "completed"
                c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
                await c.pause_task
                await natural_stop(b, local)
                assert (await healthy(c))["local_hold_verified"]
        finally:
            sys.setprofile(previous_profile)
            monkeypatch.setattr(b.charger, "send", original_send)
            await api.close()
