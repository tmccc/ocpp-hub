"""Three real failures and a new Boot compose under the unmodified standalone service poller."""

import asyncio
import copy
import json
import sqlite3
import uuid
from contextlib import closing

import pytest

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.process_helpers import process_rig
from tests.test_cloud_ack_storage import HeldReply, StorageBench, wire_count
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Install only an actual deferred-COMMIT fault for this one cloud event on the child-owned scratch database.
def refuse_ack_commit(path, token):
    with closing(sqlite3.connect(path)) as db:
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("CREATE TABLE cross_parent(id INTEGER PRIMARY KEY)")
        db.execute("CREATE TABLE cross_child(id REFERENCES cross_parent(id) DEFERRABLE INITIALLY DEFERRED)")
        db.execute(
            "CREATE TRIGGER refuse_cross_ack AFTER DELETE ON outbox WHEN OLD.token='" + token + "' "
            "BEGIN INSERT INTO cross_child VALUES(1); END"
        )
        db.commit()


# Remove the test's SQL refusal without writing any receipt, mapping, intent, resource or control state.
def repair_ack_commit(path):
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("SELECT COUNT(*) FROM cross_child").fetchone()[0] == 0
        db.execute("DROP TRIGGER IF EXISTS refuse_cross_ack")
        db.execute("DROP TABLE cross_child")
        db.execute("DROP TABLE cross_parent")
        db.commit()


# Read committed actual reply storage independently; opening another Journal would mutate startup state.
def has_saved_reply(path, held):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        reply = db.execute("SELECT answer FROM delivery_replies WHERE token=?", (held.token,)).fetchone()
        head = db.execute("SELECT token,state FROM outbox ORDER BY seq LIMIT 1").fetchone()
        return reply is not None and json.loads(reply[0]) == held.answer and head == (held.token, "inflight")


# Inspect one actual named API receipt without manually progressing a controller or retrying the command.
async def receipt(child, identifier):
    result = await child.request("receipt", request_id=identifier)
    assert result["ok"] and result["result"]["id"] == identifier
    return result["result"]


# The independent charger reports the genuine closure only after actually receiving the known-session Stop command.
async def close_after_wire_stop(child, local):
    rig = child.rig
    await asyncio.wait_for(rig.bench.remote_stop.wait(), 20)
    assert any(
        frame[2] == "RemoteStopTransaction" and frame[3]["transactionId"] == local for frame in rig.bench.device_calls
    )
    rig.peer.permission_enabled, rig.peer.plan_session = False, None
    rig.bench.firmware.requested_current = 0
    assert (
        await rig.bench.call(
            child.socket,
            uuid.uuid4().hex,
            "StopTransaction",
            {
                "transactionId": local,
                "meterStop": 2100,
                "timestamp": "2026-01-01T00:01:00Z",
            },
        )
    )[0] == 3


# Intent: real ACK COMMIT failure, native503 and rejected zero compose through both repair orders and new Boot.
# Why: isolated one-fault models and helper ticks cannot establish autonomous progress or honest receipt ownership.
@pytest.mark.parametrize("selected", [6, 32])
@pytest.mark.parametrize("repair_order", ["storage-first", "native-first"])
async def test_service_three_failures_pause_return_boot_and_later_sessions(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, selected, repair_order
):
    monkeypatch.setattr(mp, "OwnerBench", StorageBench)
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with process_rig(
        tmp_path, tls, unused_tcp_port_factory(), unused_tcp_port_factory(), monkeypatch, selected
    ) as child:
        await child.start()
        await child.status()
        rig, firmware, peer = child.rig, child.rig.bench.firmware, child.rig.peer
        original_pid = child.process.pid
        original_plan, original_config = copy.deepcopy(firmware.plans), copy.deepcopy(peer.configuration)
        path = rig.prepared.config.journal
        held = HeldReply("StartTransaction", asyncio.Event(), asyncio.Event())
        rig.bench.held_reply = held
        answer = await rig.bench.call(
            child.socket,
            uuid.uuid4().hex,
            "StartTransaction",
            START
            | {
                "idTag": "NATIVE_TIMER",
                "meterStart": 2000,
                "timestamp": "2026-01-01T00:00:00Z",
            },
        )
        assert answer[0] == 3 and answer[2]["idTagInfo"]["status"] == "Accepted"
        local = answer[2]["transactionId"]
        peer.plan_session, peer.paused, peer.permission_enabled = (local, 9), False, True
        rig.bench.cp, rig.bench.connection = "C", "Charging"
        firmware.fire_plan(9)
        assert firmware.current == firmware.effective_limit() == selected
        await asyncio.wait_for(held.arrived.wait(), 3)
        assert held.answer[2]["idTagInfo"]["status"] == "Accepted"
        refuse_ack_commit(path, held.token)
        repaired = False
        zero_rejections, actual_answer = [], firmware.answer

        # Refuse exactly one received zero profile at the independent firmware, without applying that profile.
        def answer_once(action, payload):
            if action == "SetChargingProfile" and not zero_rejections:
                profile = payload["csChargingProfiles"]
                if profile["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0:
                    zero_rejections.append(copy.deepcopy(payload))
                    return {"status": "Rejected"}
            return actual_answer(action, payload)

        firmware.answer = answer_once
        try:
            held.release.set()
            await until(lambda: has_saved_reply(path, held))
            assert wire_count(rig, held.token) == 1
            assert next(row for row in child.durable()["transactions"] if row["local"] == local)["vendor"] is None
            while not peer.failed_reads.empty():
                peer.failed_reads.get_nowait()
            peer.unavailable = True
            failed = []
            async with asyncio.timeout(28):
                for _ in range(4):
                    request = await peer.failed_reads.get()
                    assert request["action"] == "getConfig"
                    failed.append(request)
            outage = await child.status(
                lambda value: value.get("fault") == "native_observation_unavailable" and firmware.effective_limit() == 0
            )
            assert len(failed) == 4 and len(zero_rejections) == 1 and firmware.current == 0
            assert not outage["healthy"] and has_saved_reply(path, held)
            assert wire_count(rig, held.token) == 1
            pause_id, return_id = uuid.uuid4().hex, uuid.uuid4().hex
            paused = await child.request("pause", id=pause_id, revision=outage["control_revision"])
            assert paused["ok"] and paused["result"]["state"] == "accepted"
            current = (await child.request("status"))["result"]
            early_return_revision = current["control_revision"]
            returned = await child.request("app_control", id=return_id, revision=early_return_revision)
            assert returned["ok"] and returned["result"]["state"] == "accepted"
            assert (await receipt(child, pause_id))["state"] != "completed"
            assert (await receipt(child, return_id))["state"] != "completed"
            rig.bench.remote_stop.clear()
            # A genuine same-socket Boot supersedes the old introduction while both independent faults still remain.
            assert (await rig.bench.call(child.socket, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
            assert (await receipt(child, return_id))["state"] != "completed"
            if repair_order == "storage-first":
                repair_ack_commit(path)
                repaired = True
                await child.delivered(local)
                assert (await receipt(child, return_id))["state"] != "completed"
                assert firmware.effective_limit() == 0 and peer.unavailable
                peer.unavailable = False
                await close_after_wire_stop(child, local)
            else:
                peer.unavailable = False
                await close_after_wire_stop(child, local)
                first = await child.status(lambda value: value.get("local_hold_verified") is True)
                later = await child.status(
                    lambda value: (
                        value.get("checked_at", 0) > first["checked_at"] and value.get("local_hold_verified") is True
                    )
                )
                assert not first["healthy"] and not later["healthy"] and has_saved_reply(path, held)
                assert (await receipt(child, return_id))["state"] in ("accepted", "running", "uncertain")
                assert firmware.effective_limit() == 0
                repair_ack_commit(path)
                repaired = True
            await child.delivered(local, closed=True)
            # Bind the receipt to its intent in one committed snapshot; scheduling may legitimately choose either order.
            before_pause = child.durable()
            row = next(item for item in before_pause["requests"] if item["id"] == return_id)
            assert row["revision"] == early_return_revision
            old_return = {key: row[key] for key in ("id", "action", "state")}
            old_return["result"] = json.loads(row["result"])
            assert old_return["id"] == return_id and old_return["action"] == "app_control"
            if old_return["state"] == "completed":
                assert old_return["result"] == {"app_control_enabled": True, "charging_started": False}
                assert before_pause["metadata"]["managed_control_intent"] == {
                    "mode": "app",
                    "revision": early_return_revision + 1,
                }
                assert firmware.effective_limit() == selected
            else:
                assert old_return["state"] in ("accepted", "running", "uncertain")
                assert before_pause["metadata"]["managed_control_intent"] == {
                    "mode": "paused",
                    "revision": early_return_revision,
                }
                if old_return["state"] == "uncertain":
                    assert old_return["result"] == {"classification": "return_not_confirmed"}
            current = (await child.request("status"))["result"]
            newer_pause = await child.request("pause", revision=current["control_revision"])
            assert newer_pause["ok"]
            await child.completed_control(newer_pause["result"]["id"])
            after_pause = await receipt(child, return_id)
            assert after_pause["id"] == return_id and after_pause["action"] == "app_control"
            if after_pause["state"] == "completed":
                assert after_pause["result"] == {"app_control_enabled": True, "charging_started": False}
                if old_return["state"] == "completed":
                    assert after_pause == old_return
                expected_pause_revision = early_return_revision + 2
            else:
                assert old_return["state"] != "completed"
                assert after_pause["state"] == "failed" and after_pause["result"] == {
                    "classification": "superseded_by_pause"
                }
                expected_pause_revision = early_return_revision + 1
            snapshot = child.durable()
            assert snapshot["metadata"]["managed_control_intent"] == {
                "mode": "paused",
                "revision": expected_pause_revision,
            }
            pause_row = next(row for row in snapshot["requests"] if row["id"] == newer_pause["result"]["id"])
            assert pause_row["action"] == "pause" and pause_row["revision"] == current["control_revision"]
            assert expected_pause_revision in (current["control_revision"] + 1, current["control_revision"] + 2)
            terminal_old_return = copy.deepcopy(after_pause)
            assert firmware.effective_limit() == 0
            current = (await child.request("status"))["result"]
            assert current["control_revision"] == expected_pause_revision
            newer_return = await child.request("app_control", revision=expected_pause_revision)
            assert newer_return["ok"]
            await child.completed_control(newer_return["result"]["id"])
            snapshot = child.durable()
            assert snapshot["metadata"]["managed_control_intent"] == {
                "mode": "app",
                "revision": expected_pause_revision + 1,
            }
            return_row = next(row for row in snapshot["requests"] if row["id"] == newer_return["result"]["id"])
            assert return_row["action"] == "app_control" and return_row["revision"] == expected_pause_revision
            assert await receipt(child, return_id) == terminal_old_return
            final = await child.status(
                lambda value: value.get("owner_control_enabled") is True and value.get("station_ceiling_a") == selected
            )
            assert final["healthy"] and wire_count(rig, held.token) == 1
            assert not any(frame[2] == "RemoteStartTransaction" for frame in rig.bench.device_calls)
            await child.ordinary_recovery(selected, (True, False), start_minute=10)
            assert await receipt(child, return_id) == terminal_old_return
            assert child.process.pid == original_pid and child.generation == 1 and child.process.returncode is None
            assert firmware.plans == original_plan and peer.configuration == original_config
            assert not rig.bench.backend_errors and all(row["closed"] for row in rig.bench.backend_starts)
            assert child.durable()["outbox"] == [] and child.durable()["metadata"].get("managed_control_fault") is None
            (tmp_path / "cross-feature-evidence.json").write_text(
                json.dumps(
                    {
                        "selected": selected,
                        "repair_order": repair_order,
                        "pid": original_pid,
                        "first_outage_actual_failed_getConfig": len(failed),
                        "actual_zero_refusals": len(zero_rejections),
                        "original_start_wire_count": wire_count(rig, held.token),
                        "early_pause": pause_id,
                        "early_return": return_id,
                        "early_return_before_new_pause": old_return,
                        "early_return_after_new_pause": after_pause,
                        "later_return": newer_return["result"]["id"],
                        "no_manual_ticks": True,
                        "later_sessions_and_outages": 2,
                    },
                    indent=2,
                )
                + "\n"
            )
        finally:
            held.release.set()
            peer.unavailable = False
            firmware.answer = actual_answer
            if not repaired:
                repair_ack_commit(path)
