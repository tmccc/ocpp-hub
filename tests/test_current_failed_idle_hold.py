"""A failed idle selection may verify its own zero hold without borrowing another request's authority."""

import asyncio
import copy
import json
import uuid

import pytest
from ocpp_hub_core.prototype.store import StationZeroStorageError

from tests.current_rig import adjustable
from tests.managed_rig import healthy, starts
from tests.native_peer_failures import InterruptedNativePeer
from tests.test_worker_storage_recovery import fail_boundary, failing_cleanup
from tests.tls_support import tls as tls


# Intent: only the exact failed current generation can settle physical uncertainty after native HTTP recovers.
# Why: only fresh exact proof can retire the request while keeping its original failure.
async def test_failed_idle_hold_binds_request_cause_boot_and_existing_control(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch, native_peer=InterruptedNativePeer) as b:
        c, j = b.controller, b.session.journal
        fail_boundary(j, "current", "statement")
        reached, _ = failing_cleanup(
            j,
            "managed_current_failure",
            minimum_attempts=1,
            on_first_attempt=lambda: setattr(b.rig.peer, "unavailable", True),
        )
        identifier, revision = uuid.uuid4().hex, c.requests.intent().revision
        c.command(identifier, "set_current", revision, current_a=16)
        await asyncio.wait_for(reached.wait(), 3)
        await c.current.settle()
        await asyncio.wait_for(asyncio.shield(c.pause_task), 3)
        original = {
            key: copy.deepcopy(j.get(key))
            for key in (
                "managed_current_change",
                "managed_control_intent",
                "control",
                "local_start_receipt",
                "managed_control_fault",
                "managed_test_charge",
            )
        }
        result = c.requests.receipt(identifier)
        assert result["state"] == "uncertain" and c.current.failed_idle_hold()
        before_calls, before_limits = len(b.rig.bench.device_calls), list(b.rig.bench.received_limits)

        # Absent metadata must remain absent; a stored JSON null is a different input to the real controller.
        def restore(key):
            if original[key] is None:
                j.db.execute("DELETE FROM meta WHERE key=?", (key,))
            else:
                j.put(key, original[key])

        # Corrupt one durable binding at a time, restoring the genuine failed operation before the next control.
        cases = [
            ("managed_current_change", {"id": uuid.uuid4().hex}),
            ("managed_current_change", {"mode": "paused"}),
            ("managed_current_change", {"native_open": True}),
            ("managed_current_change", {"observed_open": True}),
            ("managed_current_change", {"source_boot": "different-actual-introduction"}),
            ("managed_current_change", {"previous_a": 16}),
            ("managed_current_change", {"current_a": 32}),
            ("managed_current_change", {"failure": None}),
            (
                "managed_current_change",
                {"failure": original["managed_current_change"]["failure"] | {"revision": revision + 1}},
            ),
            ("managed_control_intent", {"revision": revision + 3}),
            ("control", {"ticket": None}),
            ("control", {"ticket": "malformed"}),
            ("control", {"owner": "shared"}),
            ("control", {"start_pending": True}),
            ("managed_control_fault", {"classification": "retained_fault"}),
            ("managed_test_charge", {"state": "starting"}),
            ("local_start_receipt", {"stage": "prepared"}),
            ("local_start_receipt", {"stage": "issued"}),
            ("local_start_receipt", {"stage": "accepted", "local": None}),
        ]
        for key, fields in cases:
            j.put(key, {**(original[key] or {}), **fields})
            assert not c.current.failed_idle_hold(), (key, fields)
            restore(key)
        change = original["managed_current_change"].copy()
        del change["failure"]
        j.put("managed_current_change", change)
        assert not c.current.failed_idle_hold(), "legacy global diagnostic cannot bind this hold"
        j.put("managed_current_change", original["managed_current_change"])
        for bad_result in ({"classification": "process_replaced"}, {"classification": "current_unconfirmed"}):
            j.db.execute("UPDATE managed_requests SET result=? WHERE id=?", (json.dumps(bad_result), identifier))
            assert not c.current.failed_idle_hold()
        j.db.execute("UPDATE managed_requests SET result=? WHERE id=?", (json.dumps(result["result"]), identifier))
        assert c.current.failed_idle_hold()

        # Fresh supervision confirms zero and retires the exact request without changing its original failure.
        b.rig.peer.unavailable = False
        healed = await healthy(c)
        assert healed["desired_control"] == "paused" and healed["local_hold_verified"]
        assert healed["control_revision"] == revision + 2 and healed["station_ceiling_a"] == 0
        assert j.get("control")["uncertain"] is False
        assert c.requests.receipt(identifier) == {**result, "state": "failed"}
        assert c.current.selected() == 6 and b.rig.bench.firmware.effective_limit() == 0
        assert b.rig.bench.received_limits == before_limits and not starts(b.rig)
        assert all(
            frame[2] not in ("RemoteStartTransaction", "RemoteStopTransaction")
            for frame in b.rig.bench.device_calls[before_calls:]
        )
        j.db.execute("DROP TRIGGER review_worker_boundary")
        j.db.execute("DROP TRIGGER review_cleanup_unavailable")


# Intent: native inventory can recognize the applied current profile until its protective zero is really confirmed.
# Why: losing one profile reply must not convert an explained native profile into a permanent inventory fault.
@pytest.mark.parametrize("zero_reply", ["rejected", "lost"])
async def test_lost_current_reply_survives_unapplied_zero(tmp_path, tls, unused_tcp_port, monkeypatch, zero_reply):
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, g, j, bench = b.controller, b.session.gateway, b.session.journal, b.rig.bench
        peer_answer, peer_send = bench.firmware.answer, b.charger.send
        block_zero = True
        limits = []
        marker_key = "trial_station_zero_predecessor"

        # Inspect only the actual station writes issued by the real current worker.
        def limit(action, payload):
            if action == "SetChargingProfile":
                return payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
            return None

        # Apply the nonzero write at the device, while either declining or leaving the zero unapplied.
        def answer(action, payload):
            current = limit(action, payload)
            if current is not None:
                limits.append(current)
            if block_zero and current == 0:
                return {"status": "Rejected"}
            return peer_answer(action, payload)

        # Lose the nonzero acknowledgement and, for a transport failure during zero, the unapplied zero reply too.
        async def send(raw):
            frame = json.loads(raw)
            call = next((row for row in reversed(bench.device_calls) if row[1] == frame[1]), None)
            if frame[0] == 3 and call is not None:
                current = limit(*call[2:])
                if current == 16 or block_zero and current == 0 and zero_reply == "lost":
                    return
            await peer_send(raw)

        monkeypatch.setattr(bench.firmware, "answer", answer)
        monkeypatch.setattr(b.charger, "send", send)
        monkeypatch.setattr(g.charger, "timeout", 0.2)
        request = uuid.uuid4().hex
        c.command(request, "set_current", c.requests.intent().revision, current_a=16)
        await c.current.settle()
        await c.pause_task

        assert limits[:3] == [16, 0, 0]
        assert bench.firmware.effective_limit() == 16 and not g.controls.ready
        assert j.get("trial_inventory_fault") is None
        predecessor = j.get(marker_key)
        assert predecessor["profile"]["limit"] == 16
        for _ in range(2):
            status = await c.tick()
            assert not status["local_hold_verified"] and not status["owner_control_enabled"]
            assert j.get("trial_inventory_fault") is None and j.get(marker_key) == predecessor

        # A real zero Accepted reply cannot settle without atomically retiring its superseded inventory evidence.
        j.db.execute(f"""
            CREATE TRIGGER review_station_predecessor BEFORE DELETE ON meta WHEN OLD.key='{marker_key}'
            BEGIN SELECT RAISE(ABORT,'test refused predecessor retirement'); END
        """)
        block_zero = False
        with pytest.raises(StationZeroStorageError):
            await g.controls.ensure_ceiling()
        assert not g.controls.ready and j.get(marker_key) == predecessor
        row = j.db.execute("SELECT state,confirmed FROM trial_resources WHERE key='profile:7001'").fetchone()
        assert row["state"] == "pending" and json.loads(row["confirmed"])["limit"] == 6
        j.db.execute("DROP TRIGGER review_station_predecessor")

        healed = await healthy(c)
        assert healed["desired_control"] == "paused" and healed["local_hold_verified"]
        assert j.get(marker_key) is None and j.get("trial_inventory_fault") is None
        assert c.current.selected() == 6 and bench.firmware.effective_limit() == 0
        assert c.requests.receipt(request)["state"] == "failed"
        assert all(frame[2] not in ("RemoteStartTransaction", "RemoteStopTransaction") for frame in bench.device_calls)
