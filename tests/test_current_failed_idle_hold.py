"""A failed idle selection may verify its own zero hold without borrowing another request's authority."""

import asyncio
import copy
import json
import uuid

from tests import test_native_bounded_session as native_bench
from tests.test_managed_current import adjustable
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_proxy import healthy, starts
from tests.test_prototype_tls import tls as tls
from tests.test_worker_storage_recovery import fail_boundary, failing_cleanup


# Intent: only the exact failed current generation can settle physical uncertainty after native HTTP recovers.
# Why: an automatic zero hold must recover without retiring an unknown current outcome or borrowing a later Pause.
async def test_failed_idle_hold_binds_request_cause_boot_and_existing_control(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with adjustable(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
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
                "managed_temporary_schedule",
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
            ("managed_temporary_schedule", {"state": "armed"}),
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

        # Fresh supervision alone confirms zero, preserving both the unknown update and the previous selection.
        b.rig.peer.unavailable = False
        healed = await healthy(c)
        assert healed["desired_control"] == "paused" and healed["local_hold_verified"]
        assert healed["control_revision"] == revision + 2 and healed["station_ceiling_a"] == 0
        assert j.get("control")["uncertain"] is False and c.requests.receipt(identifier) == result
        assert c.current.selected() == 6 and b.rig.bench.firmware.effective_limit() == 0
        assert b.rig.bench.received_limits == before_limits and not starts(b.rig)
        assert all(
            frame[2] not in ("RemoteStartTransaction", "RemoteStopTransaction")
            for frame in b.rig.bench.device_calls[before_calls:]
        )
        j.db.execute("DROP TRIGGER review_worker_boundary")
        j.db.execute("DROP TRIGGER review_cleanup_unavailable")
