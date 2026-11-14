"""New owner Pause and actual charger replacement retain their authority while failed cleanup waits for storage."""

import asyncio
import sqlite3
import uuid
from contextlib import AsyncExitStack

import pytest

from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import two_later_sessions
from tests.test_managed_current import adjustable
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_normal_start import owner_rig
from tests.test_managed_proxy import healthy, introduced, starts
from tests.test_managed_test_charge import ChargeRig
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests.test_worker_storage_recovery import fail_boundary


# Refuse the real restrictive settlement transaction while leaving a newer owner's Pause writes available.
def critical_settlement_unavailable(j, operation, failure="statement"):
    reached, attempts = asyncio.Event(), []

    # Count actual failed SQLite statements rather than suspending the engine's decision code.
    def observed():
        attempts.append(len(attempts) + 1)
        if len(attempts) >= 2:
            reached.set()
        return 0

    j.db.create_function("review_current_settlement", 0, observed)
    effect = "SELECT RAISE(ABORT,'current settlement unavailable');"
    if failure == "commit":
        j.db.execute("CREATE TABLE review_settlement_parent(id INTEGER PRIMARY KEY)")
        j.db.execute(
            "CREATE TABLE review_settlement_child("
            "id REFERENCES review_settlement_parent(id) DEFERRABLE INITIALLY DEFERRED)"
        )
        effect = "INSERT INTO review_settlement_child VALUES(123);"
    target = (
        "NEW.key='managed_current_change' AND json_extract(NEW.value,'$.state')='uncertain'"
        if operation == "current"
        else "NEW.key='managed_test_charge' AND json_extract(NEW.value,'$.state') IN ('stopping','expired')"
    )
    j.db.execute(
        "CREATE TEMP TRIGGER review_cleanup_unavailable BEFORE INSERT ON meta "
        "WHEN " + target + " BEGIN SELECT review_current_settlement(); " + effect + " END"
    )
    return reached, attempts


# Keep the current-specific poll fixture bound to the genuine restrictive state transition.
def current_settlement_unavailable(j, failure="statement"):
    return critical_settlement_unavailable(j, "current", failure)


# Intent: later owner authority survives every cleanup retry, including a genuine new connection and Boot.
# Why: a failed old positive operation may restrict its own effect but cannot resurrect its old revision or ticket.
@pytest.mark.parametrize("operation", ["start", "current"])
@pytest.mark.parametrize("replacement_boot", [False, True])
async def test_storage_cleanup_preserves_new_pause_and_boot(
    tmp_path, tls, unused_tcp_port, monkeypatch, operation, replacement_boot
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    context = (
        owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch, startup=1)
        if operation == "start"
        else adjustable(tmp_path, tls, unused_tcp_port, monkeypatch)
    )
    async with context as b, AsyncExitStack() as sockets:
        c, s, rig = b.controller, b.session, b.rig
        j, sock = s.journal, b.charger
        fail_boundary(j, operation, "statement")
        reached, attempts = critical_settlement_unavailable(j, operation)
        identifier = uuid.uuid4().hex
        c.command(
            identifier,
            "start" if operation == "start" else "set_current",
            c.requests.intent().revision,
            **({} if operation == "start" else {"current_a": 16}),
        )
        worker = c.tests.worker if operation == "start" else c.current.worker
        try:
            await asyncio.wait_for(reached.wait(), 3)
            assert len(attempts) >= 2 and worker is not None and not worker.done()
            old_epoch, old_boot = s.gateway._charger_epoch, s.gateway.boot_observation
            if replacement_boot:
                await sock.close()
                await until(lambda: not s.runtime.status.charger_connected)
                sock = await sockets.enter_async_context(rig.charger(s))
                await introduced(rig, s, sock, "cleanup-new-real-Boot")
                assert s.gateway._charger_epoch != old_epoch and s.gateway.boot_observation != old_boot
            # Submit the real newer Pause while cleanup still fails; do not tick through the blocked worker.
            new_pause = uuid.uuid4().hex
            c.command(new_pause, "pause", c.requests.intent().revision)
            if c.pause_task is not None:
                try:
                    await asyncio.wait_for(asyncio.shield(c.pause_task), 3)
                except sqlite3.IntegrityError:
                    assert operation == "start"  # Zero can succeed before the deliberately refused Start settlement.
            latest_intent = c.requests.intent()
            latest_ticket = j.get("control")["ticket"]
            latest_epoch, latest_boot = s.gateway._charger_epoch, s.gateway.boot_observation
            assert latest_intent.mode == "paused" and rig.bench.firmware.effective_limit() == 0
            calls = len(rig.bench.device_calls)
            j.db.execute("DROP TRIGGER review_worker_boundary")
            j.db.execute("DROP TRIGGER review_cleanup_unavailable")
            await asyncio.wait_for(asyncio.shield(worker), 3)
            assert c.requests.intent() == latest_intent and j.get("control")["ticket"] == latest_ticket
            assert s.gateway._charger_epoch == latest_epoch and s.gateway.boot_observation == latest_boot
            assert starts(rig) == [] and all(
                frame[2] != "SetChargingProfile"
                or frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
                for frame in rig.bench.device_calls[calls:]
            )
            assert (await healthy(c))["local_hold_verified"]
            assert c.requests.receipt(new_pause)["state"] == "completed"
            c.command(uuid.uuid4().hex, "app_control", c.requests.intent().revision)
            assert (await healthy(c))["station_ceiling_a"] == 6
            assert c.requests.receipt(identifier)["state"] == "failed"
            await two_later_sessions(ChargeRig(rig, s, c, sock), selected=6, original_starts=0)
            assert j.next_delivery() is None and not j.get("managed_control_fault")
        finally:
            # Teardown releases only the test's SQL fixtures; failed assertions cannot trap the owned cleanup loop.
            j.db.execute("DROP TRIGGER IF EXISTS review_worker_boundary")
            j.db.execute("DROP TRIGGER IF EXISTS review_cleanup_unavailable")
            if worker is not None:
                await asyncio.wait_for(asyncio.gather(worker, return_exceptions=True), 3)
