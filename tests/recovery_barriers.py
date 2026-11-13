"""Explicit receipt and independent HTTP failure barriers for manually driven recovery models."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta


# Wait for one named request's durable result; generic healthy paused state is not a completed Return.
async def completed_control(b, identifier, *, mode, revision, ceiling):
    c, firmware = b.controller, b.rig.bench.firmware
    async with asyncio.timeout(10):
        while True:
            status = await c.tick()
            receipt = c.requests.receipt(identifier)
            row = b.session.journal.db.execute(
                "SELECT action,revision FROM managed_requests WHERE id=?", (identifier,)
            ).fetchone()
            assert row is not None and row["action"] == ("pause" if mode == "paused" else "app_control")
            assert row["revision"] == revision - 1 and receipt["id"] == identifier
            assert receipt["state"] in ("accepted", "running", "uncertain", "completed"), receipt
            if receipt["state"] == "completed":
                intent = c.requests.intent()
                assert intent.mode == mode and intent.revision == revision
                assert status["state"] == "healthy" and status["station_ceiling_a"] == ceiling
                assert firmware.effective_limit() == ceiling
                assert status["owner_control_enabled"] is (mode == "app")
                return status
            assert status["state"] not in ("attention", "stopping"), status
            await asyncio.sleep(0)


# Retain exactly the accepted Return UUID and wait for its own completion without adding another Pause.
async def returned_to_app(b, expected):
    c = b.controller
    intent = c.requests.intent()
    if intent.mode == "paused":
        pending = b.session.journal.db.execute(
            "SELECT id,revision FROM managed_requests WHERE action='app_control' AND state IN ('accepted','running')"
        ).fetchall()
        assert len(pending) <= 1, "Return authority is exclusive"
        if pending:
            identifier = pending[0]["id"]
            assert pending[0]["revision"] == intent.revision
        else:
            identifier = uuid.uuid4().hex
            accepted = c.command(identifier, "app_control", intent.revision)
            assert accepted["id"] == identifier and accepted["state"] == "accepted"
        return await completed_control(b, identifier, mode="app", revision=intent.revision + 1, ceiling=expected)
    async with asyncio.timeout(10):
        while True:
            status = await c.tick()
            if status["state"] == "healthy" and status["owner_control_enabled"]:
                assert c.requests.intent() == intent
                assert status["station_ceiling_a"] == b.rig.bench.firmware.effective_limit() == expected
                return status
            assert status["state"] not in ("attention", "stopping"), status
            await asyncio.sleep(0)


# Count only independent peer observations from this outage; fair manual ticks are not a substitute for failures.
async def witnessed_native_loss(b, expected, local, *, minimum_failures=3):
    c, rig, firmware = b.controller, b.rig, b.rig.bench.firmware
    assert c.requests.intent().mode == "app" and firmware.effective_limit() == expected
    original_intent = c.requests.intent()
    gateway = b.session.gateway
    epoch, boot = gateway._charger_epoch, gateway.boot_observation
    assert gateway.current_boot_seen(epoch) and boot is not None
    assert hasattr(rig.peer, "failed_reads"), "outage suffix needs independently observed real HTTP failures"
    while not rig.peer.failed_reads.empty():
        rig.peer.failed_reads.get_nowait()
    failures = []
    rig.peer.unavailable = True
    try:
        async with asyncio.timeout(10):
            while (
                len(failures) < minimum_failures
                or firmware.effective_limit() != 0
                or not gateway.controls.ready
                or gateway.controls.desired() != 0
            ):
                await c.tick()
                while not rig.peer.failed_reads.empty():
                    request = rig.peer.failed_reads.get_nowait()
                    assert request["action"] == "getConfig"
                    failures.append(request)
                assert c.requests.intent() == original_intent
                assert gateway._charger_epoch == epoch and gateway.boot_observation == boot
                assert b.session.journal.active() == local
                await asyncio.sleep(0)
        assert len(failures) >= minimum_failures and firmware.effective_limit() == 0
        assert gateway.controls.ready and gateway.controls.desired() == 0
        assert b.session.journal.active() == local
    finally:
        rig.peer.unavailable = False
    async with asyncio.timeout(10):
        while True:
            status = await c.tick()
            assert c.requests.intent() == original_intent
            assert gateway._charger_epoch == epoch and gateway.boot_observation == boot
            if status["state"] == "healthy" and status["owner_control_enabled"]:
                assert status["station_ceiling_a"] == firmware.effective_limit() == expected
                assert b.session.journal.active() == local
                return len(failures)
            assert status["state"] not in ("attention", "stopping"), status
            await asyncio.sleep(0)


# Allocate new chronological payloads from the independent cloud wire transcript, not production allocation state.
def fresh_daily_values(rig, *, not_before=None, meter_floor=1000):
    beginning = not_before or datetime(2026, 1, 1, tzinfo=UTC)
    meters = [meter_floor]
    for frame in rig.bench.calls:
        if frame[0] != 2 or frame[2] not in ("StartTransaction", "StopTransaction"):
            continue
        body = frame[3]
        stamp = body.get("timestamp")
        if isinstance(stamp, str):
            occurred = datetime.fromisoformat(stamp.replace("Z", "+00:00")).astimezone(UTC)
            beginning = max(beginning, occurred + timedelta(minutes=1))
        for key in ("meterStart", "meterStop"):
            if type(body.get(key)) is int:
                meters.append(body[key])
    # Native daily fixtures cover midnight through06:00; a later payload moves to the next original occurrence.
    if beginning.hour >= 6:
        beginning = (beginning + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return beginning, max(meters)
