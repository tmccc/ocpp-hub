"""The standalone proxy catches up several ordinary charges across vendor and service interruptions."""

import asyncio
import json
import sqlite3
import time
import uuid
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.managed_delivery_recovery import KEY as MAPPINGS
from ocpp_hub_core.prototype.managed_delivery_recovery import delivery_recovery_digest
from ocpp_hub_core.prototype.managed_release_recovery import PROOF as RELEASE_PROOF
from ocpp_hub_core.prototype.store import RecoveryRequiredError, fingerprint
from ocpp_hub_core.prototype.trial_session import save_private

from tests.cloud_start_helpers import StartReplyBench
from tests.managed_rig import starts
from tests.native_schedule_support import ZONE, SchedulePeer, window
from tests.process_helpers import process_rig
from tests.prototype_rig import START
from tests.runtime_rig import until
from tests.test_proxy_rollback_history import retain_timed_start
from tests.tls_support import tls as tls
from tests_installation.migrated_layout import seed_old_layout
from tools import reconcile_managed_debt as debt_tool
from tools import reconcile_managed_delivery as delivery_tool
from tools.rollback_recovery_state import recovery_history_settled

REPORTS = ("StartTransaction", "MeterValues", "StopTransaction")
OPERATOR = "managed_operator_debt_reconciliations"


class LostVendorReply:
    """Disconnect the simulated vendor after it has processed the selected report."""

    # Watch this connection only, so a later connection can acknowledge the same report normally.
    def __init__(self, bench, connection):
        self.bench, self.connection = bench, connection
        self.pending_token = None
        self.paused_token = None

    # Preserve every incoming report, including actual retries, and select the one to interrupt.
    async def __aiter__(self):
        async for raw in self.connection:
            frame = decode(raw)
            if frame[0] == 2 and frame[2] == "StartTransaction" and frame[3]["timestamp"] == self.bench.drop_start:
                # This Start reaches the vendor transport but never enters the independent vendor ledger.
                self.bench.drop_start = None
                self.bench.unaccepted_start = frame
                self.bench.vendor_available = False
                await self.connection.close(code=1011)
                self.bench.interrupted.set()
                return
            wanted = self.bench.interrupt
            if wanted is not None and frame[0] == 2 and (frame[2], frame[3].get("timestamp")) == wanted:
                self.pending_token = frame[1]
            if frame[0] == 2 and frame[2] == "StopTransaction" and frame[3]["timestamp"] == self.bench.hold_stop:
                self.paused_token = frame[1]
            yield raw

    # The vendor's existing ledger has processed the report before attempting to send the reply.
    async def send(self, raw):
        frame = decode(raw)
        if frame[0] == 3 and frame[1] == self.pending_token:
            self.bench.interrupt = None
            self.bench.vendor_available = False
            await self.connection.close(code=1011)
            self.bench.interrupted.set()
            return
        if frame[0] == 3 and frame[1] == self.paused_token:
            self.bench.stop_waiting.set()
            await self.bench.release_stop.wait()
        await self.connection.send(raw)


class CatchupBench(StartReplyBench):
    """Control only the independent vendor peer, including when it accepts new connections."""

    # Begin online so the unchanged fixture can commission the simulated installation.
    def __init__(self):
        super().__init__()
        self.vendor_available = True
        self.refused = asyncio.Event()
        self.interrupted = asyncio.Event()
        self.interrupt = None
        self.drop_start = None
        self.unaccepted_start = None
        self.hold_stop = None
        self.stop_waiting = asyncio.Event()
        self.release_stop = asyncio.Event()

    # Keep one vendor ledger across every socket and both proxy processes.
    async def vendor(self, connection):
        if self.vendor_available:
            await super().vendor(LostVendorReply(self, connection))
        else:
            await connection.close(code=1013)
            self.refused.set()

    # End the existing connection and refuse future connections until the test restores service.
    async def vendor_outage(self):
        self.vendor_available = False
        await self.cloud.connection.close(code=1011)
        await asyncio.wait_for(self.refused.wait(), 12)


@dataclass(frozen=True)
class ChargeReports:
    """The independent charger's reports and the number returned by the actual proxy."""

    local: int
    start: dict
    meters: tuple[dict, ...]
    stop: dict


# Start one session from the charger timer and retain its actual scheduled end.
async def begin_scheduled_charge(child, day_number):
    bench, peer = child.rig.bench, child.rig.peer
    plan = bench.firmware.plans[9]
    day = datetime(2026, 1, 5, tzinfo=ZONE) + timedelta(days=day_number)
    start, end = window(plan, day)
    started, ended = (datetime.fromtimestamp(stamp, UTC) for stamp in (start, end))
    meter_start = 10_000 + day_number * 30_000
    start_report = START | {
        "idTag": plan["idTag"],
        "timestamp": started.isoformat(),
        "meterStart": meter_start,
    }
    answer = await bench.call(child.socket, uuid.uuid4().hex, "StartTransaction", start_report)
    assert answer[0] == 3 and answer[2]["idTagInfo"]["status"] == "Accepted", answer
    local = answer[2]["transactionId"]
    peer.plan_session = (local, plan["planId"])
    peer.paused = False
    bench.firmware.fire_plan(plan["planId"])
    return local, start_report, started, ended


# Drive ordinary readings at the selected ceiling with no external Start command.
async def scheduled_charge(child, day_number, demand_minutes, amperes=16):
    bench, peer = child.rig.bench, child.rig.peer
    local, start_report, started, ended = await begin_scheduled_charge(child, day_number)
    assert bench.firmware.effective_limit() == amperes
    meter_start = start_report["meterStart"]
    reports = []
    for minute in (30, 240):
        drawing = minute < demand_minutes
        bench.firmware.requested_current = 32 if drawing else 0
        bench.cp, bench.connection = ("C", "Charging") if drawing else ("B", "Plug")
        peer.permission_enabled = drawing
        power = int(amperes * 240) if drawing else 0
        await child.status(
            lambda status: status.get("charging_session_open") is True and status.get("power_w") == power
        )
        reading = {
            "connectorId": 1,
            "transactionId": local,
            "meterValue": [
                {
                    "timestamp": (started + timedelta(minutes=minute)).isoformat(),
                    "sampledValue": [
                        {
                            "measurand": "Energy.Active.Import.Register",
                            "unit": "Wh",
                            "value": str(meter_start + round(amperes * 240 * min(minute, demand_minutes) / 60)),
                        },
                        {"measurand": "Power.Active.Import", "unit": "W", "value": str(power)},
                    ],
                }
            ],
        }
        reply = await bench.call(child.socket, uuid.uuid4().hex, "MeterValues", reading)
        assert reply[0] == 3
        reports.append(reading)

    stop = {
        "transactionId": local,
        "timestamp": ended.isoformat(),
        "meterStop": meter_start + round(amperes * 240 * demand_minutes / 60),
    }
    assert (await bench.call(child.socket, uuid.uuid4().hex, "StopTransaction", stop))[0] == 3
    peer.permission_enabled, peer.plan_session = False, None
    await child.status(lambda status: status.get("charging_session_open") is False and status.get("power_w") == 0)
    return ChargeReports(local, start_report, tuple(reports), stop)


# Check the independent vendor's assigned number, complete session and each exact translated reading.
async def check_vendor_charge(child, charge, *, stop_attempts=1):
    row = await child.delivered(charge.local, closed=True)
    assert json.loads(row["start"]) == charge.start and json.loads(row["stop"]) == charge.stop
    bench = child.rig.bench
    (entry,) = [entry for entry in bench.backend_starts if entry["body"] == charge.start]
    assert entry["answer"]["transactionId"] == row["vendor"] and entry["closed"]
    vendor = row["vendor"]
    assert entry["stop"]["body"] == charge.stop | {"transactionId": vendor}
    meters = [
        frame[3]
        for frame in bench.calls
        if frame[0] == 2 and frame[2] == "MeterValues" and frame[3].get("transactionId") == vendor
    ]
    assert meters == [body | {"transactionId": vendor} for body in charge.meters]
    stop_frames = [
        frame
        for frame in bench.lifecycle_wires
        if frame[2] == "StopTransaction" and frame[3].get("transactionId") == vendor
    ]
    assert stop_frames == [[2, entry["stop"]["token"], "StopTransaction", entry["stop"]["body"]]] * stop_attempts
    assert sum(frame[2] == "StartTransaction" and frame[3] == charge.start for frame in bench.lifecycle_wires) == 1


# Check the same history that the Home Assistant client reads, including whether the vendor confirmed each charge.
async def check_owner_history(child, charges, acknowledged_count):
    history = await child.request("history")
    assert history["ok"]
    assert history["result"]["sessions"] == [
        {
            "started_at": charge.start["timestamp"],
            "stopped_at": charge.stop["timestamp"],
            "energy_kwh": (charge.stop["meterStop"] - charge.start["meterStart"]) / 1000,
            "cloud_acknowledged": index < acknowledged_count,
        }
        for index, charge in reversed(list(enumerate(charges)))
    ]


# Intent: draw can stall, resume and reach zero before the timer closes the same session.
# Why: archived overnight samples showed positive offered current throughout zero draw, with one lower later offer.
async def test_scheduled_charge_keeps_one_session_through_zero_draw(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch
):
    port = unused_tcp_port_factory
    async with process_rig(
        tmp_path,
        tls,
        port(),
        port(),
        monkeypatch,
        32,
        bench_type=CatchupBench,
        native_peer=SchedulePeer,
        timing="fast-poll",
    ) as child:
        await child.start()
        ready = await child.status()
        bench, peer = child.rig.bench, child.rig.peer
        reported_offer = 320
        read_native = peer.native

        # Keep the independent firmware profile and measured draw while varying the separate native offer.
        def native(key):
            value = read_native(key)
            if key == "CONNECTOR":
                value[0]["offerCur"] = reported_offer
            return value

        monkeypatch.setattr(peer, "native", native)
        local, start, started, ended = await begin_scheduled_charge(child, 0)
        peer.permission_enabled = True
        bench.cp, bench.connection = "C", "Charging"
        reports = []
        # The last two meter values of each zero span agree before the timer sends its actual Stop.
        for minute, draw, reported_offer, energy in (
            (30, 32, 320, 13_840),
            (31, 0, 320, 13_840),
            (32, 0, 320, 13_840),
            (60, 32, 320, 17_424),
            (240, 6, 320, 40_464),
            (270, 0, 61, 41_184),
            (359, 0, 61, 41_184),
        ):
            bench.firmware.requested_current = draw
            power = draw * 240
            status = await child.status(lambda state: state["charging_session_open"] and state["power_w"] == power)
            assert status["current_a"] == draw and status["station_ceiling_a"] == 32
            assert status["requested_current_a"] == 32 and status["control_revision"] == ready["control_revision"]
            assert bench.firmware.active == local and bench.firmware.effective_limit() == 32
            assert not any(frame[2] == "RemoteStopTransaction" for frame in bench.device_calls)
            assert not any(frame[2] == "StopTransaction" for frame in bench.lifecycle_wires)
            reading = {
                "connectorId": 1,
                "transactionId": local,
                "meterValue": [
                    {
                        "timestamp": (started + timedelta(minutes=minute)).isoformat(),
                        "sampledValue": [
                            {"measurand": "Energy.Active.Import.Register", "unit": "Wh", "value": str(energy)},
                            {"measurand": "Current.Offered", "unit": "A", "value": str(reported_offer / 10)},
                            {"measurand": "Power.Active.Import", "unit": "W", "value": str(power)},
                        ],
                    }
                ],
            }
            assert (await bench.call(child.socket, uuid.uuid4().hex, "MeterValues", reading))[0] == 3
            reports.append(reading)

        stop = {"transactionId": local, "timestamp": ended.isoformat(), "meterStop": 41_184}
        assert (await bench.call(child.socket, uuid.uuid4().hex, "StopTransaction", stop))[0] == 3
        peer.plan_session, peer.permission_enabled = None, False
        first = ChargeReports(local, start, tuple(reports), stop)
        await check_vendor_charge(child, first)
        await check_owner_history(child, [first], 1)

        reported_offer = 320
        later = await scheduled_charge(child, 1, 360, amperes=32)
        await check_vendor_charge(child, later)
        await check_owner_history(child, [first, later], 2)
        assert later.local != local and not starts(child.rig)


# Verify that only transmission state can change when a replacement opens the same pending queue.
def queued_reports(snapshot):
    return [(row["seq"], row["token"], row["action"], row["local"], row["payload"]) for row in snapshot["outbox"]]


# Intent: queue genuine reports for three different timer sessions while the simulated vendor is unavailable.
# Why: inserting rows directly could conceal a failure in the proxy's acceptance or saved ordering.
@pytest.fixture
async def backlog(tmp_path, tls, unused_tcp_port_factory, monkeypatch):
    seed_old_layout(tmp_path / "state.db")
    port = unused_tcp_port_factory
    async with process_rig(
        tmp_path,
        tls,
        port(),
        port(),
        monkeypatch,
        16,
        bench_type=CatchupBench,
        native_peer=SchedulePeer,
        timing="fast-poll",
    ) as child:
        await child.start()
        await child.status()
        bench = child.rig.bench
        await bench.vendor_outage()
        await child.status(lambda status: status.get("cloud_connected") is False)
        charges = [await scheduled_charge(child, index, demand) for index, demand in enumerate((232, 0, 360))]
        offline = child.durable()
        assert len({charge.local for charge in charges}) == 3
        assert [row["vendor"] for row in offline["transactions"]] == [None] * 3
        assert not bench.lifecycle_wires
        assert [row["action"] for row in offline["outbox"]] == [
            "StartTransaction",
            "MeterValues",
            "MeterValues",
            "StopTransaction",
        ] * 3
        await check_owner_history(child, charges, 0)
        try:
            yield child, charges, offline
        finally:
            bench.release_stop.set()


# Restore the vendor until it processes the exact report, then wait for the peer to drop the reply and close.
async def interrupt_catchup(child, action, payload):
    bench = child.rig.bench
    bench.interrupt = (action, payload["timestamp"])
    bench.vendor_available = True
    await asyncio.wait_for(bench.interrupted.wait(), 20)


# Check the exact retained report and every row behind it without comparing transient delivery state.
def check_pending_tail(before, after, action, local):
    original = queued_reports(before)
    position = next(index for index, row in enumerate(original) if row[2:4] == (action, local))
    assert queued_reports(after) == original[position:]


# Confirm actual vendor registration from the simulated vendor's sent replies.
def accepted_boots(bench):
    accepted = {frame[1] for frame in bench.sent_vendor_replies if frame[2].get("status") == "Accepted"}
    return sum(frame[0] == 2 and frame[2] == "BootNotification" and frame[1] in accepted for frame in bench.calls)


# Accept an explicit Pause through the service even when delivery is still pending.
async def request_pause(child, status):
    identifier = uuid.uuid4().hex
    result = await child.request("pause", id=identifier, revision=status["control_revision"])
    assert result["ok"]
    await child.status(lambda value: value.get("desired_control") == "paused")
    return identifier


# Use SQLite only to inject a named failure or inspect a retained reply in the test journal.
def journal_sql(child, statement, args=()):
    with closing(sqlite3.connect(child.rig.prepared.config.journal)) as db, db:
        return db.execute(statement, args).fetchall()


# Keep negative cases at the real operator entry point and restore the exact original queued row after each refusal.
async def reject_unsafe_tail(child, args, port, before):
    head, *tail = before["outbox"]
    later_start = next(row for row in tail if row["action"] == "StartTransaction")
    later_meter = next(row for row in tail if row["action"] == "MeterValues" and row["local"] == later_start["local"])
    wrong = json.loads(later_meter["payload"]) | {"transactionId": head["local"]}
    for column, row, changed in (
        ("state", later_start, "ambiguous"),
        ("payload", later_meter, json.dumps(wrong)),
        ("seq", later_start, tail[-1]["seq"] + 1),
    ):
        statement = f"UPDATE outbox SET {column}=? WHERE token=?"
        journal_sql(child, statement, (changed, row["token"]))
        corrupted = child.durable()
        try:
            with pytest.raises(RecoveryRequiredError):
                await delivery_tool.run("inspect", *args, None, port)
            assert child.durable() == corrupted
        finally:
            journal_sql(child, statement, (row[column], row["token"]))
    (record,) = [entry for entry in child.rig.bench.backend_starts if entry["token"] == head["token"]]
    reply = json.dumps([3, record["token"], record["answer"]])
    journal_sql(
        child,
        "INSERT INTO delivery_replies VALUES(?,?,?,?,?)",
        (head["token"], head["action"], head["payload"], head["local"], reply),
    )
    try:
        with pytest.raises(RecoveryRequiredError, match="unresolved delivery"):
            await delivery_tool.run("inspect", *args, None, port)
        assert journal_sql(child, "SELECT token FROM delivery_replies") == [(head["token"],)]
    finally:
        journal_sql(child, "DELETE FROM delivery_replies WHERE token=?", (head["token"],))
    assert child.durable() == before


# Use the public offline tool and the independent vendor ledger while the service has released the journal.
async def map_accepted_start(child, charge, capsys, *, check_invalid_tail=False):
    directory = child.directory
    bench = child.rig.bench
    (vendor_record,) = [record for record in bench.backend_starts if record["body"] == charge.start]
    assert vendor_record["answer"]["idTagInfo"]["status"] == "Accepted"
    args = (
        directory / "trial.json",
        directory / "process-strategy.json",
    )
    port = child.rig.prepared.observer.port
    before, commands = child.durable(), list(bench.device_calls)
    if check_invalid_tail:
        await reject_unsafe_tail(child, args, port, before)
    assert await delivery_tool.run("inspect", *args, None, port) == 0
    candidate = json.loads(capsys.readouterr().out)["candidate"]
    assert candidate["local"] == charge.local and candidate["start_token"] == vendor_record["token"]
    assert candidate["start_digest"] == fingerprint(charge.start)
    assert candidate["stop_digest"] == fingerprint(charge.stop)
    evidence = directory / f"independent-vendor-record-{charge.local}.json"
    save_private(
        evidence,
        candidate
        | {
            "vendor": vendor_record["answer"]["transactionId"],
            "evidence_sha256": fingerprint(vendor_record),
            "verified_at_epoch": time.time(),
        },
    )
    assert await delivery_tool.run("reconcile-start", *args, evidence, port) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["mapping_recorded"] is True and first["already_recorded"] is False
    after = child.durable()
    assert after["outbox"] == before["outbox"][1:]
    assert after["transactions"] == [
        row | {"vendor": vendor_record["answer"]["transactionId"]} if row["local"] == charge.local else row
        for row in before["transactions"]
    ]
    assert await delivery_tool.run("reconcile-start", *args, evidence, port) == 0
    assert json.loads(capsys.readouterr().out)["already_recorded"] is True
    assert child.durable() == after
    assert bench.device_calls == commands and bench.firmware.effective_limit() == 0


# When a later Stop was actually rejected, use its existing operator tool and let the real wire retry settle it.
async def approve_rejected_stop(child, charge, capsys):
    directory, bench = child.directory, child.rig.bench
    args = (directory / "trial.json", directory / "process-strategy.json")
    port = child.rig.prepared.observer.port
    (record,) = [entry for entry in bench.backend_starts if entry["body"] == charge.start]
    assert not record["closed"]
    assert await debt_tool.run("inspect", *args, None, port, kind="stop-delivery") == 0
    candidate = json.loads(capsys.readouterr().out)["candidate"]
    assert candidate["local_transaction_id"] == charge.local
    assert candidate["vendor_transaction_id"] == record["answer"]["transactionId"]
    assert candidate["stop_digest"] == fingerprint(charge.stop)
    evidence = directory / f"independent-vendor-stop-review-{charge.local}.json"
    save_private(
        evidence,
        candidate
        | {
            "evidence_sha256": fingerprint(record),
            "verified_at_epoch": time.time(),
            "cloud_stop_independently_verified": True,
            "fault_origin_independently_verified": True,
            "original_state_independently_verified": True,
        },
    )
    assert await debt_tool.run("reconcile", *args, evidence, port, kind="stop-delivery") == 0
    assert json.loads(capsys.readouterr().out)["zero_hold_retained"] is True


# The peer independently confirms that this exact observed Start was never allocated.
async def approve_unaccepted_start(child, charge, capsys):
    directory, bench = child.directory, child.rig.bench
    args = (directory / "trial.json", directory / "process-strategy.json")
    port = child.rig.prepared.observer.port
    dropped = bench.unaccepted_start
    assert dropped is not None and dropped[2:] == ["StartTransaction", charge.start]
    assert all(entry["token"] != dropped[1] for entry in bench.backend_starts)
    old_records = child.durable()["metadata"][OPERATOR]
    ((old_id, old_entry),) = old_records.items()
    assert old_entry["proof"]["wire_ack"] is not None and old_entry["completed"] is None

    kind = "start-delivery"
    assert await debt_tool.run("inspect", *args, None, port, kind=kind) == 0
    candidate = json.loads(capsys.readouterr().out)["candidate"]
    assert candidate["local_transaction_id"] == charge.local
    assert candidate["delivery_token"] == dropped[1] and candidate["start_digest"] == fingerprint(charge.start)
    evidence = directory / "independent-vendor-start-review.json"
    save_private(
        evidence,
        candidate
        | {
            "evidence_sha256": fingerprint({"unaccepted_start": dropped}),
            "verified_at_epoch": time.time(),
            "cloud_start_not_accepted": True,
            "original_state_independently_verified": True,
            "fault_origin_independently_verified": True,
        },
    )
    assert await debt_tool.run("reconcile", *args, evidence, port, kind=kind) == 0
    assert json.loads(capsys.readouterr().out)["zero_hold_retained"] is True
    records = child.durable()["metadata"][OPERATOR]
    (new_id,) = records.keys() - {old_id}
    assert records[old_id]["proof"] == old_entry["proof"]
    assert records[old_id]["superseded"]["proof"] == new_id
    assert records[new_id]["proof"]["prior"]["previous_operator"]["id"] == old_id
    return new_id


# Intent: preserve three scheduled charges and their readings through two vendor outages and a service replacement.
# Why: retries during catch-up can otherwise hide lost readings, wrong mappings or duplicates.
async def test_scheduled_charge_backlog_survives_interrupted_catchup(backlog):
    child, charges, offline = backlog
    bench = child.rig.bench
    await interrupt_catchup(child, "StopTransaction", charges[1].stop)
    await check_vendor_charge(child, charges[0])
    interrupted = child.durable()
    check_pending_tail(offline, interrupted, "StopTransaction", charges[1].local)
    assert not any(frame[3] == charges[2].start for frame in bench.lifecycle_wires)
    assert len(bench.backend_starts) == 2 and bench.backend_starts[1]["closed"]
    await child.kill()
    await child.start()
    assert queued_reports(child.durable()) == queued_reports(interrupted)
    assert child.durable()["transactions"] == interrupted["transactions"]

    bench.vendor_available = True
    await until(lambda: not child.durable()["outbox"], timeout=23)
    for index, charge in enumerate(charges):
        await check_vendor_charge(child, charge, stop_attempts=2 if index == 1 else 1)
    await child.status()
    final = await scheduled_charge(child, 3, 360)
    await check_vendor_charge(child, final)
    await check_owner_history(child, [*charges, final], 4)
    assert not child.durable()["outbox"] and len(bench.backend_starts) == 4
    assert not bench.backend_errors and not starts(child.rig)


# Intent: retain a vendor-accepted Start and the later reports when the proxy loses the vendor's answer.
# Why: retrying an unknown Start can duplicate it, and skipping it can send later readings under the wrong number.
@pytest.mark.parametrize(
    "later_reply",
    ["accepted", "lost-start", "rejected-stop", "rejected-stop-then-lost-start", "rejected-stop-then-retry-start"],
)
async def test_scheduled_charge_backlog_waits_when_start_reply_is_missing(backlog, capsys, later_reply):
    child, charges, offline = backlog
    bench = child.rig.bench
    retry = later_reply == "rejected-stop-then-retry-start"
    combined = later_reply == "rejected-stop-then-lost-start" or retry
    if retry:
        charges.append(await scheduled_charge(child, 3, 360))
        offline = child.durable()
    first_index = 0 if combined else 1
    uncertain = charges[first_index]
    await interrupt_catchup(child, "StartTransaction", uncertain.start)
    for charge in charges[:first_index]:
        await check_vendor_charge(child, charge)
    await until(lambda: child.durable()["outbox"][0]["state"] == "ambiguous")
    interrupted = child.durable()
    check_pending_tail(offline, interrupted, "StartTransaction", uncertain.local)
    *delivered, unconfirmed = bench.backend_starts
    assert len(delivered) == first_index and all(record["closed"] for record in delivered)
    assert not unconfirmed["closed"] and unconfirmed["body"] == uncertain.start
    assert [row["vendor"] for row in interrupted["transactions"]] == (
        [record["answer"]["transactionId"] for record in delivered] + [None] * (len(charges) - first_index)
    )
    wire = list(bench.lifecycle_wires)
    boot_count = accepted_boots(bench)
    await child.kill()
    await child.start()
    bench.vendor_available = True
    await until(lambda: accepted_boots(bench) > boot_count, timeout=20)
    registered_at = time.time()
    held = await child.status(
        lambda state: (
            state["checked_at"] >= registered_at
            and state.get("fault") == "owner_control_or_delivery_uncertain"
            and state.get("pending_lifecycle_events") == 2 * (len(charges) - first_index)
            and bench.firmware.effective_limit() == 0
        )
    )
    assert held["cloud_connected"] is True
    assert held["owner_control_enabled"] is False and held["can_start"] is False
    assert held["healthy"] is False and held["can_return"] is False
    assert child.durable()["outbox"] == interrupted["outbox"]
    assert child.durable()["transactions"] == interrupted["transactions"]
    await check_owner_history(child, charges, first_index)
    assert bench.lifecycle_wires == wire and not starts(child.rig)
    await bench.vendor_outage()
    disconnected = await child.status(lambda state: state.get("cloud_connected") is False)
    assert disconnected["fault"] == held["fault"] and bench.firmware.effective_limit() == 0

    await request_pause(child, disconnected)
    await child.close()
    assert child.process.returncode == 0
    if retry:
        with closing(sqlite3.connect(child.rig.prepared.config.journal)) as db, db:
            retain_timed_start(db, time.time())
        old_timed_start = next(row for row in child.durable()["requests"] if row["action"] == "start_test")
    await map_accepted_start(child, uncertain, capsys, check_invalid_tail=later_reply == "accepted")
    bench.hold_stop = charges[-1].stop["timestamp"]
    if later_reply == "lost-start":
        bench.interrupted.clear()
        bench.interrupt = ("StartTransaction", charges[2].start["timestamp"])
    elif later_reply.startswith("rejected-stop"):
        bench.stop_replies = ["accepted", "error"]

    if later_reply == "accepted":
        journal_sql(
            child,
            "CREATE TRIGGER refuse_mapped_stop AFTER DELETE ON outbox "
            f"WHEN OLD.action='StopTransaction' AND OLD.local={charges[1].local} "
            "BEGIN SELECT RAISE(ABORT,'synthetic failed Stop commit'); END",
        )
    bench.vendor_available = True
    await child.start()
    if later_reply == "accepted":
        await until(lambda: journal_sql(child, "SELECT 1 FROM delivery_replies WHERE action='StopTransaction'"), 15)
        proof = child.durable()["metadata"][MAPPINGS][str(charges[1].local)]["proof"]
        assert proof["stop_wire_ack"] is None
        assert child.durable()["outbox"][0]["token"] == proof["stop_token"]
        journal_sql(child, "DROP TRIGGER refuse_mapped_stop")
    elif later_reply == "lost-start":
        await asyncio.wait_for(bench.interrupted.wait(), 15)
        await until(lambda: child.durable()["outbox"][0]["state"] == "ambiguous")
        await child.close()
        await map_accepted_start(child, charges[2], capsys)
        bench.vendor_available = True
        await child.start()
    else:
        rejected_charge = charges[first_index + 1]
        await until(lambda: child.durable()["outbox"][0]["state"] == "rejected")
        await child.close()
        await approve_rejected_stop(child, rejected_charge, capsys)
        if combined:
            bench.interrupted.clear()
            if retry:
                bench.drop_start = charges[2].start["timestamp"]
            else:
                bench.interrupt = ("StartTransaction", charges[2].start["timestamp"])
        await child.start()
        if combined:
            await asyncio.wait_for(bench.interrupted.wait(), 15)
            await until(lambda: child.durable()["outbox"][0]["state"] == "ambiguous")
            await child.close()
            if retry:
                retry_id = await approve_unaccepted_start(child, charges[2], capsys)
                bench.stop_replies = ["accepted", "error"]
            else:
                await map_accepted_start(child, charges[2], capsys)
            bench.vendor_available = True
            await child.start()
            if retry:
                await until(lambda: child.durable()["outbox"][0]["state"] == "rejected")
                await child.close()
                blocked = child.durable()
                assert blocked["outbox"][0]["local"] == charges[-1].local
                previous = blocked["metadata"][OPERATOR][retry_id]
                assert previous["superseded"] and previous["proof"]["stop_wire_ack"] is not None
                await approve_rejected_stop(child, charges[-1], capsys)
                await child.start()

    await asyncio.wait_for(bench.stop_waiting.wait(), 15)
    waiting = await child.status(lambda state: state.get("pending_lifecycle_events") == 1)
    fault = waiting["fault"]
    assert waiting["can_return"] is False and bench.firmware.effective_limit() == 0
    early = await request_pause(child, waiting)
    await check_owner_history(child, charges, len(charges) - 1)
    bench.release_stop.set()
    await until(lambda: not child.durable()["outbox"], timeout=23)
    for index, charge in enumerate(charges):
        retried = later_reply.startswith("rejected-stop") and (index == first_index + 1 or retry and index == 3)
        await check_vendor_charge(child, charge, stop_attempts=2 if retried else 1)
    await check_owner_history(child, charges, len(charges))
    drained = await child.status(lambda state: state.get("pending_lifecycle_events") == 0)
    assert drained["fault"] == fault and drained["can_return"] is False
    assert bench.firmware.effective_limit() == 0
    replay = await child.request("pause", id=early, revision=waiting["control_revision"])
    assert replay["ok"]
    assert (await child.status(lambda value: value.get("control_revision") == drained["control_revision"]))[
        "fault"
    ] == fault
    fresh = await request_pause(child, drained)
    await child.completed_control(fresh)
    paused = await child.status(lambda state: state.get("can_return") is True)
    assert paused["local_hold_verified"] and bench.firmware.effective_limit() == 0
    proofs = child.durable()["metadata"][MAPPINGS]
    mapped = [uncertain, charges[2]] if later_reply in ("lost-start", "rejected-stop-then-lost-start") else [uncertain]
    assert set(proofs) == {str(charge.local) for charge in mapped}
    for entry in proofs.values():
        proof = entry["proof"]
        assert entry["completed"]["pause"] == fresh
        assert proof["stop_wire_ack"]["delivery_token"] == proof["stop_token"]
    if later_reply in ("accepted", "rejected-stop"):
        # The stopped reader recognises records produced by ordinary recovery while keeping exact wire identity.
        metadata = child.durable()["metadata"]
        journal = child.rig.prepared.config.journal
        with closing(sqlite3.connect(f"{journal.as_uri()}?mode=ro", uri=True)) as db:
            assert recovery_history_settled(db, metadata, paused["control_revision"])
            (mapped_proof,) = [entry["proof"] for entry in metadata[MAPPINGS].values()]
            mapped_proof["stop_wire_ack"]["delivery_token"] = "e-" + uuid.uuid4().hex
            metadata[RELEASE_PROOF]["delivery_reconciliation"] = delivery_recovery_digest([mapped_proof])
            assert not recovery_history_settled(db, metadata, paused["control_revision"])
    if retry:
        recovered = child.durable()["metadata"][OPERATOR]
        start_proof = recovered[retry_id]["proof"]
        assert start_proof["start_wire_ack"] is not None and start_proof["stop_wire_ack"] is not None
        (last_stop,) = [entry for entry in recovered.values() if entry.get("completed") is not None]
        assert last_stop["completed"]["pause"] == fresh
        assert last_stop["proof"]["attestation"]["local_transaction_id"] == charges[-1].local
        assert old_timed_start in child.durable()["requests"]
        (record,) = [entry for entry in bench.backend_starts if entry["body"] == charges[2].start]
        assert record["token"] == bench.unaccepted_start[1]
    elif combined:
        recovered = child.durable()["metadata"][OPERATOR]
        parent = proofs[str(charges[2].local)]["proof"]["operator_parent"]
        assert recovered[parent]["completed"]["pause"] == fresh
    resumed = await child.request("app_control", revision=paused["control_revision"])
    assert resumed["ok"]
    await child.completed_control(resumed["result"]["id"])
    await child.status()
    final = await scheduled_charge(child, len(charges), 360)
    await check_vendor_charge(child, final)
    assert not starts(child.rig)
