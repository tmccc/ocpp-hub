"""The standalone proxy catches up several ordinary charges across vendor and service interruptions."""

import asyncio
import json
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from ocpp_hub_core.prototype.gateway import decode

from tests.cloud_start_helpers import StartReplyBench
from tests.managed_rig import starts
from tests.native_schedule_support import ZONE, SchedulePeer, window
from tests.process_helpers import process_rig
from tests.prototype_rig import START
from tests.runtime_rig import until
from tests.tls_support import tls as tls

REPORTS = ("StartTransaction", "MeterValues", "StopTransaction")


class LostVendorReply:
    """Disconnect the simulated vendor after it has processed the selected report."""

    # Watch this connection only, so a later connection can acknowledge the same report normally.
    def __init__(self, bench, connection):
        self.bench, self.connection = bench, connection
        self.pending_token = None

    # Preserve every incoming report, including actual retries, and select the one to interrupt.
    async def __aiter__(self):
        async for raw in self.connection:
            frame = decode(raw)
            wanted = self.bench.interrupt
            if wanted is not None and frame[0] == 2 and (frame[2], frame[3].get("timestamp")) == wanted:
                self.pending_token = frame[1]
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


# Drive an ordinary firmware timer and its meter reports with no external Start command.
async def scheduled_charge(child, day_number, demand_minutes):
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
    amperes = bench.firmware.effective_limit()
    assert amperes == 16
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


# Verify that only transmission state can change when a replacement opens the same pending queue.
def queued_reports(snapshot):
    return [(row["seq"], row["token"], row["action"], row["local"], row["payload"]) for row in snapshot["outbox"]]


# Intent: queue genuine reports for three different timer sessions while the simulated vendor is unavailable.
# Why: inserting rows directly could conceal a failure in the proxy's acceptance or saved ordering.
@pytest.fixture
async def backlog(tmp_path, tls, unused_tcp_port_factory, monkeypatch):
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
        yield child, charges, offline


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
async def test_scheduled_charge_backlog_waits_when_start_reply_is_missing(backlog):
    child, charges, offline = backlog
    bench = child.rig.bench
    await interrupt_catchup(child, "StartTransaction", charges[1].start)
    await check_vendor_charge(child, charges[0])
    await until(lambda: child.durable()["outbox"][0]["state"] == "ambiguous")
    interrupted = child.durable()
    check_pending_tail(offline, interrupted, "StartTransaction", charges[1].local)
    first_start, second_start = bench.backend_starts
    assert first_start["closed"] and not second_start["closed"]
    assert second_start["body"] == charges[1].start
    assert [row["vendor"] for row in interrupted["transactions"]] == [
        first_start["answer"]["transactionId"],
        None,
        None,
    ]
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
            and state.get("pending_lifecycle_events") == 4
            and bench.firmware.effective_limit() == 0
        )
    )
    assert held["cloud_connected"] is True
    assert held["owner_control_enabled"] is False and held["can_start"] is False
    assert held["healthy"] is False and held["can_return"] is False
    assert child.durable()["outbox"] == interrupted["outbox"]
    assert child.durable()["transactions"] == interrupted["transactions"]
    await check_owner_history(child, charges, 1)
    assert bench.lifecycle_wires == wire and not starts(child.rig)
    await bench.vendor_outage()
    disconnected = await child.status(lambda state: state.get("cloud_connected") is False)
    assert disconnected["fault"] == held["fault"] and bench.firmware.effective_limit() == 0
