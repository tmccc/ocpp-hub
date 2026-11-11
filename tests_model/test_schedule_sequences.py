"""Replay the bounded external-event universe against genuine HTTP/TLS/OCPP peers and SQLite state."""

from __future__ import annotations

import asyncio
import copy
import os
import socket
import ssl
import subprocess
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pytest

from tests import conftest as _engine  # noqa: F401 - load the HA-free engine package.
from tests.managed_scenario_helpers import demand_from_vehicle, next_daily, originals, select_current
from tests.test_managed_proxy import introduced, starts
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_native_history_archive import unassigned
from tests.test_prototype import START
from tests_model.schedule_cases import SequenceCase, cases, smoke_cases


# Intent: retain verified TLS while amortizing synthetic RSA generation across a worker's independent cases.
# Why: each device and journal must be fresh; repeated RSA generation adds no event-order coverage.
@pytest.fixture(scope="session")
def model_tls(tmp_path_factory):
    root = tmp_path_factory.mktemp("model-tls")
    key, certificate = root / "key.pem", root / "certificate.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=offline.test",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-keyout",
            str(key),
            "-out",
            str(certificate),
        ],
        capture_output=True,
        check=True,
        timeout=20,
    )
    return TLSMaterial(key.read_bytes(), certificate.read_bytes())


@dataclass(frozen=True, repr=False)
class TLSMaterial:
    """Synthetic certificate bytes are reusable, but should never clutter failure representations."""

    key: bytes
    certificate: bytes


# Reconstruct private per-case files because the real preflight owns and checks their permissions.
def tls_for_case(path, material):
    key, certificate = path / "key.pem", path / "certificate.pem"
    key.write_bytes(material.key)
    certificate.write_bytes(material.certificate)
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(certificate, key)
    return server, ssl.create_default_context(cafile=str(certificate)), certificate


# Allocate only inside this worker's checked non-ephemeral pool, then start its sole scenario listener immediately.
def listener_port():
    worker = int(os.environ.get("MODEL_WORKER_INDEX", "0"))
    if not 0 <= worker < 20:
        raise ValueError("model worker index outside its allocated pool")
    for port in range(14000 + worker * 500, 14500 + worker * 500):
        with socket.socket() as reservation:
            try:
                reservation.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    raise RuntimeError("no free model listener port")


@dataclass
class PeerLedger:
    """Independent facts about the physical occurrence and real messages, not engine classifications."""

    created: bool = False
    native_disclosed: bool = False
    physical_open: bool = False
    local: int | None = None
    occurred: int | None = None
    body: dict | None = None
    attempt: dict | None = None
    commands: list[tuple[str, dict]] = field(default_factory=list)
    receipts: dict[str, dict] = field(default_factory=dict)
    report_refusals: list[list] = field(default_factory=list)
    report_attempts: int = 0
    recovery_retry_advances: int = 0


class Supervisor:
    """Own exactly the current controller's deadline task through simulated process replacement."""

    # Start outside construction so the prior task is always joined before its journal closes.
    def __init__(self):
        self.task = None

    # Replace only after the new controller exists; no task survives from the old process.
    async def start(self, b):
        await self.close()
        self.task = asyncio.create_task(b.c.schedule.watch_deadline())

    # Propagate genuine deadline failures and cancel ordinary waits before fixture teardown.
    async def close(self):
        if self.task is not None:
            task, self.task = self.task, None
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            if not task.cancelled():
                task.result()


# Derive the firmware's wire representation from the owner's inputs, independently of the production encoder.
def expected_wire_plan(case, identifier, tag, start_epoch, accepted_epoch):
    start = datetime.fromtimestamp(start_epoch, ZONE)
    ending = start.replace(hour=6, minute=0, second=0, microsecond=0)
    return {
        "planId": identifier,
        "type": "Plan",
        "eco": "Fast",
        "stopModel": "Duration",
        "kindType": "Relative",
        "recurring": 0,
        "connectorId": 1,
        "limitCurrent": case.selected,
        "stopValue": (int(ending.timestamp()) - start_epoch) // 60,
        "startTime": start.hour * 60 + start.minute,
        "validTime": datetime.fromtimestamp(accepted_epoch, UTC).strftime("%Y-%m-%dT%H:%M:00Z"),
        "idTag": tag,
    }


# Deliver a bounded number of fair controller turns; a timeout or exhausted progress assertion remains a failure.
async def turns(b, count=4):
    b.c.schedule.changed.set()
    status = {}
    for _ in range(count):
        status = await b.c.tick()
        await asyncio.sleep(0)
    return status


# Compare independent wire and device facts; never use the production source/duty predicates as the oracle.
def invariant(b, case, ledger, saved, allowed_plan, cutoff, stable=False):
    assert b.c.current.selected() == case.selected
    assert b.rig.peer.configuration == saved[1]
    assert b.device.plans[9] == saved[0]
    assert set(b.device.plans) <= {9, allowed_plan["planId"]}
    if allowed_plan["planId"] in b.device.plans:
        assert b.device.plans[allowed_plan["planId"]] == allowed_plan
    current = b.session.journal.get("managed_temporary_schedule", {})
    archived = b.session.journal.get("managed_temporary_occurrences", {}) | {current["id"]: current}
    for identifier, expected in ledger.receipts.items():
        for key in (
            "id",
            "revision",
            "accepted_epoch",
            "start_epoch",
            "end_epoch",
            "floor",
            "body",
            "install",
            "original_digest",
            "augmented_digest",
        ):
            assert archived[identifier][key] == expected[key], "an occurrence borrowed another receipt's identity"
    assert not starts(b.rig), "sequence recovery emitted a synthetic RemoteStart"
    assert not b.stops(), "daily or unidentified activity received a timer Stop"
    for action, payload in ledger.commands:
        assert action not in ("RemoteStartTransaction", "RemoteStopTransaction")
        if action == "SetChargingProfile":
            assert (
                payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] <= case.selected
            )
    assert 0 <= b.device.effective_limit() <= case.selected
    assert 0 <= b.device.current <= case.selected
    if stable:
        identified_daily = (
            case.source == "daily" and ledger.native_disclosed and ledger.local is not None and ledger.physical_open
        )
        unknown_observed_source = ledger.physical_open and (case.source == "unknown" or not ledger.native_disclosed)
        unresolved_open = (
            ledger.physical_open or ledger.local is not None and b.session.journal.active() == ledger.local
        )
        if unresolved_open and (b.clock.now >= cutoff and not identified_daily or unknown_observed_source):
            assert b.device.effective_limit() == b.device.current == 0, "unresolved cutoff retained positive permission"


# Apply one external event while preserving actual occurrence time separately from late report delivery.
async def event(b, case, name, ledger, cutoff, stack, supervisor):
    if name in ("native", "report") and not ledger.created:
        ledger.created = ledger.physical_open = True
        ledger.occurred = int(b.clock.now)
        ledger.body = START | {
            "idTag": "NATIVE_TIMER",
            "timestamp": datetime.fromtimestamp(b.clock.now, UTC).isoformat(),
        }
        b.device.active = 987654
        b.rig.peer.plan_session = (987654, None)
        demand_from_vehicle(b, case.demand)
    if name == "native":
        ledger.native_disclosed = True
        if ledger.local is None and ledger.attempt is None:
            # Native history exists independently of whether the backend has allocated its OCPP number yet.
            ledger.attempt = unassigned("model-occurrence.json")
            ledger.attempt.update(startTime=ledger.occurred, startMs=0, meterStart=ledger.body["meterStart"])
            ledger.attempt["MeterValues"][0].update(timestamp=ledger.occurred, energy=ledger.body["meterStart"])
            b.rig.bench.history.append(ledger.attempt)
    elif name == "report":
        for attempt in range(4):
            token = f"model-start-{ledger.report_attempts}"
            ledger.report_attempts += 1
            answer = await b.rig.bench.call(b.sock, token, "StartTransaction", ledger.body)
            if answer[0] == 3:
                break
            ledger.report_refusals.append(answer)
            # A concurrent restrictive operation may invalidate an audit; the actual report is retried unchanged.
            assert answer[0] == 4 and answer[2] in ("InternalError", "PropertyConstraintViolation"), answer
            assert b.session.journal.active() is None
            await turns(b, 4)
            if ledger.physical_open:
                assert b.device.effective_limit() == b.device.current == 0
        if answer[0] == 3:
            assert answer[2]["idTagInfo"]["status"] == "Accepted", answer
            ledger.local = answer[2]["transactionId"]
    elif name == "observe":
        await turns(b, 2)
    elif name == "cutoff":
        b.clock.now = cutoff
        await turns(b, 6)
    elif name == "cancel":
        if case.disruption == "restart":
            from tests.managed_scenario_helpers import replace_controller

            await supervisor.close()
            await replace_controller(b)
            b.sock = await stack.enter_async_context(b.rig.charger(b.session))
            await introduced(b.rig, b.session, b.sock, "sequence-restarted")
            await supervisor.start(b)
        else:
            ledger.physical_open = False
            b.device.active = None
            demand_from_vehicle(b, False)
            b.rig.peer.plan_session = None
            b.rig.bench.cp, b.rig.bench.connection = "A", "Unplug"
            answer = await b.rig.bench.call(
                b.sock,
                "model-departure",
                "StatusNotification",
                {"connectorId": 1, "status": "Available", "errorCode": "NoError"},
            )
            assert answer[0] == 3
            b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
    if name in ("native", "report"):
        if ledger.physical_open:
            identity = ledger.local if ledger.local is not None else 987654
            b.device.active = identity
            b.rig.peer.plan_session = (identity, 9 if case.source == "daily" and ledger.native_disclosed else None)
            demand_from_vehicle(b, case.demand)
        else:
            b.device.active = None
            b.rig.peer.plan_session = None
            demand_from_vehicle(b, False)


# Require both true closure channels, exact selective cleanup and a complete following daily occurrence.
async def recovery(b, case, ledger, saved):
    assert ledger.local is not None
    if ledger.physical_open and case.first_close == "ledger":
        await b.close_transaction(ledger.local)
        b.device.active = ledger.local
        await turns(b)
        assert b.device.effective_limit() == 0
        b.device.active = None
    else:
        b.device.active = None
        b.rig.peer.plan_session = None
        demand_from_vehicle(b, False)
        await turns(b)
        assert b.device.effective_limit() == 0
        await b.close_transaction(ledger.local)
    b.rig.peer.plan_session = None
    demand_from_vehicle(b, False)
    for retry in range(3):
        await turns(b, 12)
        assert b.session.journal.active() is None
        assert b.device.current == 0 and not b.stops()
        if b.device.plans == {9: saved[0]}:
            break
        # Give bounded cleanup retries real opportunities after a superseded or rejected write.
        assert b.device.effective_limit() == 0
        if retry < 2:
            b.clock.now += 301
            ledger.recovery_retry_advances += 1
    assert b.device.plans == {9: saved[0]}
    await next_daily(b, case.selected, saved)


# Intent: execute every causally admitted permutation and seed in the declared finite two-receipt universe.
# Why: request cancellation, cleanup, source delivery and cutoff must be checked as histories, not isolated transitions.
@pytest.mark.parametrize(
    "case",
    smoke_cases(all_orders=os.environ.get("MODEL_SMOKE") == "orders")
    if os.environ.get("MODEL_SMOKE")
    else cases()[: int(os.environ["MODEL_CASE_LIMIT"])]
    if os.environ.get("MODEL_CASE_LIMIT")
    else cases(),
    ids=lambda case: case.identifier,
)
async def test_two_receipt_event_sequences(tmp_path, model_tls, monkeypatch, record_property, case: SequenceCase):
    tls = tls_for_case(tmp_path, model_tls)
    async with timer_bench(tmp_path, tls, listener_port(), monkeypatch) as b:
        await select_current(b, case.selected)
        saved = originals(b)
        await b.arm("00000001" + "0" * 24)
        older = copy.deepcopy(b.session.journal.get("managed_temporary_schedule"))
        cutoff = int(
            datetime.fromtimestamp(b.start, ZONE).replace(hour=6, minute=0, second=0, microsecond=0).timestamp()
        )
        b.clock.now = b.start + 601
        await turns(b, 6)
        assert b.device.plans == {9: saved[0]}
        await b.wait_for_admission()
        b.start = (int(b.clock.now) // 60 + 4) * 60
        b.request("00000002" + "0" * 24)
        newer = copy.deepcopy(b.session.journal.get("managed_temporary_schedule"))
        allowed_plan = expected_wire_plan(case, newer["body"]["planId"], saved[0]["idTag"], b.start, b.clock.now)
        if case.inventory != "requested":
            await turns(b, 3)
            assert allowed_plan["planId"] in b.device.plans
        if case.inventory == "complete":
            b.clock.now = b.start + 601
            await turns(b, 6)
            assert b.device.plans == {9: saved[0]}
        ledger = PeerLedger(receipts={older["id"]: older, newer["id"]: newer})
        answer = b.device.answer

        # Record every actual device-bound mutation while retaining the real firmware implementation.
        def record_command(action, payload):
            ledger.commands.append((action, copy.deepcopy(payload)))
            return answer(action, payload)

        b.device.answer = record_command
        async with AsyncExitStack() as stack:
            supervisor = Supervisor()
            await supervisor.start(b)
            stack.push_async_callback(supervisor.close)
            completed = []
            suffix_complete = False
            try:
                for name in case.events:
                    await event(b, case, name, ledger, cutoff, stack, supervisor)
                    completed.append(name)
                    invariant(b, case, ledger, saved, allowed_plan, cutoff, stable=name in ("observe", "cutoff"))
                await turns(b, 6)
                invariant(b, case, ledger, saved, allowed_plan, cutoff, stable=True)
                if ledger.local is None:
                    await event(b, case, "report", ledger, cutoff, stack, supervisor)
                    assert ledger.local is not None, "genuine report remained refused after the native history arrived"
                await recovery(b, case, ledger, saved)
                suffix_complete = True
            finally:
                record_property("model_case", case.identifier)
                record_property("completed_events", completed)
                record_property("suffix_complete", suffix_complete)
                record_property("wire_command_count", len(ledger.commands))
                record_property("report_refusals", ledger.report_refusals)
                record_property("report_attempts", ledger.report_attempts)
                record_property("recovery_retry_advances", ledger.recovery_retry_advances)
