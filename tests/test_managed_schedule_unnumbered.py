"""An original timer may report its Start before firmware has received an OCPP transaction number."""

from __future__ import annotations

import uuid
from contextlib import nullcontext
from datetime import UTC, datetime
from typing import Any

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.native_start_attempts import matching_unassigned_start, start_millisecond_range

from tests.handover_helpers import crash
from tests.test_managed_proxy import healthy, introduced, managed_session, starts
from tests.test_managed_schedule_cutoff import ZONE, TimerBench, timer_bench
from tests.test_native_history_archive import unassigned
from tests.test_prototype import START
from tests.test_prototype_tls import tls as tls


# Keep the original plan and the unremovable expired one-off side by side under the verified zero profile.
async def retain_timer(b: TimerBench) -> Any:
    await b.arm()
    answer = b.device.answer

    # Keep physical readbacks truthful and refuse only the outstanding one-off removal.
    def refuse_clear(action: str, payload: dict[str, Any]) -> dict[str, Any]:
        if action == "DataTransfer" and payload.get("messageId") == "clearPlans":
            return {"status": "Rejected", "data": ""}
        return answer(action, payload)

    b.device.answer = refuse_clear
    b.clock.now = b.start + 601
    for _ in range(4):
        await b.c.tick()
    assert b.device.effective_limit() == 0 and b.timer in b.device.plans
    return answer


# Populate a new native start-only record while SESSION still exposes no assigned OCPP transaction number.
def unnumbered(b: TimerBench, instant: int, name: str) -> tuple[dict[str, Any], dict[str, Any]]:
    b.clock.now = instant
    b.device.active = None
    b.rig.peer.plan_session = (None, 9)
    b.rig.peer.paused = False
    b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
    b.device.fire_plan(9)
    body = START | {"idTag": b.device.plans[9]["idTag"], "timestamp": datetime.fromtimestamp(instant, UTC).isoformat()}
    attempt = unassigned(name)
    attempt.update(startTime=instant, startMs=731, meterStart=body["meterStart"])
    attempt["MeterValues"][0].update(timestamp=instant, energy=body["meterStart"])
    b.rig.bench.history.append(attempt)
    return body, attempt


# Intent: an original daily Start can obtain its first OCPP number while a failed temporary cleanup holds zero.
# Why: the matching lifecycle must progress across a process restart and a full battery or persistent vehicle demand.
@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("demand", [False, True])
async def test_unnumbered_daily_while_temporary_clear_fails(
    tmp_path, tls, unused_tcp_port, monkeypatch, restart, demand
):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        answer = await retain_timer(b)
        if restart:
            await crash(b.session)
            b.session = managed_session(b.rig)
            await b.session.start()
            b.c = ManagedController(b.session)
        async with b.rig.charger(b.session) if restart else nullcontext(b.sock) as b.sock:
            if restart:
                await introduced(b.rig, b.session, b.sock, "restart-unnumbered")
            midnight = int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp())
            identifiers: set[int] = set()
            for index, instant in enumerate((midnight, midnight + 600, midnight + 86400)):
                body, _ = unnumbered(b, instant, f"daily-{index}.json")
                b.rig.peer.permission_enabled = demand
                if not demand:
                    b.device.requested_current = 0
                for _ in range(3):
                    await b.c.tick()
                native = await b.c.native.read()
                assert native.physical.native_transaction_id is None and native.session_plan_id == 9
                assert b.device.effective_limit() == 0
                if index == 1:
                    body["timestamp"] = body["timestamp"].replace("+00:00", ".731000Z")
                    b.clock.now += 1
                reply = await b.rig.bench.call(b.sock, uuid.uuid4().hex, "StartTransaction", body)
                assert reply[0] == 3 and reply[2]["idTagInfo"]["status"] == "Accepted"
                local = reply[2]["transactionId"]
                assert local not in identifiers
                identifiers.add(local)
                assert b.session.journal.active() == local and b.device.effective_limit() == 0
                # An allocated ledger number alone still cannot identify the physical source.
                b.device.active = None
                for _ in range(2):
                    await b.c.tick()
                assert b.device.effective_limit() == 0 and not b.stops()
                b.device.active = local
                b.rig.peer.plan_session = (local, 9)
                b.rig.bench.cp, b.rig.bench.connection = ("C", "Charging") if demand else ("B", "Plug")
                for _ in range(4):
                    await b.c.tick()
                assert b.session.journal.active() == local and b.device.effective_limit() == 6
                assert b.device.current == (6 if demand else 0) and not b.stops()
                b.clock.now += 60
                await b.close_transaction(local)
                for _ in range(4):
                    await b.c.tick()
                assert b.session.journal.active() is None and b.device.effective_limit() == 0
                b.rig.peer.plan_session = (None, 9)
                for precision in ("", ".7", ".73", ".731", ".731900"):
                    replay = body | {
                        "timestamp": datetime.fromtimestamp(instant, UTC).strftime("%Y-%m-%dT%H:%M:%S")
                        + precision
                        + "Z"
                    }
                    if replay == body:
                        continue
                    repeated = await b.rig.bench.call(b.sock, uuid.uuid4().hex, "StartTransaction", replay)
                    assert repeated[0] == 4 or repeated[2]["transactionId"] == local
                    assert b.session.journal.active() is None and not b.stops()
            b.device.answer = answer
            b.clock.now += 301
            await healthy(b.c)
            assert set(b.device.plans) == {9} and b.device.effective_limit() == 6
            b.clock.now = int(datetime(2026, 11, 14, tzinfo=ZONE).timestamp())
            following = await b.fire(9, demand=demand)
            assert following not in identifiers and (await healthy(b.c))["station_ceiling_a"] == 6
            assert b.device.current == (6 if demand else 0) and not b.stops() and not starts(b.rig)


# Intent: only the commissioned original plan and its own unique, matching zero-draw attempt may report without an ID.
# Why: an unassigned history filename can outlive its vehicle and the two plans can share an identification tag.
async def test_unnumbered_original_needs_matching_source_and_attempt(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        answer = await retain_timer(b)
        midnight = int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp())
        body, attempt = unnumbered(b, midnight, "current.json")
        for source, payload in (
            (b.timer, body),
            (987655, body),
            (9, body | {"meterStart": body["meterStart"] + 1}),
            (9, body | {"timestamp": datetime.fromtimestamp(midnight + 1, UTC).isoformat()}),
            (9, body | {"idTag": "CLOUD_OWNER"}),
        ):
            b.clock.now = midnight + 2
            b.rig.peer.plan_session = (None, source)
            refused = await b.rig.bench.call(b.sock, uuid.uuid4().hex, "StartTransaction", payload)
            assert refused[0] == 4 and b.session.journal.active() is None and b.device.effective_limit() == 0
        # Two plausible files also leave provenance unresolved.
        b.rig.bench.history.append(attempt | {"filename": "same-second.json", "startMs": 175})
        refused = await b.rig.bench.call(b.sock, uuid.uuid4().hex, "StartTransaction", body)
        assert refused[0] == 4 and b.session.journal.active() is None and not b.stops()
        b.rig.bench.history.pop()
        accepted = await b.rig.bench.call(b.sock, uuid.uuid4().hex, "StartTransaction", body)
        assert accepted[0] == 3
        local = accepted[2]["transactionId"]
        assert b.device.effective_limit() == 0
        b.rig.peer.plan_session = (local, 9)
        for _ in range(4):
            await b.c.tick()
        assert b.session.journal.active() == local and b.device.effective_limit() == 6 and not b.stops()
        b.device.answer = answer
        await b.close_transaction(local)
        b.clock.now += 301
        await healthy(b.c)
        assert set(b.device.plans) == {9} and not starts(b.rig)


# Intent: firmware precision and the precision actually transmitted on OCPP jointly determine a unique native attempt.
# Why: two starts can have the same energy within one second, and a whole-second report cannot distinguish them.
def test_match_unnumbered_history_start_precision():
    attempt = unassigned() | {"startMs": 179}
    same_second = attempt | {"filename": "and-again.json", "startMs": 176}
    body = START | {"idTag": "native_timer", "meterStart": 9000}
    for fraction, width, alone, both in (
        ("", 1000, True, False),
        (".0", 100, False, False),
        (".1", 100, True, False),
        (".17", 10, True, False),
        (".179", 1, True, True),
        (".179999", 1, True, True),
        (".000", 1, False, False),
    ):
        stamp = f"1970-01-01T01:16:40{fraction}+01:00"
        instant = datetime.fromisoformat(stamp)
        assert len(start_millisecond_range(stamp, instant)) == width
        report = body | {"timestamp": stamp}
        actual = (attempt["filename"], 1_000_179)
        assert matching_unassigned_start((attempt,), report, instant) == (actual if alone else None)
        assert matching_unassigned_start((attempt, same_second), report, instant) == (actual if both else None)
        assert matching_unassigned_start((attempt,), report | {"reservationId": 42}, instant) is None
        reserved = attempt | {"reservationId": 42}
        assert matching_unassigned_start((reserved,), report | {"reservationId": 42}, instant) == (
            actual if alone else None
        )


# Intent: a distinct pending owner command can coexist with an unnumbered daily Start after the one-off is removed.
# Why: neither the original schedule nor its independently matched lifecycle may spend the earlier command's grant.
@pytest.mark.parametrize("demand", [False, True])
async def test_unnumbered_daily_with_earlier_grant_after_restart(tmp_path, tls, unused_tcp_port, monkeypatch, demand):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await b.arm()
        await b.acknowledge_older_owner_start()
        daily_plan = b.device.plans[9].copy()
        b.clock.now = int(datetime(2026, 11, 11, 6, tzinfo=ZONE).timestamp())
        for _ in range(5):
            await b.c.tick()
        grant = b.session.journal.get("vendor_start_grant") | {"expires": 0}
        b.session.journal.put("vendor_start_grant", grant)
        assert b.device.effective_limit() == 0 and b.device.plans == {9: daily_plan}
        await crash(b.session)
        b.session = managed_session(b.rig)
        await b.session.start()
        b.c = ManagedController(b.session)
        async with b.rig.charger(b.session) as b.sock:
            await introduced(b.rig, b.session, b.sock, "earlier-grant-unnumbered")
            midnight = int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp())
            body, _ = unnumbered(b, midnight, "daily-and-owner.json")
            b.rig.peer.permission_enabled = demand
            if not demand:
                b.device.requested_current = 0
            for _ in range(4):
                await b.c.tick()
            assert b.device.effective_limit() == 0 and b.session.journal.active() is None
            response = await b.rig.bench.call(b.sock, uuid.uuid4().hex, "StartTransaction", body)
            assert response[0] == 3 and response[2]["idTagInfo"]["status"] == "Accepted"
            local = response[2]["transactionId"]
            assert b.session.journal.active() == local and b.device.effective_limit() == 0
            assert b.session.journal.get("vendor_start_grant") == grant
            b.rig.peer.plan_session = (local, 9)
            b.rig.bench.cp, b.rig.bench.connection = ("C", "Charging") if demand else ("B", "Plug")
            for _ in range(4):
                await b.c.tick()
            assert b.device.effective_limit() == 6 and b.device.current == (6 if demand else 0)
            assert b.session.journal.get("vendor_start_grant") == grant and not b.stops()
            b.clock.now += 60
            await b.close_transaction(local)
            for _ in range(4):
                await b.c.tick()
            assert b.session.journal.active() is None and b.device.effective_limit() == 0
            assert b.device.plans == {9: daily_plan} and b.session.journal.get("vendor_start_grant") == grant
            assert not b.stops()
