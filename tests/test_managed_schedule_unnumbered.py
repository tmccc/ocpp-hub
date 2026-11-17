"""An original timer may report its Start before firmware has received an OCPP transaction number."""

from __future__ import annotations

import uuid
from contextlib import AsyncExitStack
from datetime import UTC, datetime
from typing import Any

from ocpp_hub_core.prototype.native_start_attempts import matching_unassigned_start, start_millisecond_range

from tests.native_schedule_support import ZONE, TimerBench, timer_bench
from tests.test_managed_proxy import healthy, introduced, starts
from tests.test_native_history_archive import unassigned
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


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


# Intent: matching identifies native attempts, while an ordinary original-plan report obtains its real OCPP number.
# Why: allocation under zero grants no current until native and local source identifiers actually converge.
async def test_unnumbered_original_needs_matching_source_and_attempt(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with (
        timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b,
        AsyncExitStack() as connections,
    ):
        await b.sock.close()
        await until(lambda: not b.session.runtime.status.charger_connected)
        b.sock = await connections.enter_async_context(b.rig.charger(b.session))
        await introduced(b.rig, b.session, b.sock, "unnumbered-daily-reconnect")
        await b.c.ordinary.hold("restricted_introduction")
        assert b.device.effective_limit() == 0 and b.c.ordinary.active()
        midnight = int(datetime(2026, 11, 12, tzinfo=ZONE).timestamp())
        body, attempt = unnumbered(b, midnight, "current.json")
        b.clock.now = midnight + 2
        for payload in (
            body | {"meterStart": body["meterStart"] + 1},
            body | {"timestamp": datetime.fromtimestamp(midnight + 1, UTC).isoformat()},
            body | {"idTag": "CLOUD_OWNER"},
        ):
            assert matching_unassigned_start((attempt,), payload, datetime.fromisoformat(payload["timestamp"])) is None
        competing = attempt | {"filename": "same-second.json", "startMs": 175}
        assert matching_unassigned_start((attempt, competing), body, datetime.fromisoformat(body["timestamp"])) is None
        assert matching_unassigned_start((attempt,), body, datetime.fromisoformat(body["timestamp"])) is not None
        refused = await b.rig.bench.call(
            b.sock, uuid.uuid4().hex, "StartTransaction", body | {"idTag": "CLOUD_OWNER"}
        )
        assert refused[0] == 4 and b.session.journal.active() is None and b.device.effective_limit() == 0
        accepted = await b.rig.bench.call(b.sock, uuid.uuid4().hex, "StartTransaction", body)
        assert accepted[0] == 3
        local = accepted[2]["transactionId"]
        assert b.device.effective_limit() == 0
        # Native absence after allocation still cannot identify the original open daily source.
        b.device.active = None
        for _ in range(2):
            await b.c.tick()
        assert b.device.effective_limit() == 0 and not b.stops()
        b.device.active = local
        b.rig.peer.plan_session = (local, 987655)
        for _ in range(2):
            await b.c.tick()
        assert b.device.effective_limit() == 0 and not b.stops()
        b.rig.peer.plan_session = (local, 9)
        for _ in range(4):
            await b.c.tick()
        assert b.session.journal.active() == local and b.device.effective_limit() == 6 and not b.stops()
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
