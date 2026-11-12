"""A genuine terminal Stop retains raw clock discontinuity without weakening meter/identity/duplicate checks."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta

import pytest
from ocpp_hub_core.prototype.store import ConflictError, Journal

from tests.test_managed_proxy import healthy
from tests.test_managed_test_charge import positive_flow, ready
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Construct the exact reported terminal body; only its device timestamp moves behind the original raw Start.
def backwards_stop(local):
    instant = datetime.fromisoformat(START["timestamp"].replace("Z", "+00:00")) - timedelta(seconds=1)
    return {"transactionId": local, "meterStop": START["meterStart"] + 1, "timestamp": instant.isoformat()}


# Intent: authenticated correct-ID Stop terminalizes with increasing meter and exact raw reversed timestamps.
# Why: the physical session's closure must not become an invented continuing session because the device clock changed.
async def test_real_backward_clock_stop_closes_and_retains_exact_raw_history(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        local = await positive_flow(test, source="daily")
        assert (await healthy(test.controller))["current_a"] == 6
        body = backwards_stop(local)
        answer = await test.rig.bench.call(test.charger, "clock-corrected-stop", "StopTransaction", body)
        assert answer[0] == 3
        test.rig.peer.plan_session = None
        test.rig.peer.permission_enabled = False
        await until(lambda: test.session.journal.active() is None and test.session.journal.next_delivery() is None)
        journal = test.session.journal
        assert (
            json.loads(journal.db.execute("SELECT stop FROM transactions WHERE local=?", (local,)).fetchone()[0])
            == body
        )
        anomaly = dict(
            journal.db.execute("SELECT * FROM transaction_clock_discontinuities WHERE local=?", (local,)).fetchone()
        )
        assert anomaly["start_timestamp"] == START["timestamp"] and anomaly["stop_timestamp"] == body["timestamp"]
        assert any(
            frame[0] == 2 and frame[2] == "StopTransaction" and frame[3]["timestamp"] == body["timestamp"]
            for frame in test.rig.bench.calls
        )
        replay = await test.rig.bench.call(test.charger, "clock-corrected-replay", "StopTransaction", body)
        assert replay[0] == 3
        assert (
            dict(
                journal.db.execute("SELECT * FROM transaction_clock_discontinuities WHERE local=?", (local,)).fetchone()
            )
            == anomaly
        )
        for token, changed in (
            ("different-stop", body | {"meterStop": body["meterStop"] + 1}),
            ("lower-meter", body | {"meterStop": START["meterStart"] - 1}),
            ("unknown-stop", body | {"transactionId": local + 999}),
        ):
            assert (await test.rig.bench.call(test.charger, token, "StopTransaction", changed))[0] == 4
        assert (
            json.loads(journal.db.execute("SELECT stop FROM transactions WHERE local=?", (local,)).fetchone()[0])
            == body
        )
        assert journal.db.execute("SELECT COUNT(*) FROM transaction_clock_discontinuities").fetchone()[0] == 1
        assert (await healthy(test.controller))["owner_control_enabled"]


# Intent: queue/storage failure rolls back the terminal Stop and its discontinuity annotation together.
# Why: a diagnostic row must not claim that a Stop was durably accepted if its actual delivery record was not committed.
def test_stop_clock_annotation_rolls_back_with_delivery_failure(tmp_path):
    journal = Journal(tmp_path / "clock.db", "OFFLINE001")
    try:
        local = journal.start("start", START)["transactionId"]
        journal.db.execute(
            "CREATE TRIGGER no_stop BEFORE INSERT ON outbox WHEN NEW.action='StopTransaction' "
            "BEGIN SELECT RAISE(ABORT,'synthetic disk refusal'); END"
        )
        with pytest.raises(sqlite3.IntegrityError, match="synthetic disk refusal"):
            journal.stop("failed-stop", backwards_stop(local))
        assert journal.active() == local
        assert journal.db.execute("SELECT stop FROM transactions WHERE local=?", (local,)).fetchone()[0] is None
        assert not journal.db.execute("SELECT 1 FROM transaction_clock_discontinuities").fetchone()
        assert journal.db.execute("SELECT action FROM outbox").fetchone()[0] == "StartTransaction"
    finally:
        journal.close()


# Intent: raw-clock tolerance never permits a lower meter or guessed transaction identity at the durable boundary.
# Why: a clock discontinuity is separate from contradictory energy or lifecycle correlation.
@pytest.mark.parametrize("bad", ["meter", "identity"])
def test_stop_clock_relaxation_preserves_durable_identity_and_meter_guards(tmp_path, bad):
    journal = Journal(tmp_path / "clock.db", "OFFLINE001")
    try:
        local = journal.start("start", START)["transactionId"]
        body = backwards_stop(local)
        body.update({"meterStop": START["meterStart"] - 1} if bad == "meter" else {"transactionId": local + 1})
        with pytest.raises(ConflictError):
            journal.stop("invalid", body)
        assert journal.active() == local
        assert not journal.db.execute("SELECT 1 FROM transaction_clock_discontinuities").fetchone()
    finally:
        journal.close()
