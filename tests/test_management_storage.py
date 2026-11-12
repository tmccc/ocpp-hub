"""Receipt truthfulness and transactional Pause acceptance under a real SQLite journal."""

from __future__ import annotations

import json
import ssl
import uuid
from pathlib import Path

import pytest
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.managed_view import charger_history, session_history, summarize_charger_history
from ocpp_hub_core.prototype.store import Journal

from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session
from tests.test_prototype_tls import tls as tls


# Intent: a completed session is not cloud acknowledged while its actual Stop remains in the delivery queue.
# Why: a Start mapping alone says nothing about delivery of the final meter and session closure.
def test_completed_history_requires_stop_delivery_not_just_start_mapping(tmp_path: Path) -> None:
    journal = Journal(tmp_path / "state.db", "SYNTHETIC")
    start = json.dumps({"timestamp": "2026-01-01T00:00:00Z", "meterStart": 1000})
    stop = json.dumps({"timestamp": "2026-01-01T01:00:00Z", "meterStop": 2000})
    try:
        journal.db.execute(
            "INSERT INTO transactions(local,connector,start,stop,vendor) VALUES(1,1,?,?,99)", (start, stop)
        )
        journal.db.execute(
            "INSERT INTO outbox(token,action,payload,local) VALUES('stop','StopTransaction',?,1)", (stop,)
        )
        assert session_history(journal)[0]["cloud_acknowledged"] is False
        journal.db.execute("DELETE FROM outbox WHERE token='stop'")
        assert session_history(journal)[0]["cloud_acknowledged"] is True
        assert session_history(journal)[0]["energy_kwh"] == 1
    finally:
        journal.close()


# Intent: the native history view preserves measured values, strips private fields and rejects invalid rows.
# Why: replacing the cloud cache must not leak tags or display guessed/stale session values.
def test_native_history_is_redacted_bounded_and_validated() -> None:
    record = {
        "startTime": 1767225600,
        "stopTime": 1767229200,
        "meterStart": 1000,
        "meterStop": 2200,
        "idTagStart": "SYNTHETIC_PRIVATE_TAG",
        "transactionId": 99,
    }
    live = record | {"startTime": 1767232800, "stopTime": 0, "meterStop": 0}
    invalid = record | {"stopTime": 1767225500}
    result = charger_history((record, live, invalid))
    assert not summarize_charger_history((record, live, invalid)).complete
    assert summarize_charger_history((record, live)).complete
    assert len(result) == 2 and result[0]["stopped_at"] is None and result[0]["energy_kwh"] is None
    assert result[1]["energy_kwh"] == 1.2
    assert all(set(r) == {"started_at", "stopped_at", "energy_kwh"} for r in result)
    assert "SYNTHETIC_PRIVATE_TAG" not in json.dumps(result)
    for bad in (record | {"startTime": True}, record | {"meterStart": -1}, record | {"meterStop": "2200"}):
        assert charger_history((bad,)) == []
    assert len(charger_history(tuple(record | {"startTime": 1767225600 + i} for i in range(30)))) == 20


# Intent: a failed hold-claim commit rolls back both the Pause intent and command acceptance.
# Why: a recorded user pause must not coexist with a silently unchanged shared positive authority after a DB failure.
async def test_pause_and_hold_claim_are_atomic(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "storage-boot")
            await healthy(controller)
            actual = session.gateway.controls._claim

            # Fail only the commit boundary, without replacing the real controller, protocol or physical peer.
            def fail_claim(*args, **kwargs):
                raise OSError("synthetic storage failure")

            monkeypatch.setattr(session.gateway.controls, "_claim", fail_claim)
            request = uuid.uuid4().hex
            with pytest.raises(OSError):
                controller.command(request, "pause", 0)
            assert controller.requests.intent().mode == "app"
            assert controller.requests.receipt(request)["state"] == "not_found"
            monkeypatch.setattr(session.gateway.controls, "_claim", actual)
            controller.command(request, "pause", 0)
            assert (await healthy(controller))["local_hold_verified"]
