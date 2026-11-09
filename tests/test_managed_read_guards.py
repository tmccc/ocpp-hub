"""Exact app history reads remain harmless through native-bounded recovery and frozen resource ledgers."""

from __future__ import annotations

import copy
import json
import ssl
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.frames import build_call
from ocpp_hub_core.prototype.ics import is_safe_transaction_query
from ocpp_hub_core.prototype.policy import DeniedError

from tests.test_native_bounded_recovery import crash
from tests.test_native_bounded_session import connected_bench
from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests.test_trial_session import Bench
from tests.trial_helpers import TrialRig


# Construct only the evidenced string-body extension, independently of namespace translation in the engine.
def query(identifier: int) -> dict[str, Any]:
    return {"vendorId": "IoCharger", "messageId": "getTransaction", "data": json.dumps({"transactionId": identifier})}


# Intent: outgoing validation recognizes already translated local ids without attempting a second translation.
# Why: historical cloud and native transaction namespaces differ and a read must retain its actual target.
@pytest.mark.parametrize("identifier", [0, 1001, 9000001, 2147483647])
def test_exact_history_read_preserves_translated_identifier(identifier: int) -> None:
    payload = query(identifier)
    before = copy.deepcopy(payload)
    assert is_safe_transaction_query(payload)
    assert payload == before
    assert not is_safe_transaction_query({**payload, "messageId": "setTransaction"})
    assert not is_safe_transaction_query({**payload, "vendorId": "AnotherVendor"})


# Intent: malformed reads never gain permission from a familiar extension name.
# Why: extra fields, duplicate keys and noninteger identifiers could conceal an unobserved command shape.
@pytest.mark.parametrize(
    "body",
    [
        '{"transactionId":true}',
        '{"transactionId":1.0}',
        '{"transactionId":"1001"}',
        '{"transactionId":-1}',
        '{"transactionId":2147483648}',
        '{"transactionId":1001,"eco":"Fast"}',
        '{"transactionId":1001,"transactionId":1002}',
        '{"transactionId":NaN}',
        '{"transactionId":Infinity}',
        "[]",
        "null",
        "{",
        {"transactionId": 1001},
        " " * 4097,
    ],
)
def test_history_read_requires_exact_bounded_json_integer(body: Any) -> None:
    with pytest.raises(DeniedError):
        is_safe_transaction_query({**query(1001), "data": body})
    with pytest.raises(DeniedError):
        is_safe_transaction_query({**query(1001), "extra": body})


# Intent: exact history reads pass every frozen resource phase without creating or changing ownership receipts.
# Why: a harmless app refresh during recovery must not become an uncertain mutation or prevent cloud recovery.
@pytest.mark.parametrize("phase", ["recovering", "blocked", "rollback", "backend_restore_pending"])
async def test_frozen_resource_book_allows_only_exact_history_read(tmp_path: Path, phase: str) -> None:
    rig = TrialRig(tmp_path / "state.db")
    try:
        rig.store.put("trial_phase", phase)
        before = list(rig.store.db.execute("SELECT * FROM trial_resources"))
        rig.book.before_send("DataTransfer", query(1001))
        with pytest.raises(DeniedError):
            rig.book.before_send("DataTransfer", {**query(1001), "data": '{"transactionId":true}'})
        with pytest.raises(DeniedError):
            rig.book.before_send("DataTransfer", {**query(1001), "messageId": "setTransaction"})
        assert list(rig.store.db.execute("SELECT * FROM trial_resources")) == before
        assert rig.store.get("trial_phase") == phase
        assert rig.store.get("uncertain_vendor_command") is None
        assert not rig.charger_frames
    finally:
        await rig.close()


# Intent: an app request traverses both real TLS legs and both send guards during genuine process recovery.
# Why: rejecting this read after translation used to record false uncertain-command debt and block owner release.
async def test_real_recovery_forwards_history_read_without_write_debt(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connections: list[Any] = []
    original_vendor = Bench.vendor

    # The independent cloud peer exposes its socket so the test can issue a real incoming app request.
    async def cloud_peer(self: Bench, connection: Any) -> None:
        connections.append(connection)
        await original_vendor(self, connection)

    monkeypatch.setattr(Bench, "vendor", cloud_peer)
    async with connected_bench(tmp_path, tls, unused_tcp_port) as rig:
        actual_answer = rig.bench.firmware.answer

        # Model an evidenced Accepted metadata answer from the fake charger, using its received local id.
        def firmware_answer(action: str, payload: dict[str, Any]) -> dict[str, Any]:
            if action == "DataTransfer" and payload.get("messageId") == "getTransaction":
                return {
                    "status": "Accepted",
                    "data": json.dumps({**json.loads(payload["data"]), "eco": "Fast"}),
                }
            return actual_answer(action, payload)

        monkeypatch.setattr(rig.bench.firmware, "answer", firmware_answer)
        first = rig.session()
        await first.start()
        rig.route_to_trial()
        async with rig.charger(first) as charger:
            await rig.bench.call(charger, "first-boot", "BootNotification", BOOT)
            await until(lambda: first.gateway.controls.ready and first.gateway.vendor_registration == "Accepted")
            await first.command({"command": "release"})
            started = await rig.bench.call(charger, "history-start", "StartTransaction", START)
            local = started[2]["transactionId"]
            await until(lambda: first.journal.vendor_id(local) is not None)
            vendor = first.journal.vendor_id(local)
            assert vendor is not None and vendor != local
            await rig.bench.call(
                charger,
                "history-stop",
                "StopTransaction",
                {"transactionId": local, "meterStop": 1000, "timestamp": "2026-01-01T00:01:00Z"},
            )
            await until(lambda: first.journal.next_delivery() is None)
            await crash(first)
        second = rig.session()
        await second.start()
        async with rig.charger(second) as charger:
            await rig.bench.call(charger, "second-boot", "BootNotification", BOOT)
            await until(lambda: second.gateway.controls.ready and second.gateway.vendor_registration == "Accepted")
            await until(lambda: second.journal.next_delivery() is None and not second.gateway.vendor._pending)
            assert second.journal.get("trial_phase") == "recovering"
            assert rig.bench.firmware.effective_limit() == 0
            count = len(rig.bench.device_calls)
            mutations = second.journal.db.execute("SELECT COUNT(*) FROM mutations").fetchone()[0]
            await connections[-1].send(build_call("app-history", "DataTransfer", query(vendor)))
            await until(lambda: any(frame[0] != 2 and frame[1] == "app-history" for frame in rig.bench.calls))
            answer = next(frame for frame in rig.bench.calls if frame[0] != 2 and frame[1] == "app-history")
            assert answer[0] == 3 and answer[2]["status"] == "Accepted"
            assert json.loads(answer[2]["data"]) == {"transactionId": vendor, "eco": "Fast"}
            forwarded = rig.bench.device_calls[count:]
            assert len(forwarded) == 1 and forwarded[0][2] == "DataTransfer"
            assert json.loads(forwarded[0][3]["data"]) == {"transactionId": local}
            assert second.journal.get("trial_phase") == "recovering"
            assert second.journal.get("uncertain_vendor_command") is None
            assert second.journal.db.execute("SELECT COUNT(*) FROM mutations").fetchone()[0] == mutations
            assert rig.bench.firmware.effective_limit() == 0 and second.gateway.controls.desired() == 0
            await connections[-1].send(
                build_call("bad-history", "DataTransfer", {**query(vendor), "data": '{"transactionId":true}'})
            )
            await until(lambda: any(frame[0] != 2 and frame[1] == "bad-history" for frame in rig.bench.calls))
            bad = next(frame for frame in rig.bench.calls if frame[0] != 2 and frame[1] == "bad-history")
            assert bad[0] == 4 and bad[2] == "SecurityError"
            assert len(rig.bench.device_calls) == count + 1
            assert second.journal.get("uncertain_vendor_command") is None
