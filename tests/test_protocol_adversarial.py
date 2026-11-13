"""Timezone normalization boundaries arrive over real verified TLS and cannot poison later lifecycle delivery."""

import uuid

import pytest

from tests import test_native_bounded_session as native_bench
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_proxy import healthy
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_test_charge import positive_flow
from tests.test_prototype import START
from tests_model.test_schedule_sequences import listener_port, tls_for_case
from tests_model.test_schedule_sequences import model_tls as model_tls
from tests_model.test_start_recovery_sequences import close_daily


# Intent: parsed dates that overflow UTC normalization are rejected with a correlated reply before any durable effect.
# Why: local parsing success cannot let an arithmetic exception strand the charger call or block later valid history.
@pytest.mark.parametrize(
    "stamp", ["0001-01-01T00:00:00+01:00", "9999-12-31T23:59:59-01:00"], ids=["utc-underflow", "utc-overflow"]
)
@pytest.mark.parametrize("action", ["StartTransaction", "StopTransaction", "StatusNotification", "MeterValues"])
async def test_wire_timezone_boundary_is_rejected_and_next_lifecycle_works(
    tmp_path, model_tls, monkeypatch, stamp, action
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with commissioned(tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch, 6) as b:
        local = await positive_flow(b, "daily") if action == "StopTransaction" else None
        await healthy(b.controller)
        before = [tuple(row) for row in b.session.journal.db.execute("SELECT * FROM transactions ORDER BY local")]
        counter = b.session.journal.get("next_transaction")
        payload = {
            "StartTransaction": START | {"timestamp": stamp, "idTag": "NATIVE_TIMER"},
            "StopTransaction": {"transactionId": local, "timestamp": stamp, "meterStop": 1100},
            "StatusNotification": {"connectorId": 1, "status": "Preparing", "errorCode": "NoError", "timestamp": stamp},
            "MeterValues": {
                "connectorId": 1,
                "meterValue": [{"timestamp": stamp, "sampledValue": [{"value": "1200"}]}],
            },
        }[action]
        result = await b.rig.bench.call(b.charger, uuid.uuid4().hex, action, payload)
        assert result[0] == 4
        assert [
            tuple(row) for row in b.session.journal.db.execute("SELECT * FROM transactions ORDER BY local")
        ] == before
        assert b.session.journal.get("next_transaction") == counter
        assert (await b.rig.bench.call(b.charger, uuid.uuid4().hex, "Heartbeat", {}))[0] == 3
        if local is None:
            local = await positive_flow(b, "daily")
        await healthy(b.controller)
        assert b.rig.bench.firmware.current == 6
        await close_daily(b, local, "native", timestamp="2026-01-01T00:01:00Z", meter_stop=1100)
        assert b.session.journal.active() is None
