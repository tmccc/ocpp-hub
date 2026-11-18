"""Timezone normalization boundaries arrive over real verified TLS and cannot poison later lifecycle delivery."""

import uuid

import pytest

from tests.charge_rig import positive_flow
from tests.current_rig import adjustable, commissioned
from tests.managed_rig import healthy
from tests.model_tls_support import listener_port, tls_for_case
from tests.model_tls_support import model_tls as model_tls
from tests.native_peer_failures import InterruptedNativePeer
from tests.prototype_rig import START
from tests_model.test_start_recovery_sequences import close_daily


# Intent: an identifiable bad CALL gets a correlated error and an old status retry cannot replace newer state.
# Why: neither ordinary peer mistake should close a valid TLS connection or corrupt the current observation.
async def test_malformed_call_and_status_replay_are_isolated(tmp_path, model_tls, monkeypatch):
    async with adjustable(tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch) as b:
        peer, charger, j = b.rig.bench, b.charger, b.session.journal
        result = await peer.call(charger, "malformed-call", "Heartbeat", [])
        assert result[:3] == [4, "malformed-call", "FormationViolation"]
        status = {"connectorId": 1, "status": "Preparing", "errorCode": "NoError"}
        assert (await peer.call(charger, "prior-status", "StatusNotification", status))[0] == 3
        latest = status | {"status": "Available"}
        assert (await peer.call(charger, "latest-status", "StatusNotification", latest))[0] == 3
        assert (await peer.call(charger, "prior-status", "StatusNotification", status))[0] == 3
        assert j.get("last_status") == latest
        assert (await peer.call(charger, "same-connection", "Heartbeat", {}))[0] == 3


# Intent: parsed dates that overflow UTC normalization are rejected with a correlated reply before any durable effect.
# Why: local parsing success cannot let an arithmetic exception strand the charger call or block later valid history.
@pytest.mark.parametrize(
    "stamp", ["0001-01-01T00:00:00+01:00", "9999-12-31T23:59:59-01:00"], ids=["utc-underflow", "utc-overflow"]
)
@pytest.mark.parametrize("action", ["StartTransaction", "StopTransaction", "StatusNotification", "MeterValues"])
async def test_wire_timezone_boundary_is_rejected_and_next_lifecycle_works(
    tmp_path, model_tls, monkeypatch, stamp, action
):
    async with commissioned(
        tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch, 6, native_peer=InterruptedNativePeer
    ) as b:
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
