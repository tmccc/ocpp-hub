"""Routine report rejection is terminal and audited while real lifecycle delivery retains its semantics."""

import json

import pytest
from ocpp_hub_core.frames import build_call_error
from ocpp_hub_core.prototype.managed import ManagedController

from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session
from tests.test_prototype import START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tests.test_trial_session import Bench


class ReportReplyFilter:
    """Return a correlated error only for the named report, while the independent cloud handles lifecycle normally."""

    # Track actual received frames independently from the engine and retain normal cloud allocation.
    def __init__(self, connection, action):
        self.connection, self.action = connection, action
        self.targets = set()

    # Observe requests on the real upstream TLS socket.
    def __aiter__(self):
        # All unselected requests pass through to the real fake-cloud implementation.
        async def frames():
            async for raw in self.connection:
                frame = json.loads(raw)
                if frame[0] == 2 and frame[2] == self.action:
                    self.targets.add(frame[1])
                yield raw

        return frames()

    # A correlated rejection is a terminal outcome for telemetry, without inventing successful delivery.
    async def send(self, raw):
        frame = json.loads(raw)
        if frame[0] == 3 and frame[1] in self.targets:
            raw = build_call_error(frame[1], "NotSupported", "synthetic unsupported report")
        await self.connection.send(raw)


REPORTS = [
    ("StatusNotification", {"connectorId": 1, "status": "Available", "errorCode": "NoError"}),
    (
        "MeterValues",
        {"connectorId": 1, "meterValue": [{"timestamp": START["timestamp"], "sampledValue": [{"value": "1000"}]}]},
    ),
    ("FirmwareStatusNotification", {"status": "Downloaded"}),
    ("DiagnosticsStatusNotification", {"status": "Uploaded"}),
]


# Intent: rejected routine messages retire audibly and do not poison later real Start and Stop delivery.
# Why: Start/Stop share a FIFO with telemetry but have materially different acknowledgement semantics.
@pytest.mark.parametrize("action,payload", REPORTS)
async def test_rejected_report_does_not_block_lifecycle(tmp_path, tls, unused_tcp_port, monkeypatch, action, payload):
    actual = Bench.vendor

    # Modify only the cloud peer's reply transport, not gateway/RPC/journal methods.
    async def cloud(bench, connection):
        await actual(bench, ReportReplyFilter(connection, action))

    monkeypatch.setattr(Bench, "vendor", cloud)
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        s = managed_session(rig)
        await s.start()
        c = ManagedController(s)
        rig.route_to_trial()
        async with rig.charger(s) as charger:
            await introduced(rig, s, charger, "report-rejection-boot")
            await healthy(c)
            assert (await rig.bench.call(charger, "report", action, payload))[0] == 3
            await until(lambda: s.journal.next_delivery() is None)
            audit = s.journal.db.execute("SELECT * FROM rejected_observations").fetchall()
            assert len(audit) == 1 and audit[0]["action"] == action and audit[0]["code"] == "NotSupported"
            assert not s.runtime.status.delivery_blocked and not s.journal.get("managed_control_fault")
            local = (
                await rig.bench.call(charger, "real-start", "StartTransaction", START | {"idTag": "NATIVE_TIMER"})
            )[2]["transactionId"]
            await until(lambda: s.journal.vendor_id(local) is not None)
            assert (
                await rig.bench.call(
                    charger,
                    "real-stop",
                    "StopTransaction",
                    {"transactionId": local, "meterStop": 1100, "timestamp": START["timestamp"]},
                )
            )[0] == 3
            await until(lambda: s.journal.next_delivery() is None)
            assert (await healthy(c))["owner_control_enabled"]
            assert len(s.journal.db.execute("SELECT * FROM rejected_observations").fetchall()) == 1
