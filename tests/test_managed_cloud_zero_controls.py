"""Real managed TLS commands translate the current cloud mapping, including sequential reuse of zero."""

import json
import uuid

import pytest

from tests import test_managed_proxy as mp
from tests.cloud_start_helpers import StartReplyBench
from tests.test_managed_stop_reply_recovery import running
from tests.test_managed_test_charge import ChargeRig, positive_flow
from tests.test_prototype_tls import tls as tls
from tests_model.test_start_recovery_sequences import close_daily

COMMANDS = (("setTransaction", {"eco": "Fast"}), ("delayStop", {"stopTime": "2030-01-01T00:00:00Z"}), ("delayStop", {}))


# Exercise the real cloud socket and preserve only the independently observed proprietary charger writes.
async def commands(rig, vendor, expected_local):
    for message, extra in COMMANDS:
        before = [f for f in rig.bench.device_calls if f[2] == "DataTransfer"]
        payload = {
            "vendorId": "IoCharger",
            "messageId": message,
            "data": json.dumps({"transactionId": vendor, **extra}),
        }
        result = await rig.bench.cloud_call(uuid.uuid4().hex, "DataTransfer", payload)
        after = [f for f in rig.bench.device_calls if f[2] == "DataTransfer"]
        if expected_local is None:
            assert result[0] == 4 and after == before
        else:
            assert result[0] == 3 and result[2] == {"status": "Accepted", "data": ""}
            assert len(after) == len(before) + 1
            assert after[-1][3]["messageId"] == message
            assert json.loads(after[-1][3]["data"]) == {"transactionId": expected_local, **extra}


# Intent: Fast and delayed-stop set/cancel use only the current open mapping, including cloud ID zero and reuse.
# Why: validating the cloud namespace as a positive native ID rejects supported app controls before translation.
@pytest.mark.parametrize("vendor", [0, 1])
async def test_managed_cloud_controls_translate_zero_and_reused_mapping(
    tmp_path, tls, unused_tcp_port, monkeypatch, vendor
):
    monkeypatch.setattr(mp, "OwnerBench", StartReplyBench)
    async with mp.managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        rig.bench.fixed_vendor = vendor
        async with running(rig, "cloud-mapping-controls") as (session, controller, charger):
            await mp.healthy(controller)
            b = ChargeRig(rig, session, controller, charger)
            await commands(rig, vendor, None)
            locals_seen = []
            for number in range(2):
                local = await positive_flow(
                    b,
                    "daily",
                    call_id=uuid.uuid4().hex,
                    meter_start=1000 + 200 * number,
                    timestamp=f"2026-01-01T00:{number * 2:02d}:00Z",
                )
                locals_seen.append(local)
                assert session.journal.vendor_id(local) == vendor and local > 0
                await mp.healthy(controller)
                await commands(rig, vendor, local)
                stopped = await rig.bench.cloud_call(
                    uuid.uuid4().hex, "RemoteStopTransaction", {"transactionId": vendor}
                )
                assert stopped[0] == 3 and stopped[2]["status"] == "Accepted"
                await close_daily(
                    b,
                    local,
                    "native",
                    timestamp=f"2026-01-01T00:{number * 2 + 1:02d}:00Z",
                    meter_stop=1100 + 200 * number,
                )
                await mp.healthy(controller)
                assert session.journal.local_id(vendor) is None
                await commands(rig, vendor, None)
            assert locals_seen[0] != locals_seen[1]
            assert not rig.bench.backend_errors and all(row["closed"] for row in rig.bench.backend_starts)
            assert session.journal.next_delivery() is None and not mp.starts(rig)
