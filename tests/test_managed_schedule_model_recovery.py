"""The bounded sequence driver must advance scheduled retries while checking the real zero hold."""

from datetime import datetime

import pytest

from tests.managed_scenario_helpers import demand_from_vehicle, originals, select_current
from tests.test_managed_schedule_cutoff import ZONE, timer_bench
from tests.test_managed_schedule_mutation_scenarios import fail_one_operation
from tests.test_prototype_tls import tls as tls
from tests_model.schedule_cases import SequenceCase
from tests_model.test_schedule_sequences import PeerLedger, recovery, turns


# Intent: cleanup after two real receipts can reject or lose an ACK without an immobile test clock blocking retry.
# Why: foreground turns alone cannot reach the controller's scheduled retry; zero must persist during that wait.
@pytest.mark.parametrize("lost_ack", [False, True])
async def test_sequence_recovery_allows_bounded_timed_cleanup(tmp_path, tls, unused_tcp_port, monkeypatch, lost_ack):
    async with timer_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await select_current(b, 32)
        saved = originals(b)
        await b.arm("00000001" + "0" * 24)
        cutoff = int(
            datetime.fromtimestamp(b.start, ZONE).replace(hour=6, minute=0, second=0, microsecond=0).timestamp()
        )
        b.clock.now = b.start + 601
        await turns(b, 6)
        b.start = (int(b.clock.now) // 60 + 4) * 60
        b.request("00000002" + "0" * 24)
        await turns(b, 3)
        newer_plan = b.timer
        local = await b.start_plan(9)
        await turns(b, 4)
        b.clock.now = cutoff
        await turns(b, 6)
        assert newer_plan in b.device.plans and not b.stops()
        b.device.active = None
        b.rig.peer.plan_session = None
        demand_from_vehicle(b, False)
        b.rig.bench.cp, b.rig.bench.connection = "A", "Unplug"
        result = await b.rig.bench.call(
            b.sock,
            "recovery-departure",
            "StatusNotification",
            {"connectorId": 1, "status": "Available", "errorCode": "NoError"},
        )
        assert result[0] == 3
        b.rig.bench.cp, b.rig.bench.connection = "B", "Plug"
        case = SequenceCase("armed", 32, "daily", True, "unplug", "native", ())
        ledger = PeerLedger(created=True, physical_open=False, local=local)
        async with fail_one_operation(b, "clear", lost_ack) as entered:
            await recovery(b, case, ledger, saved)
            assert entered.is_set(), "the real selective cleanup never reached the injected device fault"
        if not lost_ack:
            assert ledger.recovery_retry_advances >= 1
        assert b.device.plans == {9: saved[0]} and not b.stops()
