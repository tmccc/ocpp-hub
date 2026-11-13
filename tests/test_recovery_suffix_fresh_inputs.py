"""Repeated recovery suffixes must create new genuine lifecycles instead of reusing old payloads or counts."""

import copy
import uuid

import pytest

from tests.cloud_start_helpers import two_later_sessions
from tests.recovery_barriers import completed_control
from tests.test_combined_recovery_proofs import selected_service
from tests.test_prototype_tls import tls as tls
from tests_model.control_helpers import ordinary_recovery


# Intent: both common suffixes create another two sessions even when the same rig already has closed history.
# Why: a fixed Start body or a global closed count can silently reuse or reject the next recovery episode.
@pytest.mark.parametrize("suffix", ["control", "cloud"])
async def test_repeated_suffix_has_fresh_wire_payloads_and_delta(tmp_path, tls, unused_tcp_port, monkeypatch, suffix):
    async with selected_service(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        j, bench = b.session.journal, b.rig.bench
        for demand in (True, False):
            before = copy.deepcopy(bench.backend_starts)
            closed_before = j.db.execute("SELECT COUNT(*) FROM transactions WHERE stop IS NOT NULL").fetchone()[0]
            if suffix == "control":
                await ordinary_recovery(b, 6, demand)
            else:
                await two_later_sessions(b, selected=6, original_starts=0)
            assert bench.backend_starts[: len(before)] == before
            assert len(bench.backend_starts) == len(before) + 2
            assert all(row["closed"] for row in bench.backend_starts)
            assert (
                j.db.execute("SELECT COUNT(*) FROM transactions WHERE stop IS NOT NULL").fetchone()[0]
                == closed_before + 2
            )
        bodies = [row["body"] for row in bench.backend_starts]
        assert len({(body["timestamp"], body["meterStart"]) for body in bodies}) == 4
        assert [body["timestamp"] for body in bodies] == sorted(body["timestamp"] for body in bodies)
        assert j.active() is None and j.next_delivery() is None


# Intent: completed-control proofs reject an exact request used with a foreign action or submitted revision.
# Why: a completed receipt and healthy physical view do not establish the caller's claimed authority transition.
@pytest.mark.parametrize("wrong", ["action", "revision"])
async def test_completed_barrier_requires_exact_action_and_submitted_revision(
    tmp_path, tls, unused_tcp_port, monkeypatch, wrong
):
    async with selected_service(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        c = b.controller
        identifier, revision = uuid.uuid4().hex, c.requests.intent().revision
        c.command(identifier, "pause", revision)
        await completed_control(b, identifier, mode="paused", revision=revision + 1, ceiling=0)
        # Preserve the genuine physical Pause and actual terminal receipt, but contradict one persisted attribution.
        j = b.session.journal
        if wrong == "action":
            j.db.execute("UPDATE managed_requests SET action='set_current' WHERE id=?", (identifier,))
        else:
            j.db.execute("UPDATE managed_requests SET revision=revision+5 WHERE id=?", (identifier,))
        with pytest.raises(AssertionError):
            await completed_control(b, identifier, mode="paused", revision=revision + 1, ceiling=0)


# Intent: a physical zero whose genuine TLS acknowledgement is withheld cannot complete the outage barrier.
# Why: applied output and a verified station hold are distinct boundaries, so native restoration must wait for both.
async def test_native_suffix_waits_for_actual_zero_ack(tmp_path, tls, unused_tcp_port, monkeypatch):
    import asyncio

    from tests.recovery_barriers import witnessed_native_loss
    from tests.test_managed_test_charge import positive_flow
    from tests_model.test_start_recovery_sequences import close_daily

    async with selected_service(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        entered, release = asyncio.Event(), asyncio.Event()
        gateway, rig = b.session.gateway, b.rig
        local = await positive_flow(b, "daily")

        # The independent charger has already applied the actual received zero before this real reply gate.
        async def held_zero_ack(action, payload):
            if action == "SetChargingProfile":
                limit = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                if limit == 0:
                    entered.set()
                    await release.wait()

        rig.bench.before_reply = held_zero_ack
        task = asyncio.create_task(witnessed_native_loss(b, 6, local))
        try:
            await asyncio.wait_for(entered.wait(), 3)
            assert rig.bench.firmware.effective_limit() == 0 and not gateway.controls.ready
            assert rig.peer.unavailable and not task.done()
            for _ in range(5):
                await asyncio.sleep(0)
            assert rig.peer.unavailable and not task.done()
        finally:
            release.set()
        assert await task >= 3
        assert not rig.peer.unavailable and gateway.controls.ready and gateway.controls.desired() == 6
        assert rig.bench.firmware.effective_limit() == 6
        rig.bench.before_reply = None
        await close_daily(b, local, "ledger")
