"""A denied closed lifecycle remains associated through another exact operator recovery and later real sessions."""

import copy
from contextlib import asynccontextmanager

import pytest
from ocpp_hub_core.prototype import managed_start_delivery
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.managed_debt_recovery import KEY
from ocpp_hub_core.prototype.managed_release_recovery import release_evidence
from ocpp_hub_core.prototype.managed_start_delivery import DENIALS
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests import test_managed_proxy as mp
from tests import test_native_bounded_session as native_bench
from tests.cloud_start_helpers import StartReplyBench, two_later_sessions
from tests.handover_helpers import crash
from tests.test_combined_recovery_proofs import queue_closed_pairs
from tests.test_managed_debt_reconciliation import strategy_file
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_start_delivery_recovery import approve_retry, approve_stop_after_start
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_stop_reply_recovery import pause, running
from tests.test_managed_test_charge import ChargeRig, ready
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls
from tools import reconcile_managed_debt as operator


class UnallocatedStartPeer:
    """The independent cloud receives chosen actual Start frames but never allocates a backend transaction for them."""

    # Preserve every ordinary websocket method and the normal independent cloud protocol implementation.
    def __init__(self, connection, bench):
        self.connection, self.bench = connection, bench

    # Forward only unchanged ordinary frames; dropped Starts remain independently recorded as never allocated.
    async def __aiter__(self):
        async for raw in self.connection:
            frame = decode(raw)
            if frame[0] == 2 and frame[2] == "StartTransaction":
                self.bench.start_receipts += 1
                if self.bench.start_receipts in self.bench.drop_start_receipts:
                    self.bench.nonaccepted_starts.append(copy.deepcopy(frame))
                    self.bench.lifecycle_wires.append(frame)
                    self.bench.calls.append(frame)
                    continue
            yield raw

    # Keep correlated responses on the same independently accepted real TLS socket.
    async def send(self, raw):
        await self.connection.send(raw)

    # Cloud-originated commands and transport closure retain the original socket behavior.
    def __getattr__(self, name):
        return getattr(self.connection, name)


class NonacceptingBench(StartReplyBench):
    """Backend allocation stays independent from the proxy, including true nonacceptance with no reply."""

    # Keep all initial commissioning normal; only the specified second actual Start is dropped later.
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_receipts, self.drop_start_receipts, self.nonaccepted_starts = 0, set(), []

    # Run the existing complete cloud implementation behind one actual receive boundary.
    async def vendor(self, connection):
        await super().vendor(UnallocatedStartPeer(connection, self))


# Admit native6 or genuine native32 before choosing the current through the actual managed worker.
@asynccontextmanager
async def selected(tmp_path, tls, port, monkeypatch, current):
    monkeypatch.setattr(mp, "OwnerBench", NonacceptingBench)
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    factory = (
        ready(tmp_path, tls, port, monkeypatch, cli_route=True)
        if current == 6
        else commissioned(tmp_path, tls, port, monkeypatch, current, cli_route=True)
    )
    async with factory as b:
        strategy_file(b.rig, tmp_path)
        yield b


# Refused replay must not replace the exact generic fault, denial receipt or newly superseded permission.
async def old_statement_is_refused(rig, tmp_path):
    before = list(rig.bench.device_calls)
    with pytest.raises(RecoveryRequiredError, match="superseded"):
        await operator.run(
            "reconcile",
            tmp_path / "trial.json",
            strategy_file(rig, tmp_path),
            tmp_path / "start-retry-evidence.json",
            rig.prepared.observer.port,
            kind="start-delivery",
        )
    assert rig.bench.device_calls == before


# Intent: a prior denied ACK and unknown unaccepted Start recover together through actual tools and wire ACKs.
# Why: replacing the generic fault or refusing the needed retry tool each strands the first genuine denial at zero.
@pytest.mark.parametrize("current", [6, 32])
@pytest.mark.parametrize(
    "fault", ["none", "error-once", "lost-again", "exhaust-again", "stop-error", "stop-bad-info", "new-denial"]
)
async def test_denial_and_other_start_retry_recover_together(
    tmp_path, tls, unused_tcp_port, monkeypatch, capsys, current, fault
):
    monkeypatch.setattr(managed_start_delivery, "RETRY_INTERVAL", 0.01)
    async with selected(tmp_path, tls, unused_tcp_port, monkeypatch, current) as original:
        rig, s, c = original.rig, original.session, original.controller
        rig.bench.drop_start_receipts = {2}
        ids = await queue_closed_pairs(original, ["Blocked", "Accepted"])
        await until(
            lambda: s.journal.next_delivery() is not None and s.journal.next_delivery().state == "ambiguous",
            timeout=12,
        )
        first_fault = copy.deepcopy(s.journal.get("managed_control_fault"))
        assert s.journal.next_delivery().transaction == ids[1]
        assert len(rig.bench.backend_starts) == 1 and rig.bench.backend_starts[0]["closed"] is True
        assert len(rig.bench.nonaccepted_starts) == 1
        assert s.journal.get(DENIALS)[str(ids[0])]["wire_ack"]["fault"] == first_fault
        await pause(c)
        assert s.journal.get("managed_control_fault") == first_fault
        assert s.journal.get(DENIALS)[str(ids[0])]["completed"] is None
        await c.current.close()
        await c.tests.close()
        await crash(s)
        await approve_retry(rig, tmp_path, capsys)
        if fault in ("lost-again", "exhaust-again"):
            if fault == "lost-again":
                rig.bench.drop_start_receipts.add(rig.bench.start_receipts + 1)
                rig.bench.start_replies = []
            else:
                rig.bench.start_replies = ["error"] * 3
            async with running(rig, "denial-retry-fails-again") as (s, c, sock):
                await until(
                    lambda: (
                        s.journal.next_delivery() is not None
                        and s.journal.next_delivery().state in ("ambiguous", "rejected")
                    ),
                    timeout=12,
                )
                assert s.journal.get("managed_control_fault") == first_fault
                assert any(entry.get("superseded") for entry in s.journal.get(KEY).values())
                assert s.journal.get(DENIALS)[str(ids[0])]["completed"] is None
                await pause(c)
                assert s.journal.get("managed_control_fault") == first_fault
                assert rig.bench.firmware.effective_limit() == 0
            await old_statement_is_refused(rig, tmp_path)
            await approve_retry(rig, tmp_path, capsys)
        rig.bench.start_replies = ["Blocked"] if fault == "new-denial" else ["error"] if fault == "error-once" else []
        if fault in ("stop-error", "stop-bad-info"):
            rig.bench.stop_replies = ["error" if fault == "stop-error" else "bad-info"]
            async with running(rig, "denial-retry-stop-refused") as (s, c, sock):
                await until(
                    lambda: (
                        s.journal.next_delivery() is not None
                        and s.journal.next_delivery().action == "StopTransaction"
                        and s.journal.next_delivery().state == "rejected"
                    )
                )
                assert s.journal.get("managed_control_fault") == first_fault
                assert any(entry.get("superseded") for entry in s.journal.get(KEY).values())
                await pause(c)
                assert s.journal.get("managed_control_fault") == first_fault
            await old_statement_is_refused(rig, tmp_path)
            await approve_stop_after_start(rig, tmp_path, capsys)
        async with running(rig, "denial-retry-complete") as (s, c, sock):
            await until(lambda: s.journal.next_delivery() is None)
            active = [entry for entry in s.journal.get(KEY).values() if not entry.get("superseded")]
            assert len(active) == 1 and active[0]["completed"] is None
            installed = s.journal.get("managed_control_fault")
            assert installed == active[0]["proof"]["installed_fault"]
            denials = copy.deepcopy(s.journal.get(DENIALS))
            assert len(denials) == (2 if fault == "new-denial" else 1)
            assert all(entry["fault"] == first_fault and entry["completed"] is None for entry in denials.values())
            assert release_evidence(c, installed) is not None
            identifier, _ = await pause(c)
            assert (await mp.healthy(c))["local_hold_verified"]
            assert s.journal.get("managed_control_fault") is None
            assert all(entry["completed"]["pause"] == identifier for entry in s.journal.get(DENIALS).values())
            assert all(
                entry.get("superseded") or entry["completed"]["pause"] == identifier
                for entry in s.journal.get(KEY).values()
            )
            assert rig.bench.firmware.effective_limit() == 0
            terminal = copy.deepcopy(s.journal.get(DENIALS)), copy.deepcopy(s.journal.get(KEY))
            await two_later_sessions(ChargeRig(rig, s, c, sock), selected=current, original_starts=0)
            assert (s.journal.get(DENIALS), s.journal.get(KEY)) == terminal
            assert not rig.bench.backend_errors and all(entry["closed"] for entry in rig.bench.backend_starts)
