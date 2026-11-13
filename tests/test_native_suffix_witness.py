"""Every declared native-loss suffix requires fresh independently observed HTTP failures."""

import copy

import pytest

from tests.recovery_barriers import witnessed_native_loss
from tests.test_combined_recovery_proofs import selected_service
from tests.test_managed_proxy import healthy
from tests.test_managed_test_charge import positive_flow
from tests.test_prototype_tls import tls as tls
from tests_model.test_start_recovery_sequences import close_daily


class ObservedFailures:
    """Observe the independent native peer's actual parsed 503 requests while retaining its ordinary queue."""

    # Retain the original queue instead of changing when the native handler decides to fail.
    def __init__(self, queue):
        self.queue, self.records = queue, []

    # This method is invoked only by the real HTTP peer after parsing an unavailable getConfig request.
    def put_nowait(self, request):
        self.records.append(copy.deepcopy(request))
        self.queue.put_nowait(request)

    # Preserve the queue methods used by ordinary fixture helpers.
    def __getattr__(self, name):
        return getattr(self.queue, name)


# Intent: each fresh native-loss episode proves three actual failed reads, excluding an earlier real failure.
# Why: five helper ticks or one old queue item cannot prove that the new session's outage was supervised.
@pytest.mark.parametrize("selected", [6, 16, 32])
@pytest.mark.parametrize("demand", [False, True])
async def test_native_suffix_counts_fresh_http_failures(tmp_path, tls, unused_tcp_port, monkeypatch, selected, demand):
    async with selected_service(tmp_path, tls, unused_tcp_port, monkeypatch, selected) as b:
        rig, c = b.rig, b.controller
        observed = ObservedFailures(rig.peer.failed_reads)
        rig.peer.failed_reads = observed
        local = await positive_flow(b, "daily")
        if not demand:
            rig.peer.permission_enabled, rig.bench.firmware.requested_current = False, 0
        assert (await healthy(c))["current_a"] == (selected if demand else 0)
        rig.peer.unavailable = True
        await c.tick()
        rig.peer.unavailable = False
        assert len(observed.records) == 1 and observed.qsize() == 1
        assert (await healthy(c))["owner_control_enabled"]
        for _ in range(2):
            before = len(observed.records)
            witnessed = await witnessed_native_loss(b, selected, local)
            assert witnessed >= 3 and len(observed.records) - before == witnessed
            assert all(request["action"] == "getConfig" for request in observed.records[before:])
            assert observed.empty() and b.session.journal.active() == local
            assert rig.bench.firmware.effective_limit() == selected
        await close_daily(b, local, "ledger")
        assert b.session.journal.active() is None and b.session.journal.next_delivery() is None
