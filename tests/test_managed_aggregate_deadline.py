"""Read-only review reproduction: aggregate native audit timeout leaves the prior positive offer installed."""

import asyncio
import json
import time

import pytest

from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import NativeVehiclePeer
from tests.test_managed_introduction_retry import selected_ready
from tests.test_managed_proxy import healthy
from tests.test_managed_test_charge import positive_flow
from tests.test_ordinary_recovery import close_session
from tests.test_prototype_tls import tls as tls

pytestmark = pytest.mark.asyncio


class SlowCompleteReplies(NativeVehiclePeer):
    """Independent HTTP peer: each complete response is delayed less than the five-second request deadline."""

    # Retain normal physical behavior while delaying only this independent HTTP peer.
    def __init__(self, bench):
        super().__init__(bench)
        self.delay = 0
        self.slow_requests = 0
        self.handlers = set()

    # Every reply is individually timely; the complete multi-read audit exceeds its own deadline.
    async def handle(self, reader, writer):
        task = asyncio.current_task()
        self.handlers.add(task)
        try:
            if self.delay:
                self.slow_requests += 1
                await asyncio.sleep(self.delay)
            await super().handle(reader, writer)
        except ConnectionError, OSError:
            writer.close()
        finally:
            self.handlers.discard(task)


# Intent: aggregate native deadlines enter the same restrictive recovery as individually unavailable reads.
# Why: individually timely HTTP responses must not leave an unobserved positive offer indefinitely.
async def test_complete_native_audit_timeout_enters_zero_recovery(tmp_path, tls, unused_tcp_port, monkeypatch):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", SlowCompleteReplies)
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, 32) as b:
        local = await positive_flow(b, "daily")
        before = await healthy(b.controller)
        assert before["station_ceiling_a"] == before["current_a"] == 32
        assert b.controller.ordinary.ordinary()
        assert b.controller.native_unavailable == 0
        profile_count = len(b.rig.bench.received_limits)
        observations = []
        b.rig.peer.delay = 1.7
        try:
            for _ in range(3):
                began = time.monotonic()
                status = await b.controller.tick()
                observations.append(
                    {
                        "elapsed_seconds": round(time.monotonic() - began, 3),
                        "state": status["state"],
                        "fault": status.get("fault"),
                        "native_unavailable_count": b.controller.native_unavailable,
                        "peer_effective_limit_a": b.rig.bench.firmware.effective_limit(),
                        "peer_current_a": b.rig.bench.firmware.current,
                    }
                )
            print(
                json.dumps(
                    {
                        "audit_results": observations,
                        "slow_http_requests": b.rig.peer.slow_requests,
                        "new_station_limits": b.rig.bench.received_limits[profile_count:],
                        "ordinary_recovery": b.session.journal.get("managed_ordinary_recovery"),
                    },
                    sort_keys=True,
                )
            )
            assert b.controller.native_unavailable >= 3
            assert b.controller.native.last_verified_at == 0
            assert b.controller.ordinary.active()
            assert b.rig.bench.received_limits[profile_count:] == [0]
            assert b.rig.bench.firmware.effective_limit() == 0, (
                "Three aggregate native-read timeouts must enter the same zero-hold recovery as other lost telemetry"
            )
        finally:
            b.rig.peer.delay = 0
            if b.rig.peer.handlers:
                await asyncio.gather(*list(b.rig.peer.handlers), return_exceptions=True)
            await close_session(b.rig, b.session, b.charger, local)
            await healthy(b.controller)
