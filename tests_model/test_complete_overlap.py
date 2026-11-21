"""Replay every order in the held-Start model against a standalone proxy and independent peers."""

import pytest

from tests.model_tls_support import model_tls as model_tls
from tests_discovery.overlap import CASE_SECONDS, simulate
from tests_discovery.schedules import complete_orders


# Intent: run every complete held-Start history, including both delivered and lost replies.
# Why: a sample can skip a different placement of a failed write or process replacement.
@pytest.mark.parametrize("order", complete_orders(), ids="-".join)
async def test_complete_held_start_overlap(tmp_path, model_tls, unused_tcp_port_factory, monkeypatch, order):
    await simulate(tmp_path / "replay", order, model_tls, unused_tcp_port_factory, monkeypatch, [], CASE_SECONDS)
