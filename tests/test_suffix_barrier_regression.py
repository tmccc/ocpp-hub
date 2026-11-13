"""Common suffix must exercise Return completion after an accepted but not yet verified Pause."""

import uuid

import pytest

from tests.cloud_start_helpers import two_later_sessions
from tests.test_combined_recovery_proofs import selected_service
from tests.test_managed_proxy import starts
from tests.test_prototype_tls import tls as tls


# Intent: an early legitimate Return completes before the common suffix asserts restored app ceiling and charging.
# Why: healthy paused status is an intermediate state, not proof that the accepted Return has completed.
@pytest.mark.parametrize("selected", [6, 16, 32])
async def test_suffix_waits_for_exact_early_return_receipt(tmp_path, tls, unused_tcp_port, monkeypatch, selected):
    async with selected_service(tmp_path, tls, unused_tcp_port, monkeypatch, selected) as b:
        c, j = b.controller, b.session.journal
        pause = uuid.uuid4().hex
        c.command(pause, "pause", c.requests.intent().revision)
        assert c.pause_task is not None
        await c.pause_task
        assert c.requests.receipt(pause)["state"] == "accepted"
        before = j.db.execute("SELECT COUNT(*) FROM managed_requests WHERE action='app_control'").fetchone()[0]
        await two_later_sessions(b, selected=selected, original_starts=0)
        returns = j.db.execute(
            "SELECT id,state FROM managed_requests WHERE action='app_control' ORDER BY rowid"
        ).fetchall()
        assert len(returns) == before + 1 and returns[-1]["state"] == "completed"
        assert c.requests.receipt(pause)["state"] == "completed"
        assert c.requests.intent().mode == "app" and not starts(b.rig)
