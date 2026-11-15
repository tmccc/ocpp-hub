"""Introduction waits recover on their actual Boot while strict native contradictions remain visible."""

from __future__ import annotations

import asyncio
import copy
import uuid

import pytest
from ocpp_hub_core.prototype.managed import ManagedController

from tests.recovery_barriers import completed_control
from tests.test_managed_introduction_retry import selected_ready
from tests.test_managed_proxy import healthy, managed_bench, managed_session, starts
from tests.test_managed_test_charge import ChargeRig
from tests.test_prototype import BOOT
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls

pytestmark = pytest.mark.asyncio


# Count complete native HTTP request sequences independently of the engine's tasks and cached readings.
async def complete_native_audits(peer, mark, minimum=3):
    await until(
        lambda: (
            peer.requests[mark:].count("getTransaction") >= minimum
            and peer.requests[mark:].count("getConfig") >= 2 * minimum
        ),
        timeout=3,
    )
    expected = ["getConfig", *(["statistics"] * 7), "getTransaction", "getConfig"]
    assert peer.requests[mark : mark + 10 * minimum] == expected * minimum
    # The following audit cannot start until the preceding final HTTP response was fully validated.
    assert peer.requests[mark:].count("getTransaction") >= minimum


# Check physical calls and the durable lifecycle rather than inferring safety from a healthy-looking status.
def no_synthetic_lifecycle(rig, journal):
    assert not starts(rig)
    assert not [frame for frame in rig.bench.device_calls if frame[2] == "RemoteStopTransaction"]
    assert journal.active() is None
    assert journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0


# Intent: a first Boot with a valid but not-yet-zero offer retries and completes the exact app or Pause/Return intent.
# Why: temporary output readiness must not strand one socket or substitute for the actual first owned zero receipt.
@pytest.mark.parametrize("initial", ["app", "paused"])
async def test_first_boot_readiness_wait_recovers_on_same_boot(
    tmp_path, tls, unused_tcp_port, monkeypatch, initial
):
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        c, g, j = ManagedController(session), session.gateway, session.journal
        rig.route_to_trial()
        configuration, plans = copy.deepcopy(rig.peer.configuration), copy.deepcopy(rig.bench.firmware.plans)
        pause = None
        if initial == "paused":
            pause = uuid.uuid4().hex
            assert c.command(pause, "pause", c.requests.intent().revision)["state"] == "accepted"
            await c.pause_task
        # This independent native sample has no transaction or measured draw; only its output offer is not zero.
        rig.peer.permission_enabled = True
        mark = len(rig.peer.requests)
        try:
            async with rig.charger(session) as charger:
                assert (await rig.bench.call(charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
                epoch, boot = g._charger_epoch, g.boot_observation
                await complete_native_audits(rig.peer, mark)
                assert rig.bench.firmware.current == 0 and rig.bench.firmware.active is None
                assert c.native.introduced_epoch != epoch and not g.controls.ready
                assert j.get("trial_binding") is None and j.get("trial_starting") is True
                assert j.get("managed_control_fault") is None and rig.bench.device_calls == []
                no_synthetic_lifecycle(rig, j)
                returned = None
                if pause is not None:
                    assert c.requests.receipt(pause)["state"] == "accepted"
                    returned = uuid.uuid4().hex
                    assert c.command(returned, "app_control", c.requests.intent().revision)["state"] == "accepted"
                    assert c.requests.intent().mode == "paused"
                rig.peer.permission_enabled = False
                b = ChargeRig(rig, session, c, charger)
                if returned is None:
                    status = await healthy(c)
                else:
                    status = await completed_control(b, returned, mode="app", revision=2, ceiling=6)
                    assert c.requests.receipt(pause)["state"] == "completed"
                assert status["owner_control_enabled"] and status["station_ceiling_a"] == 6
                assert c.native.introduced_epoch == g._charger_epoch == epoch and g.boot_observation == boot
                assert g.controls.ready and j.get("trial_starting") is False
                assert rig.bench.received_limits[0] == 0 and rig.bench.received_limits[-1] == 6
                assert set(rig.bench.received_limits) <= {0, 6}
                assert j.get("managed_control_fault") is None
                no_synthetic_lifecycle(rig, j)
                assert rig.peer.configuration == configuration and rig.bench.firmware.plans == plans
        finally:
            rig.peer.permission_enabled = False
            await c.tests.close()
            if c.pause_task is not None:
                await asyncio.gather(c.pause_task, return_exceptions=True)
            await session.close()


# Intent: reconnect introduction waits for independent native closure without adopting an unknown session or Stop.
# Why: repeated fresh observations may resolve lifecycle readiness, while persistent external activity stays fenced.
@pytest.mark.parametrize("current", [6, 32])
@pytest.mark.parametrize("closure", ["closes", "remains-open"])
async def test_reconnect_native_session_wait_retains_same_boot_and_unknown_session_fence(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, closure
):
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        rig, c, g, j = b.rig, b.controller, b.session.gateway, b.session.journal
        configuration, plans = copy.deepcopy(rig.peer.configuration), copy.deepcopy(rig.bench.firmware.plans)
        await b.charger.close()
        await until(lambda: not b.session.runtime.status.charger_connected)
        rig.bench.firmware.active = 9000011
        mark, first_profile = len(rig.peer.requests), len(rig.bench.received_limits)
        try:
            async with rig.charger(b.session) as charger:
                assert (await rig.bench.call(charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
                epoch, boot = g._charger_epoch, g.boot_observation
                await complete_native_audits(rig.peer, mark)
                assert rig.bench.firmware.active == 9000011 and rig.bench.firmware.current == 0
                assert c.native.introduced_epoch != epoch and not g.controls.ready
                assert j.get("managed_control_fault") is None
                assert rig.bench.received_limits[first_profile:] == []
                no_synthetic_lifecycle(rig, j)
                if closure == "remains-open":
                    later = len(rig.peer.requests)
                    # This boundary begins between audits; count another pair of actual full native snapshots.
                    await until(lambda: len(rig.peer.requests) >= later + 20, timeout=3)
                    for _ in range(3):
                        status = await c.tick()
                        assert not status["owner_control_enabled"] and not status.get("local_hold_verified", False)
                    assert rig.bench.firmware.active == 9000011
                    assert c.native.introduced_epoch != epoch
                    assert g.controls.desired() == 0 and rig.bench.firmware.effective_limit() == 0
                    assert set(rig.bench.received_limits[first_profile:]) <= {0}
                else:
                    # Only the independent physical endpoint changes; no proxy method or ledger result is replaced.
                    rig.bench.firmware.active = None
                    status = await healthy(c)
                    limits = rig.bench.received_limits[first_profile:]
                    assert status["owner_control_enabled"] and status["station_ceiling_a"] == current
                    assert c.native.introduced_epoch == epoch and g.controls.ready
                    assert limits[0] == 0 and limits[-1] == current and set(limits) <= {0, current}
                    assert rig.bench.firmware.effective_limit() == current
                assert g._charger_epoch == epoch and g.boot_observation == boot
                assert j.get("managed_control_fault") is None
                no_synthetic_lifecycle(rig, j)
                assert rig.peer.configuration == configuration and rig.bench.firmware.plans == plans
        finally:
            rig.bench.firmware.active = None


# Intent: a strict introduction configuration refusal latches its original cause and reports attention immediately.
# Why: restored settings cannot erase an observed contradiction or leave the owner in indefinite generic starting.
@pytest.mark.parametrize("current", [6, 32])
async def test_strict_introduction_refusal_latches_diagnostic_and_attention(
    tmp_path, tls, unused_tcp_port, monkeypatch, current
):
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, current) as b:
        rig, c, g, j = b.rig, b.controller, b.session.gateway, b.session.journal
        await b.charger.close()
        await until(lambda: not b.session.runtime.status.charger_connected)
        original = rig.peer.configuration["untouched"]
        rig.peer.configuration["untouched"] = "independent-introduction-configuration-change"
        first_profile = len(rig.bench.received_limits)
        try:
            async with rig.charger(b.session) as charger:
                assert (await rig.bench.call(charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
                epoch, boot = g._charger_epoch, g.boot_observation
                await until(lambda: j.get("trial_phase") == "blocked")
                status = await c.tick()
                assert status["state"] == "attention"
                fault, diagnostic = copy.deepcopy(j.get("managed_control_fault")), copy.deepcopy(
                    j.get("managed_native_failure")
                )
                assert fault["classification"] == status["fault"] == "native_authority_or_preservation_failed"
                assert diagnostic["type"] == "SecurityError" and diagnostic["at_epoch"] > 0
                assert diagnostic["detail"] == (
                    "managed full configuration or saved schedule changed"
                    if current == 32
                    else "native bounded configuration or saved schedule changed"
                )
                assert c.native.introduced_epoch != epoch
                read_count = len(rig.peer.requests)
                rig.peer.configuration["untouched"] = original
                for _ in range(5):
                    assert (await c.tick())["state"] == "attention"
                    await asyncio.sleep(0.03)
                assert j.get("managed_control_fault") == fault and j.get("managed_native_failure") == diagnostic
                assert len(rig.peer.requests) == read_count
                assert g._charger_epoch == epoch and g.boot_observation == boot
                assert c.native.introduced_epoch != epoch and not status["owner_control_enabled"]
                assert all(limit == 0 for limit in rig.bench.received_limits[first_profile:])
                no_synthetic_lifecycle(rig, j)
        finally:
            rig.peer.configuration["untouched"] = original


# Intent: an owned-profile contradiction wins when the same native sample also lacks closed-zero readiness.
# Why: classifying a legitimate readiness wait must not hide an independently proved resource ownership change.
async def test_unready_introduction_cannot_hide_changed_owned_profile(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with selected_ready(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as b:
        rig, c, g, j = b.rig, b.controller, b.session.gateway, b.session.journal
        await b.charger.close()
        await until(lambda: not b.session.runtime.status.charger_connected)
        profile = copy.deepcopy(rig.bench.firmware.profiles[7001])
        rig.bench.firmware.profiles[7001]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] = 1
        # An unplugged connector with a positive independent offer fails readiness even without a charging session.
        rig.bench.cp, rig.bench.connection, rig.peer.permission_enabled = "A", "Unplug", True
        try:
            async with rig.charger(b.session) as charger:
                assert (await rig.bench.call(charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
                epoch = g._charger_epoch
                await until(lambda: j.get("trial_phase") == "blocked")
                status = await c.tick()
                assert status["state"] == "attention" and not status["owner_control_enabled"]
                assert j.get("managed_control_fault")["classification"] == "native_authority_or_preservation_failed"
                diagnostic = j.get("managed_native_failure")
                assert diagnostic["type"] == "SecurityError" and "owned_profile_changed" in diagnostic["detail"]
                assert c.native.introduced_epoch != epoch
                rig.bench.firmware.profiles[7001] = profile
                rig.bench.cp, rig.bench.connection, rig.peer.permission_enabled = "B", "Plug", False
                for _ in range(3):
                    assert (await c.tick())["state"] == "attention"
                no_synthetic_lifecycle(rig, j)
        finally:
            rig.bench.firmware.profiles[7001] = profile
            rig.bench.cp, rig.bench.connection, rig.peer.permission_enabled = "B", "Plug", False
