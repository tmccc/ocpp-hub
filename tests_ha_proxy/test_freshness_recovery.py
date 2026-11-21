"""Actual HA keeps snapshot transit, reload recovery and incomplete history honest."""

import asyncio
from types import SimpleNamespace

import pytest
from homeassistant.components import persistent_notification

try:
    from homeassistant.components.persistent_notification.helpers import _async_get_or_create_notifications
except ImportError:  # HA10 exposes the same real notification store from the component module.
    from homeassistant.components.persistent_notification import _async_get_or_create_notifications

from tests import conftest as _engine  # noqa: F401 - register the standalone engine package.
from tests.tls_support import tls as tls
from tests_ha_proxy.test_client import peer as peer
from tests_ha_proxy.test_client import setup


# Intent: the first healthy status can still report a persistent Pause selected by the proxy.
# Why: a fresh HA process may never have observed the cloud failure, and a normal manual Pause should clear the notice.
async def test_healthy_proxy_pause_shows_notice_on_setup(actual_hass, peer):
    peer["state"].update(
        desired_control="paused",
        owner_control_enabled=False,
        local_hold_verified=True,
        station_ceiling_a=0,
        can_return=True,
        pause_reason="cloud_connection_failed",
    )
    entry = await setup(actual_hass, peer)
    coordinator = entry.runtime_data
    await actual_hass.async_block_till_done()
    notifications = _async_get_or_create_notifications(actual_hass)
    assert coordinator.fresh_healthy and coordinator.notified
    assert "scheduled charging are still paused" in notifications[coordinator.notification_id]["message"]
    assert actual_hass.states.get("button.ics_proxy_app_control").state != "unavailable"

    peer["state"].update(healthy=False, local_hold_verified=False)
    await coordinator.async_refresh()
    await actual_hass.async_block_till_done()
    assert "Waiting for the charger to confirm the pause" in notifications[coordinator.notification_id]["message"]

    peer["state"].pop("pause_reason")
    peer["state"].update(healthy=True, local_hold_verified=True, control_revision=peer["state"]["control_revision"] + 1)
    await coordinator.async_refresh()
    await actual_hass.async_block_till_done()
    assert coordinator.fresh_healthy and coordinator.notification_id not in notifications
    assert set(peer["commands"]) == {"status"}


# Intent: expire native and service-only entities at their own boundaries while the next status is held.
# Why: aging one half of a snapshot must not leave the other half visible indefinitely during a slow poll.
async def test_app_connection_expires_after_native_reading_during_held_poll(actual_hass, peer, monkeypatch):
    from custom_components.ics_proxy import coordinator

    clock = SimpleNamespace(now=1000.0)
    scheduled = []
    monkeypatch.setattr(coordinator, "time", SimpleNamespace(monotonic=lambda: clock.now))

    # Drive the real coordinator expiry callbacks without making the peer wait for wall-clock seconds.
    def later(hass, delay, action):
        event = (clock.now + delay, action)
        scheduled.append(event)
        return lambda: scheduled.remove(event)

    monkeypatch.setattr(coordinator, "async_call_later", later)
    peer["state"].update(native_age_seconds=12, status_age_seconds=10)
    entry = await setup(actual_hass, peer)
    power = "sensor.ics_proxy_power"
    app = "binary_sensor.ics_proxy_cloud_connected"
    assert actual_hass.states.get(app).state == "on"
    held, release = asyncio.Event(), asyncio.Event()

    # Hold the transport reply so only the original snapshot's timers can publish new entity states.
    async def hold(command, request, result):
        if command == "status":
            held.set()
            await release.wait()

    peer["response_gate"] = hold
    poll = asyncio.create_task(entry.runtime_data.async_refresh())
    try:
        await asyncio.wait_for(held.wait(), 3)
        clock.now, expire = scheduled.pop(0)
        expire(None)
        assert actual_hass.states.get(power).state == "unavailable"
        assert actual_hass.states.get(app).state == "on"
        clock.now, expire = scheduled.pop(0)
        expire(None)
        assert actual_hass.states.get(app).state == "unavailable"
        assert not scheduled and not poll.done()
    finally:
        release.set()
        await asyncio.wait_for(poll, 3)


# Intent: response transit consumes native freshness immediately and in the timer between polls.
# Why: a successful response cannot renew the lifetime of the evidence it carries.
@pytest.mark.parametrize("age,delay,expired", [(13, 0, False), (14.5, 0.8, True), (-0.2, 0.8, True)])
async def test_status_transit_counts_toward_native_expiry(actual_hass, peer, age, delay, expired):
    peer["state"]["native_age_seconds"] = age
    peer["status_delay"] = delay
    entry = await setup(actual_hass, peer)
    c = entry.runtime_data
    assert (actual_hass.states.get("sensor.ics_proxy_power").state == "unavailable") == expired
    assert (actual_hass.states.get("button.ics_proxy_start").state == "unavailable") == expired
    if not expired:
        assert c.fresh_healthy
        count = len(peer["commands"])
        await asyncio.sleep(2.2)
        await actual_hass.async_block_till_done()
        assert len(peer["commands"]) == count
        assert actual_hass.states.get("sensor.ics_proxy_power").state == "unavailable"
    else:
        assert not c.fresh_healthy and c.problem_since is not None
    assert set(peer["commands"]) == {"status"}


# Intent: a failed poll cannot renew the original snapshot's observation clock.
# Why: held or failed requests must keep aging the previously verified reading.
async def test_failed_status_retains_original_age_anchor(actual_hass, peer):
    entry = await setup(actual_hass, peer)
    c = entry.runtime_data
    anchor = c.received_at
    peer["status_delay"], peer["status_failure"] = 0.2, True
    await c.async_refresh()
    assert c.received_at == anchor and c.age("native_age_seconds") >= 0.2
    assert not c.last_update_success


# Intent: first fresh recovery dismisses the stable notice from an earlier coordinator; unhealthy reload retains it.
# Why: notifications belong to the entry across reloads, not to one coordinator's transient boolean.
@pytest.mark.parametrize("healthy", [False, True])
async def test_persistent_fault_notice_survives_only_unhealthy_reload(actual_hass, peer, healthy):
    entry = await setup(actual_hass, peer)
    notice = entry.runtime_data.notification_id
    persistent_notification.async_create(actual_hass, "Synthetic existing fault", notification_id=notice)
    await actual_hass.async_block_till_done()
    peer["state"]["healthy"] = healthy
    assert await actual_hass.config_entries.async_reload(entry.entry_id)
    await actual_hass.async_block_till_done()
    assert (notice in _async_get_or_create_notifications(actual_hass)) != healthy
    assert set(peer["commands"]) == {"status"}


# Intent: malformed latest history yields unknown while valid older rows remain available as history.
# Why: an older session's measured energy cannot silently become the current last-session measurement.
async def test_incomplete_charger_history_does_not_relabel_previous_energy(actual_hass, peer):
    peer["state"]["charger_history_complete"] = False
    await setup(actual_hass, peer)
    entity = actual_hass.states.get("sensor.ics_proxy_last_session_energy")
    assert entity.state == "unknown"
    assert entity.attributes["charger_sessions"][0]["energy_kwh"] == 5.614


# Intent: complete native HTTP history feeds the real published status and actual HA last-energy sensor.
# Why: unreadable later history hides last energy, while an unreadable older entry leaves a valid new charge visible.
@pytest.mark.parametrize(
    "clock",
    [
        "normal",
        "reversed",
        "future_stop",
        "older_reversed",
        "zero_energy",
        "delayed_history",
        "delayed_older_unreadable",
    ],
)
async def test_native_history_to_actual_ha_sensor(
    actual_hass, tmp_path, tls, unused_tcp_port_factory, monkeypatch, clock
):
    import time
    from datetime import UTC, datetime

    from ocpp_hub_core.prototype.managed import ManagedController
    from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings

    from tests.managed_rig import healthy, introduced, managed_bench, managed_session, starts
    from tests.prototype_rig import START
    from tests_ha_proxy.test_client import TOKEN

    now = int(time.time())
    old = {"transactionId": 1, "startTime": now - 3600, "stopTime": now - 1800, "meterStart": 1000, "meterStop": 6614}
    latest = {"transactionId": 2, "startTime": now - 100, "stopTime": now - 10, "meterStart": 1000, "meterStop": 2000}
    if clock in ("reversed", "delayed_older_unreadable"):
        latest["stopTime"] = now - 200
    elif clock == "future_stop":
        latest["stopTime"] = now + 45
    elif clock == "older_reversed":
        old["stopTime"] = now - 3700
    elif clock == "zero_energy":
        latest["meterStop"] = latest["meterStart"]
    original = [latest, old]
    async with managed_bench(tmp_path, tls, unused_tcp_port_factory(), initial_history=original) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "history-real-boot")
            published = await healthy(controller)
            assert published["charger_history_complete"] == (clock in ("normal", "zero_energy", "delayed_history"))
            latest_known = clock in ("normal", "older_reversed", "zero_energy", "delayed_history")
            expected_energy = (latest["meterStop"] - latest["meterStart"]) / 1000 if latest_known else None
            assert published["charger_latest_completion_known"] is latest_known
            assert published["last_session_energy_kwh"] == expected_energy
            server = ManagementServer(
                ManagementSettings(unused_tcp_port_factory(), tmp_path / "unused", TOKEN),
                controller,
                lambda: dict(published, running=True, certificate_remaining_seconds=86400),
            )
            await server.start()
            try:
                entry = await setup(actual_hass, {"port": server.settings.port})
                entity = actual_hass.states.get("sensor.ics_proxy_last_session_energy")
                assert entity.state == ("unknown" if expected_energy is None else str(expected_energy))
                assert rig.bench.history == original and not starts(rig)
                if clock in ("delayed_history", "delayed_older_unreadable"):
                    timestamp = datetime.fromtimestamp(now, UTC).isoformat()
                    body = START | {"idTag": "NATIVE_TIMER", "timestamp": timestamp}
                    opened = await rig.bench.call(charger, "recent-scheduled-start", "StartTransaction", body)
                    local = opened[2]["transactionId"]
                    rig.peer.permission_enabled = True
                    rig.bench.cp, rig.bench.connection = "C", "Charging"
                    rig.bench.firmware.fire_plan(9)
                    published = await healthy(controller)
                    assert published["charging_session_open"]
                    meter_stop = body["meterStart"] + 3000
                    await rig.bench.call(
                        charger,
                        "recent-scheduled-stop",
                        "StopTransaction",
                        {"transactionId": local, "meterStop": meter_stop, "timestamp": timestamp},
                    )
                    rig.peer.permission_enabled = False
                    rig.bench.cp, rig.bench.connection = "B", "Plug"
                    # A live receipt read can see genuine closure before the next native poll.
                    await entry.runtime_data.async_refresh()
                    assert actual_hass.states.get(entity.entity_id).state == "unknown"
                    published = await healthy(controller)
                    assert not published["charging_session_open"] and published["last_session_energy_kwh"] is None
                    await entry.runtime_data.async_refresh()
                    assert actual_hass.states.get(entity.entity_id).state == "unknown"
                    rig.bench.history = [
                        {
                            "transactionId": local,
                            # The device clock moved backward between these independently recorded sessions.
                            "startTime": now - 1200,
                            "stopTime": now - 1100,
                            "meterStart": body["meterStart"],
                            "meterStop": meter_stop,
                        },
                        *original,
                    ]
                    published = await healthy(controller)
                    assert published["last_session_energy_kwh"] == 3.0
                    await entry.runtime_data.async_refresh()
                    assert actual_hass.states.get(entity.entity_id).state == "3.0"
                await actual_hass.config_entries.async_unload(entry.entry_id)
            finally:
                await server.close()
