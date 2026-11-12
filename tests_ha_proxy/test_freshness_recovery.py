"""Actual HA keeps snapshot transit, reload recovery and incomplete history honest."""

import asyncio

import pytest
from homeassistant.components import persistent_notification

try:
    from homeassistant.components.persistent_notification.helpers import _async_get_or_create_notifications
except ImportError:  # HA10 exposes the same real notification store from the component module.
    from homeassistant.components.persistent_notification import _async_get_or_create_notifications

from tests import conftest as _engine  # noqa: F401 - register the standalone engine package.
from tests.test_prototype_tls import tls as tls
from tests_ha_proxy.test_client import peer as peer
from tests_ha_proxy.test_client import setup


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
# Why: independently invalid chronology must survive producer filtering as unknown instead of an older energy value.
@pytest.mark.parametrize("clock", ["normal", "reversed", "future_stop"])
async def test_native_history_to_actual_ha_sensor(
    actual_hass, tmp_path, tls, unused_tcp_port_factory, monkeypatch, clock
):
    import time

    from ocpp_hub_core.prototype.managed import ManagedController
    from ocpp_hub_core.prototype.managed_api import ManagementServer, ManagementSettings

    from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session, starts
    from tests_ha_proxy.test_client import TOKEN

    now = int(time.time())
    old = {"transactionId": 1, "startTime": now - 3600, "stopTime": now - 1800, "meterStart": 1000, "meterStop": 6614}
    latest = {"transactionId": 2, "startTime": now - 100, "stopTime": now - 10, "meterStart": 1000, "meterStop": 2000}
    if clock == "reversed":
        latest["stopTime"] = now - 200
    elif clock == "future_stop":
        latest["stopTime"] = now + 45
    original = [latest, old]
    async with managed_bench(tmp_path, tls, unused_tcp_port_factory(), monkeypatch, initial_history=original) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "history-real-boot")
            published = await healthy(controller)
            assert published["charger_history_complete"] == (clock == "normal")
            server = ManagementServer(
                ManagementSettings(unused_tcp_port_factory(), tmp_path / "unused", TOKEN),
                controller,
                lambda: dict(published, running=True, certificate_remaining_seconds=86400),
            )
            await server.start()
            try:
                entry = await setup(actual_hass, {"port": server.settings.port})
                entity = actual_hass.states.get("sensor.ics_proxy_last_session_energy")
                assert entity.state == ("1.0" if clock == "normal" else "unknown")
                assert rig.bench.history == original and not starts(rig)
                await actual_hass.config_entries.async_unload(entry.entry_id)
            finally:
                await server.close()
