"""The actual HA coordinator scheduler must recover two peer outages without test-driven refresh or callbacks."""

import asyncio
import importlib
import time
from datetime import timedelta

import pytest
from homeassistant.core import CoreState

from tests_ha_proxy.conftest import actual_hass as actual_hass
from tests_ha_proxy.test_client import peer as peer
from tests_ha_proxy.test_client import setup
from tests_ha_proxy.test_setup_outage_notice import _async_get_or_create_notifications


# Observe bounded progress and return a Boolean so a missing scheduled poll fails a behavioral assertion.
async def progressed(predicate, budget):
    end = time.monotonic() + budget
    while time.monotonic() < end:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


# Configure only the coordinator's interval before construction; HA still owns every scheduled refresh callback.
def configured_timing(monkeypatch, seconds):
    module = importlib.import_module("custom_components.ics_proxy.coordinator")
    notice = importlib.import_module("custom_components.ics_proxy.notification")
    if seconds != 5:
        # Leave the real monotonic clock and scheduler untouched; supply the test interval to the real constructor.
        def interval(**kwargs):
            assert kwargs == {"seconds": 5}
            return timedelta(seconds=seconds)

        monkeypatch.setattr(module, "timedelta", interval)
    monkeypatch.setattr(notice, "NOTICE_DELAY", 0.25)


# Read only public coordinator/entity observations after independent peer changes and genuine scheduled requests.
async def two_periodic_outages(hass, peer, monkeypatch, kind, seconds):
    configured_timing(monkeypatch, seconds)
    entry = await setup(hass, peer)
    await hass.async_start()
    assert hass.state is CoreState.running
    current = entry.runtime_data
    budget = 2 * seconds + 3
    count = peer["commands"].count("status")
    peer["state"]["power_w"] = 17
    assert await progressed(
        lambda: (
            peer["commands"].count("status") > count
            and current.data.get("power_w") == 17
            and hass.states.get("sensor.ics_proxy_power").state == "17"
        ),
        budget,
    ), "real periodic polling did not publish a new independently supplied status"
    for episode in range(2):
        if kind == "socket-close":
            peer["status_failure"] = True
        elif kind == "invalid-status":
            # Corrupt only the loopback peer's actual response schema while retaining real websocket exchange IDs.
            def response(command, request, result):
                return {**result, "api_version": 0} if command == "status" else result

            peer["result_filter"] = response
        else:
            peer["state"].update(healthy=False, local_healthy=True, cloud_connected=False)
        previous_count = peer["commands"].count("status")
        assert await progressed(
            lambda: (
                peer["commands"].count("status") > previous_count
                and current.problem_since is not None
                and hass.states.get("binary_sensor.ics_proxy_problem").state == "on"
            ),
            budget,
        ), "periodic polling did not observe the independent peer outage"
        since = current.problem_since
        assert await progressed(
            lambda: current.notified and current.notification_id in _async_get_or_create_notifications(hass),
            2,
        ), "HA's own notice timer did not report the observed outage"
        assert current.problem_since == since
        if kind == "cloud-only":
            assert current.last_update_success
            assert current.fresh_local_healthy
            assert hass.states.get("button.ics_proxy_start").state != "unavailable"
            assert hass.states.get("button.ics_proxy_start_test").state == "unavailable"
            assert hass.states.get("binary_sensor.ics_proxy_cloud_connected").state == "off"
        else:
            # A recent cached body remains fresh; entity availability also requires the failed update flag.
            assert not current.last_update_success
            assert hass.states.get("button.ics_proxy_start").state == "unavailable"
            assert hass.states.get("sensor.ics_proxy_power").state == "unavailable"
        peer["status_failure"] = False
        peer.pop("result_filter", None)
        peer["state"].update(healthy=True, local_healthy=True, cloud_connected=True, power_w=31 + episode)
        previous_count = peer["commands"].count("status")
        assert await progressed(
            lambda: (
                peer["commands"].count("status") > previous_count
                and current.fresh_healthy
                and current.problem_since is None
                and not current.notified
                and current.notification_id not in _async_get_or_create_notifications(hass)
                and hass.states.get("binary_sensor.ics_proxy_problem").state == "off"
                and hass.states.get("sensor.ics_proxy_power").state == str(31 + episode)
            ),
            budget,
        ), "periodic polling did not recover the independent peer and dismiss its old notice"
        assert hass.states.get("button.ics_proxy_start").state != "unavailable"
        assert entry.runtime_data is current
    assert set(peer["commands"]) == {"status"}


# Intent: real scheduled status reads, notices and recovery succeed twice for transport, schema and cloud-only loss.
# Why: manual refresh/retry or manually fired notice callbacks cannot establish autonomous loaded-entry recovery.
@pytest.mark.parametrize("kind", ["socket-close", "invalid-status", "cloud-only"])
async def test_periodic_poll_recovers_two_outages_without_assistance(actual_hass, peer, monkeypatch, kind):
    await two_periodic_outages(actual_hass, peer, monkeypatch, kind, 1)


# Intent: the production five-second interval also performs repeated autonomous loss and recovery.
# Why: accelerated test intervals alone cannot prove the actual configured coordinator schedule is enrolled.
async def test_production_interval_recovers_without_assistance(actual_hass, peer, monkeypatch):
    await two_periodic_outages(actual_hass, peer, monkeypatch, "socket-close", 5)
