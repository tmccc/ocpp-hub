"""Real HA entry lifetimes cross failed setup, notice deadlines and obsolete callbacks, then recover twice."""

import copy
import importlib
from types import SimpleNamespace

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.setup import async_setup_component

from tests_ha_proxy.test_client import TOKEN, setup
from tests_ha_proxy.test_client import peer as peer
from tests_ha_proxy.test_setup_outage_notice import _async_get_or_create_notifications, retry
from tests_model.catalog import input_digest
from tests_model.ha_cases import outage_candidates, outage_exclusion

CASES = tuple(case for case in outage_candidates() if outage_exclusion(case) is None)


# Observe a genuine failed first coordinator refresh after config-flow validation, or an actual failed reload.
async def first_failure(hass, peer, case, fault):
    if case.origin == "reload":
        entry = await setup(hass, peer)
        fault["enabled"] = True
        await hass.config_entries.async_reload(entry.entry_id)
    else:
        assert await async_setup_component(hass, "persistent_notification", {})
        fault.update(enabled=True, allow_validation=True)
        result = await hass.config_entries.flow.async_init(
            "ics_proxy", context={"source": "import"}, data={"port": peer["port"], "token": TOKEN}
        )
        assert result["type"] == "create_entry", result
        entry = result["result"]
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.SETUP_RETRY
    entry.async_cancel_retry_setup()
    return entry


# Intent: every admitted lifetime/deadline order rejects obsolete work and recovers through a second actual outage.
# Why: a passing first refresh alone does not prove future notifications, removal cleanup or failed-setup recovery.
@pytest.mark.parametrize("case", CASES, ids=lambda case: case.identifier)
async def test_outage_lifecycle_sequences(actual_hass, peer, monkeypatch, record_property, case):
    hass = actual_hass
    coordinator = importlib.import_module("custom_components.ics_proxy.coordinator")
    notification = importlib.import_module("custom_components.ics_proxy.notification")
    original_init, original_later = coordinator.ProxyCoordinator.__init__, notification.async_call_later
    clock, coordinators, callbacks, events = SimpleNamespace(value=1000.0), [], [], []
    fault = {"enabled": False, "allow_validation": False}
    original_state = copy.deepcopy(peer["state"])

    # Trace real construction without bypassing its entry ownership or lifecycle hooks.
    def traced_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        coordinators.append(self)

    # Keep real HA scheduling and cancellation; deliver captured callbacks at explicitly controlled integration ages.
    def traced_later(hass, delay, action):
        callbacks.append(action)
        return original_later(hass, delay, action)

    # Corrupt only the independent management peer's status contract, after the first import validation read.
    def response(command, request, result):
        if command == "status" and fault["enabled"]:
            if fault["allow_validation"]:
                assert not coordinators
                fault["allow_validation"] = False
            else:
                return {**result, "api_version": 0}
        return result

    monkeypatch.setattr(coordinator.ProxyCoordinator, "__init__", traced_init)
    monkeypatch.setattr(coordinator, "time", SimpleNamespace(monotonic=lambda: clock.value))
    monkeypatch.setattr(notification, "async_call_later", traced_later)
    peer["result_filter"] = response
    suffix_complete = False
    try:
        entry = await first_failure(hass, peer, case, fault)
        obsolete = coordinators[-1]
        notice, since, old_id = obsolete.notice, obsolete.notice.problem_since, entry.entry_id
        assert since == clock.value and callbacks
        events.append("setup-failure")
        await retry(hass, entry)
        entry.async_cancel_retry_setup()
        assert coordinators[-1] is not obsolete and coordinators[-1].notice is notice
        assert notice.problem_since == since
        events.append("owner-replacement")
        saved_callback = callbacks[-1]
        terminal = False
        terminal_visible = None
        for event in case.order:
            if event == "retry":
                await retry(hass, entry)
                assert entry.state is ConfigEntryState.SETUP_RETRY and notice.problem_since == since
                entry.async_cancel_retry_setup()
            elif event == "deadline":
                clock.value = since + 61
                callbacks[-1](None)
                if not terminal:
                    assert notice.notification_id in _async_get_or_create_notifications(hass)
            elif event == "stale":
                before = (notice.problem_since, notice.notified, tuple(_async_get_or_create_notifications(hass)))
                obsolete.update_notification(case.stale_okay)
                assert before == (
                    notice.problem_since,
                    notice.notified,
                    tuple(_async_get_or_create_notifications(hass)),
                )
            elif case.lifecycle == "reload":
                await hass.config_entries.async_reload(entry.entry_id)
                assert entry.state is ConfigEntryState.SETUP_RETRY and notice.problem_since == since
                entry.async_cancel_retry_setup()
            elif case.lifecycle == "unload":
                assert await hass.config_entries.async_unload(entry.entry_id)
                assert entry.state is ConfigEntryState.NOT_LOADED and not notice.active and notice.cancel_timer is None
                terminal = True
            else:
                assert await hass.config_entries.async_remove(entry.entry_id)
                assert hass.config_entries.async_get_entry(old_id) is None
                assert old_id not in hass.data[notification.NOTICES] and not notice.active
                assert notice.notification_id not in _async_get_or_create_notifications(hass)
                terminal = True
            await hass.async_block_till_done()
            assert peer["state"] == original_state and set(peer["commands"]) == {"status"}
            if terminal:
                assert not notice.active and notice.cancel_timer is None
                visible = dict(_async_get_or_create_notifications(hass))
                if terminal_visible is None:
                    terminal_visible = visible
                assert visible == terminal_visible
            for action in ("pause", "app_control", "start", "start_test"):
                entity = hass.states.get("button.ics_proxy_" + action)
                assert entity is None or entity.state == "unavailable"
            events.append(event)
        fault["enabled"] = False
        if case.lifecycle == "remove":
            entry = await setup(hass, peer)
            assert entry.entry_id != old_id
        elif case.lifecycle == "unload":
            assert await hass.config_entries.async_setup(entry.entry_id)
        else:
            await retry(hass, entry)
        await hass.async_block_till_done()
        current = entry.runtime_data
        assert entry.state is ConfigEntryState.LOADED and current.fresh_healthy
        assert hass.states.get("sensor.ics_proxy_power").state != "unavailable"
        assert not _async_get_or_create_notifications(hass)
        events.append("recover")
        saved_callback(None)
        obsolete.update_notification(False)
        assert current.problem_since is None and not _async_get_or_create_notifications(hass)
        events.append("late-callbacks")
        fault["enabled"] = True
        await current.async_refresh()
        since2 = current.problem_since
        assert since2 == clock.value and not current.notified
        events.append("second-failure")
        clock.value = since2 + 59
        callbacks[-1](None)
        assert not current.notified
        clock.value = since2 + 61
        callbacks[-1](None)
        assert current.notification_id in _async_get_or_create_notifications(hass)
        obsolete.update_notification(True)
        obsolete.update_notification(False)
        saved_callback(None)
        assert current.problem_since == since2 and current.notified
        assert current.notification_id in _async_get_or_create_notifications(hass)
        events.append("second-deadline")
        fault["enabled"] = False
        await current.async_refresh()
        assert current.fresh_healthy and current.problem_since is None and not _async_get_or_create_notifications(hass)
        assert peer["state"] == original_state and set(peer["commands"]) == {"status"}
        events.append("second-recovery")
        assert events == list(case.events)
        suffix_complete = True
    finally:
        record_property("model_case", case.identifier)
        record_property("model_family", "ha_outage_lifecycle")
        record_property("model_input_sha256", input_digest("ha_outage_lifecycle", case))
        record_property("completed_events", events)
        record_property("suffix_complete", suffix_complete)
