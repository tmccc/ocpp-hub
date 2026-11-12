"""Actual HA setup retries retain an independent fault deadline and retire stale coordinators."""

import asyncio
import sys
import time

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.exceptions import HomeAssistantError

try:
    from homeassistant.components.persistent_notification.helpers import _async_get_or_create_notifications
except ImportError:
    from homeassistant.components.persistent_notification import _async_get_or_create_notifications

from tests_ha_proxy.test_client import peer as peer
from tests_ha_proxy.test_client import setup


# Await a real independent HA callback rather than assuming the scheduler ran after one fixed sleep.
async def wait_for_notice(hass, identifier):
    async with asyncio.timeout(2):
        while identifier not in _async_get_or_create_notifications(hass):
            await asyncio.sleep(0.01)


# Use the real retry setup path without another unload or a second imported config entry.
async def retry(hass, entry):
    entry.async_cancel_retry_setup()
    async with entry.setup_lock:
        await entry.async_setup(hass)
    await hass.async_block_till_done()


# Begin a genuine failed first refresh after the original healthy entry is unloaded.
async def failed_setup(hass, peer, monkeypatch, delay=0.05):
    entry = await setup(hass, peer)
    module = sys.modules["custom_components.ics_proxy.notification"]
    monkeypatch.setattr(module, "NOTICE_DELAY", delay)
    old = entry.runtime_data
    peer["status_failure"] = True
    await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.SETUP_RETRY
    notice = hass.data[module.NOTICES][entry.entry_id]
    return entry, old, notice


# Intent: setup loss notifies at its own deadline even when HA performs no second retry or refresh.
# Why: merely carrying problem_since cannot alert during a long retry interval.
async def test_setup_outage_notifies_without_another_retry(actual_hass, peer, monkeypatch):
    entry, old, notice = await failed_setup(actual_hass, peer, monkeypatch)
    requests = len(peer["commands"])
    await wait_for_notice(actual_hass, notice.notification_id)
    assert len(peer["commands"]) == requests and notice.notified
    assert entry.state is ConfigEntryState.SETUP_RETRY
    assert set(peer["commands"]) == {"status"}


# Intent: failed retry coordinators share one outage, then actual recovery dismisses its stable notice.
# Why: retries must neither postpone the alert nor leave it behind after fresh setup succeeds.
async def test_setup_retries_share_notice_and_recovery(actual_hass, peer, monkeypatch):
    entry, old, notice = await failed_setup(actual_hass, peer, monkeypatch)
    since = notice.problem_since
    for _ in range(3):
        await retry(actual_hass, entry)
        assert entry.state is ConfigEntryState.SETUP_RETRY
        assert notice.problem_since == since
    await wait_for_notice(actual_hass, notice.notification_id)
    assert len(_async_get_or_create_notifications(actual_hass)) == 1
    peer["status_failure"] = False
    await retry(actual_hass, entry)
    assert entry.state is ConfigEntryState.LOADED
    assert entry.runtime_data.notice is notice
    assert notice.problem_since is None and not notice.notified
    assert notice.notification_id not in _async_get_or_create_notifications(actual_hass)
    assert set(peer["commands"]) == {"status"}


# Intent: an old successful callback cannot dismiss a replacement coordinator's ongoing failure.
# Why: entry-scoped notification storage must not let an obsolete coordinator overwrite newer evidence.
async def test_stale_coordinator_cannot_clear_setup_outage(actual_hass, peer, monkeypatch):
    entry, old, notice = await failed_setup(actual_hass, peer, monkeypatch)
    since = notice.problem_since
    old.update_notification(True)
    assert notice.problem_since == since
    await wait_for_notice(actual_hass, notice.notification_id)
    old.update_notification(True)
    assert notice.notified and notice.notification_id in _async_get_or_create_notifications(actual_hass)


# Intent: unloaded or removed failed entries cannot create orphan notifications after their old deadline.
# Why: the independent timer outlives failed setup, but it must not outlive the entry's active intent.
@pytest.mark.parametrize("remove", [False, True])
async def test_failed_entry_cleanup_retires_notice_deadline(actual_hass, peer, monkeypatch, remove):
    entry, old, notice = await failed_setup(actual_hass, peer, monkeypatch, delay=60)
    if remove:
        assert await actual_hass.config_entries.async_remove(entry.entry_id)
    else:
        assert await actual_hass.config_entries.async_unload(entry.entry_id)
        assert entry.state is ConfigEntryState.NOT_LOADED
    await asyncio.sleep(0.1)
    await actual_hass.async_block_till_done()
    notice.deadline(notice.generation)
    old.update_notification(False)
    assert notice.notification_id not in _async_get_or_create_notifications(actual_hass)
    assert notice.cancel_timer is None and not notice.active


# Intent: actual failed unload retains a working coordinator and its independent outage monitor.
# Why: HA keeps runtime_data in FAILED_UNLOAD, so its subsequent failure and recovery still need notification.
async def test_failed_unload_retains_monitor(actual_hass, peer, monkeypatch):
    entry = await setup(actual_hass, peer)
    coordinator = entry.runtime_data
    monkeypatch.setattr(sys.modules["custom_components.ics_proxy.notification"], "NOTICE_DELAY", 0.05)

    # Fail at HA's real platform-unload boundary before it can retire the current coordinator.
    async def failed_unload(*args, **kwargs):
        raise HomeAssistantError("synthetic platform unload failure")

    with monkeypatch.context() as boundary:
        boundary.setattr(actual_hass.config_entries, "async_unload_platforms", failed_unload)
        assert not await actual_hass.config_entries.async_unload(entry.entry_id)
    assert entry.state is ConfigEntryState.FAILED_UNLOAD and entry.runtime_data is coordinator
    peer["status_failure"] = True
    await coordinator.async_refresh()
    await wait_for_notice(actual_hass, coordinator.notification_id)
    peer["status_failure"] = False
    await coordinator.async_refresh()
    assert coordinator.notification_id not in _async_get_or_create_notifications(actual_hass)


# Intent: repeated genuine failed setup attempts share one sixty-second outage rather than restarting debounce.
# Why: this public setup regression must detect the original bug without depending on the replacement monitor API.
async def test_failed_first_refresh_retries_eventually_notify(actual_hass, peer, monkeypatch):
    entry = await setup(actual_hass, peer)
    module = sys.modules["custom_components.ics_proxy.coordinator"]
    original, elapsed = time.monotonic, 0

    # Advance only this integration's debounce clock, preserving HA's actual event-loop and retry setup behavior.
    class Clock:
        # Keep a real monotonic origin plus independently controlled outage age.
        @staticmethod
        def monotonic():
            return original() + elapsed

    monkeypatch.setattr(module, "time", Clock)
    peer["status_failure"] = True
    await actual_hass.config_entries.async_reload(entry.entry_id)
    for _ in range(3):
        elapsed += 70
        await retry(actual_hass, entry)
        assert entry.state is ConfigEntryState.SETUP_RETRY
    assert "ics_proxy_" + entry.entry_id in _async_get_or_create_notifications(actual_hass)
    peer["status_failure"] = False
    await retry(actual_hass, entry)
    assert entry.state is ConfigEntryState.LOADED
    assert "ics_proxy_" + entry.entry_id not in _async_get_or_create_notifications(actual_hass)
