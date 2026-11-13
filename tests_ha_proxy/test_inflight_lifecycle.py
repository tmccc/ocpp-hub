"""Actual HA stop and held refresh/replacement orders use independent real peer response barriers."""

import asyncio
import copy
import importlib
from dataclasses import dataclass, field
from itertools import product
from types import SimpleNamespace

import pytest
from homeassistant.config_entries import ConfigEntryDisabler, ConfigEntryState
from homeassistant.core import CoreState

from tests_ha_proxy.test_client import peer as peer
from tests_ha_proxy.test_client import setup
from tests_ha_proxy.test_setup_outage_notice import _async_get_or_create_notifications


@dataclass
class StatusGate:
    """The independent peer captures real status snapshots before waiting to send them."""

    entered: tuple = field(default_factory=lambda: (asyncio.Event(), asyncio.Event()))
    release: tuple = field(default_factory=lambda: (asyncio.Event(), asyncio.Event()))
    snapshots: list = field(default_factory=list)
    request_ids: list = field(default_factory=list)
    forwarded: list = field(default_factory=list)

    # Delay just the first two selected status exchanges; every later recovery exchange stays real and immediate.
    async def __call__(self, command, request, result):
        if command != "status":
            return
        index = len(self.snapshots)
        self.snapshots.append(copy.deepcopy(result))
        self.request_ids.append(request["id"])
        if index < len(self.entered):
            self.entered[index].set()
            await self.release[index].wait()
        self.forwarded.append(index)

    # A failed assertion cannot leave a peer handler waiting during fixture/server cleanup.
    def release_all(self):
        for event in self.release:
            event.set()


# Keep both timers and clock genuine except the integration's own explicitly controlled monotonic age.
def observe_notice_callbacks(monkeypatch):
    coordinator = importlib.import_module("custom_components.ics_proxy.coordinator")
    notice_module = importlib.import_module("custom_components.ics_proxy.notification")
    original = notice_module.async_call_later
    clock, callbacks = SimpleNamespace(value=1000.0), []

    # Record the real scheduled callback and preserve HA's original cancellation handle and callback marker.
    def later(hass, delay, action):
        callbacks.append(action)
        return original(hass, delay, action)

    monkeypatch.setattr(coordinator, "time", SimpleNamespace(monotonic=lambda: clock.value))
    monkeypatch.setattr(notice_module, "async_call_later", later)
    return clock, callbacks, notice_module


# Establish a real failed health observation; optional delivery makes the stable fault visible before a race.
async def establish_outage(hass, peer, coordinator, clock, callbacks, *, visible):
    peer["state"]["healthy"] = False
    await coordinator.async_refresh()
    assert coordinator.problem_since == clock.value
    assert not coordinator.notified and callbacks
    if visible:
        clock.value += 61
        callbacks[-1](None)
        assert coordinator.notification_id in _async_get_or_create_notifications(hass)
    return callbacks[-1]


# Wait only for the particular public operation; background request gates must never depend on global draining.
async def finished(task):
    return await asyncio.wait_for(asyncio.shield(task), 3)


# Join test-owned tasks after releasing all peer barriers, without leaving a half-finished HA lifecycle behind.
async def cleanup_tasks(tasks):
    for task in tasks:
        if not task.done():
            try:
                await finished(task)
            except TimeoutError, asyncio.CancelledError:
                task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


# Check published current owner truth, rather than the retired coordinator's final private data object.
def assert_current(hass, entry, *, healthy, revision):
    current = entry.runtime_data
    assert entry.state is ConfigEntryState.LOADED
    assert current.data["control_revision"] == revision
    assert current.fresh_healthy is healthy
    notices = _async_get_or_create_notifications(hass)
    if healthy:
        assert current.problem_since is None and not current.notified
        assert current.notification_id not in notices
        assert hass.states.get("sensor.ics_proxy_power").state == "0"
        assert hass.states.get("button.ics_proxy_start").state != "unavailable"
    else:
        assert current.problem_since is not None and current.notified
        assert current.notification_id in notices
        assert hass.states.get("button.ics_proxy_start").state == "unavailable"
    return current


# Intent: a real HA shutdown cancels its held poll and forbids late old callbacks from changing its fault store.
# Why: no live request or orphan notice may survive merely because the read began before the stop event.
@pytest.mark.parametrize("visible,old_healthy", product((False, True), (False, True)))
async def test_ha_stop_cancels_held_status(actual_hass, peer, monkeypatch, visible, old_healthy):
    hass = actual_hass
    clock, callbacks, notice_module = observe_notice_callbacks(monkeypatch)
    entry = await setup(hass, peer)
    old = entry.runtime_data
    saved = await establish_outage(hass, peer, old, clock, callbacks, visible=visible)
    peer["state"]["healthy"] = old_healthy
    gate = StatusGate()
    peer["response_gate"] = gate
    # This matches HA's own background poll ownership; unlike a raw pytest task it is canceled on hass shutdown.
    read = hass.async_create_background_task(old.async_refresh(), "ics-test-stop-held-poll", eager_start=False)
    try:
        await asyncio.wait_for(gate.entered[0].wait(), 3)
        assert gate.snapshots[0]["healthy"] is old_healthy
        before_notice = dict(_async_get_or_create_notifications(hass))
        anchor, prior_data = old.received_at, copy.deepcopy(old.data)
        await hass.async_stop(force=True)
        assert hass.state is CoreState.stopped
        assert read.done() and read.cancelled()
        assert entry.entry_id not in hass.data.get(notice_module.NOTICES, {})
        assert not old.notice.active and old.notice.cancel_timer is None
        count = len(peer["commands"])
        gate.release_all()
        await asyncio.gather(read, return_exceptions=True)
        await asyncio.sleep(0)
        assert old.received_at == anchor and old.data == prior_data
        saved(None)
        old.update_notification(True)
        old.update_notification(False)
        assert dict(_async_get_or_create_notifications(hass)) == before_notice
        assert not old.notice.active and old.notice.cancel_timer is None
        assert len(peer["commands"]) == count and set(peer["commands"]) == {"status"}
        # Terminal cleanup is the suffix. This test does not claim a cross-process HA restart was exercised.
    finally:
        gate.release_all()
        await cleanup_tasks([read])


# Intent: a genuine old status read cannot overwrite a new owner's truth or revive its retired expiry timer.
# Why: unload cancellation runs before a surviving HA-owned poll returns, so its completion needs a lifetime guard.
@pytest.mark.parametrize(
    "transition,old_healthy,new_healthy,release_order",
    product(("reload", "disable-enable"), (False, True), (False, True), ("old-first", "new-first")),
)
async def test_held_status_crosses_entry_replacement(
    actual_hass, peer, monkeypatch, transition, old_healthy, new_healthy, release_order
):
    hass = actual_hass
    clock, callbacks, _ = observe_notice_callbacks(monkeypatch)
    entry = await setup(hass, peer)
    old = entry.runtime_data
    saved = await establish_outage(hass, peer, old, clock, callbacks, visible=True)
    # Start this read after the prefix clock advance, so its healthy response is genuinely fresh if it arrives.
    peer["state"].update(healthy=old_healthy, control_revision=0)
    gate = StatusGate()
    peer["response_gate"] = gate
    tasks = []
    old_read = hass.async_create_background_task(old.async_refresh(), "ics-test-old-poll", eager_start=False)
    tasks.append(old_read)
    try:
        await asyncio.wait_for(gate.entered[0].wait(), 3)
        assert gate.snapshots[0]["healthy"] is old_healthy
        peer["state"].update(healthy=new_healthy, control_revision=1)
        if transition == "disable-enable":
            await hass.config_entries.async_set_disabled_by(entry.entry_id, ConfigEntryDisabler.USER)
            assert entry.disabled_by is ConfigEntryDisabler.USER
            assert entry.state is ConfigEntryState.NOT_LOADED
            assert not old.notice.active and old.cancel_timer is None
            before_notice = dict(_async_get_or_create_notifications(hass))
            saved(None)
            old.update_notification(True)
            assert dict(_async_get_or_create_notifications(hass)) == before_notice
            operation = hass.config_entries.async_set_disabled_by(entry.entry_id, None)
        else:
            operation = hass.config_entries.async_reload(entry.entry_id)
        replacing = hass.async_create_background_task(operation, "ics-test-new-setup", eager_start=False)
        tasks.append(replacing)
        await asyncio.wait_for(gate.entered[1].wait(), 3)
        assert gate.snapshots[1]["control_revision"] == 1
        assert gate.snapshots[1]["healthy"] is new_healthy
        assert old.notice.generation != old.notice_generation
        assert old.cancel_timer is None
        if release_order == "old-first":
            gate.release[0].set()
            await finished(old_read)
            # The current owner has not supplied a result yet: an old healthy snapshot cannot dismiss its fault.
            assert old.notification_id in _async_get_or_create_notifications(hass)
            assert old.cancel_timer is None
            gate.release[1].set()
            await finished(replacing)
        else:
            gate.release[1].set()
            await finished(replacing)
            assert_current(hass, entry, healthy=new_healthy, revision=1)
            gate.release[0].set()
            await finished(old_read)
            assert old.cancel_timer is None
        current = assert_current(hass, entry, healthy=new_healthy, revision=1)
        assert current is not old and entry.disabled_by is None
        assert len(gate.request_ids) == len(set(gate.request_ids))
        # Old callbacks remain inert after both success and ongoing fault of the replacement owner.
        before = (current.problem_since, current.notified, dict(_async_get_or_create_notifications(hass)))
        saved(None)
        old.update_notification(True)
        old.update_notification(False)
        assert before == (current.problem_since, current.notified, dict(_async_get_or_create_notifications(hass)))
        peer["state"]["healthy"] = True
        await current.async_refresh()
        assert_current(hass, entry, healthy=True, revision=1)
        assert old.cancel_timer is None and set(peer["commands"]) == {"status"}
    finally:
        gate.release_all()
        await cleanup_tasks(tasks)


# Intent: a late read is inert while the same generation is inactive, before any replacement can change generation.
# Why: generation mismatch alone cannot protect a successfully unloaded or disabled entry's canceled timers/data.
@pytest.mark.parametrize("disable", [False, True], ids=["unload", "disable"])
async def test_held_status_after_terminal_cleanup_before_reactivation(actual_hass, peer, monkeypatch, disable):
    hass = actual_hass
    clock, callbacks, _ = observe_notice_callbacks(monkeypatch)
    entry = await setup(hass, peer)
    old = entry.runtime_data
    saved = await establish_outage(hass, peer, old, clock, callbacks, visible=True)
    peer["state"]["healthy"] = True
    gate = StatusGate()
    peer["response_gate"] = gate
    read = hass.async_create_background_task(old.async_refresh(), "ics-test-inactive-poll", eager_start=False)
    try:
        await asyncio.wait_for(gate.entered[0].wait(), 3)
        anchor, data = old.received_at, copy.deepcopy(old.data)
        if disable:
            await hass.config_entries.async_set_disabled_by(entry.entry_id, ConfigEntryDisabler.USER)
        else:
            assert await hass.config_entries.async_unload(entry.entry_id)
        assert entry.state is ConfigEntryState.NOT_LOADED
        assert not old.notice.active and old.notice.generation == old.notice_generation
        notices = dict(_async_get_or_create_notifications(hass))
        gate.release_all()
        await finished(read)
        assert old.received_at == anchor and old.data == data and old.cancel_timer is None
        saved(None)
        assert dict(_async_get_or_create_notifications(hass)) == notices
        peer["response_gate"] = None
        if disable:
            await hass.config_entries.async_set_disabled_by(entry.entry_id, None)
        else:
            assert await hass.config_entries.async_setup(entry.entry_id)
        assert_current(hass, entry, healthy=True, revision=0)
        assert set(peer["commands"]) == {"status"}
    finally:
        gate.release_all()
        await cleanup_tasks([read])
