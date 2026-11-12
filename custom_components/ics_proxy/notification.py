"""Entry-owned outage notification survives failed coordinator setup without owning any charger command."""

from __future__ import annotations

import time
from collections.abc import Callable

from homeassistant.components import persistent_notification
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.const import EVENT_HOMEASSISTANT_STOP
from homeassistant.core import Event, HomeAssistant, callback
from homeassistant.helpers.event import async_call_later

NOTICE_DELAY = 60.0
NOTICES = "ics_proxy_notices"


class ProxyNotice:
    """One outage deadline belongs to the config entry, including its setup-retry intervals."""

    # The HA-stop subscription is independent of the failed setup's unload callbacks.
    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        self.hass, self.entry = hass, entry
        self.notification_id = "ics_proxy_" + entry.entry_id
        self.problem_since: float | None = None
        self.notified = False
        self.cleanup_pending = True
        self.generation = 0
        self.active = False
        self.clock: Callable[[], float] = time.monotonic
        self.cancel_timer: Callable[[], None] | None = None
        self.cancel_stop = hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, self.stopped)
        self.cancel_state = entry.async_on_state_change(self.state_changed)

    # Each replacement coordinator invalidates old refresh/expiry callbacks while retaining the same outage.
    def claim(self, clock: Callable[[], float]) -> int:
        self.cancel_deadline()
        self.generation += 1
        self.active, self.clock = True, clock
        self.cleanup_pending = True
        if self.problem_since is not None:
            self.arm()
        return self.generation

    # A configured entry in setup retry still needs its alert even if HA's next retry is minutes away.
    def eligible(self) -> bool:
        return (
            self.active
            and self.hass.config_entries.async_get_entry(self.entry.entry_id) is self.entry
            and self.entry.disabled_by is None
            and self.entry.state
            in (
                ConfigEntryState.LOADED,
                ConfigEntryState.SETUP_IN_PROGRESS,
                ConfigEntryState.SETUP_RETRY,
                ConfigEntryState.FAILED_UNLOAD,
            )
        )

    # Cancel only this monitor's deadline, never coordinator polling or a service-side operation.
    def cancel_deadline(self) -> None:
        if self.cancel_timer is not None:
            self.cancel_timer()
            self.cancel_timer = None

    # Retain debounce time and a visible fault across reload, but forbid late work from an unloaded coordinator.
    def suspend(self) -> None:
        self.cancel_deadline()
        self.active = False

    # Setup generations revoke stale reads; an unsuccessful unload can resume its still-current coordinator.
    @callback
    def state_changed(self) -> None:
        if self.entry.disabled_by is not None or self.entry.state not in (
            ConfigEntryState.LOADED,
            ConfigEntryState.SETUP_IN_PROGRESS,
            ConfigEntryState.SETUP_RETRY,
            ConfigEntryState.FAILED_UNLOAD,
        ):
            self.suspend()
            return
        if self.entry.state is ConfigEntryState.SETUP_IN_PROGRESS:
            self.cancel_deadline()
            self.generation += 1
        self.active = True
        self.arm()

    # The callback independently rechecks entry lifetime and never revives an old coordinator generation.
    @callback
    def deadline(self, generation: int) -> None:
        if generation != self.generation:
            return
        self.cancel_timer = None
        if not self.eligible():
            return
        self.arm()

    # A single timer reaches the original deadline; repeated failed refreshes cannot postpone it.
    def arm(self) -> None:
        if self.problem_since is None or self.notified or not self.eligible():
            return
        remaining = NOTICE_DELAY - (self.clock() - self.problem_since)
        if remaining <= 0:
            self.cancel_deadline()
            persistent_notification.async_create(
                self.hass,
                "The local iCS charger proxy needs attention. Check its status on the iCS Charger dashboard. "
                "The proxy retains its charging limits and any requested pause.",
                title="Charger proxy needs attention",
                notification_id=self.notification_id,
            )
            self.notified = True
        elif self.cancel_timer is None:
            generation = self.generation

            # HA must run the deadline on its event loop, not dispatch an unmarked lambda to a thread.
            @callback
            def fire(_now: object) -> None:
                self.deadline(generation)

            self.cancel_timer = async_call_later(self.hass, remaining, fire)

    # Only the current coordinator can establish recovery or extend this entry's outage evidence.
    def update(self, okay: bool, generation: int) -> None:
        if generation != self.generation or not self.active:
            return
        if okay:
            self.problem_since = None
            self.cancel_deadline()
            if self.notified or self.cleanup_pending:
                persistent_notification.async_dismiss(self.hass, self.notification_id)
                self.notified, self.cleanup_pending = False, False
            return
        if self.problem_since is None:
            self.problem_since = self.clock()
        self.arm()

    # Removal drops the bounded entry cache; HA shutdown retains an existing visible fault for next startup.
    def close(self, *, dismiss: bool) -> None:
        self.suspend()
        if self.cancel_stop is not None:
            self.cancel_stop()
            self.cancel_stop = None
        self.cancel_state()
        if dismiss:
            persistent_notification.async_dismiss(self.hass, self.notification_id)
        notices = self.hass.data.get(NOTICES, {})
        if notices.get(self.entry.entry_id) is self:
            notices.pop(self.entry.entry_id)

    # HA stopping cancels this independent timer before any later callback can create an orphan notice.
    @callback
    def stopped(self, _event: Event) -> None:
        self.cancel_stop = None
        self.close(dismiss=False)


# One cache entry, rather than one coordinator, owns the entire outage and its stable notification identifier.
def notice_for(hass: HomeAssistant, entry: ConfigEntry) -> ProxyNotice:
    notices = hass.data.setdefault(NOTICES, {})
    if entry.entry_id not in notices:
        notices[entry.entry_id] = ProxyNotice(hass, entry)
    notice: ProxyNotice = notices[entry.entry_id]
    return notice
