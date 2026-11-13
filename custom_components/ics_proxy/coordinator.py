"""Polling and user notifications for the existing proxy, without owning any charger connection."""

from __future__ import annotations

import logging
import time
from datetime import timedelta
from sys import float_info
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import ProxyClient, ProxyError
from .notification import notice_for

DOMAIN = "ics_proxy"
LOGGER = logging.getLogger(__name__)


# Bound before any float arithmetic: JSON integers can exceed the representable range without exceeding message size.
def usable_age(value: Any) -> bool:
    return type(value) in (int, float) and 0 <= value <= float_info.max


class ProxyCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """A deliberate pause is normal; prolonged faults produce one persistent HA notification until recovery."""

    # Setup and refresh only read status; neither HA restart nor unload sends any control command.
    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        # Reports can already be several seconds old; poll before their strict 15-second native expiry.
        super().__init__(hass, LOGGER, config_entry=entry, name=DOMAIN, update_interval=timedelta(seconds=5))
        self.client = ProxyClient(async_get_clientsession(hass), entry.data["port"], entry.data["token"])
        self.notice = notice_for(hass, entry)
        self.notice_generation = self.notice.claim(lambda: time.monotonic())
        self.notification_id = self.notice.notification_id
        self.received_at = time.monotonic()
        self.cancel_timer = None
        entry.async_on_unload(self.cancel_expiry)

    # Keep the existing coordinator inspection surface while its debounce belongs to the entry.
    @property
    def problem_since(self) -> float | None:
        return self.notice.problem_since

    # A controlled diagnostic clock adjustment changes the shared outage, never an independent new deadline.
    @problem_since.setter
    def problem_since(self, value: float | None) -> None:
        self.notice.problem_since = value

    # Entity/tests can inspect the same notification state through any current coordinator.
    @property
    def notified(self) -> bool:
        return self.notice.notified

    # Snapshot ages continue advancing while awaiting a poll or a failed network request.
    def age(self, key: str) -> float | None:
        value = (self.data or {}).get(key)
        if not usable_age(value):
            return None
        return value + max(0, time.monotonic() - self.received_at)

    # Health cannot outlive either the independent native reading or the service report which verified it.
    @property
    def fresh_healthy(self) -> bool:
        ages = [self.age("status_age_seconds"), self.age("native_age_seconds")]
        return (self.data or {}).get("healthy") is True and all(a is not None and 0 <= a <= 15 for a in ages)

    # Explicit local health can retain safe native control during a manufacturer-cloud outage.
    @property
    def fresh_local_healthy(self) -> bool:
        data = self.data or {}
        ages = [self.age("status_age_seconds"), self.age("native_age_seconds")]
        return (
            data.get("running") is True
            and data.get("local_healthy", data.get("healthy")) is True
            and all(a is not None and 0 <= a <= 15 for a in ages)
        )

    # Retire a previous snapshot expiry when new data arrives or HA unloads the entry.
    @callback
    def cancel_expiry(self) -> None:
        if self.cancel_timer is not None:
            self.cancel_timer()
            self.cancel_timer = None

    # Entity invalidation runs on time without performing another request or issuing a charger command.
    @callback
    def expire(self, now: Any) -> None:
        self.cancel_timer = None
        self.update_notification(self.fresh_healthy)
        self.async_update_listeners()

    # Short reading/cloud blips settle quietly; sustained trouble is visible without repeatedly creating alerts.
    @callback
    def update_notification(self, okay: bool) -> None:
        self.notice.update(okay, self.notice_generation)

    # API errors are classified before HA logs them; tokens and server payloads never appear in UpdateFailed.
    async def _async_update_data(self) -> dict[str, Any]:
        started = time.monotonic()
        try:
            value = dict(await self.client.request("status"))
        except ProxyError:
            self.update_notification(False)
            raise UpdateFailed("The local charger proxy is unavailable") from None
        if not self.notice.active or self.notice.generation != self.notice_generation:
            raise UpdateFailed("The charger proxy status belongs to an inactive entry")
        self.received_at = time.monotonic()
        transit = max(0, self.received_at - started)
        for key in ("status_age_seconds", "native_age_seconds"):
            age = value.get(key)
            if usable_age(age):
                value[key] = age + transit
        self.cancel_expiry()
        ages = [value.get(key) for key in ("status_age_seconds", "native_age_seconds")]
        remaining = [15 - age for age in ages if usable_age(age)]
        if remaining:
            self.cancel_timer = async_call_later(self.hass, max(0.1, min(remaining)) + 0.05, self.expire)
        self.update_notification(
            value.get("healthy") is True
            and len(remaining) == 2
            and all(remaining_age >= 0 for remaining_age in remaining)
        )
        return value
