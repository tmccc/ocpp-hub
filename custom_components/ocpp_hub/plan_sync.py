"""The plan of the hub: windows per weekday that the app shows, Home Assistant sets, and the charger obeys.

The plan is the master. When the app writes a planning, or Home Assistant sets one, it is turned into a recurring OCPP
profile with a limit inside the windows and a limit outside them, under the owner's ceiling, and written to the charger;
the plan is kept only when that worked. The app is answered from the plan when it asks for the planning.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Mapping

from homeassistant.core import HomeAssistant
from homeassistant.helpers.dispatcher import async_dispatcher_send

from .const import CONF_SCHEDULE_INSIDE, CONF_SCHEDULE_OUTSIDE, plan_signal
from .core.central import CentralSystem
from .core.control import ControlError
from .core.model import ChargerSnapshot
from .core.plan_bridge import windows_to_blocks
from .core.raedian import PlanWindow
from .core.schedule_plan import SchedulePlanError, plan_week
from .schedule_helper import anchor_for
from .session_store import PlanStore

_LOGGER = logging.getLogger(__name__)


# The plan as (weekday, start, end) windows the charger obeys, or None when nobody has told the hub one. The plan of
# the hub wins; until there is one, it is what the app last read from the charger. Switched-off windows are left out.
def shown_windows(plans: PlanSync, snapshot: ChargerSnapshot) -> list[tuple[int, int, int]] | None:
    held = plans.plan(snapshot.cp_id)
    if held is not None:
        return [(w.weekday, w.start_s, w.end_s) for w in held if w.enabled]
    seen = snapshot.app_plan
    return None if seen is None else list(seen)


class PlanSync:
    """Holds the plan of each charger and writes it to the charger. Implements the mirror's PlanHandler."""

    # Keep the store, the options that say the limits inside and outside the windows, and what is needed to signal.
    def __init__(self, hass: HomeAssistant, entry_id: str, store: PlanStore, options: Mapping[str, object]) -> None:
        self._hass = hass
        self._entry_id = entry_id
        self._store = store
        self._options = options
        self._central: CentralSystem | None = None
        self._seen: dict[str, int] = {}
        self._locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    # Give the plan the central system it writes through; it is made after the mirror that needs the plan.
    def bind(self, central: CentralSystem) -> None:
        self._central = central

    # The plan of a charger, or None when the hub has none yet.
    def plan(self, cp_id: str) -> list[PlanWindow] | None:
        return self._store.get(cp_id)

    # Take over the plan the charger reported on the first question of the app, without writing anything to the charger:
    # it is what the charger already has.
    def imported(self, cp_id: str, windows: list[PlanWindow]) -> None:
        self._keep(cp_id, windows)

    # The app wrote a planning: write it to the charger as a profile, and keep it as the plan when that worked.
    async def app_wrote(self, cp_id: str, windows: list[PlanWindow]) -> bool:
        try:
            await self.set_plan(cp_id, windows)
        except (ControlError, SchedulePlanError) as error:
            _LOGGER.warning("[plan] The planning of the app for %s could not be applied: %s", cp_id, error)
            return False
        return True

    # Whether the plan applies to the charger now: the app's switch for the planning (on until the app says otherwise).
    def planning_on(self, cp_id: str) -> bool:
        return self._store.planning_on(cp_id)

    # Expose a failed secondary profile separately from the charger's genuinely accepted native switch.
    def profile_sync_error(self, cp_id: str) -> str | None:
        return self._store.profile_sync_error(cp_id)

    # The app switched its planning on or off. The plan is kept either way; off clears the hub's profile so the charger
    # is no longer held outside the windows, on writes the profile of the plan again.
    async def planning_switched(self, cp_id: str, on: bool) -> None:
        self._store.set_planning(cp_id, on)
        async with self._locks[cp_id]:
            if self.planning_on(cp_id) != on:
                return
            windows = self._store.get(cp_id)
            error_code = None
            if windows:
                try:
                    await self._apply(cp_id, windows, on)
                except (ControlError, SchedulePlanError) as error:
                    error_code = error.code if isinstance(error, ControlError) else "invalid_schedule"
                    _LOGGER.warning(
                        "[plan] Switching the planning %s for %s failed: %s", "on" if on else "off", cp_id, error
                    )
            if self.planning_on(cp_id) == on and self._store.get(cp_id) == windows:
                self._store.set_profile_sync_error(cp_id, error_code)
        async_dispatcher_send(self._hass, plan_signal(self._entry_id, cp_id))

    # Set the plan: turn the windows into one recurring profile and write it, then keep the plan. An empty plan clears
    # the hub's profile instead, because a profile of nothing but the outside limit would hold the charger at that limit
    # all week. Raises ControlError or SchedulePlanError when it cannot be written, and keeps the old plan then.
    async def set_plan(self, cp_id: str, windows: list[PlanWindow]) -> None:
        async with self._locks[cp_id]:
            on = self.planning_on(cp_id)
            await self._apply(cp_id, windows, on)
            if self.planning_on(cp_id) == on:
                self._store.set_profile_sync_error(cp_id, None)
            self._keep(cp_id, windows)

    # Write the plan to the charger as a profile, or clear the profile when the planning is off, while the snapshot
    # guard knows that the write is the plan's own.
    async def _apply(self, cp_id: str, windows: list[PlanWindow], on: bool) -> None:
        control = self._central.control(cp_id) if self._central is not None else None
        if control is None:
            raise ControlError("not_connected", "the charger is not connected")
        if on and any(window.enabled for window in windows):
            inside = self._options.get(CONF_SCHEDULE_INSIDE)
            outside = self._options.get(CONF_SCHEDULE_OUTSIDE)
            plan = plan_week(
                windows_to_blocks(windows),
                float(control.policy.max_current_a if inside is None else inside),  # type: ignore[arg-type]
                float(0 if outside is None else outside),  # type: ignore[arg-type]
            )
            await control.set_schedule(anchor_for(plan), plan.periods, plan.kind, from_plan=True)
        else:
            await control.clear_current_limit(from_plan=True)

    # Save the plans now; used when the entry unloads.
    async def async_flush(self) -> None:
        await self._store.async_flush()

    # Called with every snapshot. The plan rides on the hub's own profile, which a single limit, a schedule or a clear
    # also use; when such a write happens that the plan did not make, the profile is no longer the plan, so the plan is
    # emptied and the app is told so, instead of both going on to claim what the charger no longer follows.
    def snapshot_changed(self, snapshot: ChargerSnapshot) -> None:
        cp_id = snapshot.cp_id
        if not snapshot.connected:
            return
        seen = self._seen.get(cp_id, 0)
        self._seen[cp_id] = snapshot.external_profile_writes
        if snapshot.external_profile_writes > seen and self._store.get(cp_id):
            _LOGGER.info("[plan] The profile of %s was replaced by another write; the plan is now empty", cp_id)
            self._keep(cp_id, [])

    # Keep a plan and tell the entities that show it.
    def _keep(self, cp_id: str, windows: list[PlanWindow]) -> None:
        if not windows:
            self._store.set_profile_sync_error(cp_id, None)
        self._store.set(cp_id, windows)
        async_dispatcher_send(self._hass, plan_signal(self._entry_id, cp_id))
