"""The history of finished charges: kept in memory, saved to Home Assistant's storage, never truncated."""

from __future__ import annotations

import logging
from pathlib import Path
from threading import Lock
from time import time
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import STORAGE_DIR, Store
from homeassistant.util.file import write_utf8_file_atomic

from .const import DOMAIN
from .core.model import ChargeSession, LimitMemory
from .core.raedian import PlanWindow

_LOGGER = logging.getLogger(__name__)

STORAGE_VERSION = 1
STORAGE_KEY = f"{DOMAIN}.sessions"
TRANSACTION_ID_KEY = f"{DOMAIN}.transaction_id"
MAX_TRANSACTION_ID = 2**31 - 1
# An executor write can outlive cancellation of its session. All entry lifetimes use the same reservation lock.
_TRANSACTION_ID_LOCK = Lock()
# A save is delayed a few seconds so that a burst of events becomes one write; unload saves at once.
SAVE_DELAY_S = 5.0


class SessionStore:
    """Every finished charge of every charger, oldest first. A charge is about 300 bytes, so no limit is needed."""

    # Keep the storage handle; nothing is read until async_load.
    def __init__(self, hass: HomeAssistant) -> None:
        self._hass = hass
        self._store: Store[dict[str, Any]] = Store(hass, STORAGE_VERSION, STORAGE_KEY)
        self._sessions: list[ChargeSession] = []
        self._transaction_id_path = Path(hass.config.path(STORAGE_DIR, TRANSACTION_ID_KEY))
        self._history_high_water = 0

    # Read the saved history. A record that cannot be read is skipped with a warning, so one damaged entry does not
    # cost the owner the rest of the history.
    async def async_load(self) -> None:
        stored = await self._store.async_load()
        records = stored.get("sessions") if isinstance(stored, dict) else None
        loaded: list[ChargeSession] = []
        for record in records if isinstance(records, list) else []:
            try:
                loaded.append(ChargeSession.from_dict(record))
            except KeyError, TypeError, ValueError, OverflowError:
                _LOGGER.warning("Skipping an unreadable charge record in the session history")
        self._sessions = loaded
        self._history_high_water = max(
            (session.transaction_id for session in loaded if 0 < session.transaction_id <= MAX_TRANSACTION_ID),
            default=0,
        )

    # The charger must receive an ID only after its reservation survives an HA restart.
    async def async_next_transaction_id(self) -> int:
        return await self._hass.async_add_executor_job(self._reserve_transaction_id)

    # Write the high-water mark independently of the delayed, potentially large finished-session history.
    def _reserve_transaction_id(self) -> int:
        path = self._transaction_id_path
        with _TRANSACTION_ID_LOCK:
            try:
                last_id = int(path.read_text(encoding="ascii"))
            except FileNotFoundError:
                last_id = int(time()) % (MAX_TRANSACTION_ID + 1)
            if not 0 <= last_id <= MAX_TRANSACTION_ID:
                raise ValueError("Invalid saved transaction ID")
            transaction_id = max(last_id, self._history_high_water) + 1
            if transaction_id > MAX_TRANSACTION_ID:
                raise OverflowError("Transaction IDs are exhausted")
            path.parent.mkdir(parents=True, exist_ok=True)
            write_utf8_file_atomic(str(path), f"{transaction_id}\n")
            return transaction_id

    # Add a finished charge and schedule a save.
    def add(self, session: ChargeSession) -> None:
        self._sessions.append(session)
        self._store.async_delay_save(self._data, SAVE_DELAY_S)

    # Save now; used when the entry unloads so that nothing waits in the delay.
    async def async_flush(self) -> None:
        await self._store.async_save(self._data())

    # The history as the JSON that is written.
    def _data(self) -> dict[str, Any]:
        return {"sessions": [session.to_dict() for session in self._sessions]}

    # The charges of one charger, or of all, newest first; `limit` keeps only the newest ones.
    def newest_first(self, cp_id: str | None = None, limit: int | None = None) -> list[ChargeSession]:
        chosen = [s for s in reversed(self._sessions) if cp_id is None or s.cp_id == cp_id]
        return chosen if limit is None else chosen[:limit]

    # How many charges one charger, or all, have in the history.
    def count(self, cp_id: str | None = None) -> int:
        return sum(1 for s in self._sessions if cp_id is None or s.cp_id == cp_id)


LIMITS_STORAGE_KEY = f"{DOMAIN}.limits"


class LimitStore:
    """The limits of each charger (the hub's and the vendor's), so they show again after a restart of Home Assistant."""

    # Keep the storage handle; nothing is read until async_load.
    def __init__(self, hass: HomeAssistant) -> None:
        self._store: Store[dict[str, Any]] = Store(hass, STORAGE_VERSION, LIMITS_STORAGE_KEY)
        self._limits: dict[str, LimitMemory] = {}

    # Read the saved limits. A record that cannot be read is skipped with a warning.
    async def async_load(self) -> dict[str, LimitMemory]:
        stored = await self._store.async_load()
        records = stored.get("limits") if isinstance(stored, dict) else None
        for cp_id, record in records.items() if isinstance(records, dict) else []:
            try:
                self._limits[cp_id] = LimitMemory.from_dict(record)
            except KeyError, TypeError, ValueError, AttributeError, OverflowError:
                _LOGGER.warning("Skipping an unreadable limit record in storage")
        return dict(self._limits)

    # Keep the limits of one charger and schedule a save; an empty memory removes the charger.
    def update(self, cp_id: str, memory: LimitMemory) -> None:
        if memory.empty:
            self._limits.pop(cp_id, None)
        else:
            self._limits[cp_id] = memory
        self._store.async_delay_save(self._data, SAVE_DELAY_S)

    # Save now; used when the entry unloads.
    async def async_flush(self) -> None:
        await self._store.async_save(self._data())

    # The limits as the JSON that is written.
    def _data(self) -> dict[str, Any]:
        return {"limits": {cp_id: memory.to_dict() for cp_id, memory in self._limits.items()}}


PLAN_STORAGE_KEY = f"{DOMAIN}.plan"


class PlanStore:
    """The plan of each charger (windows per weekday), so the planning survives a restart of Home Assistant."""

    # Keep the storage handle; nothing is read until async_load.
    def __init__(self, hass: HomeAssistant) -> None:
        self._store: Store[dict[str, Any]] = Store(hass, STORAGE_VERSION, PLAN_STORAGE_KEY)
        self._plans: dict[str, list[PlanWindow]] = {}
        self._planning: dict[str, bool] = {}
        self._profile_sync_errors: dict[str, str] = {}

    # Read the saved plans. A record that cannot be read is skipped with a warning.
    async def async_load(self) -> None:
        stored = await self._store.async_load()
        records = stored.get("plans") if isinstance(stored, dict) else None
        for cp_id, windows in records.items() if isinstance(records, dict) else []:
            try:
                self._plans[cp_id] = [
                    PlanWindow(int(w[0]), int(w[1]), int(w[2]), bool(w[3]) if len(w) > 3 else True) for w in windows
                ]
            except KeyError, TypeError, ValueError, IndexError, OverflowError:
                _LOGGER.warning("Skipping an unreadable plan in storage")
        switches = stored.get("planning") if isinstance(stored, dict) else None
        self._planning = {cp: bool(on) for cp, on in switches.items()} if isinstance(switches, dict) else {}
        errors = stored.get("profile_sync_errors") if isinstance(stored, dict) else None
        self._profile_sync_errors = (
            {cp: error for cp, error in errors.items() if isinstance(cp, str) and isinstance(error, str)}
            if isinstance(errors, dict)
            else {}
        )

    # A genuine native planning flag is separate from the result of the hub's secondary profile write.
    def profile_sync_error(self, cp_id: str) -> str | None:
        return self._profile_sync_errors.get(cp_id)

    # Retain a failed synchronization through reload; only a matching success or retired plan clears it.
    def set_profile_sync_error(self, cp_id: str, error: str | None) -> None:
        if error is None:
            self._profile_sync_errors.pop(cp_id, None)
        else:
            self._profile_sync_errors[cp_id] = error
        self._store.async_delay_save(self._data, SAVE_DELAY_S)

    # The plan of a charger, or None when the hub has none yet.
    def get(self, cp_id: str) -> list[PlanWindow] | None:
        return list(self._plans[cp_id]) if cp_id in self._plans else None

    # Whether the plan applies to the charger: the app's switch for the planning. On until the app says otherwise.
    def planning_on(self, cp_id: str) -> bool:
        return self._planning.get(cp_id, True)

    # Keep the switch of the planning and schedule a save.
    def set_planning(self, cp_id: str, on: bool) -> None:
        self._planning[cp_id] = on
        self._store.async_delay_save(self._data, SAVE_DELAY_S)

    # Keep the plan of a charger and schedule a save.
    def set(self, cp_id: str, windows: list[PlanWindow]) -> None:
        self._plans[cp_id] = list(windows)
        self._store.async_delay_save(self._data, SAVE_DELAY_S)

    # Save now; used when the entry unloads.
    async def async_flush(self) -> None:
        await self._store.async_save(self._data())

    # The plans as the JSON that is written.
    def _data(self) -> dict[str, Any]:
        return {
            "plans": {
                cp: [[w.weekday, w.start_s, w.end_s, w.enabled] for w in windows] for cp, windows in self._plans.items()
            },
            "planning": dict(self._planning),
            "profile_sync_errors": dict(self._profile_sync_errors),
        }
