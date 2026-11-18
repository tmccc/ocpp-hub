"""OCPP Hub: a local OCPP 1.6 central system for Home Assistant."""

from __future__ import annotations

import logging
from datetime import time
from pathlib import Path

import voluptuous as vol
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, ServiceCall, SupportsResponse
from homeassistant.exceptions import ConfigEntryNotReady, HomeAssistantError, ServiceValidationError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.typing import ConfigType
from homeassistant.util import dt as dt_util

from .const import (
    BIND_HOST,
    BUILD_FILE,
    CONF_MAX_CURRENT,
    CONF_PORT,
    CONF_VENDOR_COMMANDS,
    CONF_VENDOR_PASSWORD,
    CONF_VENDOR_URL,
    CONF_VENDOR_USER,
    DEFAULT_MAX_CURRENT,
    DOMAIN,
    PACKAGE_LOGGER,
    RETIRED_ENTITY_KEYS,
    SERVICE_GET_SESSIONS,
    SERVICE_SET_PLAN,
    SERVICE_SET_SCHEDULE,
)
from .control import run_write
from .core.central import CentralSystem
from .core.control import CHARGING_MIN_A, ControlError, ControlPolicy
from .core.mirror import VendorMirror
from .core.mirror_model import COMMANDS_REJECT, MirrorSettings, MirrorSettingsError
from .core.raedian import PlanWindow
from .core.schedule_plan import SchedulePlanError
from .plan_sync import PlanSync
from .runtime import HubListener, HubRuntime, OcppHubConfigEntry
from .schedule_helper import WEEKDAYS
from .session_store import LimitStore, PlanStore, SessionStore

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [
    Platform.BINARY_SENSOR,
    Platform.BUTTON,
    Platform.CALENDAR,
    Platform.NUMBER,
    Platform.SENSOR,
    Platform.SWITCH,
]


# The commit the running code comes from, as written by the deploy tool; "unknown" for a copy that was not deployed.
# A log line has to say which code wrote it, otherwise an old build and the current one look the same.
def _read_build_stamp() -> str:
    try:
        return (Path(__file__).parent / BUILD_FILE).read_text(encoding="utf-8").strip() or "unknown"
    except OSError:
        return "unknown"


GET_SESSIONS_SCHEMA = vol.Schema(
    {vol.Optional("charger_id"): str, vol.Optional("limit"): vol.All(int, vol.Range(min=1))}
)


# A limit from the service call as a float; text, nan or a number too big to be one is refused with a normal error.
def _limit_value(value: object) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError) as error:
        raise vol.Invalid("the limit must be a number") from error


SET_PLAN_SCHEMA = vol.Schema(
    {
        vol.Optional("charger_id"): str,
        vol.Required("windows"): vol.All(
            [
                {
                    vol.Required("day"): vol.In(WEEKDAYS),
                    vol.Required("from"): cv.time,
                    vol.Required("to"): cv.time,
                }
            ],
            vol.Length(max=21),
        ),
    }
)

SET_SCHEDULE_SCHEMA = vol.Schema(
    {
        vol.Optional("charger_id"): str,
        vol.Required("periods"): vol.All(
            [{vol.Required("time"): cv.time, vol.Required("limit"): _limit_value}], vol.Length(min=1, max=24)
        ),
    }
)


# The seconds after midnight of a time of the day.
def _seconds_of_day(moment: time) -> int:
    return moment.hour * 3600 + moment.minute * 60 + moment.second


# Register the service that returns the charge history. It lives at the integration level, so it exists as soon as
# Home Assistant has loaded the integration, and it answers with data instead of changing anything.
async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    # Return the stored charges, newest first, for one charger or all of them.
    async def get_sessions(call: ServiceCall) -> dict[str, object]:
        entries = hass.config_entries.async_loaded_entries(DOMAIN)
        if not entries:
            raise ServiceValidationError(translation_domain=DOMAIN, translation_key="no_hub_loaded")
        sessions = entries[0].runtime_data.sessions.newest_first(call.data.get("charger_id"), call.data.get("limit"))
        return {"sessions": [session.to_dict() for session in sessions]}

    # Install a daily schedule on a charger: from each time of the day on, that current. The day starts at midnight
    # local time. The charger named, or the only one that has booted, is written to under the owner's ceiling.
    async def set_schedule(call: ServiceCall) -> None:
        entries = hass.config_entries.async_loaded_entries(DOMAIN)
        if not entries:
            raise ServiceValidationError(translation_domain=DOMAIN, translation_key="no_hub_loaded")
        runtime = entries[0].runtime_data
        cp_id = call.data.get("charger_id") or (
            runtime.listener.announced[0] if len(runtime.listener.announced) == 1 else None
        )
        if cp_id is None:
            raise ServiceValidationError(translation_domain=DOMAIN, translation_key="charger_not_chosen")
        periods = [(_seconds_of_day(p["time"]), p["limit"]) for p in call.data["periods"]]
        anchor = dt_util.start_of_local_day()
        await run_write(runtime, cp_id, lambda control: control.set_daily_schedule(anchor, periods))

    hass.services.async_register(
        DOMAIN, SERVICE_GET_SESSIONS, get_sessions, schema=GET_SESSIONS_SCHEMA, supports_response=SupportsResponse.ONLY
    )

    # Set the plan: windows per weekday, written to the charger as a recurring profile and shown to the app. An empty
    # list clears the hub's profile. A window that ends at or before its start runs overnight.
    async def set_plan(call: ServiceCall) -> None:
        entries = hass.config_entries.async_loaded_entries(DOMAIN)
        if not entries:
            raise ServiceValidationError(translation_domain=DOMAIN, translation_key="no_hub_loaded")
        runtime = entries[0].runtime_data
        announced = runtime.listener.announced
        cp_id = call.data.get("charger_id") or (announced[0] if len(announced) == 1 else None)
        if cp_id is None:
            raise ServiceValidationError(translation_domain=DOMAIN, translation_key="charger_not_chosen")
        windows = [
            PlanWindow(WEEKDAYS.index(w["day"]) + 1, _seconds_of_day(w["from"]), _seconds_of_day(w["to"]))
            for w in call.data["windows"]
        ]
        try:
            await runtime.plans.set_plan(cp_id, windows)
        except (ControlError, SchedulePlanError) as error:
            code = error.code if isinstance(error, ControlError) else "invalid_schedule"
            raise HomeAssistantError(
                translation_domain=DOMAIN,
                translation_key=f"control_{code}",
                translation_placeholders={"detail": str(error)},
            ) from error

    hass.services.async_register(DOMAIN, SERVICE_SET_SCHEDULE, set_schedule, schema=SET_SCHEDULE_SCHEMA)
    hass.services.async_register(DOMAIN, SERVICE_SET_PLAN, set_plan, schema=SET_PLAN_SCHEMA)
    return True


# Remove the registry entries of entities that no longer exist. Home Assistant would otherwise keep them for ever as
# "unavailable" entities that nothing updates.
def _retire_entities(hass: HomeAssistant, entry: OcppHubConfigEntry) -> None:
    registry = er.async_get(hass)
    for entity in er.async_entries_for_config_entry(registry, entry.entry_id):
        if entity.unique_id.rsplit(":", 1)[-1] in RETIRED_ENTITY_KEYS:
            registry.async_remove(entity.entity_id)


# The ceiling for writes from the entry options. A value that cannot be right falls back to the default instead of
# widening what is allowed; the error says so.
def _control_policy(entry: OcppHubConfigEntry) -> ControlPolicy:
    wanted = entry.options.get(CONF_MAX_CURRENT, DEFAULT_MAX_CURRENT)
    try:
        return ControlPolicy(max_current_a=float(wanted))
    except TypeError, ValueError, OverflowError:
        # The lowest ceiling that can work, not the default: an unreadable ceiling must never be wider than meant.
        _LOGGER.error("The current ceiling %r is not valid; using the minimum of %s A", wanted, CHARGING_MIN_A)
        return ControlPolicy(max_current_a=CHARGING_MIN_A)


# The mirror settings from the entry options, or None when no vendor URL is set.
# Options are validated by the options flow, so an invalid combination here means the entry was edited by hand;
# the error says what is wrong and the hub runs without a mirror instead of not at all.
def _mirror_settings(entry: OcppHubConfigEntry) -> MirrorSettings | None:
    url = entry.options.get(CONF_VENDOR_URL)
    if not url:
        return None
    try:
        return MirrorSettings(
            url=url,
            user=entry.options.get(CONF_VENDOR_USER) or None,
            password=entry.options.get(CONF_VENDOR_PASSWORD) or None,
            commands=entry.options.get(CONF_VENDOR_COMMANDS) or COMMANDS_REJECT,
        )
    except MirrorSettingsError as error:
        _LOGGER.error("The vendor mirror is off, its options are invalid (%s): %s", error.code, error)
        return None


# Set up one config entry: start the OCPP server and, when a vendor URL is set, the mirror in front of it.
# A port that is taken is not fatal: the other OCPP integration may still be shutting down, so setup is retried.
async def async_setup_entry(hass: HomeAssistant, entry: OcppHubConfigEntry) -> bool:
    logging.getLogger(PACKAGE_LOGGER).setLevel(logging.INFO)
    build = await hass.async_add_executor_job(_read_build_stamp)
    settings = _mirror_settings(entry)
    _retire_entities(hass, entry)
    sessions = SessionStore(hass)
    await sessions.async_load()
    limits = LimitStore(hass)
    stored_limits = await limits.async_load()
    policy = _control_policy(entry)
    plan_store = PlanStore(hass)
    await plan_store.async_load()
    plans = PlanSync(hass, entry.entry_id, plan_store, entry.options)
    mirror = VendorMirror(settings, policy=policy, plan_handler=plans) if settings is not None else None
    if mirror is not None:
        await mirror.start()
    central = CentralSystem([], policy=policy, allocate_transaction_id=sessions.async_next_transaction_id)
    central.restore_limits(stored_limits)
    plans.bind(central)
    listener = HubListener(
        hass,
        entry.entry_id,
        build,
        sessions,
        limits,
        plans.snapshot_changed,
        mirror_enabled=settings is not None,
    )
    central.add_listeners([listener, *([mirror] if mirror is not None else [])])
    port = entry.data[CONF_PORT]
    try:
        await central.start(BIND_HOST, port)
    except OSError as error:
        if mirror is not None:
            await mirror.stop()
        raise ConfigEntryNotReady(f"cannot listen on port {port}: {error}") from error
    try:
        entry.runtime_data = HubRuntime(central, listener, mirror, sessions, limits, plans)
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    except BaseException:
        # Home Assistant never unloads an entry whose setup failed or was cancelled, so nothing else would close the
        # server and the mirror, and the port would stay taken until a restart. Clean up, then let the error go on.
        await _stop_runtime(central, mirror)
        raise
    entry.async_on_unload(entry.add_update_listener(_async_reload_entry))
    _LOGGER.info(
        "[build %s] OCPP Hub listening on port %s, vendor mirror %s", build, central.port, "on" if mirror else "off"
    )
    return True


# Reload the entry when its options change, so a new vendor URL takes effect without a restart.
# Reloading drops the charger connection; it redials by itself.
async def _async_reload_entry(hass: HomeAssistant, entry: OcppHubConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)


# Close the server, then the mirror, and close the mirror even when closing the server raised.
# No link to the vendor may outlive the entry. Frames still waiting for the vendor are lost; the README lists that.
async def _stop_runtime(central: CentralSystem, mirror: VendorMirror | None) -> None:
    try:
        await central.stop()
    finally:
        if mirror is not None:
            await mirror.stop()


# Unload one config entry: remove the entities, close the chargers, then the mirror.
async def async_unload_entry(hass: HomeAssistant, entry: OcppHubConfigEntry) -> bool:
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        try:
            await _stop_runtime(entry.runtime_data.central, entry.runtime_data.mirror)
        finally:
            await entry.runtime_data.sessions.async_flush()
            await entry.runtime_data.limits.async_flush()
            await entry.runtime_data.plans.async_flush()
    return unloaded
