"""Sensors: what the charger reports, and how the link to the vendor backend is doing."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import (
    EntityCategory,
    UnitOfElectricCurrent,
    UnitOfElectricPotential,
    UnitOfEnergy,
    UnitOfPower,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.typing import StateType

from .const import MIRROR_POLL_INTERVAL, plan_signal, session_signal
from .core import readings
from .core.mirror_model import MirrorStatus
from .core.model import ChargerSnapshot, ChargeSession
from .entity import OcppHubEntity, async_add_charger_entities
from .plan_sync import shown_windows
from .runtime import HubRuntime, OcppHubConfigEntry

# The mirror pushes no events, so the entities that show it are polled; a status ten seconds old is fresh enough.
# Only the mirror entities poll, the charger sensors are pushed.
SCAN_INTERVAL = MIRROR_POLL_INTERVAL

PHASES = ("L1", "L2", "L3")
MIRROR_STATES = ["idle", "connecting", "connected"]


@dataclass(frozen=True, kw_only=True)
class ChargerSensorDescription(SensorEntityDescription):
    """A sensor whose value is read from the charger snapshot."""

    value_fn: Callable[[ChargerSnapshot], StateType]


# The charger's own values. Voltage is off by default: most installations only need the currents.
CHARGER_SENSORS: tuple[ChargerSensorDescription, ...] = (
    ChargerSensorDescription(
        key="status",
        translation_key="status",
        device_class=SensorDeviceClass.ENUM,
        options=readings.CONNECTOR_STATUS_OPTIONS,
        value_fn=readings.connector_status,
    ),
    ChargerSensorDescription(
        key="error_code",
        translation_key="error_code",
        device_class=SensorDeviceClass.ENUM,
        options=readings.ERROR_CODE_OPTIONS,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=readings.error_code,
    ),
    ChargerSensorDescription(
        key="power",
        translation_key="power",
        device_class=SensorDeviceClass.POWER,
        native_unit_of_measurement=UnitOfPower.WATT,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=readings.power_w,
    ),
    ChargerSensorDescription(
        key="energy",
        translation_key="energy",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL_INCREASING,
        suggested_display_precision=2,
        value_fn=readings.energy_kwh,
    ),
    *(
        ChargerSensorDescription(
            key=f"current_{phase.lower()}",
            translation_key="current_phase",
            translation_placeholders={"phase": phase},
            device_class=SensorDeviceClass.CURRENT,
            native_unit_of_measurement=UnitOfElectricCurrent.AMPERE,
            state_class=SensorStateClass.MEASUREMENT,
            value_fn=lambda snapshot, phase=phase: readings.phase_current_a(snapshot, phase),
        )
        for phase in PHASES
    ),
    *(
        ChargerSensorDescription(
            key=f"voltage_{phase.lower()}",
            translation_key="voltage_phase",
            translation_placeholders={"phase": phase},
            device_class=SensorDeviceClass.VOLTAGE,
            native_unit_of_measurement=UnitOfElectricPotential.VOLT,
            state_class=SensorStateClass.MEASUREMENT,
            entity_registry_enabled_default=False,
            value_fn=lambda snapshot, phase=phase: readings.phase_voltage_v(snapshot, phase),
        )
        for phase in PHASES
    ),
    ChargerSensorDescription(
        key="current_offered",
        translation_key="current_offered",
        device_class=SensorDeviceClass.CURRENT,
        native_unit_of_measurement=UnitOfElectricCurrent.AMPERE,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=readings.current_offered_a,
    ),
    ChargerSensorDescription(
        key="model",
        translation_key="model",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda snapshot: snapshot.info.model,
    ),
    ChargerSensorDescription(
        key="firmware",
        translation_key="firmware",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda snapshot: snapshot.info.firmware,
    ),
)


# Create the sensors of every charger that boots, plus the mirror sensors when a vendor mirror is set up.
async def async_setup_entry(
    hass: HomeAssistant, entry: OcppHubConfigEntry, async_add_entities: AddConfigEntryEntitiesCallback
) -> None:
    runtime = entry.runtime_data

    # The sensors of one charger.
    def build(cp_id: str) -> list[OcppHubEntity]:
        entities: list[OcppHubEntity] = [
            ChargerSensor(runtime, entry.entry_id, cp_id, description) for description in CHARGER_SENSORS
        ]
        entities.append(LimitInEffectSensor(runtime, entry.entry_id, cp_id))
        entities.append(AppPlanSensor(runtime, entry.entry_id, cp_id))
        entities.append(CurrentSessionSensor(runtime, entry.entry_id, cp_id))
        entities.append(LastSessionSensor(runtime, entry.entry_id, cp_id))
        if runtime.mirror is not None:
            entities += [
                MirrorStatusSensor(runtime, entry.entry_id, cp_id),
                MirrorQueueSensor(runtime, entry.entry_id, cp_id),
            ]
        return entities

    async_add_charger_entities(hass, entry, async_add_entities, build)


class ChargerSensor(OcppHubEntity, SensorEntity):
    """One value of the charger. It is unavailable while the charger is not connected."""

    entity_description: ChargerSensorDescription

    # Build the sensor for one charger from its description.
    def __init__(self, runtime: HubRuntime, entry_id: str, cp_id: str, description: ChargerSensorDescription) -> None:
        super().__init__(runtime, entry_id, cp_id, "sensor", description.key)
        self.entity_description = description

    @property
    def native_value(self) -> StateType:
        """The value from the latest snapshot."""
        return self.entity_description.value_fn(self._snapshot)

    @property
    def available(self) -> bool:
        """A charger that is gone has no current value."""
        return self._snapshot.connected


# How many finished charges the last-session sensor lists in its attributes. The full history is in the store and the
# get_sessions service; an attribute must stay small, because Home Assistant limits its size.
RECENT_SESSIONS = 20


# A whole number, or None.
def _rounded(value: float | None) -> int | None:
    return round(value) if value is not None else None


# The numbers of a finished charge as attributes: the same keys for the latest charge and for the list of recent ones.
def _session_attributes(session: ChargeSession) -> dict[str, Any]:
    return {
        "started_at": session.started_at.isoformat(),
        "ended_at": session.ended_at.isoformat(),
        "energy_kwh": round(session.energy_wh / 1000, 3) if session.energy_wh is not None else None,
        "duration_s": int(session.duration_s),
        "max_power_w": session.max_power_w,
        "average_power_w": _rounded(session.average_power_w),
        "max_current_a": dict(session.max_current_a),
        "offered_a": session.offered_a,
        "limit_a": session.limit_a,
        "id_tag": session.id_tag,
        "stop_reason": session.stop_reason,
    }


# The schedule the charger reported as a list of absolute times and limits. A time that cannot be represented gives an
# empty list instead of an error, because a sensor that raises stops updating; the reader refuses such answers already.
def _schedule_entries(snapshot: ChargerSnapshot) -> list[dict[str, Any]]:
    if snapshot.schedule_start is None:
        return []
    try:
        return [
            {"from": (snapshot.schedule_start + timedelta(seconds=offset)).isoformat(), "limit_a": limit}
            for offset, limit in snapshot.schedule_periods
        ]
    except OverflowError:
        return []


# A time of day from seconds after midnight, as HH:MM (24:00 for the end of the day).
def _clock_text(seconds: int) -> str:
    return f"{seconds // 3600:02d}:{seconds % 3600 // 60:02d}"


class AppPlanSensor(OcppHubEntity, SensorEntity):
    """The charging plan: how many windows, and the windows as attributes. The app shows and changes the same plan.

    It is the plan of the hub when there is one (set here or by the app); until then it is what the app last read from
    the charger, and unknown if neither has happened.
    """

    _attr_translation_key = "app_plan"
    _unrecorded_attributes = frozenset({"windows"})
    _DAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")

    # Build the sensor for one charger.
    def __init__(self, runtime: HubRuntime, entry_id: str, cp_id: str) -> None:
        super().__init__(runtime, entry_id, cp_id, "sensor", "app_plan")

    # Also follow the plan: a plan set by the app or by a service changes this sensor, not only the snapshot.
    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_on_remove(
            async_dispatcher_connect(self.hass, plan_signal(self._entry_id, self._cp_id), self._plan_changed)
        )

    # Show the new plan.
    @callback
    def _plan_changed(self) -> None:
        self.async_write_ha_state()

    # The plan as (weekday, start, end) windows, or None when nobody has told the hub one.
    def _windows(self) -> list[tuple[int, int, int]] | None:
        return shown_windows(self._runtime.plans, self._snapshot)

    @property
    def native_value(self) -> int | None:
        """The number of windows, or None while the plan is not known."""
        windows = self._windows()
        return None if windows is None else len(windows)

    @property
    def available(self) -> bool:
        """The plan stays readable while the charger is away: it is the hub's."""
        return True

    @property
    def extra_state_attributes(self) -> Mapping[str, Any]:
        """The windows with the day, the start and the end; a window that ends before it starts runs overnight."""
        return {
            "planning": "on" if self._runtime.plans.planning_on(self._cp_id) else "off",
            "profile_sync_pending": self._runtime.plans.profile_sync_error(self._cp_id) is not None,
            "profile_sync_error": self._runtime.plans.profile_sync_error(self._cp_id),
            "windows": [
                {"day": self._DAYS[day - 1], "from": _clock_text(start), "to": _clock_text(end)}
                for day, start, end in self._windows() or ()
            ],
        }


class LimitInEffectSensor(OcppHubEntity, SensorEntity):
    """The current limit that applies to the charger now, whoever set it, with the limits it comes from as attributes.

    The charger applies the lower of the hub's limit and the vendor's. When the charger can say what is in effect (the
    "charger" source) that is shown; otherwise the lowest of the limits the hub knows (the "calculated" source).
    """

    _attr_translation_key = "limit_in_effect"
    # The schedule changes with every read and can be long; it is shown but not kept in the database.
    _unrecorded_attributes = frozenset({"schedule"})
    _attr_device_class = SensorDeviceClass.CURRENT
    _attr_native_unit_of_measurement = UnitOfElectricCurrent.AMPERE
    _attr_state_class = SensorStateClass.MEASUREMENT

    # Build the sensor for one charger.
    def __init__(self, runtime: HubRuntime, entry_id: str, cp_id: str) -> None:
        super().__init__(runtime, entry_id, cp_id, "sensor", "limit_in_effect")

    @property
    def native_value(self) -> float | None:
        """The limit in amperes, or None when none is known."""
        return readings.effective_limit_a(self._snapshot)

    @property
    def extra_state_attributes(self) -> Mapping[str, Any]:
        """Where the number comes from: the hub's limit, the vendor's limit and what the charger itself reported."""
        snapshot = self._snapshot
        return {
            "hub_limit_a": snapshot.charge_limit_a,
            "vendor_limit_a": snapshot.vendor_limit_a,
            "charger_reported_a": snapshot.composite_limit_a,
            "source": "charger" if snapshot.composite_limit_a is not None else "calculated",
            "restored_unconfirmed": snapshot.limits_restored,
            "schedule": _schedule_entries(snapshot),
        }


class CurrentSessionSensor(OcppHubEntity, SensorEntity):
    """The charge that is running: its energy so far, with everything else about it as attributes.

    There is one sensor and no loose ones for start, duration or peak: all session information belongs to the session.
    """

    _attr_translation_key = "current_session"
    _attr_device_class = SensorDeviceClass.ENERGY
    _attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
    _attr_state_class = SensorStateClass.TOTAL_INCREASING
    _attr_suggested_display_precision = 2

    # Build the sensor for one charger.
    def __init__(self, runtime: HubRuntime, entry_id: str, cp_id: str) -> None:
        super().__init__(runtime, entry_id, cp_id, "sensor", "current_session")

    @property
    def native_value(self) -> float | None:
        """The energy delivered since the charge started, or None while no charge runs."""
        return readings.session_energy_kwh(self._snapshot)

    @property
    def available(self) -> bool:
        """A charger that is gone has no running charge."""
        return self._snapshot.connected

    @property
    def extra_state_attributes(self) -> Mapping[str, Any]:
        """The running charge in plain numbers; empty while none runs."""
        charge = self._snapshot.connectors.get(readings.PRIMARY_CONNECTOR)
        if charge is None or charge.transaction_id is None:
            return {}
        return {
            "transaction_id": charge.transaction_id,
            "started_at": charge.started_at.isoformat() if charge.started_at else None,
            "duration_s": readings.session_duration_s(self._snapshot),
            "id_tag": charge.id_tag,
            "max_power_w": charge.stats.max_power_w,
            "average_power_w": round(charge.stats.average_power_w)
            if charge.stats.average_power_w is not None
            else None,
            "max_current_a": dict(charge.stats.max_current_a),
            "offered_a": charge.stats.offered_a,
            "limit_a": self._snapshot.charge_limit_a,
        }


class LastSessionSensor(OcppHubEntity, SensorEntity):
    """The most recent finished charge: when it ended, with the numbers as attributes and the newest ones listed."""

    _attr_translation_key = "last_session"
    _attr_device_class = SensorDeviceClass.TIMESTAMP
    # The list changes with every charge and is large; keeping it out of the recorder keeps the database small.
    _unrecorded_attributes = frozenset({"recent_sessions"})

    # Build the sensor for one charger.
    def __init__(self, runtime: HubRuntime, entry_id: str, cp_id: str) -> None:
        super().__init__(runtime, entry_id, cp_id, "sensor", "last_session")

    # Also follow the history: a charge that finishes changes this sensor, not only the snapshot.
    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_on_remove(
            async_dispatcher_connect(self.hass, session_signal(self._entry_id, self._cp_id), self._history_changed)
        )

    # Show the new history.
    @callback
    def _history_changed(self) -> None:
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        """The history stays readable while the charger is away."""
        return True

    @property
    def native_value(self) -> datetime | None:
        """When the latest charge ended, or None while there is none."""
        latest = self._runtime.sessions.newest_first(self._cp_id, 1)
        return latest[0].ended_at if latest else None

    @property
    def extra_state_attributes(self) -> Mapping[str, Any]:
        """The latest charge in plain numbers, the count, and the newest charges for a dashboard table."""
        recent = self._runtime.sessions.newest_first(self._cp_id, RECENT_SESSIONS)
        attributes: dict[str, Any] = {"session_count": self._runtime.sessions.count(self._cp_id)}
        if recent:
            attributes |= _session_attributes(recent[0])
        attributes["recent_sessions"] = [_session_attributes(s) for s in recent]
        return attributes


class MirrorEntity(OcppHubEntity):
    """Base of the mirror sensors: they read the live status of the charger's mirror link."""

    _attr_should_poll = True
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    # The status of this charger's link, read live.
    def _status(self) -> MirrorStatus | None:
        """The mirror status of this charger, or None when it has no link right now."""
        assert self._runtime.mirror is not None
        return self._runtime.mirror.status(self._cp_id)


class MirrorStatusSensor(MirrorEntity, SensorEntity):
    """Whether the link to the vendor backend is up, with the last error and counters as attributes."""

    _attr_translation_key = "mirror_status"
    _attr_device_class = SensorDeviceClass.ENUM
    _attr_options = MIRROR_STATES

    # Build the sensor for one charger.
    def __init__(self, runtime: HubRuntime, entry_id: str, cp_id: str) -> None:
        super().__init__(runtime, entry_id, cp_id, "sensor", "mirror_status")

    @property
    def native_value(self) -> str:
        """connected, connecting (no link yet or the vendor is not reachable) or idle (the charger is not here)."""
        status = self._status()
        if status is None:
            return "idle"
        return "connected" if status.connected else "connecting"

    @property
    def extra_state_attributes(self) -> Mapping[str, Any]:
        """The last error and success and the counters, for the owner who wants to know why."""
        status = self._status()
        if status is None:
            return {}
        return {
            "last_error": status.last_error,
            "last_success": status.last_success.isoformat() if status.last_success else None,
            "queue_length": status.queue_length,
            "dropped_frames": status.dropped_frames,
            "unmapped_transactions": status.unmapped_transactions,
        }


class MirrorQueueSensor(MirrorEntity, SensorEntity):
    """How many frames wait for the vendor backend."""

    _attr_translation_key = "mirror_queue"
    _attr_state_class = SensorStateClass.MEASUREMENT

    # Build the sensor for one charger.
    def __init__(self, runtime: HubRuntime, entry_id: str, cp_id: str) -> None:
        super().__init__(runtime, entry_id, cp_id, "sensor", "mirror_queue")

    @property
    def native_value(self) -> int:
        """The number of frames waiting; zero when there is no link."""
        status = self._status()
        return status.queue_length if status is not None else 0
