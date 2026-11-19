"""How a charger snapshot reads as plain values: the states and numbers the Home Assistant layer shows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import ceil

from .model import ChargerSnapshot, ConnectorState

# The connector that carries the socket; connector 0 is the charger itself.
PRIMARY_CONNECTOR = 1

# OCPP 1.6 ChargePointStatus values as stable lower-case keys, so translations and automations never depend on the
# spelling the protocol uses.
CONNECTOR_STATUS_KEYS = {
    "Available": "available",
    "Preparing": "preparing",
    "Charging": "charging",
    "SuspendedEVSE": "suspended_evse",
    "SuspendedEV": "suspended_ev",
    "Finishing": "finishing",
    "Reserved": "reserved",
    "Unavailable": "unavailable",
    "Faulted": "faulted",
}
CONNECTOR_STATUS_OPTIONS = list(CONNECTOR_STATUS_KEYS.values())


# The status of the socket connector as a stable key, or None before the first status or for an unknown status.
# A status this table does not know is not guessed at: showing nothing beats showing something wrong.
def connector_status(snapshot: ChargerSnapshot) -> str | None:
    connector = snapshot.connectors.get(PRIMARY_CONNECTOR)
    return CONNECTOR_STATUS_KEYS.get(connector.status) if connector is not None else None


# Whether the socket is known not to deliver power: its status is known and it is not charging.
# Nothing flows then, so power and current are zero, which is a fact and not a missing value. Before the first status
# nothing is known, and the reading shows what the meter said.
def _not_delivering(snapshot: ChargerSnapshot) -> bool:
    status = connector_status(snapshot)
    return status is not None and status != "charging"


# The latest power drawn, in watts; zero while the socket delivers nothing.
def power_w(snapshot: ChargerSnapshot) -> float | None:
    if _not_delivering(snapshot):
        return 0.0
    return snapshot.meter.power_w if snapshot.meter is not None else None


# The charger's energy register in kilowatt-hours; rounded to the watt-hour so float noise does not show as a change.
def energy_kwh(snapshot: ChargerSnapshot) -> float | None:
    if snapshot.meter is None or snapshot.meter.energy_wh is None:
        return None
    return round(snapshot.meter.energy_wh / 1000, 3)


# The latest current of one phase (L1, L2 or L3), in amperes; zero while the socket delivers nothing, and zero for a
# phase the meter leaves out while it reports others (a car that charges on one phase).
def phase_current_a(snapshot: ChargerSnapshot, phase: str) -> float | None:
    if _not_delivering(snapshot):
        return 0.0
    if snapshot.meter is None:
        return None
    if phase in snapshot.meter.current_a:
        return snapshot.meter.current_a[phase]
    return 0.0 if snapshot.meter.current_a else None


# The latest voltage of one phase (L1, L2 or L3), in volts.
def phase_voltage_v(snapshot: ChargerSnapshot, phase: str) -> float | None:
    return snapshot.meter.voltage_v.get(phase) if snapshot.meter is not None else None


# The current the charger says it offers the car, in amperes.
def current_offered_a(snapshot: ChargerSnapshot) -> float | None:
    return snapshot.meter.current_offered_a if snapshot.meter is not None else None


# The connector that has a charge running, or None. Only the socket connector counts; connector 0 is the charger.
def _running_charge(snapshot: ChargerSnapshot) -> ConnectorState | None:
    connector = snapshot.connectors.get(PRIMARY_CONNECTOR)
    return connector if connector is not None and connector.transaction_id is not None else None


# The energy delivered in the charge that is running, in kilowatt-hours; None when no charge runs or the start reading
# is missing. Never negative, because a meter that was reset must not show a negative charge.
def session_energy_kwh(snapshot: ChargerSnapshot) -> float | None:
    charge = _running_charge(snapshot)
    if charge is None or charge.meter_start_wh is None:
        return None
    if snapshot.meter is None or snapshot.meter.energy_wh is None:
        return None
    return round(max(snapshot.meter.energy_wh - charge.meter_start_wh, 0.0) / 1000, 3)


# When the charge that is running started, or None.
def session_started(snapshot: ChargerSnapshot) -> datetime | None:
    charge = _running_charge(snapshot)
    return charge.started_at if charge is not None else None


# How long the charge that is running has lasted, in seconds, up to the latest meter reading; None when none runs.
def session_duration_s(snapshot: ChargerSnapshot) -> int | None:
    charge = _running_charge(snapshot)
    if charge is None or charge.started_at is None or snapshot.meter is None:
        return None
    return max(int((snapshot.meter.sampled_at - charge.started_at).total_seconds()), 0)


# Whether a charge is running on the socket connector.
def charge_running(snapshot: ChargerSnapshot) -> bool:
    return _running_charge(snapshot) is not None


# The sockets statuses with a vehicle connected. "Available" means nobody is plugged in; the others (reserved,
# unavailable, faulted) say nothing about the plug.
_PLUGGED_STATUSES = frozenset({"preparing", "charging", "suspended_evse", "suspended_ev", "finishing"})

# OCPP 1.6 ChargePointErrorCode values as stable lower-case keys.
ERROR_CODE_KEYS = {
    "ConnectorLockFailure": "connector_lock_failure",
    "EVCommunicationError": "ev_communication_error",
    "GroundFailure": "ground_failure",
    "HighTemperature": "high_temperature",
    "InternalError": "internal_error",
    "LocalListConflict": "local_list_conflict",
    "NoError": "no_error",
    "OtherError": "other_error",
    "OverCurrentFailure": "over_current_failure",
    "PowerMeterFailure": "power_meter_failure",
    "PowerSwitchFailure": "power_switch_failure",
    "ReaderFailure": "reader_failure",
    "ResetFailure": "reset_failure",
    "UnderVoltage": "under_voltage",
    "OverVoltage": "over_voltage",
    "WeakSignal": "weak_signal",
}
ERROR_CODE_OPTIONS = list(ERROR_CODE_KEYS.values())


# Whether a vehicle is connected to the socket; None when the status says nothing about it.
def plugged_in(snapshot: ChargerSnapshot) -> bool | None:
    status = connector_status(snapshot)
    if status is None:
        return None
    if status in _PLUGGED_STATUSES:
        return True
    return False if status == "available" else None


# Whether the socket is delivering power to the car right now; None before the first status.
def car_charging(snapshot: ChargerSnapshot) -> bool | None:
    status = connector_status(snapshot)
    return None if status is None else status == "charging"


# The error code of the socket as a stable key, or None before the first status or for a code this table does not know.
def error_code(snapshot: ChargerSnapshot) -> str | None:
    connector = snapshot.connectors.get(PRIMARY_CONNECTOR)
    return ERROR_CODE_KEYS.get(connector.error_code) if connector is not None else None


# Whether the socket reports a problem: a faulted status or an error code other than "no error".
# An unknown code counts as a problem, because the charger said something is wrong; None before the first status.
def problem(snapshot: ChargerSnapshot) -> bool | None:
    connector = snapshot.connectors.get(PRIMARY_CONNECTOR)
    if connector is None:
        return None
    return connector.status == "Faulted" or connector.error_code != "NoError"


# The limit in effect on the charger, in amperes. What the charger itself says (GetCompositeSchedule) wins; when it
# cannot say, the lowest of the limits the hub knows of: its own and the vendor's, because the charger applies the
# lower. None when no limit is known.
def effective_limit_a(snapshot: ChargerSnapshot) -> float | None:
    if snapshot.composite_limit_a is not None:
        return snapshot.composite_limit_a
    known = [limit for limit in (snapshot.charge_limit_a, snapshot.vendor_limit_a) if limit is not None]
    return min(known) if known else None


# The limit that is certain: what the charger reported, or the lowest limit the hub knows when it has written or
# accepted it on this connection. A limit restored from storage and not yet confirmed is not certain, and is not told
# to the vendor. None when nothing is certain.
def confirmed_limit_a(snapshot: ChargerSnapshot) -> float | None:
    if snapshot.composite_limit_a is not None:
        return snapshot.composite_limit_a
    if snapshot.limits_restored:
        return None
    return effective_limit_a(snapshot)


# Return the last period that has already started. A future period gives no information about the present limit.
def schedule_limit_at(periods: tuple[tuple[int, float], ...], elapsed: float) -> float | None:
    return next((limit for offset, limit in reversed(periods) if offset <= elapsed), None)


@dataclass(frozen=True)
class ScheduleWindow:
    """The known span from now and its current and future periods."""

    duration_s: int
    periods: tuple[tuple[int, float], ...]


# Describe the known current limit and each future change within the requested interval.
def schedule_from_now(snapshot: ChargerSnapshot, now: datetime, duration_s: int) -> ScheduleWindow | None:
    start = snapshot.schedule_start
    if start is None or not snapshot.schedule_periods:
        return None
    elapsed = (now - start).total_seconds()
    covered = min(duration_s, int(snapshot.schedule_duration_s - elapsed))
    if covered <= 0:
        return None
    current = schedule_limit_at(snapshot.schedule_periods, elapsed)
    if current is None:
        current = confirmed_limit_a(snapshot)
    if current is None:
        return None
    changes = tuple(
        (ceil(offset - elapsed), limit)
        for offset, limit in snapshot.schedule_periods
        if offset > elapsed and ceil(offset - elapsed) < covered
    )
    return ScheduleWindow(covered, ((0, current), *changes))
