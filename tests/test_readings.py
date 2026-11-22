"""How a snapshot reads as the values Home Assistant shows."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from ocpp_hub_core import readings
from ocpp_hub_core.model import ChargerSnapshot, ConnectorState, MeterSample


# A snapshot with a meter reading and a socket connector in the given status.
def _snapshot(status: str | None = "Charging", **meter: Any) -> ChargerSnapshot:
    connectors = {0: ConnectorState(0, "Available")}
    if status is not None:
        connectors[1] = ConnectorState(1, status)
    sample = MeterSample(datetime(2026, 10, 20, tzinfo=UTC), **meter) if meter else None
    return ChargerSnapshot("TEST0001", connectors=connectors, meter=sample)


# Intent: every OCPP 1.6 connector status maps to its own stable key, and anything else shows as no state.
# Why: translations and automations depend on these keys; a guessed key for an unknown status would show wrong data.
@pytest.mark.parametrize(
    ("status", "key"),
    [
        ("Available", "available"),
        ("Preparing", "preparing"),
        ("Charging", "charging"),
        ("SuspendedEVSE", "suspended_evse"),
        ("SuspendedEV", "suspended_ev"),
        ("Finishing", "finishing"),
        ("Reserved", "reserved"),
        ("Unavailable", "unavailable"),
        ("Faulted", "faulted"),
        ("SomethingNew", None),
        (None, None),
    ],
)
def test_connector_status_keys(status: str | None, key: str | None) -> None:
    assert readings.connector_status(_snapshot(status)) == key
    assert sorted(readings.CONNECTOR_STATUS_OPTIONS) == sorted(set(readings.CONNECTOR_STATUS_KEYS.values()))


# Intent: the status comes from the socket connector, never from connector 0 (the charger itself).
# Why: connector 0 reports Available while the car charges on connector 1.
def test_status_uses_the_socket_connector_only() -> None:
    snapshot = ChargerSnapshot("TEST0001", connectors={0: ConnectorState(0, "Faulted")})

    assert readings.connector_status(snapshot) is None


# Intent: readings are taken from the latest meter sample in the units Home Assistant shows, and are None without one.
# Why: an entity must show unknown, not zero, before the first sample.
def test_meter_readings_and_their_units() -> None:
    snapshot = _snapshot(
        power_w=7400.0,
        energy_wh=1234567.0,
        current_a={"L1": 10.0, "L2": 11.5},
        voltage_v={"L1": 231.0},
        current_offered_a=16.0,
    )
    empty = _snapshot()

    assert readings.power_w(snapshot) == 7400.0 and readings.energy_kwh(snapshot) == 1234.567
    assert readings.phase_current_a(snapshot, "L2") == 11.5 and readings.phase_current_a(snapshot, "L3") == 0.0
    assert readings.phase_voltage_v(snapshot, "L1") == 231.0 and readings.phase_voltage_v(snapshot, "L2") is None
    assert readings.current_offered_a(snapshot) == 16.0
    assert [
        readings.power_w(empty),
        readings.energy_kwh(empty),
        readings.phase_current_a(empty, "L1"),
        readings.phase_voltage_v(empty, "L1"),
        readings.current_offered_a(empty),
    ] == [None] * 5


# Intent: an energy register without a value reads as None, and float noise is rounded away.
# Why: a total_increasing sensor that flickers by 1e-13 kWh would write a new statistic at every sample.
def test_energy_without_a_value_and_rounding() -> None:
    assert readings.energy_kwh(_snapshot(power_w=1.0)) is None
    assert readings.energy_kwh(_snapshot(energy_wh=1000.0000000001)) == 1.0


# Intent: while the socket is known not to deliver, power and every phase current read as zero; while it charges, a
# phase the meter leaves out reads zero when other phases are reported; before any status the meter's values show.
# Why: nothing flows when nothing charges, so zero is the fact and unknown is a gap; a missing phase of a one-phase car
# is zero too, but without any status or any current there is nothing to claim.
def test_zero_when_nothing_flows_and_unknown_only_when_nothing_is_known() -> None:
    idle = _snapshot("Available", power_w=4000.0, current_a={"L1": 5.9})
    charging = _snapshot("Charging", power_w=4000.0, current_a={"L1": 5.9})
    no_status = _snapshot(None, power_w=4000.0)
    no_current = _snapshot("Charging", power_w=4000.0)
    aggregate_current = _snapshot("Charging", current_a={"total": 5.9})

    assert readings.power_w(idle) == 0.0 and readings.phase_current_a(idle, "L1") == 0.0
    assert readings.power_w(charging) == 4000.0 and readings.phase_current_a(charging, "L1") == 5.9
    assert readings.phase_current_a(charging, "L2") == 0.0
    assert readings.power_w(no_status) == 4000.0
    assert readings.phase_current_a(no_current, "L1") is None
    assert all(readings.phase_current_a(aggregate_current, phase) is None for phase in ("L1", "L2", "L3"))


# Intent: plugged in, car charging and problem come from the socket's status and error code, and stay unknown when the
# status says nothing: available is unplugged, the five connected statuses are plugged, and reserved or faulted say
# nothing about the plug.
# Why: these become automation triggers (the car arrived, charging started, something broke), so a guess would fire
# them.
@pytest.mark.parametrize(
    ("status", "plugged", "charging"),
    [
        ("Available", False, False),
        ("Preparing", True, False),
        ("Charging", True, True),
        ("SuspendedEV", True, False),
        ("SuspendedEVSE", True, False),
        ("Finishing", True, False),
        ("Reserved", None, False),
        ("Faulted", None, False),
        (None, None, None),
    ],
)
def test_plugged_in_and_car_charging_follow_the_status(
    status: str | None, plugged: bool | None, charging: bool | None
) -> None:
    snapshot = _snapshot(status)

    assert readings.plugged_in(snapshot) is plugged and readings.car_charging(snapshot) is charging


# Intent: the problem flag and the error code follow the connector: no error is fine, a faulted status or any other code
# is a problem, and a code the table does not know is a problem with no key.
# Why: a charger that says something is wrong must show as a problem even when this integration cannot name it.
def test_problem_and_error_code_follow_the_connector() -> None:
    fine = ChargerSnapshot("T", connectors={1: ConnectorState(1, "Available", "NoError")})
    broken = ChargerSnapshot("T", connectors={1: ConnectorState(1, "Faulted", "GroundFailure")})
    strange = ChargerSnapshot("T", connectors={1: ConnectorState(1, "Available", "SomethingNew")})
    nothing = ChargerSnapshot("T")

    assert (readings.problem(fine), readings.error_code(fine)) == (False, "no_error")
    assert (readings.problem(broken), readings.error_code(broken)) == (True, "ground_failure")
    assert (readings.problem(strange), readings.error_code(strange)) == (True, None)
    assert (readings.problem(nothing), readings.error_code(nothing)) == (None, None)
