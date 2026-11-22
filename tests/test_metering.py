"""Reading MeterValues payloads into one sample with fixed units."""

from __future__ import annotations

from datetime import UTC, datetime

from ocpp_hub_core.metering import parse_meter_values, phase_key

NOW = datetime(2026, 10, 20, 18, 0, tzinfo=UTC)


# Intent: build the MeterValues payload of a charging three-phase charger, one value per measurand and phase.
# Why: this is the shape that matters for load balancing, so every test starts from the same realistic payload.
def _payload(**overrides: object) -> dict[str, object]:
    sampled = [
        {"measurand": "Energy.Active.Import.Register", "value": "12.5", "unit": "kWh"},
        {"measurand": "Power.Active.Import", "value": "4.14", "unit": "kW"},
        {"measurand": "Current.Import", "phase": "L1", "value": "6.0", "unit": "A"},
        {"measurand": "Current.Import", "phase": "L2", "value": "6.1", "unit": "A"},
        {"measurand": "Current.Import", "phase": "L3", "value": "5.9", "unit": "A"},
        {"measurand": "Voltage", "phase": "L1-N", "value": "230.4", "unit": "V"},
        {"measurand": "Current.Offered", "value": "16", "unit": "A"},
    ]
    return {
        "connectorId": 1,
        "meterValue": [{"timestamp": "2026-10-20T17:59:50Z", "sampledValue": sampled}],
        **overrides,
    }


# Intent: kilowatt and kilowatt-hour readings come out in watt and watt-hour, phases land under L1 to L3.
# Why: Home Assistant entities and the load balancer both assume the fixed base units.
def test_units_are_converted_and_phases_keyed() -> None:
    sample = parse_meter_values(_payload(), NOW)

    assert sample is not None
    assert sample.power_w == 4140.0 and sample.energy_wh == 12500.0
    assert sample.current_a == {"L1": 6.0, "L2": 6.1, "L3": 5.9}
    assert sample.voltage_v == {"L1": 230.4}
    assert sample.current_offered_a == 16.0
    assert sample.sampled_at == datetime(2026, 10, 20, 17, 59, 50, tzinfo=UTC)


# Intent: without a total power the per-phase powers are summed.
# Why: some chargers report power per phase only, and the total is what the entity shows.
def test_power_is_summed_over_phases_when_no_total_is_given() -> None:
    payload = {
        "meterValue": [
            {
                "sampledValue": [
                    {"measurand": "Power.Active.Import", "phase": "L1", "value": "1000", "unit": "W"},
                    {"measurand": "Power.Active.Import", "phase": "L2", "value": "1500", "unit": "W"},
                ]
            }
        ]
    }

    sample = parse_meter_values(payload, NOW)

    assert sample is not None and sample.power_w == 2500.0


# Intent: unusable values are skipped one by one, the rest of the reading survives.
# Why: a single signed-data or unknown-unit value must never blank out a whole reading.
def test_unusable_values_are_skipped_not_fatal() -> None:
    payload = {
        "meterValue": [
            {
                "sampledValue": [
                    {"measurand": "Voltage", "value": "230", "unit": "Furlongs"},
                    {"measurand": "Current.Import", "value": "abc"},
                    {"measurand": "Energy.Active.Import.Register", "value": "00AA", "format": "SignedData"},
                    {"measurand": "Current.Import", "phase": "L1", "value": "7", "unit": "A"},
                    {"measurand": "Current.Import", "phase": "L1", "value": "inf", "unit": "A"},
                    {"measurand": "Current.Import", "phase": "L1", "value": "100", "unit": "kWh"},
                    {"measurand": "Current.Import", "phase": ["L1"], "value": "100", "unit": "A"},
                ]
            }
        ]
    }

    sample = parse_meter_values(payload, NOW)

    assert sample is not None
    assert sample.current_a == {"L1": 7.0} and sample.voltage_v == {} and sample.energy_wh is None


# Intent: with several timestamps the latest entry wins, and a missing timestamp falls back to the receive time.
# Why: a charger can batch buffered readings; the hub must show the newest one.
def test_latest_entry_wins_and_missing_timestamp_falls_back_to_now() -> None:
    payload = {
        "meterValue": [
            {
                "timestamp": "2026-10-20T17:00:00Z",
                "sampledValue": [{"measurand": "Power.Active.Import", "value": "1", "unit": "kW"}],
            },
            {
                "timestamp": "2026-10-20T17:59:00Z",
                "sampledValue": [{"measurand": "Power.Active.Import", "value": "2", "unit": "kW"}],
            },
        ]
    }
    undated = {"meterValue": [{"sampledValue": [{"measurand": "Power.Active.Import", "value": "3", "unit": "kW"}]}]}

    latest, fallback = parse_meter_values(payload, NOW), parse_meter_values(undated, NOW)

    assert latest is not None and latest.power_w == 2000.0
    assert fallback is not None and fallback.sampled_at == NOW and parse_meter_values({}, NOW) is None


# Intent: an unusable meter entry cannot replace the reading or advance its timestamp.
# Why: an empty or invalid report contains no new current or energy evidence.
def test_latest_usable_entry_wins() -> None:
    valid = _payload()["meterValue"][0]
    empty = {"timestamp": "2026-10-20T18:00:00Z", "sampledValue": []}
    invalid = {"timestamp": "2026-10-20T18:00:01Z", "sampledValue": [{"value": "unavailable"}]}

    assert parse_meter_values({"meterValue": [empty, invalid]}, NOW) is None
    assert parse_meter_values({"meterValue": [empty, valid, invalid]}, NOW) == parse_meter_values(_payload(), NOW)


# Intent: phase labels reduce to L1 to L3, a missing phase means total, and the neutral is dropped.
# Why: voltage arrives as L1-N while current arrives as L1; both must land under one key.
def test_phase_key_normalisation() -> None:
    assert phase_key(None) == "total" and phase_key("L2") == "L2" and phase_key("L3-N") == "L3"
    assert phase_key("N") is None and phase_key("L1-L2") == "L1-L2"
