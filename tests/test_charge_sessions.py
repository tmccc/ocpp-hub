"""Finished charges as the engine reports them, driven by a fake charger over a real websocket."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest
from ocpp_hub_core import readings
from ocpp_hub_core.model import ChargeSession


# A MeterValues payload with the energy register in Wh and the power in W.
def _meter(timestamp: str, energy_wh: float, power_w: float) -> dict[str, Any]:
    return {
        "connectorId": 1,
        "meterValue": [
            {
                "timestamp": timestamp,
                "sampledValue": [
                    {"measurand": "Energy.Active.Import.Register", "value": str(energy_wh), "unit": "Wh"},
                    {"measurand": "Power.Active.Import", "value": str(power_w), "unit": "W"},
                ],
            }
        ],
    }


# Intent: a charge from StartTransaction to StopTransaction is reported once, with energy, peak power, tag, times and
# the stop reason, and the connector is clear again afterwards.
# Why: this record is the whole point of the session history; a wrong energy or a missing report would be unnoticed.
async def test_a_finished_charge_is_reported_with_its_numbers(start_hub) -> None:
    hub = await start_hub()
    charger = await hub.charger()

    started = await charger.call(
        "StartTransaction",
        {"connectorId": 1, "idTag": "TAG1", "meterStart": 1000, "timestamp": "2026-10-21T08:00:00Z"},
    )
    await charger.call("MeterValues", _meter("2026-10-21T08:10:00Z", 2000, 7000))
    await charger.call("MeterValues", _meter("2026-10-21T08:20:00Z", 3500, 4000))
    running = hub.listener.latest
    await charger.call(
        "StopTransaction",
        {
            "transactionId": started[2]["transactionId"],
            "meterStop": 4000,
            "timestamp": "2026-10-21T09:00:00Z",
            "reason": "EVDisconnected",
        },
    )

    assert readings.session_energy_kwh(running) == 2.5 and readings.session_duration_s(running) == 1200
    assert readings.session_energy_kwh(hub.listener.latest) is None
    assert hub.listener.latest.connectors[1].transaction_id is None
    assert hub.listener.finished == [
        ChargeSession(
            cp_id="TEST0001",
            connector_id=1,
            transaction_id=started[2]["transactionId"],
            id_tag="TAG1",
            started_at=datetime(2026, 10, 21, 8, 0, tzinfo=UTC),
            ended_at=datetime(2026, 10, 21, 9, 0, tzinfo=UTC),
            meter_start_wh=1000.0,
            meter_stop_wh=4000.0,
            energy_wh=3000.0,
            max_power_w=7000.0,
            average_power_w=5500.0,
            stop_reason="EVDisconnected",
        )
    ]


# Intent: a delayed reading from a closed charge cannot change the next charge on the same connector.
# Why: the prior transaction ID or an earlier timestamp can identify a sample that belongs to the previous charge.
@pytest.mark.parametrize("tagged", [False, True])
@pytest.mark.parametrize("before_first_sample", [False, True])
async def test_delayed_meter_values_stay_with_their_charge(start_hub, before_first_sample, tagged) -> None:
    hub = await start_hub()
    charger = await hub.charger()
    first = await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "T", "meterStart": 0, "timestamp": "2026-10-21T08:00:00Z"}
    )
    first_id = first[2]["transactionId"]
    await charger.call(
        "StopTransaction", {"transactionId": first_id, "meterStop": 1000, "timestamp": "2026-10-21T08:30:00Z"}
    )
    second = await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "T", "meterStart": 1000, "timestamp": "2026-10-21T09:00:00Z"}
    )
    second_id = second[2]["transactionId"]
    delayed = _meter("2026-10-21T08:25:00Z", 9000, 7000)
    if tagged:
        delayed["transactionId"] = first_id
    if before_first_sample:
        await charger.call("MeterValues", delayed)
        assert hub.listener.latest.meter is None
    await charger.call("MeterValues", _meter("2026-10-21T09:10:00Z", 1300, 2000) | {"transactionId": second_id})
    if not before_first_sample:
        await charger.call("MeterValues", delayed)
    buffered = _meter("2026-10-21T09:05:00Z", 1100, 1000)
    buffered["meterValue"].append({"timestamp": "2026-10-21T09:15:00Z", "sampledValue": []})
    await charger.call("MeterValues", buffered | {"transactionId": second_id})
    running = hub.listener.latest
    assert running.meter.power_w == 2000 and readings.session_energy_kwh(running) == 0.3
    assert readings.session_duration_s(running) == 600

    await charger.call(
        "StopTransaction", {"transactionId": second_id, "meterStop": 1800, "timestamp": "2026-10-21T09:30:00Z"}
    )
    assert [charge.transaction_id for charge in hub.listener.finished] == [first_id, second_id]
    last = hub.listener.finished[-1]
    assert last.max_power_w == 2000 and last.average_power_w == 1500


# Intent: a stop for a transaction the hub never saw start produces no charge, and a stop without a stop reading falls
# back to the last meter value.
# Why: after a restart of the hub a charger can end a charge that began earlier; inventing a record for it would
# show a wrong start and energy. The fallback keeps the energy right for chargers that omit meterStop.
async def test_unknown_stops_are_ignored_and_a_missing_stop_reading_uses_the_last_meter_value(start_hub) -> None:
    hub = await start_hub()
    charger = await hub.charger()

    await charger.call("StopTransaction", {"transactionId": 4242, "meterStop": 10, "timestamp": "2026-10-21T08:00:00Z"})
    started = await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "TAG1", "meterStart": 500, "timestamp": "2026-10-21T08:00:00Z"}
    )
    await charger.call("MeterValues", _meter("2026-10-21T08:30:00Z", 1700, 3000))
    await charger.call("StopTransaction", {"transactionId": started[2]["transactionId"], "timestamp": "garbled"})

    assert len(hub.listener.finished) == 1
    assert hub.listener.finished[0].energy_wh == 1200.0 and hub.listener.finished[0].meter_stop_wh is None


# Intent: a missing stop reading uses only meter values matched to the closing connector and transaction.
# Why: a second connector can have a later and entirely different energy register.
async def test_missing_stop_reading_uses_its_own_connector(start_hub) -> None:
    hub = await start_hub()
    charger = await hub.charger()
    timestamp = "2026-10-21T08:00:00Z"
    first, second = [
        (
            await charger.call(
                "StartTransaction",
                {
                    "connectorId": connector,
                    "idTag": "TAG1",
                    "meterStart": 500,
                    "timestamp": timestamp,
                },
            )
        )[2]["transactionId"]
        for connector in (1, 2)
    ]
    await charger.call(
        "MeterValues",
        _meter("2026-10-21T08:10:00Z", 1700, 3000)
        | {
            "connectorId": 1,
            "transactionId": first,
        },
    )
    await charger.call(
        "MeterValues",
        _meter("2026-10-21T08:20:00Z", 9000, 4000)
        | {
            "connectorId": 2,
            "transactionId": second,
        },
    )
    assert readings.session_energy_kwh(hub.listener.latest) == 1.2
    await charger.call("MeterValues", _meter("2026-10-21T08:05:00Z", 600, 3000) | {"transactionId": first})
    assert readings.session_energy_kwh(hub.listener.latest) == 1.2
    await charger.call("StopTransaction", {"transactionId": first, "timestamp": "2026-10-21T08:30:00Z"})
    assert hub.listener.finished[0].energy_wh == 1200.0
    assert hub.listener.latest.connectors[2].transaction_id == second


# Intent: a charge survives a trip through to_dict and from_dict unchanged, and a damaged record raises.
# Why: sessions are stored as JSON and read back after every restart; a lossy round trip would corrupt the history.
def test_a_session_round_trips_through_its_dictionary() -> None:
    session = ChargeSession(
        cp_id="TEST0001",
        connector_id=1,
        transaction_id=7,
        id_tag=None,
        started_at=datetime(2026, 10, 21, 8, 0, tzinfo=UTC),
        ended_at=datetime(2026, 10, 21, 9, 30, tzinfo=UTC),
        energy_wh=11000.0,
    )

    assert ChargeSession.from_dict(session.to_dict()) == session
    assert session.duration_s == 5400.0
    try:
        ChargeSession.from_dict({"cp_id": "x"})
    except KeyError:
        pass
    else:
        raise AssertionError("a damaged record must raise")


# Intent: a charge that is running while the charger reconnects is still reported when it stops, with the data from
# its start, and the live values survive the reconnect.
# Why: a wifi drop in the middle of a charge is common; losing the charge from the history would break the promise that
# every charge is kept.
async def test_a_charge_survives_a_reconnect_of_the_charger(start_hub) -> None:
    hub = await start_hub()
    charger = await hub.charger()
    started = await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "TAG1", "meterStart": 1000, "timestamp": "2026-10-21T08:00:00Z"}
    )
    await charger.call("MeterValues", _meter("2026-10-21T08:10:00Z", 2000, 5000))
    await charger.close()
    await charger.wait_closed()

    again = await hub.charger()
    resumed = hub.listener.latest.connectors[1]
    await again.call(
        "StopTransaction",
        {"transactionId": started[2]["transactionId"], "meterStop": 3000, "timestamp": "2026-10-21T08:30:00Z"},
    )

    assert resumed.transaction_id == started[2]["transactionId"] and resumed.stats.max_power_w == 5000.0
    assert len(hub.listener.finished) == 1
    assert hub.listener.finished[0].energy_wh == 2000.0 and hub.listener.finished[0].id_tag == "TAG1"
    assert hub.listener.finished[0].started_at == datetime(2026, 10, 21, 8, 0, tzinfo=UTC)


# Intent: a StartTransaction on a connector that still has a running charge closes the old charge first, ended at the
# new start and with the last meter reading, and the new charge runs on.
# Why: a charger that lost its stop (power cut, a firmware that starts again) must not make the first charge vanish.
async def test_a_second_start_closes_the_charge_that_was_still_running(start_hub) -> None:
    hub = await start_hub()
    charger = await hub.charger()
    first = await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "TAG1", "meterStart": 1000, "timestamp": "2026-10-21T08:00:00Z"}
    )
    await charger.call("MeterValues", _meter("2026-10-21T08:10:00Z", 2500, 6000))
    session = hub.central.session("TEST0001")
    assert session is not None
    session.record_limit(10, "TxProfile")
    session.record_vendor_limit(6, 812, "TxProfile", connector_id=2)
    second = await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "TAG2", "meterStart": 2500, "timestamp": "2026-10-21T09:00:00Z"}
    )

    assert len(hub.listener.finished) == 1
    closed = hub.listener.finished[0]
    assert closed.transaction_id == first[2]["transactionId"] and closed.energy_wh == 1500.0
    assert closed.ended_at == datetime(2026, 10, 21, 9, 0, tzinfo=UTC) and closed.stop_reason == "Other"
    assert hub.listener.latest.connectors[1].transaction_id == second[2]["transactionId"]
    assert hub.listener.latest.charge_limit_a is None and hub.listener.latest.vendor_limit_a == 6


# Intent: hostile numbers and texts in the start and stop do not break the answer or reach the history unbounded: an
# impossible meter value becomes "unknown", an idTag that is not a string is dropped, and long texts are cut.
# Why: the charger port has no authentication; an overflowing number raised inside the handler and left the charge
# open, and a megabyte tag would have bloated a history that is never truncated.
async def test_hostile_values_are_contained(start_hub) -> None:
    hub = await start_hub()
    charger = await hub.charger()
    started = await charger.call(
        "StartTransaction",
        {"connectorId": 1, "idTag": {"not": "a string"}, "meterStart": 10**400, "timestamp": "2026-10-21T08:00:00Z"},
    )
    stopped = await charger.call(
        "StopTransaction",
        {
            "transactionId": started[2]["transactionId"],
            "meterStop": float("inf"),
            "timestamp": "2026-10-21T09:00:00Z",
            "reason": "X" * 5000,
        },
    )

    assert started[0] == 3 and stopped[0] == 3
    assert len(hub.listener.finished) == 1
    finished = hub.listener.finished[0]
    assert finished.id_tag is None and finished.energy_wh is None and finished.meter_stop_wh is None
    assert finished.stop_reason is not None and len(finished.stop_reason) == 64


# Intent: transaction ids keep growing across a restart of the central system.
# Why: the history stores ids and a charger may stop an id from before the restart; a counter that starts at 1 again
# would repeat ids and could match a new charge with an old stop.
async def test_transaction_ids_do_not_restart_at_one(start_hub) -> None:
    hub = await start_hub()
    charger = await hub.charger()

    started = await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "TAG1", "meterStart": 0, "timestamp": "2026-10-21T08:00:00Z"}
    )

    assert started[2]["transactionId"] > 1_000_000_000


# Intent: when a charger reconnects before the hub noticed the old link dying, the charge goes on with the new
# connection and is gone for good once it stopped; a later connection does not bring it back.
# Why: the old connection's cleanup used to leave a copy of the running charge behind, which came back as a ghost and
# was then recorded a second time when the next charge started.
async def test_no_ghost_charge_after_a_reconnect_over_a_live_link(start_hub) -> None:
    hub = await start_hub()
    first = await hub.charger()
    started = await first.call(
        "StartTransaction", {"connectorId": 1, "idTag": "TAG1", "meterStart": 1000, "timestamp": "2026-10-21T08:00:00Z"}
    )
    second = await hub.charger()
    await second.call(
        "StopTransaction",
        {"transactionId": started[2]["transactionId"], "meterStop": 2000, "timestamp": "2026-10-21T08:30:00Z"},
    )
    await second.close()
    await second.wait_closed()
    third = await hub.charger()
    await third.call(
        "StartTransaction", {"connectorId": 1, "idTag": "TAG2", "meterStart": 2000, "timestamp": "2026-10-21T09:00:00Z"}
    )

    assert len(hub.listener.finished) == 1
    assert hub.listener.finished[0].transaction_id == started[2]["transactionId"]


# Intent: an old reader still reserving a transaction retires before the new socket can report a session.
# Why: closing a websocket alone does not join a handler awaiting the allocator.
async def test_replacement_joins_the_previous_charge_reader(start_hub, monkeypatch) -> None:
    hub = await start_hub()
    first = await hub.charger()
    old = hub.central.session("TEST0001")
    assert old is not None
    entered, retired = asyncio.Event(), asyncio.Event()

    # Open a genuine await in the old reader at its normal transaction reservation boundary.
    async def waiting_for_reservation():
        entered.set()
        try:
            await asyncio.Future()
        finally:
            retired.set()

    monkeypatch.setattr(old, "_next_transaction_id", waiting_for_reservation)
    start = {"connectorId": 1, "idTag": "TAG1", "meterStart": 0, "timestamp": "2026-10-21T08:00:00Z"}
    caller = asyncio.create_task(first.call("StartTransaction", start))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        replacement = await hub.charger()
        reply = await replacement.call("StartTransaction", start)
        assert reply[0] == 3 and retired.is_set()
        assert hub.listener.latest.connectors[1].transaction_id == reply[2]["transactionId"]
        await asyncio.wait_for(hub.central.stop(), 2)
    finally:
        caller.cancel()
        await asyncio.gather(caller, return_exceptions=True)


# Intent: a reading with only the energy register, as the NEO sends on the clock, does not erase the power, currents and
# offered current of the reading before it.
# Why: the history showed power and currents as unknown at 12:00:00 and 13:30:00 sharp; a partial reading replaced the
# whole one.
async def test_an_energy_only_reading_keeps_the_rest(start_hub) -> None:
    hub = await start_hub()
    charger = await hub.charger()
    full = _meter("2026-10-21T08:10:00Z", 2000, 4000)
    full["meterValue"][0]["sampledValue"] += [
        {"measurand": "Current.Import", "phase": "L1", "value": "5.9", "unit": "A"},
        {"measurand": "Current.Offered", "value": "6", "unit": "A"},
    ]
    await charger.call("MeterValues", full)

    await charger.call(
        "MeterValues",
        {
            "connectorId": 1,
            "meterValue": [
                {
                    "timestamp": "2026-10-21T08:30:00Z",
                    "sampledValue": [{"measurand": "Energy.Active.Import.Register", "value": "2100", "unit": "Wh"}],
                }
            ],
        },
    )

    meter = hub.listener.latest.meter
    assert meter.energy_wh == 2100.0 and meter.power_w == 4000.0
    assert meter.current_a == {"L1": 5.9} and meter.current_offered_a == 6.0


# Intent: a finished charge carries its own numbers: the mean power over the power readings (an energy-only reading does
# not dilute it), the highest current per phase and the highest offered current.
# Why: the session record is where all session information lives; it must not be rebuilt from loose sensors later.
async def test_a_finished_charge_carries_its_own_statistics(start_hub) -> None:
    hub = await start_hub()
    charger = await hub.charger()
    started = await charger.call(
        "StartTransaction", {"connectorId": 1, "idTag": "T", "meterStart": 0, "timestamp": "2026-10-21T08:00:00Z"}
    )
    for stamp, power, amps in (("08:10:00", 4000, "5.9"), ("08:20:00", 3800, "5.95")):
        reading = _meter(f"2026-10-21T{stamp}Z", 1000, power)
        reading["meterValue"][0]["sampledValue"] += [
            {"measurand": "Current.Import", "phase": "L1", "value": amps, "unit": "A"},
            {"measurand": "Current.Offered", "value": "6", "unit": "A"},
        ]
        await charger.call("MeterValues", reading)
        await charger.call(
            "MeterValues",
            {
                "connectorId": 1,
                "meterValue": [
                    {
                        "timestamp": f"2026-10-21T{stamp[:3]}30:00Z",
                        "sampledValue": [{"measurand": "Energy.Active.Import.Register", "value": "1", "unit": "Wh"}],
                    }
                ],
            },
        )

    await charger.call(
        "StopTransaction",
        {"transactionId": started[2]["transactionId"], "meterStop": 2000, "timestamp": "2026-10-21T09:00:00Z"},
    )

    finished = hub.listener.finished[0]
    assert finished.average_power_w == 3900.0 and finished.max_power_w == 4000.0
    assert finished.max_current_a == {"L1": 5.95} and finished.offered_a == 6.0


# Intent: a full reading replaces the phases, power and offered current of the one before it, so a phase that is no
# longer reported does not keep its old value; only a reading with nothing but the energy register keeps the rest.
# Why: after a three-phase car, a one-phase car would otherwise show the old currents of L2 and L3 while it
# charges on L1.
async def test_a_full_reading_replaces_phases_that_are_no_longer_reported(start_hub) -> None:
    hub = await start_hub()
    charger = await hub.charger()

    # A MeterValues payload with the given phase currents and, optionally, the power.
    def reading(stamp: str, phases: dict[str, str], power: int | None) -> dict[str, Any]:
        values = [{"measurand": "Current.Import", "phase": p, "value": v, "unit": "A"} for p, v in phases.items()]
        if power is not None:
            values.append({"measurand": "Power.Active.Import", "value": str(power), "unit": "W"})
        return {"connectorId": 1, "meterValue": [{"timestamp": f"2026-10-21T{stamp}Z", "sampledValue": values}]}

    await charger.call("MeterValues", reading("08:00:00", {"L1": "16", "L2": "16", "L3": "16"}, 11000))
    await charger.call("MeterValues", reading("08:10:00", {"L1": "6"}, None))

    meter = hub.listener.latest.meter
    assert meter.current_a == {"L1": 6.0} and meter.power_w is None
