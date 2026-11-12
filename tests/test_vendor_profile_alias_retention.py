"""Only independently absent trial resources stop consuming the vendor's live alias allowance."""

import copy

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.prototype.gateway import Gateway
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.policy import DeniedError
from ocpp_hub_core.prototype.store import Journal

from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session
from tests.test_prototype_tls import tls as tls


# Intent: independently verified old aliases survive more turnover than the small live allowance without blocking work.
# Why: fixing resource retention must not move its lifetime failure to the vendor alias map or recycle historical IDs.
async def test_absent_profile_alias_history_does_not_exhaust_live_namespace(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        actual = rig.bench.firmware.answer

        # Only the independent fake device replaces same-purpose/stack profiles as it receives real requests.
        def replacing(action, payload):
            if action == "SetChargingProfile":
                profile = payload["csChargingProfiles"]
                if profile["chargingProfilePurpose"] == commands.PURPOSE_TX_DEFAULT:
                    for key, previous in list(rig.bench.firmware.profiles.items()):
                        if (previous["chargingProfilePurpose"], previous["stackLevel"]) == (
                            profile["chargingProfilePurpose"],
                            profile["stackLevel"],
                        ):
                            rig.bench.firmware.profiles.pop(key)
            return actual(action, payload)

        monkeypatch.setattr(rig.bench.firmware, "answer", replacing)
        session = managed_session(rig)
        session.journal.capacity = 2
        await session.start()
        c = ManagedController(session)
        rig.route_to_trial()
        try:
            async with rig.charger(session) as charger:
                await introduced(rig, session, charger, "alias-retention-boot")
                await healthy(c)
                first_local = None
                for number in range(1, 6):
                    result = await rig.bench.cloud_call(
                        "turnover-" + str(number),
                        "SetChargingProfile",
                        commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 1000 + number, stack_level=0),
                    )
                    assert result[0] == 3 and result[2] == {"status": "Accepted"}
                    await healthy(c)
                    if first_local is None:
                        first_local = session.journal.get("vendor_profiles")["1001"]
                    assert len(rig.bench.firmware.profiles) == 2
                j = session.journal
                aliases = copy.deepcopy(j.get("vendor_profiles"))
                counter = j.get("next_vendor_profile")
                assert len(aliases) == 5 and aliases["1001"] == first_local
                assert (
                    j.db.execute(
                        "SELECT state FROM trial_resources WHERE key=?", ("profile:" + str(first_local),)
                    ).fetchone()[0]
                    == "absent_verified"
                )
                result = await rig.bench.cloud_call(
                    "explicit-historical-id",
                    "SetChargingProfile",
                    commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 1001, stack_level=0),
                )
                assert result[0] == 3 and result[2] == {"status": "Accepted"}
                await healthy(c)
                assert j.get("vendor_profiles") == aliases and j.get("next_vendor_profile") == counter
                assert first_local in rig.bench.firmware.profiles
                assert rig.bench.firmware.effective_limit() == 6
        finally:
            await session.close()


# Intent: unknown, pending, uncertain and confirmed live aliases all consume the complete small namespace allowance.
# Why: missing a ledger row or an unconfirmed profile is not independent evidence that physical ownership retired.
@pytest.mark.parametrize("state", ["confirmed", "pending", "uncertain", "unknown"])
async def test_unverified_or_live_aliases_cannot_be_discounted(tmp_path, tls, unused_tcp_port, monkeypatch, state):
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        session.journal.capacity = 2
        await session.start()
        c = ManagedController(session)
        rig.route_to_trial()
        try:
            async with rig.charger(session) as charger:
                await introduced(rig, session, charger, "alias-capacity-boot")
                await healthy(c)
                for number in range(1, 2 if state == "unknown" else 3):
                    response = await rig.bench.cloud_call(
                        "live-" + str(number),
                        "SetChargingProfile",
                        commands.set_charging_profile(
                            1, commands.PURPOSE_TX_DEFAULT, 6, 1000 + number, stack_level=number
                        ),
                    )
                    assert response[0] == 3 and response[2] == {"status": "Accepted"}
                    await healthy(c)
                j = session.journal
                if state == "unknown":
                    j.put("vendor_profiles", {**j.get("vendor_profiles"), "7777": 999999})
                    assert not j.db.execute("SELECT 1 FROM trial_resources WHERE key='profile:999999'").fetchone()
                elif state != "confirmed":
                    first = j.get("vendor_profiles")["1001"]
                    j.db.execute("UPDATE trial_resources SET state=? WHERE key=?", (state, "profile:" + str(first)))
                aliases, counter = copy.deepcopy(j.get("vendor_profiles")), j.get("next_vendor_profile")
                before = copy.deepcopy(rig.bench.device_calls)
                refused = await rig.bench.cloud_call(
                    "new-id-at-live-capacity",
                    "SetChargingProfile",
                    commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 8000, stack_level=3),
                )
                assert refused[0] == 4 and refused[2] == "SecurityError"
                assert rig.bench.device_calls == before
                assert j.get("vendor_profiles") == aliases and j.get("next_vendor_profile") == counter
                assert not j.get("uncertain_vendor_command")
        finally:
            await session.close()


# Intent: a nontrial gateway without independent resource receipts keeps its original conservative alias limit.
# Why: absence discounting must never be guessed when no native ResourceBook supplied the required proof.
def test_nontrial_alias_capacity_stays_conservative(tmp_path):
    j = Journal(tmp_path / "nontrial.db", "OFFLINE001", capacity=2)
    try:
        g = Gateway(j)
        for vendor in (1, 2):
            g._translated_command(
                "SetChargingProfile", commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, vendor)
            )
        aliases = copy.deepcopy(j.get("vendor_profiles"))
        with pytest.raises(DeniedError, match="namespace"):
            g._translated_command(
                "SetChargingProfile", commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 3)
            )
        assert j.get("vendor_profiles") == aliases
        assert not j.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='trial_resources'").fetchone()
    finally:
        j.close()


# Intent: a retired existing ID needs a new live slot, while updating a currently live ID does not.
# Why: historical alias reuse must not bypass the concurrent namespace limit after absent rows are discounted.
async def test_full_live_alias_budget_rejects_retired_reactivation_but_allows_live_update(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        actual = rig.bench.firmware.answer

        # Only the independent device replaces a matching purpose and stack, leaving other live stacks intact.
        def replacing(action, payload):
            if action == "SetChargingProfile":
                profile = payload["csChargingProfiles"]
                if profile["chargingProfilePurpose"] == commands.PURPOSE_TX_DEFAULT:
                    for key, previous in list(rig.bench.firmware.profiles.items()):
                        if (previous["chargingProfilePurpose"], previous["stackLevel"]) == (
                            profile["chargingProfilePurpose"],
                            profile["stackLevel"],
                        ):
                            rig.bench.firmware.profiles.pop(key)
            return actual(action, payload)

        monkeypatch.setattr(rig.bench.firmware, "answer", replacing)
        session = managed_session(rig)
        session.journal.capacity = 2
        await session.start()
        c = ManagedController(session)
        rig.route_to_trial()
        try:
            async with rig.charger(session) as charger:
                await introduced(rig, session, charger, "alias-reactivation-boot")
                await healthy(c)
                for vendor, stack in ((1001, 0), (1002, 0), (1003, 1)):
                    answer = await rig.bench.cloud_call(
                        "admit-" + str(vendor),
                        "SetChargingProfile",
                        commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, vendor, stack_level=stack),
                    )
                    assert answer[0] == 3 and answer[2] == {"status": "Accepted"}
                    await healthy(c)
                j = session.journal
                aliases, counter = copy.deepcopy(j.get("vendor_profiles")), j.get("next_vendor_profile")
                retired = aliases["1001"]
                assert (
                    j.db.execute(
                        "SELECT state FROM trial_resources WHERE key=?", ("profile:" + str(retired),)
                    ).fetchone()[0]
                    == "absent_verified"
                )
                before = copy.deepcopy(rig.bench.device_calls)
                refused = await rig.bench.cloud_call(
                    "reactivate-at-full-live-cap",
                    "SetChargingProfile",
                    commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 1001, stack_level=0),
                )
                assert refused[0] == 4 and refused[2] == "SecurityError"
                assert rig.bench.device_calls == before and retired not in rig.bench.firmware.profiles
                assert j.get("vendor_profiles") == aliases and j.get("next_vendor_profile") == counter
                updated = await rig.bench.cloud_call(
                    "update-live-at-cap",
                    "SetChargingProfile",
                    commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 1002, stack_level=0),
                )
                assert updated[0] == 3 and updated[2] == {"status": "Accepted"}
                assert len(rig.bench.device_calls) == len(before) + 1
                assert j.get("vendor_profiles") == aliases and j.get("next_vendor_profile") == counter
                assert len(rig.bench.firmware.profiles) == 3 and retired not in rig.bench.firmware.profiles
                assert (await healthy(c))["owner_control_enabled"]
        finally:
            await session.close()
