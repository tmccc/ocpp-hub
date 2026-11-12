"""Live ownership is bounded independently of retained, verified-absent profile history."""

from __future__ import annotations

import copy
import time

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.frames import build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.managed import ManagedController
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError, fingerprint
from ocpp_hub_core.prototype.trial_evidence import Evidence
from ocpp_hub_core.prototype.trial_resources import Baseline, ResourceBook

from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session
from tests.test_prototype_tls import tls as tls


# Intent: more than127 independently absent turnover profiles cannot exhaust normal new-ID cloud operation.
# Why: a two-profile physical inventory should remain usable while historical ownership/reappearance evidence survives.
async def test_real_turnover_retains_history_without_lifetime_exhaustion(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        actual = rig.bench.firmware.answer

        # Model only the independent device's same-purpose/stack replacement, preserving station and engine policy.
        def replacing(action, payload):
            if action == "SetChargingProfile":
                profile = payload["csChargingProfiles"]
                if profile["chargingProfilePurpose"] == commands.PURPOSE_TX_DEFAULT:
                    for key, previous in list(rig.bench.firmware.profiles.items()):
                        if (
                            previous["chargingProfilePurpose"] == profile["chargingProfilePurpose"]
                            and previous["stackLevel"] == profile["stackLevel"]
                        ):
                            rig.bench.firmware.profiles.pop(key)
            return actual(action, payload)

        monkeypatch.setattr(rig.bench.firmware, "answer", replacing)
        first_absent = None
        second_absent = None
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "retained-turnover")
            await healthy(controller)
            for number in range(1, 131):
                answer = await rig.bench.cloud_call(
                    "retained-profile-" + str(number),
                    "SetChargingProfile",
                    commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 1000 + number, stack_level=0),
                )
                assert answer[0] == 3 and answer[2] == {"status": "Accepted"}
                profile = next(
                    (key, copy.deepcopy(value)) for key, value in rig.bench.firmware.profiles.items() if key != 7001
                )
                if number == 1:
                    first_absent = profile
                elif number == 2:
                    second_absent = profile
                assert (await healthy(controller))["owner_control_enabled"]
                assert len(rig.bench.firmware.profiles) == 2
            assert session.journal.db.execute("SELECT COUNT(*) FROM trial_resources").fetchone()[0] == 131
            assert (
                session.journal.db.execute(
                    "SELECT COUNT(*) FROM trial_resources WHERE state='absent_verified'"
                ).fetchone()[0]
                == 129
            )
            assert first_absent is not None and second_absent is not None
            reused = await rig.bench.cloud_call(
                "explicit-old-id",
                "SetChargingProfile",
                commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 1001, stack_level=0),
            )
            assert reused[0] == 3 and rig.bench.firmware.profiles[first_absent[0]] == first_absent[1]
            await healthy(controller)
            # Reappearance without a corresponding explicit new intent remains an independent inventory fault.
            rig.bench.firmware.profiles[second_absent[0]] = second_absent[1]
            await controller.tick()
            assert session.journal.get("trial_inventory_fault")["reason"] == "resource_reappeared"
            await session.close()


# Intent:128 live/unresolved receipts block old absent ID reactivation, while existing live updates remain valid.
# Why: retaining historical rows must not create an alternate route around the concurrent ownership bound.
def test_live_capacity_blocks_absent_reactivation_without_losing_history(tmp_path):
    journal = Journal(tmp_path / "live.db", "OFFLINE001")
    try:
        evidence = Evidence(
            fingerprint(journal.identity), time.time(), "A", "Unplug", 0, 0, False, fingerprint("config"), ()
        )
        book = ResourceBook(journal, Baseline.capture(journal.identity, evidence))
        book.activate(evidence)
        old = commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 100001, stack_level=0)
        book.before_send("SetChargingProfile", old)
        book.after_result("SetChargingProfile", old, decode(build_call_result("old", {"status": "Accepted"})))
        journal.db.execute("UPDATE trial_resources SET state='absent_verified' WHERE key='profile:100001'")
        for number in range(128):
            book.before_send(
                "SetChargingProfile",
                commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 200000 + number, stack_level=0),
            )
        before = list(journal.db.execute("SELECT * FROM trial_resources ORDER BY key"))
        for request in (old, commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 999999, stack_level=0)):
            with pytest.raises(RecoveryRequiredError, match="capacity"):
                book.before_send("SetChargingProfile", request)
            assert list(journal.db.execute("SELECT * FROM trial_resources ORDER BY key")) == before
        book.before_send(
            "SetChargingProfile",
            commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 0, 200000, stack_level=0),
        )
        assert journal.db.execute("SELECT COUNT(*) FROM trial_resources").fetchone()[0] == 129
        assert (
            journal.db.execute("SELECT state FROM trial_resources WHERE key='profile:100001'").fetchone()[0]
            == "absent_verified"
        )
    finally:
        journal.close()
