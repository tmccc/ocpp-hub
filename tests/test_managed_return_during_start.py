"""Return during an actual Start acknowledgement wait retains zero and accepted lifecycle debt."""

import asyncio
import copy
import uuid

import pytest
from ocpp_hub_core.prototype.local_control import LocalControl

from tests.charge_rig import owner_rig
from tests.managed_rig import healthy, starts
from tests.native_peer_failures import InterruptedNativePeer
from tests.prototype_rig import BOOT
from tests.runtime_rig import until
from tests.tls_support import tls as tls

pytestmark = pytest.mark.asyncio


# Intent: Return accepted during a normal Start's actual ACK wait revokes positive permission and still drives zero.
# Why: an already-paused owner intent must not leave an accepted Start's positive ceiling in place during HTTP loss.
@pytest.mark.parametrize("matching_boot", [False, True], ids=["same-boot", "matching-boot-current-pause"])
async def test_return_during_start_ack_restores_zero_without_native_http(
    tmp_path, tls, unused_tcp_port, monkeypatch, matching_boot
):
    async with owner_rig(
        tmp_path, tls, unused_tcp_port, monkeypatch, startup=10, native_peer=InterruptedNativePeer
    ) as b:
        c, g, j, rig = b.controller, b.session.gateway, b.session.journal, b.rig
        assert isinstance(rig.peer, InterruptedNativePeer)
        plans, configuration = copy.deepcopy(rig.bench.firmware.plans), copy.deepcopy(rig.peer.configuration)
        paused = uuid.uuid4().hex
        c.command(paused, "pause", c.requests.intent().revision)
        assert (await healthy(c))["local_hold_verified"]
        intent = c.requests.intent()
        assert intent.mode == "paused" and c.requests.receipt(paused)["state"] == "completed"
        owner_pause = copy.deepcopy(j.get("managed_owner_pause"))
        boot, epoch, revision = g.boot_observation, g._charger_epoch, g.controls._revision
        metadata_revision, expected_boot = g.boot_metadata_revision, boot
        boot_reply, start_child = None, None
        zero_writers = []
        assert boot is not None and rig.bench.firmware.effective_limit() == 0
        start_seen, release_start = asyncio.Event(), asyncio.Event()
        old_hook, old_timeout = rig.bench.before_reply, g.charger.timeout
        g.charger.timeout = 6

        # The independent real charger has already applied the positive cap before its actual Start reply is held.
        async def hold_start_reply(action, payload):
            if (
                matching_boot
                and action == "SetChargingProfile"
                and start_seen.is_set()
                and payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
            ):
                zero_writers.append(
                    {
                        "old_start_done": start_child is not None and start_child.done(),
                        "current_pause_pending": c.pause_task is not None and not c.pause_task.done(),
                        "boot": g.boot_observation,
                    }
                )
            if action == "RemoteStartTransaction":
                start_seen.set()
                async with asyncio.timeout(5):
                    await release_start.wait()
            if old_hook is not None:
                await old_hook(action, payload)

        rig.bench.before_reply = hold_start_reply
        identifier, returning = uuid.uuid4().hex, uuid.uuid4().hex
        try:
            assert c.command(identifier, "start", intent.revision)["state"] == "accepted"
            await asyncio.wait_for(start_seen.wait(), 4)
            assert len(starts(rig)) == 1 and not c.charging.worker.done()
            owned = [task for task in g.controls._operations if not task.done()]
            assert len(owned) == 1 and owned[0].get_coro().cr_code is LocalControl._start_owned.__code__
            start_child = owned[0]
            receipt = copy.deepcopy(j.get("local_start_receipt"))
            assert receipt["request_id"] == identifier and receipt["stage"] == "issued"
            assert receipt.get("local") is None and j.active() is None
            control = j.get("control")
            assert control["ticket"] == receipt["ticket"]
            assert control["inhibit"] is False and control["start_pending"] is True
            assert rig.bench.firmware.effective_limit() == c.current.selected() == 6
            assert c.requests.intent() == intent and j.get("managed_owner_pause") == owner_pause
            if matching_boot:
                # Send real Boot while the fake reader holds Start ACK; waiting for its reply here would deadlock.
                boot_reply = asyncio.create_task(rig.bench.call(b.charger, uuid.uuid4().hex, "BootNotification", BOOT))
                await until(lambda: g.boot_observation != boot, timeout=2)
                expected_boot = g.boot_observation
                assert expected_boot is not None and not boot_reply.done()
                assert g._charger_epoch == epoch and g.controls._revision == revision
                assert g.boot_metadata_revision == metadata_revision
                assert j.get("local_start_receipt") == receipt and j.get("control")["ticket"] == receipt["ticket"]
            accepted = c.command(returning, "app_control", intent.revision)
            assert accepted["id"] == returning and accepted["state"] == "accepted"
            assert c.requests.return_in_progress() and c.requests.pending_return() == (returning, intent.revision)
            assert c.requests.intent() == intent and not c.charging.worker.done()
            wire = len(rig.bench.device_calls)
            rig.peer.unavailable = True
            release_start.set()
            if boot_reply is not None:
                assert (await asyncio.wait_for(asyncio.shield(boot_reply), 2))[2]["status"] == "Accepted"
            # The real Accepted reply retains debt; old-scope rollback yields to the current persistent Pause.
            await asyncio.wait_for(c.charging.settle(), 4)
            assert c.charging.worker.done() and not c.charging.worker.cancelled()
            accepted_start = j.get("local_start_receipt")
            assert accepted_start["request_id"] == identifier and accepted_start["stage"] == "accepted"
            assert accepted_start["ticket"] == receipt["ticket"] and accepted_start.get("local") is None
            assert c.charging.state()["state"] == "stopping"
            assert c.charging.state()["failure"]["type"] == ("UnavailableError" if matching_boot else "DeniedError")
            assert c.requests.receipt(identifier)["state"] == "uncertain"
            assert c.pause_task is not None
            await asyncio.wait_for(asyncio.shield(c.pause_task), 3)
            failed = await asyncio.wait_for(rig.peer.failed_reads.get(), 2)
            assert failed["action"] == "getConfig" and rig.peer.unavailable
            assert c.requests.intent() == intent and j.get("managed_owner_pause") == owner_pause
            assert g.boot_observation == expected_boot and g._charger_epoch == epoch
            assert g.controls._revision == revision and g.boot_metadata_revision == metadata_revision
            if matching_boot:
                assert zero_writers and all(
                    item["old_start_done"] and item["current_pause_pending"] and item["boot"] == expected_boot
                    for item in zero_writers
                ), zero_writers
            assert len(starts(rig)) == 1 and j.active() is None
            assert j.get("control")["start_pending"] is True
            assert j.get("local_start_receipt")["stage"] == "accepted"
            assert c.requests.receipt(returning)["state"] != "completed"
            assert not any(item[2] == "RemoteStopTransaction" for item in rig.bench.device_calls)
            # Intended baseline-red assertion follows the genuine admission/Accepted/guard/HTTP failure witnesses.
            zeros = [
                item
                for item in rig.bench.device_calls[wire:]
                if item[2] == "SetChargingProfile"
                and item[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
            ]
            assert zeros, {
                "control": j.get("control"),
                "accepted_start": j.get("local_start_receipt"),
                "failure": c.charging.state().get("failure"),
                "actual_limit": rig.bench.firmware.effective_limit(),
                "wire_after_accepted_return": rig.bench.device_calls[wire:],
            }
            assert rig.bench.firmware.effective_limit() == 0
            assert j.get("control")["owner"] == "local" and j.get("control")["inhibit"] is True
            assert j.get("control")["start_pending"] is True and j.get("control")["uncertain"] is True
            assert j.get("local_start_receipt")["stage"] == "accepted"
            assert c.requests.intent() == intent and c.requests.receipt(returning)["state"] != "completed"
            assert rig.bench.firmware.plans == plans and rig.peer.configuration == configuration
            assert len(starts(rig)) == 1 and not any(
                item[2] == "RemoteStopTransaction" for item in rig.bench.device_calls
            )
        finally:
            release_start.set()
            rig.peer.unavailable = False
            rig.bench.before_reply = old_hook
            if c.charging.worker is not None:
                await asyncio.gather(c.charging.worker, return_exceptions=True)
            if c.pause_task is not None:
                await asyncio.gather(c.pause_task, return_exceptions=True)
            if boot_reply is not None:
                if not boot_reply.done():
                    boot_reply.cancel()
                await asyncio.gather(boot_reply, return_exceptions=True)
            g.charger.timeout = old_timeout
