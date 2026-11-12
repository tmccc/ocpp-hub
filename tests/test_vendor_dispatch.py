"""Vendor dispatch and confirmed profile ownership against independent TLS peers."""

import asyncio

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.frames import build_call, build_call_error, build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.policy import DeniedError
from ocpp_hub_core.prototype.security import TRANSPORT_LOG, SecurityError
from ocpp_hub_core.prototype.trial_session import TrialSession, preflight
from websockets.asyncio.server import serve

from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import FAST, Bench, until
from tests.test_prototype_tls import admission as admission
from tests.test_prototype_tls import tls as tls
from tests.test_trial_session import Bench as TrialBench
from tests.trial_helpers import config_files


# Intent: an accepted profile replacement without a transaction remains clearable after the old session ends.
# Why: removing the old transaction association must not discard the mapping for the live replacement.
@pytest.mark.parametrize("tx_to_default", [True, False])
@pytest.mark.parametrize("replacement_status", ["Accepted", "Rejected", "malformed", "CALLERROR"])
@pytest.mark.parametrize("stop_before_reply", [False, True])
async def test_replaced_transaction_profile_keeps_mapping(
    tmp_path, tls, admission, tx_to_default, replacement_status, stop_before_reply
):
    bench = Bench(tmp_path / "journal.db", tls, admission)
    replies = {}
    replacement_seen, release_replacement = asyncio.Event(), asyncio.Event()
    profile_calls = 0
    pending_replies = []

    async with serve(
        bench.vendor, "127.0.0.1", 0, ssl=bench.server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
    ) as cloud:
        await bench.start(cloud.sockets[0].getsockname()[1])
        try:
            async with bench.charger() as charger:
                await bench.boot(charger)
                await until(lambda: bench.gateway.vendor_registration == "Accepted")

                # Delay only the acknowledgement; independent lifecycle replies must keep flowing.
                async def replacement_reply(frame, actual):
                    await release_replacement.wait()
                    if replacement_status == "CALLERROR":
                        await charger.send(build_call_error(frame[1], "InternalError", "unknown outcome"))
                    else:
                        body = {"unexpected": "reply"} if replacement_status == "malformed" else actual
                        await charger.send(build_call_result(frame[1], body))

                # Apply accepted commands at receipt, independently from their delayed acknowledgements.
                async def device_reader():
                    nonlocal profile_calls
                    async for raw in charger:
                        frame = decode(raw)
                        if frame[0] == 2:
                            if (
                                frame[2] == "SetChargingProfile"
                                and frame[3]["csChargingProfiles"]["chargingProfileId"] != 7001
                            ):
                                profile_calls += 1
                                if profile_calls == 2:
                                    actual = (
                                        {"status": "Rejected"}
                                        if replacement_status == "Rejected"
                                        else (bench.firmware.answer(frame[2], frame[3]))
                                    )
                                    pending_replies.append(asyncio.create_task(replacement_reply(frame, actual)))
                                    replacement_seen.set()
                                    continue
                            await charger.send(build_call_result(frame[1], bench.firmware.answer(frame[2], frame[3])))
                        else:
                            replies[frame[1]] = frame

                # Wait for a real correlated reply on the charger socket.
                async def local_call(token, action, body):
                    await charger.send(build_call(token, action, body))
                    await until(lambda: token in replies)
                    return replies[token]

                # Wait for a real correlated reply on the cloud socket.
                async def vendor_call(token, action, body):
                    await bench.connections[-1].send(build_call(token, action, body))
                    await until(lambda: any(f[0] != 2 and f[1] == token for f in bench.calls))
                    return next(f for f in bench.calls if f[0] != 2 and f[1] == token)

                reader = asyncio.create_task(device_reader())
                try:
                    opened = await local_call("open", "StartTransaction", START)
                    local = opened[2]["transactionId"]
                    bench.firmware.active = local
                    await until(lambda: bench.store.vendor_id(local) is not None)
                    vendor_id = bench.store.vendor_id(local)
                    initial_purpose = commands.PURPOSE_TX if tx_to_default else commands.PURPOSE_TX_DEFAULT
                    new_purpose = commands.PURPOSE_TX_DEFAULT if tx_to_default else commands.PURPOSE_TX
                    original = commands.set_charging_profile(
                        1,
                        initial_purpose,
                        6,
                        101,
                        **({"transaction_id": vendor_id} if tx_to_default else {}),
                    )
                    assert (await vendor_call("first-profile", "SetChargingProfile", original))[2] == {
                        "status": "Accepted"
                    }
                    mapped = bench.store.get("vendor_profiles")["101"]
                    replacement = commands.set_charging_profile(
                        1,
                        new_purpose,
                        0,
                        101,
                        **({"transaction_id": vendor_id} if not tx_to_default else {}),
                    )
                    replacing = asyncio.create_task(
                        vendor_call("replacement-profile", "SetChargingProfile", replacement)
                    )
                    await asyncio.wait_for(replacement_seen.wait(), 1)

                    # A real lifecycle may arrive while the command's real reply remains in flight.
                    async def stop():
                        stopped = await local_call(
                            "closed",
                            "StopTransaction",
                            {"transactionId": local, "meterStop": 1000, "timestamp": START["timestamp"]},
                        )
                        assert stopped[0] == 3
                        bench.firmware.active = None

                    if stop_before_reply:
                        await stop()
                    release_replacement.set()
                    result = await replacing
                    if replacement_status in ("Accepted", "Rejected"):
                        assert result[0] == 3 and result[2] == {"status": replacement_status}
                    else:
                        assert result[0] == 4
                    if not stop_before_reply:
                        if replacement_status != "Accepted":
                            assert bench.store.get("vendor_profile_transactions", {}).get("101") == (
                                local if tx_to_default else None
                            )
                        await stop()
                    live_default = (
                        replacement_status == "Accepted"
                        and tx_to_default
                        or replacement_status == "Rejected"
                        and not tx_to_default
                    )
                    if replacement_status in ("malformed", "CALLERROR"):
                        assert bench.store.db.execute("SELECT 1 FROM mutations WHERE state='uncertain'").fetchone()
                    elif live_default:
                        assert bench.store.get("vendor_profiles")["101"] == mapped
                        assert "101" not in bench.store.get("vendor_profile_transactions", {})
                        assert bench.firmware.profiles[mapped]["chargingProfilePurpose"] == "TxDefaultProfile"
                        cleared = await vendor_call("clear-replacement", "ClearChargingProfile", {"id": 101})
                        assert cleared[0] == 3 and cleared[2]["status"] == "Accepted"
                        assert mapped not in bench.firmware.profiles
                    else:
                        assert "101" not in bench.store.get("vendor_profiles", {})
                finally:
                    release_replacement.set()
                    await asyncio.gather(*pending_replies, return_exceptions=True)
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            await bench.close()


# Intent: refusal at the final resource gate does not quarantine a command that was never dispatched.
# Why: an unsupported profile must not permanently disable every later valid cloud write.
async def test_unsent_resource_refusal_does_not_poison_vendor_writes(tmp_path, tls, unused_tcp_port):
    bench = TrialBench()
    connections = []

    # Keep the independent cloud socket available for actual vendor CALLs.
    async def vendor(connection):
        connections.append(connection)
        await bench.vendor(connection)

    async with serve(vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        session = TrialSession(preflight(path), FAST, bench.observe)
        try:
            await session.start()
            async with bench.connection_to(session) as charger:
                reader = asyncio.create_task(bench.read_charger(charger))
                try:
                    await bench.call(charger, "boot", "BootNotification", BOOT)
                    await until(
                        lambda: session.gateway.controls.ready and session.gateway.vendor_registration == "Accepted"
                    )

                    # Submit real cloud requests and observe their correlated results on the wire.
                    async def vendor_call(token, body):
                        await connections[-1].send(build_call(token, "SetChargingProfile", body))
                        await until(lambda: any(f[0] != 2 and f[1] == token for f in bench.calls))
                        return next(f for f in bench.calls if f[0] != 2 and f[1] == token)

                    before = len(bench.device_calls)
                    unsupported = commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 111)
                    unsupported["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"].append(
                        {"startPeriod": 60, "limit": 6}
                    )
                    denied = await vendor_call("unsupported-profile", unsupported)
                    assert denied[0] == 4 and denied[2] == "SecurityError"
                    assert len(bench.device_calls) == before, "the resource rejection happened before physical dispatch"
                    rows = [dict(row) for row in session.journal.db.execute("SELECT action,state FROM mutations")]
                    result = await vendor_call(
                        "supported-profile", commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 112)
                    )
                    print("UNSENT_MUTATIONS", rows)
                    print("LATER_SUPPORTED_RESULT", result)
                    print("DEVICE_CALLS_DURING_BOTH_REQUESTS", bench.device_calls[before:])
                    assert result[0] == 3 and result[2]["status"] == "Accepted"
                finally:
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            await session.close()


# Intent: interrupted or refused validation retires a prepared-only grant; attempted transport retains its debt.
# Why: interrupted validation or acknowledgement cannot prove whether a Start reached the charger.
@pytest.mark.parametrize("after_dispatch", [False, True], ids=["before-dispatch", "after-dispatch"])
@pytest.mark.parametrize("failure", ["cancel", "denied", "native-security"])
async def test_cancelled_vendor_start_retains_only_possible_dispatch(
    tmp_path, tls, unused_tcp_port, after_dispatch, failure
):
    bench = TrialBench()
    async with serve(bench.vendor, "127.0.0.1", 0, ssl=tls[0], subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG) as cloud:
        _, path = config_files(tmp_path, unused_tcp_port, cloud.sockets[0].getsockname()[1])
        session = TrialSession(preflight(path), FAST, bench.observe)
        entered, release = asyncio.Event(), asyncio.Event()
        reserved_grants = []
        task = None
        try:
            await session.start()
            async with bench.connection_to(session) as charger:
                reader = asyncio.create_task(bench.read_charger(charger))
                try:
                    await bench.call(charger, "boot", "BootNotification", BOOT)
                    await until(
                        lambda: session.gateway.controls.ready and session.gateway.vendor_registration == "Accepted"
                    )
                    gateway = session.gateway
                    if after_dispatch:
                        sender = gateway.charger.sender
                        assert sender is not None

                        # The actual socket send occurs; only its coroutine's return is delayed.
                        async def blocked_sender(raw):
                            await sender(raw)
                            reserved_grants.append(bool(session.journal.get("vendor_start_grant")))
                            entered.set()
                            if failure != "cancel":
                                error = DeniedError if failure == "denied" else SecurityError
                                raise error("injected validation or transport failure")
                            await release.wait()

                        gateway.charger.sender = blocked_sender
                    else:
                        validate = gateway.charger.validate_send

                        # Keep the real native validation result, delaying its return before physical dispatch.
                        async def blocked_validation(action, payload):
                            if validate is not None:
                                await validate(action, payload)
                            reserved_grants.append(bool(session.journal.get("vendor_start_grant")))
                            entered.set()
                            if failure != "cancel":
                                error = DeniedError if failure == "denied" else SecurityError
                                raise error("injected validation or transport failure")
                            await release.wait()

                        gateway.charger.validate_send = blocked_validation
                    frame = [2, "cancelled-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": "LOCAL_TEST"}]
                    task = asyncio.create_task(gateway._vendor_call(frame, gateway._vendor_epoch))
                    await asyncio.wait_for(entered.wait(), 2)
                    assert reserved_grants == [True]
                    if failure == "cancel":
                        task.cancel()
                        with pytest.raises(asyncio.CancelledError):
                            await task
                    elif failure == "native-security":
                        answer = decode(await task)
                        assert answer[0] == 4 and answer[2] == "InternalError"
                    else:
                        assert decode(await task)[0] == 4
                    rows = session.journal.db.execute("SELECT state FROM mutations WHERE origin='vendor'").fetchall()
                    assert [row[0] for row in rows] == (["uncertain"] if after_dispatch else [])
                    assert bool(session.journal.get("vendor_start_grant")) is after_dispatch
                    assert bool(session.journal.get("uncertain_vendor_command")) is after_dispatch
                finally:
                    release.set()
                    if task is not None and not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            await session.close()
