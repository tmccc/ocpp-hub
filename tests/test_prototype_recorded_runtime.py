"""The complete recorded iCS request sequence over both real offline TLS legs."""

from __future__ import annotations

import asyncio
import json
import ssl
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.frames import build_call, build_call_result
from ocpp_hub_core.prototype.gateway import Gateway, decode
from ocpp_hub_core.prototype.policy import Policy
from ocpp_hub_core.prototype.runtime import OfflineRuntime
from ocpp_hub_core.prototype.security import TRANSPORT_LOG, Admission
from ocpp_hub_core.prototype.store import Journal
from websockets.asyncio.client import connect
from websockets.asyncio.server import ServerConnection, serve

from tests.prototype_firmware import Firmware
from tests.test_prototype import BOOT
from tests.test_prototype_runtime import FAST, NOW, until
from tests.test_prototype_tls import admission as admission
from tests.test_prototype_tls import tls as tls


# Compare embedded JSON without requiring whitespace/key order to remain identical after translation.
def semantic(payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    if result.get("data"):
        result["data"] = json.loads(result["data"])
    return result


# Intent: exercise every captured CALL through the actual TLS runtime, including new vendor-generated tags.
# Why: isolated payload tests cannot establish request ordering, grant lifetimes or transaction/profile alignment.
@pytest.mark.parametrize(
    ("fixture_name", "frame_count", "expected_open_vendor"),
    [
        ("ics-vehicle-controls.sanitized.json", 721, None),
        ("ics-completion-observation.sanitized.json", 789, 6000004),
    ],
)
async def test_complete_recorded_runtime(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    admission: Admission,
    fixture_name: str,
    frame_count: int,
    expected_open_vendor: int | None,
) -> None:
    capture = json.loads((Path(__file__).parent / "fixtures" / fixture_name).read_text())
    rows = capture["frames"]
    replies = {(row["direction"], row["frame"][1]): row["frame"] for row in rows if row["frame"][0] != 2}
    assert len(rows) == capture["frame_count"] == frame_count
    server_tls, client_tls, _ = tls
    start_calls = [r["frame"] for r in rows if r["frame"][0] == 2 and r["frame"][2] == "StartTransaction"]
    vendor_ids = [replies[("cloud_to_charger", f[1])][2]["transactionId"] for f in start_calls]
    starts = [f[3] for f in start_calls]
    assert len(starts) >= 3 and starts[-1]["idTag"] != starts[0]["idTag"]
    policy = Policy(
        max_current_a=32,
        allowed_tags=frozenset({starts[0]["idTag"]}),
        trust_vendor_start_tags=True,
        extensions={
            ("IoCharger", name): mode
            for name, mode in (
                ("getTransaction", "read"),
                ("setTransaction", "write"),
                ("setPlan", "write"),
                ("clearPlans", "write"),
                ("delayStop", "write"),
            )
        },
    )
    store = Journal(tmp_path / "journal.db", admission.identity)
    gateway = Gateway(store, policy, timeout=0.5)
    gateway.plans.adopt(8000001, 2000001)  # Explicit verified initial resource; never inferred from an unknown clear.
    firmware = Firmware()
    firmware.plans[2000001] = {"planId": 2000001, "limitCurrent": 32}
    cloud_connection: asyncio.Future[ServerConnection] = asyncio.get_running_loop().create_future()
    cloud_requests: dict[str, asyncio.Future[list[Any]]] = {}
    charger_requests: dict[str, asyncio.Future[list[Any]]] = {}
    expected_cloud: tuple[str, dict[str, Any], dict[str, Any]] | None = None
    expected_charger: tuple[str, dict[str, Any]] | None = None
    forwarded = 0

    # Correlate real cloud-bound requests independently, using recorded replies only where actually present.
    async def cloud_handler(connection: ServerConnection) -> None:
        nonlocal forwarded
        cloud_connection.set_result(connection)
        async for raw in connection:
            frame = decode(raw)
            if frame[0] != 2:
                cloud_requests[frame[1]].set_result(frame)
                continue
            if frame[2] == "BootNotification":
                answer = {"status": "Accepted", "interval": 1, "currentTime": NOW}
            elif frame[2] == "Heartbeat":
                answer = {"currentTime": NOW}
            else:
                assert expected_cloud is not None
                action, payload, answer = expected_cloud
                assert frame[2:] == [action, payload]
                forwarded += 1
            await connection.send(build_call_result(frame[1], answer))

    async with serve(
        cloud_handler, "127.0.0.1", 0, ssl=server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
    ) as cloud:
        runtime = OfflineRuntime(
            gateway,
            admission,
            f"wss://127.0.0.1:{cloud.sockets[0].getsockname()[1]}/ocpp",
            server_tls,
            client_tls,
            FAST,
        )
        await runtime.start()
        try:
            async with connect(
                f"wss://127.0.0.1:{runtime.port}/charger/{admission.identity}",
                ssl=client_tls,
                additional_headers=[("Authorization", admission.authorization)],
                subprotocols=["ocpp1.6"],
                logger=TRANSPORT_LOG,
                proxy=None,
            ) as charger:
                # Independently answer server RPCs while the device's own outgoing CALL waits for a response.
                async def charger_reader() -> None:
                    async for raw in charger:
                        frame = decode(raw)
                        if frame[0] != 2:
                            charger_requests[frame[1]].set_result(frame)
                            continue
                        action, payload = frame[2:]
                        if action == "SetChargingProfile" or action == "GetCompositeSchedule":
                            answer = firmware.answer(action, payload)
                            if action == "SetChargingProfile" and "transactionId" in payload["csChargingProfiles"]:
                                profile = payload["csChargingProfiles"]
                                assert profile["transactionId"] == firmware.active
                                firmware.requested_current = 32
                                assert (
                                    firmware.current
                                    == profile["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                                )
                        elif action == "DataTransfer":
                            data = json.loads(payload["data"])
                            if payload["messageId"] == "getTransaction":
                                answer = {
                                    "status": "Accepted",
                                    "data": json.dumps({"transactionId": data["transactionId"], "eco": "Fast"}),
                                }
                            else:
                                answer = firmware.answer(action, payload)
                        elif action == "RemoteStopTransaction":
                            answer = firmware.answer(action, payload)
                        else:
                            assert expected_charger is not None and expected_charger[0] == action
                            answer = expected_charger[1]
                        await charger.send(build_call_result(frame[1], answer))

                reader = asyncio.create_task(charger_reader())

                # Submit one real charger CALL with a separate result future, never reading the socket twice.
                async def device_call(token: str, action: str, payload: dict[str, Any]) -> list[Any]:
                    future: asyncio.Future[list[Any]] = asyncio.get_running_loop().create_future()
                    charger_requests[token] = future
                    await charger.send(build_call(token, action, payload))
                    return await asyncio.wait_for(future, 2)

                try:
                    assert (await device_call("synthetic-boot", "BootNotification", BOOT))[2]["status"] == "Accepted"
                    await until(lambda: gateway.controls.ready and gateway.vendor_registration == "Accepted")
                    connection = await cloud_connection
                    missing_meter_acks = 0
                    expected_refusals = 0
                    count = 0
                    for row in rows:
                        frame = row["frame"]
                        if frame[0] != 2:
                            continue
                        direction = row["direction"]
                        opposite = "cloud_to_charger" if direction == "charger_to_cloud" else "charger_to_cloud"
                        reply = replies.get((opposite, frame[1]))
                        if direction == "charger_to_cloud":
                            if reply is None:
                                assert frame[2] == "MeterValues"
                                missing_meter_acks += 1
                            expected_cloud = frame[2], frame[3], reply[2] if reply is not None else {}
                            payload = json.loads(json.dumps(frame[3]))
                            if "transactionId" in payload:
                                payload["transactionId"] = store.historical_local_id(payload["transactionId"])
                                assert payload["transactionId"] is not None
                            answer = await device_call(frame[1], frame[2], payload)
                            assert answer[0] == 3, frame[2]
                            if frame[2] == "StartTransaction":
                                firmware.active = answer[2]["transactionId"]
                            elif frame[2] == "StopTransaction":
                                firmware.active = None
                            await until(lambda: store.next_delivery() is None)
                        else:
                            assert reply is not None
                            expected_charger = frame[2], reply[2]
                            future = asyncio.get_running_loop().create_future()
                            cloud_requests[frame[1]] = future
                            await connection.send(build_call(frame[1], frame[2], frame[3]))
                            answer = await asyncio.wait_for(future, 2)
                            if frame[2] == "GetDiagnostics":
                                assert answer[0] == 4
                                expected_refusals += 1
                            else:
                                assert answer[0] == 3, frame[2:]
                                assert semantic(answer[2]) == semantic(reply[2])
                        count += 1
                    assert count == sum(row["frame"][0] == 2 for row in rows)
                    assert missing_meter_acks == 9 and expected_refusals == 1
                    assert forwarded > 200
                    active = store.active()
                    assert (None if active is None else store.vendor_id(active)) == expected_open_vendor
                    if expected_open_vendor is not None:
                        assert (
                            store.db.execute("SELECT stop FROM transactions WHERE local=?", (active,)).fetchone()[0]
                            is None
                        )
                        assert store.get("last_status")["status"] == "SuspendedEV"
                    assert [store.vendor_id(1001 + i) for i in range(len(starts))] == vendor_ids
                    assert not gateway.failures
                    assert store.get("vendor_profiles") == {}
                    assert list(firmware.plans) == [2000001]
                finally:
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            await runtime.close()
            store.close()
