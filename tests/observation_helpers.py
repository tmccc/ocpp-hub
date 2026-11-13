"""Independent loopback TLS peers for durable observation acceptance and real cloud publication."""

import asyncio
import uuid
from contextlib import AsyncExitStack
from datetime import datetime, timedelta

from ocpp_hub_core.frames import basic_auth_header, build_call, build_call_error, build_call_result
from ocpp_hub_core.prototype.gateway import Gateway, decode
from ocpp_hub_core.prototype.security import TRANSPORT_LOG, Admission, offline_server, offline_vendor
from ocpp_hub_core.prototype.store import Journal
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from tests.prototype_firmware import Firmware
from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import charger_reply


class ObservationTLS:
    """Peers outlive gateway replacement and never obtain their expected result from journal decisions."""

    # Keep only synthetic credentials and test-owned database paths, with bounded loopback endpoints.
    def __init__(self, path, contexts, capacity=10000):
        self.path, self.contexts, self.capacity = path, contexts, capacity
        self.admission = Admission("OBSERVATION_TEST", basic_auth_header("OBSERVATION_TEST", "synthetic-only")[1])
        self.stack, self.reader, self.upstream, self.socket = AsyncExitStack(), None, None, None
        self.cloud_calls, self.cloud_counter, self.reject_report = [], 9000, False
        self.fixed_vendor = None
        self.start_errors = 0
        self.cloud_ledger, self.cloud_replies, self.cloud_errors = [], [], []
        self.firmware, self.drop_token, self.dropped = Firmware(), None, asyncio.Event()
        self.open_gateway()

    # Replacement opens the same real durable journal while preserving independent peer history and physical state.
    def open_gateway(self):
        self.j = Journal(self.path, self.admission.identity, self.capacity)
        self.g = Gateway(self.j, timeout=0.5)

    # Authentication finishes before any accepted charger frame can reach the actual gateway.
    async def charger_handler(self, connection):
        gateway = self.g

        # Lose a real transport result only after engine acceptance; do not replace the admission or journal logic.
        async def send(raw):
            frame = decode(raw)
            if frame[0] == 3 and frame[1] == self.drop_token:
                self.drop_token = None
                self.dropped.set()
                raise OSError("synthetic local reply transport loss")
            await connection.send(raw)

        epoch = gateway.attach_charger(send)
        try:
            async for raw in connection:
                await gateway.receive("charger", raw, epoch)
        finally:
            gateway.detach_charger(epoch)

    # Return actual independently assigned cloud transaction IDs and explicitly refuse only the chosen routine row.
    async def vendor_handler(self, connection):
        async for raw in connection:
            frame = decode(raw)
            if frame[0] != 2:
                continue
            self.cloud_calls.append(frame)
            action = frame[2]
            if action == "StartTransaction" and self.start_errors:
                self.start_errors -= 1
                await connection.send(build_call_error(frame[1], "InternalError", "synthetic Start refusal"))
                continue
            if self.reject_report and action in (
                "MeterValues",
                "StatusNotification",
                "FirmwareStatusNotification",
                "DiagnosticsStatusNotification",
            ):
                self.reject_report = False
                await connection.send(build_call_error(frame[1], "InternalError", "synthetic report refusal"))
                continue
            answer = {}
            if action == "BootNotification":
                answer = {"status": "Accepted", "interval": 60, "currentTime": "2030-01-01T00:00:00Z"}
            elif action == "StartTransaction":
                self.cloud_counter += 1
                answer = {
                    "transactionId": self.cloud_counter if self.fixed_vendor is None else self.fixed_vendor,
                    "idTagInfo": {"status": "Accepted"},
                }
                self.cloud_ledger.append(
                    {"token": frame[1], "start": frame[3], "reply": answer, "closed": False, "meters": [], "stop": None}
                )
            elif action in ("MeterValues", "StopTransaction") and "transactionId" in frame[3]:
                matches = [
                    entry
                    for entry in self.cloud_ledger
                    if entry["reply"]["transactionId"] == frame[3]["transactionId"] and not entry["closed"]
                ]
                if not matches and action == "MeterValues":
                    matches = [
                        entry
                        for entry in self.cloud_ledger
                        if entry["reply"]["transactionId"] == frame[3]["transactionId"]
                    ]
                if len(matches) != 1:
                    self.cloud_errors.append({"action": action, "payload": frame[3]})
                    await connection.send(
                        build_call_error(
                            frame[1], "PropertyConstraintViolation", "unknown independent cloud transaction"
                        )
                    )
                    continue
                entry = matches[0]
                if action == "MeterValues":
                    start_at = datetime.fromisoformat(entry["start"]["timestamp"].replace("Z", "+00:00"))
                    if any(
                        datetime.fromisoformat(value["timestamp"].replace("Z", "+00:00")) < start_at
                        for value in frame[3]["meterValue"]
                    ):
                        self.cloud_errors.append({"action": action, "payload": frame[3]})
                    entry["meters"].append(frame[3])
                else:
                    entry["closed"], entry["stop"] = True, frame[3]
            self.cloud_replies.append([3, frame[1], answer])
            await connection.send(build_call_result(frame[1], answer))

    # Open both real TLS listeners before admitting their independent charger and cloud sockets.
    async def __aenter__(self):
        server_tls, _, _ = self.contexts
        self.server = await self.stack.enter_async_context(
            offline_server(self.charger_handler, self.admission, server_tls)
        )
        self.cloud = await self.stack.enter_async_context(
            serve(
                self.vendor_handler,
                "127.0.0.1",
                0,
                ssl=server_tls,
                subprotocols=["ocpp1.6"],
                logger=TRANSPORT_LOG,
            )
        )
        await self.connect(boot=True)
        return self

    # End live handlers before database close; all listeners and peers remain strictly task-owned.
    async def __aexit__(self, *exc):
        await self.disconnect()
        await self.close_upstream()
        await self.g.close()
        self.j.close()
        await self.stack.aclose()

    # A new socket need not send Boot before retransmitting a previously unanswered observation.
    async def connect(self, *, boot=False):
        _, client_tls, _ = self.contexts
        self.socket = await self.stack.enter_async_context(
            connect(
                f"wss://127.0.0.1:{self.server.sockets[0].getsockname()[1]}/charger/{self.admission.identity}",
                ssl=client_tls,
                additional_headers=[("Authorization", self.admission.authorization)],
                subprotocols=["ocpp1.6"],
                logger=TRANSPORT_LOG,
                proxy=None,
            )
        )
        await until(lambda: self.g.charger.sender is not None)
        if boot:
            assert (await self.call(uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3

    # Wait for the actual server to retire this reader before replacing its gateway or opening another socket.
    async def disconnect(self):
        if self.socket is not None:
            await self.socket.close()
            await until(lambda: self.g.charger.sender is None)
            self.socket = None

    # The independent device answers profiles from physical fake state while receiving its own correlated reply.
    async def call(self, token, action, payload):
        await self.socket.send(build_call(token, action, payload))
        result = await charger_reply(self.socket, token, self.firmware)
        if result[0] == 3 and action == "StartTransaction":
            self.firmware.active = result[2]["transactionId"]
        elif result[0] == 3 and action == "StopTransaction" and self.firmware.active == payload["transactionId"]:
            self.firmware.active, self.firmware.requested_current = None, 0
        return result

    # The reply sender raises only after the real journal commit; the client observes no fabricated success.
    async def lose_reply(self, token, action, payload):
        self.drop_token = token
        self.dropped.clear()
        await self.socket.send(build_call(token, action, payload))
        await asyncio.wait_for(self.dropped.wait(), 2)
        await self.g.drain()

    # Stop the independent cloud reader before reopening its endpoint around a gateway process replacement.
    async def close_upstream(self):
        if self.reader is not None:
            self.reader.cancel()
            await asyncio.gather(self.reader, return_exceptions=True)
            self.reader = None
        if self.upstream is not None:
            await self.upstream.close()
            self.upstream = None

    # A gateway restart preserves the private database and never resets the independent backend's ID allocator.
    async def replace(self, *, restart=False, boot=False):
        await self.disconnect()
        if restart:
            await self.close_upstream()
            await self.g.close()
            self.j.close()
            self.open_gateway()
        await self.connect(boot=boot)

    # Actual cloud responses are processed by a separate receiver while the production FIFO performs delivery.
    async def flush(self):
        if self.upstream is None:
            self.upstream = await self.stack.enter_async_context(
                offline_vendor(
                    f"wss://127.0.0.1:{self.cloud.sockets[0].getsockname()[1]}/ocpp",
                    self.admission,
                    self.contexts[1],
                )
            )
            self.g.attach_vendor(self.upstream.send)
            gateway, upstream = self.g, self.upstream

            # The peer's actual correlation frames are delivered without sharing gateway RPC futures.
            async def receive():
                async for raw in upstream:
                    await gateway.receive("vendor", raw)

            self.reader = asyncio.create_task(receive())
        await self.g.flush()


# Produce six valid external payload classes without consulting a gateway receipt or expected publication count.
def observation_payload(kind, local):
    if kind.startswith("meter-"):
        payload = {
            "connectorId": 1,
            "meterValue": [{"timestamp": "2026-01-01T00:01:00Z", "sampledValue": [{"value": "1200"}]}],
        }
        if kind == "meter-transaction":
            payload["transactionId"] = local
        return "MeterValues", payload
    if kind.startswith("status-"):
        payload = {"connectorId": 1, "status": "Charging", "errorCode": "NoError"}
        if kind == "status-timed":
            payload["timestamp"] = "2026-01-01T00:01:00Z"
        return "StatusNotification", payload
    return (
        ("FirmwareStatusNotification", {"status": "Downloaded"})
        if kind == "firmware"
        else ("DiagnosticsStatusNotification", {"status": "Uploaded"})
    )


# Close the original lifecycle and prove two independently mapped later Start/Meter/Stop histories work normally.
async def observation_suffix(peer, original):
    assert (
        await peer.call(
            uuid.uuid4().hex,
            "StopTransaction",
            {
                "transactionId": original,
                "meterStop": 1300,
                "timestamp": "2026-01-01T00:02:00Z",
            },
        )
    )[0] == 3
    await peer.flush()
    later = []
    for number in range(2):
        stamp = f"2026-01-02T00:0{number}:00Z"
        result = await peer.call(uuid.uuid4().hex, "StartTransaction", START | {"timestamp": stamp})
        assert result[0] == 3
        local = result[2]["transactionId"]
        later.append(local)
        action, payload = observation_payload("meter-transaction", local)
        payload["meterValue"][0]["timestamp"] = (
            datetime.fromisoformat(stamp.replace("Z", "+00:00")) + timedelta(seconds=30)
        ).isoformat()
        assert (await peer.call(uuid.uuid4().hex, action, payload))[0] == 3
        assert (
            await peer.call(
                uuid.uuid4().hex,
                "StopTransaction",
                {
                    "transactionId": local,
                    "meterStop": 1300,
                    "timestamp": stamp,
                },
            )
        )[0] == 3
        await peer.flush()
        assert peer.j.vendor_id(local) is not None and peer.j.next_delivery() is None
        expected = next(entry for entry in peer.cloud_ledger if entry["start"] == START | {"timestamp": stamp})
        assert expected["closed"] and expected["stop"]["transactionId"] == expected["reply"]["transactionId"]
        assert (
            len(expected["meters"]) == 1
            and expected["meters"][0]["transactionId"] == expected["reply"]["transactionId"]
        )
        assert not peer.cloud_errors
    assert len({original, *later}) == 3
