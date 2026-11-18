"""Independent fake physical charger and cloud over real TLS for commissioning."""

from __future__ import annotations

import asyncio
import copy
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from ocpp_hub_core.frames import build_call, build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.security import TRANSPORT_LOG
from ocpp_hub_core.prototype.store import fingerprint
from ocpp_hub_core.prototype.trial_allocation import observe_history
from ocpp_hub_core.prototype.trial_evidence import Evidence, Resource, native_profile_projection
from ocpp_hub_core.prototype.trial_session import TrialSession
from websockets.asyncio.client import connect
from websockets.asyncio.server import ServerConnection

from tests.prototype_firmware import Firmware
from tests.prototype_rig import BOOT
from tests.trial_helpers import native_profile


# Produce fresh actual-looking metadata without treating fixture-shifted 2030 dates as the test clock.
def info(**changes: Any) -> dict[str, Any]:
    return {
        "status": "Accepted",
        "expiryDate": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        "parentIdTag": "PARENT",
        **changes,
    }


class Bench:
    """Device and backend own their own state; neither reads the proxy's receipt table to fabricate effects."""

    # Keep one fake physical device alive across proxy process/session replacements.
    def __init__(self) -> None:
        self.firmware = Firmware()
        self.calls: list[list[Any]] = []
        self.sent_vendor_replies: list[list[Any]] = []
        self.configuration = "trial"
        self.cp, self.connection = "A", "Unplug"
        self.online = True
        self.remote_stop = asyncio.Event()
        self.replies: dict[str, asyncio.Future[list[Any]]] = {}
        self.reader: asyncio.Task[None] | None = None
        self.received_profiles: list[int] = []
        self.received_limits: list[float] = []
        self.device_calls: list[list[Any]] = []
        self.composite_reply_delay = 0.0
        self.observation_failure = False
        self.history: list[dict[str, Any]] = []
        self.next_vendor = 9000
        self.boot_on_probe = False
        self.auto_boots = 0
        self.before_reply: Callable[[str, dict[str, Any]], Awaitable[None]] | None = None

    # Use the actual fake firmware's profile list as independent native inventory evidence.
    async def observe(self) -> Evidence:
        if self.observation_failure:
            raise OSError("synthetic native observation failure")
        resources = []
        for identifier, profile in self.firmware.profiles.items():
            connector = 0 if profile["chargingProfilePurpose"] == "ChargePointMaxProfile" else 1
            body = native_profile(profile, connector)
            resources.append(
                Resource(
                    "profile",
                    identifier,
                    body,
                    native_profile_projection(body, connector, body["purpose"], time.time()),
                )
            )
        resources.extend(
            Resource("plan", identifier, copy.deepcopy(body), {"unsupported": True})
            for identifier, body in self.firmware.plans.items()
        )
        return Evidence(
            fingerprint("OFFLINE001"),
            time.time(),
            self.cp,
            self.connection,
            self.firmware.current,
            self.firmware.current * 240,
            self.firmware.active is not None,
            fingerprint(self.configuration),
            tuple(resources),
            self.online,
            self.firmware.active,
            observe_history(self.history, "OFFLINE001", "HARDWARE001", time.time()),
        )

    # The fake cloud answers real correlated frames without sharing transaction mappings with the local gateway.
    async def vendor(self, connection: ServerConnection) -> None:
        async for raw in connection:
            frame = decode(raw)
            self.calls.append(frame)
            if frame[0] != 2:
                continue
            if frame[2] == "BootNotification":
                answer = {"status": "Accepted", "interval": 1, "currentTime": "2030-01-01T00:00:00Z"}
            elif frame[2] == "Authorize":
                answer = {"idTagInfo": info()}
            elif frame[2] == "StartTransaction":
                self.next_vendor += 1
                answer = {"transactionId": self.next_vendor, "idTagInfo": info()}
            elif frame[2] == "Heartbeat":
                answer = {"currentTime": "2030-01-01T00:00:00Z"}
            else:
                answer = {}
            await connection.send(build_call_result(frame[1], answer))
            self.sent_vendor_replies.append([3, frame[1], copy.deepcopy(answer)])

    # Read replies independently from server commands, so local control and lifecycle RPCs cannot deadlock the peer.
    async def read_charger(self, charger: Any) -> None:
        async for raw in charger:
            frame = decode(raw)
            if frame[0] != 2:
                self.replies[frame[1]].set_result(frame)
                continue
            action, payload = frame[2:]
            self.device_calls.append(copy.deepcopy(frame))
            answer = self.firmware.answer(action, payload)
            if action == "GetCompositeSchedule":
                await asyncio.sleep(self.composite_reply_delay)
            if action == "SetChargingProfile":
                self.received_profiles.append(payload["csChargingProfiles"]["chargingProfileId"])
                self.received_limits.append(
                    payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
                )
            elif action == "RemoteStopTransaction":
                self.remote_stop.set()
            if self.before_reply is not None:
                await self.before_reply(action, payload)
            await charger.send(build_call_result(frame[1], answer))
            if (
                action == "TriggerMessage"
                and payload.get("requestedMessage") == "BootNotification"
                and self.boot_on_probe
            ):
                self.auto_boots += 1
                token = "device-boot-" + str(self.auto_boots)
                self.replies[token] = asyncio.get_running_loop().create_future()
                await charger.send(build_call(token, "BootNotification", BOOT))

    # Submit an actual device CALL while the separate reader continues answering proxy RPCs.
    async def call(self, charger: Any, token: str, action: str, payload: dict[str, Any]) -> list[Any]:
        future: asyncio.Future[list[Any]] = asyncio.get_running_loop().create_future()
        self.replies[token] = future
        await charger.send(build_call(token, action, payload))
        answer = await asyncio.wait_for(future, 2)
        if answer[0] == 3 and action == "StartTransaction":
            self.firmware.active = answer[2]["transactionId"]
            if not any(row["transactionId"] == self.firmware.active for row in self.history):
                self.history.append({"transactionId": self.firmware.active, "start": copy.deepcopy(payload)})
        elif answer[0] == 3 and action == "StopTransaction":
            self.firmware.active = None
            self.firmware.requested_current = 0
            if self.cp == "C":
                self.cp, self.connection = "B", "Plug"
            for row in self.history:
                if row["transactionId"] == payload["transactionId"]:
                    row["stop"] = copy.deepcopy(payload)
        return answer

    # A new authenticated charger socket represents the same independent physical device after proxy restart.
    def connection_to(self, session: TrialSession) -> Any:
        config = session.prepared.config
        return connect(
            f"wss://127.0.0.1:{config.listener_port}" + config.charger_path,
            ssl=session.prepared.tls.client,
            additional_headers=[]
            if session.prepared.tls.incoming is None
            else [("Authorization", session.prepared.tls.incoming)],
            subprotocols=["ocpp1.6"],
            logger=TRANSPORT_LOG,
            proxy=None,
        )
