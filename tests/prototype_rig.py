"""In-process protocol peers backed by a real journal and independent device firmware."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ocpp_hub_core.frames import build_call, build_call_result
from ocpp_hub_core.prototype.gateway import Gateway, decode
from ocpp_hub_core.prototype.policy import Policy
from ocpp_hub_core.prototype.store import Journal

from tests.prototype_firmware import Firmware

START = {"connectorId": 1, "idTag": "LOCAL_TEST", "meterStart": 1000, "timestamp": "2026-01-01T00:00:00Z"}


BOOT = {"chargePointVendor": "SYNTHETIC", "chargePointModel": "OFFLINE_TEST"}


class Rig:
    """Synthetic peers observe the real engine and supply configurable results."""

    # Attach in-memory transports without mocking any gateway or journal behavior.
    def __init__(self, path: Path, policy: Policy | None = None, attach: bool = True) -> None:
        self.store = Journal(path, "OFFLINE001")
        self.gateway = Gateway(self.store, policy, timeout=0.08)
        self.charger_frames: list[list[Any]] = []
        self.vendor_frames: list[list[Any]] = []
        self.charger_result: dict[str, Any] = {"status": "Accepted"}
        self.vendor_result: dict[str, Any] = {"status": "Accepted", "data": "synthetic reply"}
        self.drop_cloud_start = False
        self.drop_charger = False
        self.crash_reply = False
        self.next_vendor = 9000
        self.firmware = Firmware()
        if attach:
            self.gateway.attach_charger(self.charger_send)

    # Record charger-bound requests and return synthetic peer results under their actual correlation id.
    async def charger_send(self, raw: str) -> None:
        frame = decode(raw)
        self.charger_frames.append(frame)
        if frame[0] != 2:
            if self.crash_reply:
                raise OSError("simulated crash before local ACK delivery")
            return
        if not self.drop_charger:
            answer = self.charger_result
            if frame[2] == "GetCompositeSchedule":
                answer = self.firmware.answer(frame[2], frame[3])
            elif frame[2] == "SetChargingProfile" and answer.get("status") == "Accepted":
                self.firmware.answer(frame[2], frame[3])
            await self.gateway.receive("charger", build_call_result(frame[1], answer))

    # Record cloud-bound messages; each synthetic Start receives an independently assigned cloud id.
    async def vendor_send(self, raw: str) -> None:
        frame = decode(raw)
        self.vendor_frames.append(frame)
        if frame[0] != 2:
            return
        if frame[2] == "StartTransaction":
            if self.drop_cloud_start:
                return
            self.next_vendor += 1
            answer = {"transactionId": self.next_vendor, "idTagInfo": {"status": "Accepted"}}
        elif frame[2] == "BootNotification":
            answer = {"status": "Accepted", "interval": 60, "currentTime": "2030-01-01T00:00:00Z"}
        else:
            answer = self.vendor_result
        await self.gateway.receive("vendor", build_call_result(frame[1], answer))

    # Introduce the synthetic charger without automatically connecting its cloud mirror.
    async def boot(self) -> None:
        await self.charger("boot", "BootNotification", BOOT)

    # Submit one charger CALL and wait for its actual response task.
    async def charger(self, token: str, action: str, payload: dict[str, Any]) -> list[Any]:
        await self.gateway.receive("charger", build_call(token, action, payload))
        await self.gateway.drain()
        answer = next(frame for frame in reversed(self.charger_frames) if frame[0] != 2 and frame[1] == token)
        if action == "StartTransaction" and answer[0] == 3:
            self.firmware.active = self.store.active()
        elif action == "StopTransaction" and answer[0] == 3 and self.firmware.active == payload["transactionId"]:
            self.firmware.active = None
            self.firmware.requested_current = 0
        return answer

    # Submit one vendor CALL and wait for its real charger reply or explicit refusal.
    async def vendor(self, token: str, action: str, payload: dict[str, Any]) -> list[Any]:
        await self.gateway.receive("vendor", build_call(token, action, payload))
        await self.gateway.drain()
        return next(frame for frame in reversed(self.vendor_frames) if frame[0] != 2 and frame[1] == token)

    # Connect only the synthetic vendor sender and deliver the journal in order.
    async def cloud(self) -> None:
        self.gateway.attach_vendor(self.vendor_send)
        await self.gateway.flush()

    # Stop handlers before closing the private journal.
    async def close(self) -> None:
        await self.gateway.close()
        self.store.close()
