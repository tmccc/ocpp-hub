"""Independent fake firmware with native timers and highest-stack-per-purpose OCPP profiles."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any


class Firmware:
    """The device stores plans separately from profiles, so a timer can run without any new network CALL."""

    # Begin with an ordinary 32 A single-phase charger and no session or profiles.
    def __init__(self) -> None:
        self.profiles: dict[int, dict[str, Any]] = {}
        self.plans: dict[int, dict[str, Any]] = {}
        self.active: int | None = None
        self.requested_current = 0.0
        self.timer_fires = 0

    # The highest stack wins within each purpose, then the lowest applicable purpose wins overall.
    def effective_limit(self) -> float:
        purposes: dict[str, tuple[int, float]] = {}
        for profile in self.profiles.values():
            purpose = profile["chargingProfilePurpose"]
            if purpose == "TxProfile" and profile.get("transactionId") != self.active:
                continue
            level = profile["stackLevel"]
            limit = profile["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"]
            if purpose not in purposes or level >= purposes[purpose][0]:
                purposes[purpose] = level, float(limit)
        return min([32.0, *(row[1] for row in purposes.values())])

    # A planned start is autonomous: it does not ask the proxy for permission or a new profile.
    def fire_plan(self, plan_id: int) -> None:
        self.timer_fires += 1
        self.requested_current = float(self.plans[plan_id]["limitCurrent"])

    # Model actual current separately from acknowledgements and requested setpoints.
    @property
    def current(self) -> float:
        return min(self.requested_current, self.effective_limit())

    # Execute actual synthetic peer commands and retain their device-side effects.
    def answer(self, action: str, payload: dict[str, Any]) -> dict[str, Any]:
        if action == "SetChargingProfile":
            profile = json.loads(json.dumps(payload["csChargingProfiles"]))
            self.profiles[profile["chargingProfileId"]] = profile
        elif action == "GetCompositeSchedule":
            return {
                "status": "Accepted",
                "connectorId": 1,
                "scheduleStart": datetime.now(UTC).isoformat(),
                "chargingSchedule": {
                    "duration": 1,
                    "chargingRateUnit": "A",
                    "chargingSchedulePeriod": [{"startPeriod": 0, "limit": self.effective_limit()}],
                },
            }
        elif action == "RemoteStopTransaction":
            self.requested_current = 0.0
        elif action == "DataTransfer":
            data = json.loads(payload["data"])
            if payload["messageId"] == "setPlan":
                self.plans[data["planId"]] = data
            elif payload["messageId"] == "clearPlans":
                for plan_id in data:
                    self.plans.pop(plan_id, None)
            return {"status": "Accepted", "data": ""}
        return {"status": "Accepted"}
