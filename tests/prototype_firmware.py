"""Independent fake firmware with native timers and highest-stack-per-purpose OCPP profiles."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any


class Firmware:
    """The device stores plans separately from profiles, so a timer can run without any new network CALL."""

    # Begin with an ordinary 32 A single-phase charger and no session or profiles.
    def __init__(self, clock: Callable[[], datetime] | None = None) -> None:
        self.profiles: dict[int, dict[str, Any]] = {}
        self.plans: dict[int, dict[str, Any]] = {}
        self.active: int | None = None
        self.requested_current = 0.0
        self.timer_fires = 0
        self.clear_mode = "scoped"
        self.clock = clock or (lambda: datetime.now(UTC))
        self.profile_kinds = frozenset({"Relative", "Absolute", "Recurring"})
        self.configuration = {
            "NumberOfConnectors": "1",
            "SupportedFeatureProfiles": "Core,SmartCharging,RemoteTrigger",
            "ChargingScheduleAllowedChargingRateUnit": "Current,Power",
            "ChargeProfileMaxStackLevel": "16",
            "ChargingScheduleMaxPeriods": "96",
            "MaxChargingProfilesInstalled": "64",
        }

    # Derive time applicability from the fake device's own clock, without asking the proxy whether its cap is ready.
    def scheduled_limit(self, profile: dict[str, Any]) -> float | None:
        now = self.clock()
        for key, future in (("validFrom", True), ("validTo", False)):
            if key in profile:
                instant = datetime.fromisoformat(profile[key].replace("Z", "+00:00"))
                if (future and now < instant) or (not future and now >= instant):
                    return None
        schedule = profile["chargingSchedule"]
        elapsed = 0.0
        if profile["chargingProfileKind"] != "Relative":
            start = datetime.fromisoformat(schedule["startSchedule"].replace("Z", "+00:00"))
            elapsed = (now - start).total_seconds()
            if elapsed < 0:
                return None
            if profile["chargingProfileKind"] == "Recurring":
                elapsed %= 604800 if profile.get("recurrencyKind") == "Weekly" else 86400
        if "duration" in schedule and elapsed >= schedule["duration"]:
            return None
        applicable = [period for period in schedule["chargingSchedulePeriod"] if period["startPeriod"] <= elapsed]
        return float(applicable[-1]["limit"]) if applicable else None

    # The highest stack wins within each purpose, then the lowest applicable purpose wins overall.
    def effective_limit(self) -> float:
        purposes: dict[str, tuple[int, float]] = {}
        for profile in self.profiles.values():
            purpose = profile["chargingProfilePurpose"]
            if purpose == "TxProfile" and profile.get("transactionId") != self.active:
                continue
            level = profile["stackLevel"]
            limit = self.scheduled_limit(profile)
            if limit is None:
                continue
            if purpose not in purposes or level >= purposes[purpose][0]:
                purposes[purpose] = level, float(limit)
        if "TxProfile" in purposes:
            purposes.pop("TxDefaultProfile", None)
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
            try:
                if (
                    profile["chargingProfileKind"] not in self.profile_kinds
                    or type(profile["stackLevel"]) is not int
                    or not 0 <= profile["stackLevel"] <= int(self.configuration["ChargeProfileMaxStackLevel"])
                ):
                    return {"status": "Rejected"}
                if profile["chargingProfileKind"] != "Relative":
                    start = datetime.fromisoformat(profile["chargingSchedule"]["startSchedule"].replace("Z", "+00:00"))
                    if start.tzinfo is None:
                        return {"status": "Rejected"}
                if profile["chargingProfileId"] not in self.profiles and len(self.profiles) >= int(
                    self.configuration["MaxChargingProfilesInstalled"]
                ):
                    return {"status": "Rejected"}
            except KeyError, ValueError, TypeError:
                return {"status": "Rejected"}
            self.profiles[profile["chargingProfileId"]] = profile
        elif action == "GetConfiguration":
            keys = payload.get("key", list(self.configuration))
            return {
                "configurationKey": [
                    {"key": key, "readonly": True, "value": self.configuration[key]}
                    for key in keys
                    if key in self.configuration
                ],
                "unknownKey": [key for key in keys if key not in self.configuration],
            }
        elif action == "GetCompositeSchedule":
            return {
                "status": "Accepted",
                "connectorId": 1,
                "scheduleStart": self.clock().astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "chargingSchedule": {
                    "duration": payload["duration"],
                    "chargingRateUnit": "A",
                    "chargingSchedulePeriod": [{"startPeriod": 0, "limit": self.effective_limit()}],
                },
            }
        elif action == "ClearChargingProfile":
            existed = self.profiles.pop(payload["id"], None) is not None
            # These two fault modes are separate hypotheses, not established iCS firmware semantics.
            if self.clear_mode == "all_profiles":
                self.profiles.clear()
            elif self.clear_mode == "prune_inactive":
                self.profiles = {
                    key: profile
                    for key, profile in self.profiles.items()
                    if profile["chargingProfilePurpose"] != "TxProfile" or profile.get("transactionId") == self.active
                }
            return {"status": "Accepted" if existed else "Unknown"}
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
