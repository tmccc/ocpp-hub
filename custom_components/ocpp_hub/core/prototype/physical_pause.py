"""Fresh, reversible car refusal under an actual positive6A offer; never a durable permission lease."""

from __future__ import annotations

import math
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .security import SecurityError
from .store import fingerprint
from .trial_evidence import NativeObserver
from .trial_handover_evidence import HandoverEvidence, require_digest


@dataclass(frozen=True, repr=False)
class PhysicalPauseSample:
    """A complete direct native read is bracketed by this process's actual monotonic clock."""

    native: HandoverEvidence
    started: float
    completed: float


class PhysicalPauseObserver:
    """The existing read-only observer remains the sole source of native facts."""

    # No socket or credential operation is added merely by constructing the physical verifier.
    def __init__(self, observer: NativeObserver) -> None:
        self.observer = observer

    # Bracket a distinct full read instead of assigning a fresh time to an old object or API cache.
    async def observe(self) -> PhysicalPauseSample:
        started = time.monotonic()
        native = await self.observer.observe_handover()
        return PhysicalPauseSample(native, started, time.monotonic())


class PhysicalPauseVerifier:
    """Charging→Stop→zero offer window→Start→charging→Stop→zero offer window establishes reversible behaviour."""

    # Process replacement intentionally loses current authority; private historical JSON cannot initialise a proof.
    def __init__(
        self,
        identity: str,
        hardware: str,
        vehicle_digest: str,
        nonce: str,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        require_digest(vehicle_digest)
        if not isinstance(nonce, str) or re.fullmatch(r"[a-f0-9]{32}", nonce) is None:
            raise SecurityError("physical pause requires a fresh challenge nonce")
        self.identity, self.hardware, self.vehicle_digest, self.nonce = identity, hardware, vehicle_digest, nonce
        self.clock = clock
        self.created = clock()
        self.phase = "awaiting_charging"
        self.context: str | None = None
        self.last_sample: PhysicalPauseSample | None = None
        self.command_at = self.created
        self.operations: set[str] = set()
        self.window_started: float | None = None
        self.window_count = 0
        self.first_window_samples = 0
        self.completed_challenge = False

    # Bind every sample to actual settings, complete resources/history, native session and the known vehicle.
    def context_digest(self, native: HandoverEvidence) -> str:
        # The active row's MeterValues is live telemetry; every completed row and all other fields stay pinned.
        history = [
            {
                key: value
                for key, value in row.items()
                if not (
                    row.get("transactionId") == native.physical.native_transaction_id
                    and type(row.get("stopTime")) is int
                    and row.get("stopTime") == 0
                    and key == "MeterValues"
                )
            }
            for row in native.history
        ]
        return fingerprint(
            {
                "identity": native.physical.identity_digest,
                "hardware": native.hardware_digest,
                "vehicle": self.vehicle_digest,
                "challenge": self.nonce,
                "configuration": native.configuration,
                "resources": [resource.digest for resource in native.physical.resources],
                "history": history,
                "saved_schedule": native.saved_schedule_digest,
                "transaction": native.physical.native_transaction_id,
            }
        )

    # A schedule-driven Start, charger-side Stop, dropped offer, changed phase or replay invalidates the current proof.
    def require_sample(self, sample: PhysicalPauseSample) -> None:
        now, native = self.clock(), sample.native
        native.physical.require_fresh(self.identity, 5)
        require_digest(native.saved_schedule_digest)
        dates = (sample.started, sample.completed)
        if (
            any(type(v) not in (float, int) or not math.isfinite(v) for v in dates)
            or not self.command_at <= sample.started <= sample.completed <= now
            or sample.completed - sample.started > 2
            or now - sample.completed > 2
            or now - self.created > 600
            or native.hardware_digest != fingerprint(self.hardware)
            or native.configuration.get("Id") != self.identity
            or native.configuration.get("bPSN") != self.hardware
            or native.configuration.get("AmpLmt") != "6"
            or native.configuration.get("maxOutCurrent0") != "32"
            or native.saved_user_a != 6
            or native.runtime_user_a != 6
            or native.runtime_hardware_a != 32
            or native.physical.upstream_connected is not True
            or native.offered_current_a != 6
            or native.output_limit_a != 6
            or native.physical.cp not in {"B", "C"}
            or native.physical.connection not in {"Plug", "Charging"}
            or not native.physical.transaction_open
            or native.physical.native_transaction_id is None
            or len(native.currents_a) != 3
            or len(native.powers_w) != 4
            or any(type(v) not in (float, int) or not math.isfinite(v) or not 0 <= v <= 6.5 for v in native.currents_a)
            or any(type(v) not in (float, int) or not math.isfinite(v) or not 0 <= v <= 2000 for v in native.powers_w)
            or native.physical.current_a != max(native.currents_a)
            or native.physical.power_w != max(native.powers_w)
        ):
            raise SecurityError("physical pause requires fresh complete native evidence under a positive6A offer")
        if self.context is not None and self.context_digest(native) != self.context:
            raise SecurityError("physical pause native session, settings or inventory changed")
        if self.last_sample is not None and (
            sample.started <= self.last_sample.completed
            or sample.started - self.last_sample.completed > 7
            or native.physical.observed_at <= self.last_sample.native.physical.observed_at
        ):
            raise SecurityError("physical pause observations were replayed, interrupted or not distinct")

    # Require genuine nominal6A draw; a full car, absent demand or an accepted Start cannot pass this transition.
    def charging(self, native: HandoverEvidence) -> bool:
        return (
            native.physical.cp == "C"
            and native.physical.connection == "Charging"
            and 5 <= max(native.currents_a) <= 6.5
            and max(native.powers_w) >= 800
        )

    # Zero means every current and power channel is exactly zero while the positive offer remains independently proved.
    def refusing(self, native: HandoverEvidence) -> bool:
        return (
            native.physical.cp == "B"
            and native.physical.connection in {"Plug", "Charging"}
            and all(v == 0 for v in native.currents_a + native.powers_w)
        )

    # Record one already-journalled official-app control intent; an uncertain click must be reconciled, never replayed.
    def control_intent(self, action: str, operation: str) -> None:
        if (
            not isinstance(operation, str)
            or re.fullmatch(r"[a-f0-9]{32}", operation) is None
            or operation in self.operations
        ):
            raise SecurityError("physical pause control intent requires a new operation nonce")
        expected = {"charging_initial": "stop", "paused_first": "start", "charging_returned": "stop"}
        if (
            expected.get(self.phase) != action
            or self.last_sample is None
            or not 0 <= self.clock() - self.last_sample.completed <= 5
        ):
            self.supersede()
            raise SecurityError("physical pause control does not match the observed challenge stage")
        self.operations.add(operation)
        self.command_at = self.clock()
        self.phase = {
            "charging_initial": "stopping_first",
            "paused_first": "resuming",
            "charging_returned": "stopping_final",
        }[self.phase]
        self.window_started, self.window_count = None, 0
        self.last_sample = None

    # Consume an actual fresh sample; terminal contradictions cannot be repaired by simply waiting for another zero.
    def observe(self, sample: PhysicalPauseSample) -> dict[str, Any]:
        try:
            if self.phase in {"inconclusive", "superseded"}:
                raise SecurityError("physical pause challenge is no longer current")
            self.require_sample(sample)
            native = sample.native
            if (
                self.phase in {"stopping_first", "stopping_final", "resuming"}
                and sample.completed - self.command_at > 90
            ):
                raise SecurityError("physical car control did not return the expected state within its bounded window")
            if self.phase == "awaiting_charging":
                if not self.charging(native):
                    raise SecurityError("physical pause requires real charging before the first car Stop")
                self.context = self.context_digest(native)
                self.phase = "charging_initial"
            elif self.phase == "resuming" and self.charging(native):
                self.phase = "charging_returned"
            elif self.phase in {"charging_initial", "charging_returned"} and not self.charging(native):
                raise SecurityError("real charging ceased before the next owned car control")
            elif self.phase in {"stopping_first", "stopping_final"}:
                if self.refusing(native):
                    self.window_started = sample.completed
                    self.window_count = 1
                    self.phase = "zero_first" if self.phase == "stopping_first" else "zero_final"
                elif sample.completed - self.command_at > 90:
                    raise SecurityError("physical car Stop was not observed within its bounded settling window")
            elif self.phase in {"zero_first", "zero_final", "paused_first", "observed"}:
                if not self.refusing(native):
                    raise SecurityError("car ceased refusing the positive charging offer")
                self.window_count += 1
                if self.window_started is not None and sample.completed - self.window_started >= 60:
                    if self.window_count < 13:
                        raise SecurityError("physical pause has too few distinct positive-offer zero samples")
                    if self.phase == "zero_first":
                        self.first_window_samples = self.window_count
                        self.phase = "paused_first"
                    elif self.phase == "zero_final":
                        self.phase, self.completed_challenge = "observed", True
            elif self.phase == "resuming" and sample.completed - self.command_at > 90:
                raise SecurityError("real charging did not return; physical pause challenge is inconclusive")
            self.last_sample = sample
        except SecurityError:
            self.phase = "inconclusive"
            raise
        return self.summary()

    # Any failed native read or subsequent original-permission restoration immediately retires current evidence.
    def supersede(self) -> None:
        self.phase = "superseded"

    # A successful challenge is historical after restore/expiry; no permission revision or durable lease is invented.
    def summary(self) -> dict[str, Any]:
        fresh = self.last_sample is not None and 0 <= self.clock() - self.last_sample.started <= 5
        current = self.phase == "observed" and fresh
        return {
            "physical_pause_phase": self.phase,
            "current_vehicle_refusal_verified": current,
            "reversible_challenge_passed": self.completed_challenge,
            "fresh_native_read_required": not current,
            "native_observed_at": self.last_sample.native.physical.observed_at if self.last_sample else None,
            "current_evidence_expires_at": self.last_sample.native.physical.observed_at + 5
            if current and self.last_sample
            else None,
            "first_zero_samples": self.first_window_samples,
            "last_zero_samples": self.window_count,
            "sampled_zero_seconds": 0
            if self.last_sample is None or self.window_started is None
            else max(0, self.last_sample.completed - self.window_started),
            "current_pause_permission_verified": False,
            "durable_pause_verified": False,
            "handover_authority": False,
        }
