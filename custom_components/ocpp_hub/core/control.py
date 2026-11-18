"""Writes to a charger. Everything that changes what a charger does goes through here, under the owner's ceiling."""

from __future__ import annotations

import logging
import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from . import commands
from .session import CallError, CallRejectedError, ChargerSession

_LOGGER = logging.getLogger(__name__)

# A charger below this current does not charge a car, so a wish between zero and this value means pause.
CHARGING_MIN_A = 6.0
# The id of the one charging profile the hub installs, so that it replaces its own earlier limit and never somebody
# else's profile.
HUB_PROFILE_ID = 7001
# The tag used when Home Assistant starts a charge. The hub accepts every tag, and the history shows who started it.
HUB_ID_TAG = "OCPPHUB"
# The connector of the socket; connector 0 is the charger as a whole.
SOCKET_CONNECTOR = 1
STATION_CONNECTOR = 0


# Text from a charger, made safe to put in a log line or a message: printable, one line, and short.
def plain_text(value: str, limit: int = 120) -> str:
    return " ".join("".join(c for c in value if c.isprintable() or c.isspace()).split())[:limit]


class ControlError(Exception):
    """A write was refused or failed. `code` is a stable key for translations, `detail` has the numbers."""

    # Keep the stable code and a human detail.
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {plain_text(detail)}" if detail else code)
        self.code = code
        self.detail = plain_text(detail)


@dataclass(frozen=True, slots=True)
class ControlPolicy:
    """What the owner allows the hub to do to a charger. The ceiling is the hardware limit's side of the bargain."""

    # The highest current per phase the hub may ask for, below the installer's own limit. It is never raised from here.
    max_current_a: float = 16.0

    # Refuse a ceiling that cannot be right, so a typo cannot widen what is allowed.
    def __post_init__(self) -> None:
        if not math.isfinite(self.max_current_a) or not CHARGING_MIN_A <= self.max_current_a <= 80.0:
            raise ValueError(f"max_current_a must be between {CHARGING_MIN_A:g} and 80, got {self.max_current_a!r}")


# The profile purposes to try for a limit, best first. A station-wide ceiling covers a running charge and every later
# one. The per-charge profiles are the fallback for a charger that refuses it, and only the one that fits the moment
# counts: a charge that runs needs a TxProfile, because a TxDefaultProfile only applies to charges that start later and
# its "Accepted" would say nothing about the charge in progress. The purpose learned earlier goes first, but only among
# those.
def _purposes(transaction_id: int | None, learned: str | None) -> list[tuple[str, int]]:
    ladder = [(commands.PURPOSE_CHARGE_POINT_MAX, STATION_CONNECTOR)]
    ladder.append(
        (commands.PURPOSE_TX, SOCKET_CONNECTOR)
        if transaction_id is not None
        else (commands.PURPOSE_TX_DEFAULT, SOCKET_CONNECTOR)
    )
    if learned is not None:
        ladder.sort(key=lambda step: step[0] != learned)
    return ladder


# The longest schedule the hub installs, and the seconds in a day (a daily schedule has no period at or after it).
MAX_SCHEDULE_PERIODS = 24
MAX_WEEKLY_PERIODS = 96
SECONDS_PER_DAY = 86400


# Check a schedule given as (seconds after its start, wished current), over a day or a week, and return it with every
# current turned into what is sent: the first period must start at 0, starts must grow and stay within the span, and
# every current obeys the same rules as a single limit (the ceiling, and under 6 A meaning pause). Nothing is
# repaired.
def checked_schedule(
    periods: Sequence[tuple[int, object]],
    policy: ControlPolicy,
    span_s: int = SECONDS_PER_DAY,
    max_periods: int = MAX_SCHEDULE_PERIODS,
) -> list[tuple[int, float]]:
    if not 1 <= len(periods) <= max_periods:
        raise ControlError("invalid_schedule", f"a schedule has 1 to {max_periods} periods")
    checked: list[tuple[int, float]] = []
    for start, wish in periods:
        if isinstance(start, bool) or not isinstance(start, int) or not 0 <= start < span_s:
            raise ControlError("invalid_schedule", f"{start!r} is not a time of the day")
        if (not checked and start != 0) or (checked and start <= checked[-1][0]):
            raise ControlError("invalid_schedule", "the first period starts at 00:00 and the starts must increase")
        checked.append((start, effective_current_a(wish, policy)))
    return checked


# The current to ask the charger for: the wish, or zero (pause) when the wish is below what a car needs to charge.
# The wish is checked, not repaired: a number above the ceiling, not a number, or negative is refused.
def effective_current_a(wish: object, policy: ControlPolicy) -> float:
    if isinstance(wish, bool) or not isinstance(wish, (int, float)) or not math.isfinite(wish) or wish < 0:
        raise ControlError("invalid_value", f"{wish!r} is not a current")
    if wish > policy.max_current_a:
        raise ControlError("above_ceiling", f"{wish:g} A is above the ceiling of {policy.max_current_a:g} A")
    return 0.0 if wish < CHARGING_MIN_A else float(wish)


class ChargerControl:
    """The writes to one connected charger. It lives as long as the charger's connection."""

    # Keep the live session and the policy.
    def __init__(self, session: ChargerSession, policy: ControlPolicy) -> None:
        self._session = session
        self._policy = policy

    @property
    def policy(self) -> ControlPolicy:
        """The ceiling this control enforces."""
        return self._policy

    # Ask the charger to limit its current per phase. Below the minimum a car charges at, the limit is zero (pause).
    # The purposes are tried in turn until the charger accepts one, and the accepted one is remembered, because the
    # engine records what a charger accepts instead of assuming it. Returns the current that was applied.
    async def set_current_limit(self, wish: object) -> float:
        amps = effective_current_a(wish, self._policy)
        snapshot = self._session.snapshot
        connector = snapshot.connectors.get(SOCKET_CONNECTOR)
        transaction_id = connector.transaction_id if connector is not None else None
        outcomes: list[str] = []
        for purpose, connector_id in _purposes(transaction_id, snapshot.limit_purpose):
            payload = commands.set_charging_profile(
                connector_id, purpose, amps, HUB_PROFILE_ID, transaction_id=transaction_id
            )
            status = await self._ask("SetChargingProfile", payload)
            outcomes.append(f"{purpose}={status}")
            if status == "Accepted":
                self._session.record_limit(amps, purpose)
                self._session.request_effective_limit()
                _LOGGER.info(
                    "[write] Charge limit %s A (asked %s A) accepted by %s as %s",
                    amps,
                    wish,
                    self._session.cp_id,
                    purpose,
                )
                return amps
        _LOGGER.warning("[write] Charge limit %s A refused by %s: %s", amps, self._session.cp_id, ", ".join(outcomes))
        raise ControlError("rejected", ", ".join(outcomes))

    # Install a daily schedule: a recurring profile that starts at `anchor` (the start of today, local time) and repeats
    # every 24 hours. It replaces the hub's single limit, which uses the same profile id. The station-wide purpose is
    # tried first, then the default one; a charge profile cannot recur, so it is not used. Returns the schedule as sent.
    async def set_daily_schedule(
        self, anchor: datetime, periods: Sequence[tuple[int, object]]
    ) -> list[tuple[int, float]]:
        return await self.set_schedule(anchor, periods, "Daily")

    # Install a recurring schedule, daily or weekly, with the same rules as a daily one. The number of periods is held
    # to what the charger says it supports (ChargingScheduleMaxPeriods) when the hub knows it. A weekly schedule starts
    # on the weekday of `anchor`.
    async def set_schedule(
        self, anchor: datetime, periods: Sequence[tuple[int, object]], recurrency: str, *, from_plan: bool = False
    ) -> list[tuple[int, float]]:
        weekly = recurrency == "Weekly"
        known = self._session.snapshot.configuration.get("ChargingScheduleMaxPeriods", "")
        limit = int(known) if known.isascii() and known.isdigit() and len(known) <= 6 and int(known) > 0 else None
        cap = MAX_WEEKLY_PERIODS if weekly else MAX_SCHEDULE_PERIODS
        schedule = checked_schedule(
            periods,
            self._policy,
            SECONDS_PER_DAY * 7 if weekly else SECONDS_PER_DAY,
            min(cap, limit) if limit is not None else cap,
        )
        anchor_iso = anchor.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        learned = self._session.snapshot.limit_purpose
        ladder = [
            (commands.PURPOSE_CHARGE_POINT_MAX, STATION_CONNECTOR),
            (commands.PURPOSE_TX_DEFAULT, SOCKET_CONNECTOR),
        ]
        if learned is not None:
            ladder.sort(key=lambda step: step[0] != learned)
        outcomes: list[str] = []
        for purpose, connector_id in ladder:
            payload = commands.set_charging_schedule(
                connector_id, purpose, schedule, HUB_PROFILE_ID, anchor_iso, recurrency=recurrency
            )
            status = await self._ask("SetChargingProfile", payload)
            outcomes.append(f"{purpose}={status}")
            if status == "Accepted":
                self._session.record_limit(None, purpose, from_plan=from_plan)
                self._session.request_effective_limit()
                _LOGGER.info(
                    "[write] %s schedule %s accepted by %s as %s", recurrency, schedule, self._session.cp_id, purpose
                )
                return schedule
        _LOGGER.warning("[write] %s schedule refused by %s: %s", recurrency, self._session.cp_id, ", ".join(outcomes))
        raise ControlError("rejected", ", ".join(outcomes))

    # Remove the hub's own limit again, so the charger goes back to what it had.
    # Only the hub's profile id is cleared.
    async def clear_current_limit(self, *, from_plan: bool = False) -> None:
        status = await self._ask("ClearChargingProfile", commands.clear_charging_profile(HUB_PROFILE_ID))
        if status not in ("Accepted", "Unknown"):
            raise ControlError("rejected", f"ClearChargingProfile={status}")
        self._session.record_limit(None, None, from_plan=from_plan)
        self._session.request_effective_limit()
        _LOGGER.info("[write] Charge limit of %s cleared (%s)", self._session.cp_id, status)

    # Start a charge on the socket, as if a tag was presented. The car must be connected for the charger to accept.
    async def remote_start(self) -> None:
        status = await self._ask(
            "RemoteStartTransaction", commands.remote_start_transaction(HUB_ID_TAG, SOCKET_CONNECTOR)
        )
        _LOGGER.info("[write] Remote start on %s answered %s", self._session.cp_id, status)
        if status != "Accepted":
            raise ControlError("rejected", f"RemoteStartTransaction={status}")

    # Stop the charge that is running on the socket.
    async def remote_stop(self) -> None:
        connector = self._session.snapshot.connectors.get(SOCKET_CONNECTOR)
        if connector is None or connector.transaction_id is None:
            raise ControlError("no_charge", "no charge is running")
        status = await self._ask("RemoteStopTransaction", commands.remote_stop_transaction(connector.transaction_id))
        _LOGGER.info("[write] Remote stop on %s answered %s", self._session.cp_id, status)
        if status != "Accepted":
            raise ControlError("rejected", f"RemoteStopTransaction={status}")

    # Send one request and return the `status` of the answer. A CALLERROR counts as a refusal, so the next purpose can
    # be tried; a charger that does not answer or is gone is a failure of its own.
    async def _ask(self, action: str, payload: dict[str, object]) -> str:
        try:
            answer = await self._session.call(action, payload)
        except CallRejectedError as error:
            return plain_text(error.code, 40)
        except CallError as error:
            # A timeout does not say the charger ignored the request: it may have applied the limit and answered late.
            raise ControlError("unreachable", str(error)) from error
        status = answer.get("status")
        return plain_text(status, 40) if isinstance(status, str) else "NoStatus"
