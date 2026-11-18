"""One charger connection: answers the charger's requests, tracks its state and sends requests of its own."""

from __future__ import annotations

import asyncio
import logging
import math
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Protocol

from websockets.exceptions import ConnectionClosed

from . import commands
from .frames import (
    ERROR_FORMATION_VIOLATION,
    ERROR_INTERNAL,
    ERROR_NOT_IMPLEMENTED,
    MSG_CALL,
    MSG_CALL_ERROR,
    MSG_CALL_RESULT,
    Frame,
    build_call,
    build_call_error,
    build_call_result,
    parse_frame,
)
from .metering import merge_samples, parse_meter_values, parse_timestamp
from .model import (
    MAX_STORED_LIMIT_A,
    ChargerInfo,
    ChargerSnapshot,
    ChargeSession,
    ChargeStats,
    ConnectorState,
    LimitMemory,
)
from .readings import PRIMARY_CONNECTOR

_LOGGER = logging.getLogger(__name__)

# Who sent a frame, as the listeners see it.
SENDER_CHARGER = "CP"
SENDER_HUB = "CSMS"

# The configuration keys read after a charger boots; asked in one request, the charger answers what it knows.
CONFIG_KEYS = [
    "SupportedFeatureProfiles",
    "MeterValuesSampledData",
    "MeterValueSampleInterval",
    "HeartbeatInterval",
    "NumberOfConnectors",
    "ChargeProfileMaxStackLevel",
    "ChargingScheduleAllowedChargingRateUnit",
    "ChargingScheduleMaxPeriods",
]


class Connection(Protocol):
    """The part of a websocket connection a session needs."""

    async def send(self, message: str) -> None: ...

    def __aiter__(self) -> AsyncIterator[str | bytes]: ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...


class SessionListener(Protocol):
    """Receives what happens on the charger connections. The Home Assistant layer and the mirror both implement it."""

    def on_connected(self, session: ChargerSession) -> None: ...

    def on_snapshot(self, snapshot: ChargerSnapshot) -> None: ...

    def on_frame(self, cp_id: str, sender: str, frame: Frame) -> None: ...

    def on_disconnected(self, cp_id: str) -> None: ...

    def on_session_finished(self, session: ChargeSession) -> None: ...


class CallError(Exception):
    """A request the hub sent to the charger did not get a usable answer."""


class CallTimeoutError(CallError):
    """The charger did not answer in time."""


class CallRejectedError(CallError):
    """The charger answered with a CALLERROR."""

    # Keep the code and description the charger gave.
    # Callers log them and tests assert on them.
    def __init__(self, code: str, description: str) -> None:
        super().__init__(f"{code}: {description}")
        self.code = code
        self.description = description


class FormationError(ValueError):
    """A request from the charger is missing a field or has one of the wrong type."""


@dataclass(frozen=True, slots=True)
class SessionSettings:
    """Timing and metering choices of a session."""

    heartbeat_interval_s: int = 300
    call_timeout_s: float = 15.0
    # A charger that has just connected ignores requests for a while (the NEO for about ten seconds, and a first
    # request at five seconds was never answered), so setup waits this long before its first try.
    setup_delay_s: float = 15.0
    # How long setup waits before each further try when the charger did not answer; no more tries after the last one.
    setup_retry_delays_s: tuple[float, ...] = (30.0, 60.0, 120.0, 300.0)
    # A charger that reconnects after the hub restarted often sends no BootNotification and no StatusNotification,
    # because the hub accepted it before. After this wait the hub asks for them itself.
    boot_wait_s: float = 15.0
    # How long the hub lets a requested BootNotification arrive before it judges what is still missing.
    trigger_settle_s: float = 3.0
    meter_sample_interval_s: int = 15
    measurands: str = "Energy.Active.Import.Register,Power.Active.Import,Current.Import,Voltage,Current.Offered"


# Read a required integer field from a payload or raise FormationError.
# A wrong type is the charger's mistake and is answered with FormationViolation instead of crashing the session.
def _int_field(payload: dict[str, Any], name: str) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise FormationError(f"{name} must be an integer")
    return value


# The longest text from a charger that is kept: an idTag is 20 characters in OCPP, a stop reason is a short word.
MAX_TEXT = 64


# The limit in amperes that a GetCompositeSchedule answer says is in effect now (its first period), or None when the
# answer is not an accepted schedule in amperes with a usable limit.
def _composite_limit(answer: dict[str, Any]) -> float | None:
    schedule = answer.get("chargingSchedule")
    periods = schedule.get("chargingSchedulePeriod") if isinstance(schedule, dict) else None
    if answer.get("status") != "Accepted" or not isinstance(schedule, dict) or not isinstance(periods, list):
        return None
    if schedule.get("chargingRateUnit") not in (None, "A") or not periods or not isinstance(periods[0], dict):
        return None
    limit = _finite(periods[0].get("limit"))
    return limit if limit is not None and 0 <= limit <= MAX_STORED_LIMIT_A else None


# How far ahead the schedule is read, in seconds, and the most periods of it that are kept.
SCHEDULE_HORIZON_S = 86400
MAX_SCHEDULE_PERIODS = 48
# A schedule reaches at most a week ahead, and its start may differ from the hub's clock by at most two days; anything
# else is not a schedule the hub can trust or show.
MAX_SCHEDULE_OFFSET_S = 7 * 86400
MAX_SCHEDULE_SKEW_S = 2 * 86400


# The start and the periods of a GetCompositeSchedule answer, as (seconds after the start, amperes). An answer that is
# not accepted, not in amperes or with unusable periods gives nothing. The start is the one the charger names, or now.
def _composite_schedule(answer: dict[str, Any], now: datetime) -> tuple[datetime | None, tuple[tuple[int, float], ...]]:
    schedule = answer.get("chargingSchedule")
    raw = schedule.get("chargingSchedulePeriod") if isinstance(schedule, dict) else None
    if answer.get("status") != "Accepted" or not isinstance(schedule, dict) or not isinstance(raw, list):
        return None, ()
    if schedule.get("chargingRateUnit") not in (None, "A") or len(raw) > MAX_SCHEDULE_PERIODS:
        return None, ()
    periods: list[tuple[int, float]] = []
    for period in raw:
        offset = period.get("startPeriod") if isinstance(period, dict) else None
        limit = _finite(period.get("limit")) if isinstance(period, dict) else None
        if isinstance(offset, bool) or not isinstance(offset, int) or not 0 <= offset <= MAX_SCHEDULE_OFFSET_S:
            return None, ()
        if periods and offset <= periods[-1][0]:
            return None, ()
        if limit is None or not 0 <= limit <= MAX_STORED_LIMIT_A:
            return None, ()
        periods.append((offset, limit))
    start = parse_timestamp(answer.get("scheduleStart") or schedule.get("startSchedule"), now)
    if abs((start - now).total_seconds()) > MAX_SCHEDULE_SKEW_S:
        return None, ()
    return start, tuple(periods)


# A text the charger sent, cut to MAX_TEXT; None when it is not a string or is empty.
# The port has no authentication and the history is never truncated, so no charger-supplied text is kept unbounded.
def _text(value: object) -> str | None:
    return value[:MAX_TEXT] if isinstance(value, str) and value else None


# A number the charger sent as a finite float; None when it is missing, not a number, too large or not finite.
# float() raises OverflowError on a huge integer, and JSON allows Infinity and NaN, so none of that may reach the
# energy arithmetic or a sensor state.
def _finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = float(value)
    except OverflowError:
        return None
    return number if math.isfinite(number) else None


class ChargerSession:
    """The hub's side of one charger connection."""

    # Keep the connection, listener and settings; nothing is sent until run() is called.
    def __init__(
        self,
        cp_id: str,
        protocol: str,
        connection: Connection,
        listener: SessionListener,
        next_transaction_id: Callable[[], Awaitable[int]],
        settings: SessionSettings | None = None,
        clock: Callable[[], datetime] | None = None,
        resume: Mapping[int, ConnectorState] | None = None,
        limit: LimitMemory | None = None,
    ) -> None:
        self._connection = connection
        self._listener = listener
        self._next_transaction_id = next_transaction_id
        self._settings = settings or SessionSettings()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._pending: dict[str, asyncio.Future[Frame]] = {}
        self._call_lock = asyncio.Lock()
        self._tasks: set[asyncio.Task[None]] = set()
        # The setup that is running for this connection, so another boot does not start a second one.
        self._setup_task: asyncio.Task[None] | None = None
        # The read of the limit in effect that is running, and whether another was asked for while it ran.
        self._refresh_task: asyncio.Task[None] | None = None
        self._refresh_again = False
        # Metering settings the charger rejected on this connection; they are not offered again.
        self._rejected_keys: set[str] = set()
        # Charges that were running when the charger's previous connection ended, so a stop after a reconnect still
        # finds its start.
        self._snapshot = ChargerSnapshot(
            cp_id=cp_id, connected=True, protocol=protocol, last_seen=self._clock(), connectors=dict(resume or {})
        )
        remembered = limit or LimitMemory()
        self._snapshot = replace(
            self._snapshot,
            charge_limit_a=remembered.hub_a,
            limit_purpose=remembered.hub_purpose,
            vendor_limit_a=remembered.vendor_a,
            vendor_profile_id=remembered.vendor_profile_id,
            vendor_purpose=remembered.vendor_purpose,
            vendor_profile_connector_id=remembered.vendor_profile_connector_id,
            vendor_profile_stack_level=remembered.vendor_profile_stack_level,
            hub_limit_restored=remembered.hub_restored,
            vendor_limit_restored=remembered.vendor_restored,
        )
        self._handlers: dict[str, Callable[[dict[str, Any]], dict[str, Any] | Awaitable[dict[str, Any]]]] = {
            "BootNotification": self._boot_notification,
            "Heartbeat": self._heartbeat,
            "StatusNotification": self._status_notification,
            "MeterValues": self._meter_values,
            "StartTransaction": self._start_transaction,
            "StopTransaction": self._stop_transaction,
            "Authorize": self._authorize,
            "DataTransfer": self._data_transfer,
            "FirmwareStatusNotification": self._acknowledge,
            "DiagnosticsStatusNotification": self._acknowledge,
        }

    @property
    def cp_id(self) -> str:
        """The identity the charger connected with."""
        return self._snapshot.cp_id

    @property
    def snapshot(self) -> ChargerSnapshot:
        """The latest state of the charger."""
        return self._snapshot

    # Read frames until the connection ends, then mark the charger as gone.
    # The listener is told on every exit path, so a crashed session can never leave a charger shown as connected.
    async def run(self) -> None:
        self._listener.on_connected(self)
        self._listener.on_snapshot(self._snapshot)
        self._start_task(self._ask_for_introduction())
        try:
            async for message in self._connection:
                await self._handle_message(message)
        except ConnectionClosed:
            pass
        finally:
            for task in list(self._tasks):
                task.cancel()
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(CallError("connection closed"))
                    # A caller that was cancelled with the connection never reads this error; reading it here keeps
                    # the event loop from reporting it as an exception nobody retrieved.
                    future.exception()
            self._snapshot = replace(self._snapshot, connected=False)
            self._listener.on_snapshot(self._snapshot)
            self._listener.on_disconnected(self.cp_id)

    # Close the connection from the hub's side.
    # Used when the same charger reconnects before the hub noticed the old link dying.
    async def close(self) -> None:
        await self._connection.close()

    # Drop the connection without a closing handshake, the way a crash or a restart of Home Assistant would.
    # The Raedian NEO stays away for minutes after a proper close, whatever the close code (1000, 1001 and 1012 were all
    # tried live), and is back within half a minute when the connection simply vanishes.
    def abort(self) -> None:
        transport = getattr(self._connection, "transport", None)
        if transport is not None:
            transport.abort()

    # Send a request to the charger and return the payload of its answer.
    # One request at a time per direction is a protocol rule, so calls are serialised with a lock.
    async def call(
        self,
        action: str,
        payload: dict[str, Any],
        timeout: float | None = None,
        *,
        before_send: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        async with self._call_lock:
            unique_id = uuid.uuid4().hex
            future: asyncio.Future[Frame] = asyncio.get_running_loop().create_future()
            self._pending[unique_id] = future
            try:
                if before_send is not None:
                    before_send()
                await self._send(build_call(unique_id, action, payload))
                frame = await asyncio.wait_for(future, timeout or self._settings.call_timeout_s)
            except TimeoutError:
                raise CallTimeoutError(f"{action} was not answered") from None
            except ConnectionClosed as error:
                raise CallError(f"{action} could not be sent: the connection closed") from error
            finally:
                self._pending.pop(unique_id, None)
        if frame[0] == MSG_CALL_ERROR:
            raise CallRejectedError(str(frame[2]), str(frame[3]) if len(frame) > 3 else "")
        result = frame[2]
        return result if isinstance(result, dict) else {}

    # Remember the limit the charger accepted and the purpose it accepted it as.
    # The snapshot is what Home Assistant shows, and the purpose spares the next write the purposes that were refused.
    # A session whose connection has ended records nothing: the charger may already be on a newer session, and an
    # old snapshot must not overwrite what the entities show.
    def record_limit(self, amps: float | None, purpose: str | None, *, from_plan: bool = False) -> None:
        if not self._snapshot.connected:
            return
        # New knowledge replaces the old answer of the charger, which no longer describes what is in effect. The hub's
        # own limit is certain now; the vendor's restored one stays marked until the charger confirms it.
        self._update(
            charge_limit_a=amps,
            limit_purpose=purpose,
            composite_limit_a=None,
            hub_limit_restored=False,
            external_profile_writes=self._snapshot.external_profile_writes + int(not from_plan),
        )

    # Keep the planning that the Raedian app set in the charger, as the windows it wrote or the charger answered.
    def record_app_plan(self, windows: list[tuple[int, int, int]]) -> None:
        if self._snapshot.connected:
            self._update(app_plan=tuple(windows))

    # Remember the limit of a command from the vendor that the charger accepted, with the id and purpose of its profile.
    # A vendor limit of None means the vendor's profile is gone.
    def record_vendor_limit(
        self,
        amps: float | None,
        profile_id: int | None,
        purpose: str | None,
        *,
        connector_id: int | None = None,
        stack_level: int | None = None,
    ) -> None:
        if not self._snapshot.connected:
            return
        self._update(
            vendor_limit_a=amps,
            vendor_profile_id=profile_id,
            vendor_purpose=purpose,
            vendor_profile_connector_id=connector_id,
            vendor_profile_stack_level=stack_level,
            composite_limit_a=None,
            vendor_limit_restored=False,
        )

    # Compare the limits restored from storage with what the charger says is in effect. When the lowest limit known
    # agrees with it, the restored limits are confirmed. When it does not, the restored sides are gone from the charger
    # (or changed) and are dropped, while a side written on this connection stays. A charger that said nothing confirms
    # nothing, and they stay marked as restored.
    def _reconcile_restored(self, composite: float | None) -> dict[str, Any]:
        snapshot = self._snapshot
        if not snapshot.limits_restored or composite is None:
            return {}
        known = [x for x in (snapshot.charge_limit_a, snapshot.vendor_limit_a) if x is not None]
        if known and abs(min(known) - composite) < 0.01:
            return {"hub_limit_restored": False, "vendor_limit_restored": False}
        changes: dict[str, Any] = {"hub_limit_restored": False, "vendor_limit_restored": False}
        if snapshot.hub_limit_restored:
            changes |= {"charge_limit_a": None, "limit_purpose": None}
        if snapshot.vendor_limit_restored:
            changes |= {
                "vendor_limit_a": None,
                "vendor_profile_id": None,
                "vendor_purpose": None,
                "vendor_profile_connector_id": None,
                "vendor_profile_stack_level": None,
            }
        return changes

    # Ask for the limit in effect in a task of its own, so a charger that answers slowly or never does not hold up the
    # write or the command that caused the question.
    def request_effective_limit(self) -> None:
        if self._refresh_task is not None and not self._refresh_task.done():
            self._refresh_again = True
            return
        self._refresh_task = self._start_task(self._refresh_loop())

    # Read until nobody asked again meanwhile. Requests that come in while a read is running (a flood of vendor
    # commands, say) are folded into one more read afterwards, so the number of reads stays small.
    async def _refresh_loop(self) -> None:
        while True:
            self._refresh_again = False
            await self._refresh_limit_and_schedule()
            if not self._refresh_again:
                return

    # Read the limit in effect first, then the schedule for the next 24 hours. They are separate questions on purpose: a
    # charger that refuses the long one must not cost the answer to the short one.
    async def _refresh_limit_and_schedule(self) -> None:
        await self.refresh_effective_limit()
        await self.refresh_schedule()

    # Ask the charger for its schedule over the next 24 hours and keep it. A read; a charger that cannot answer leaves
    # the schedule empty.
    async def refresh_schedule(self) -> None:
        start: datetime | None = None
        periods: tuple[tuple[int, float], ...] = ()
        try:
            answer = await self.call(
                "GetCompositeSchedule", commands.get_composite_schedule(PRIMARY_CONNECTOR, SCHEDULE_HORIZON_S)
            )
        except CallError as error:
            _LOGGER.info("[read] %s could not tell its schedule: %s", self.cp_id, error)
        else:
            start, periods = _composite_schedule(answer, self._clock())
            _LOGGER.info("[read] %s reports %s schedule period(s) for the next day", self.cp_id, len(periods))
        if self._snapshot.connected:
            self._update(schedule_start=start, schedule_periods=periods)

    # Ask the charger what limit is in effect now and keep the answer. It is a read, and a charger that does not support
    # it (or does not answer) simply leaves the limit unknown: the hub then shows the lowest of the limits it knows.
    async def refresh_effective_limit(self) -> None:
        limit: float | None = None
        try:
            answer = await self.call("GetCompositeSchedule", commands.get_composite_schedule(PRIMARY_CONNECTOR))
        except CallError as error:
            _LOGGER.info("[read] %s could not tell its effective limit: %s", self.cp_id, error)
        else:
            limit = _composite_limit(answer)
            _LOGGER.info(
                "[read] %s says its limit in effect is %s A (answer %s)", self.cp_id, limit, answer.get("status")
            )
        if self._snapshot.connected:
            self._update(composite_limit_a=limit, **self._reconcile_restored(limit))

    # Send one frame to the charger and tell the listener about it.
    # Every frame the hub sends passes here, so listeners such as the mirror see the hub's answers too.
    async def _send(self, text: str) -> None:
        frame = parse_frame(text)
        await self._connection.send(text)
        if frame is not None:
            self._listener.on_frame(self.cp_id, SENDER_HUB, frame)

    # Handle one raw message from the charger: tell the listener, then answer a request or resolve a pending call.
    # Garbage is logged and ignored; closing the link over one bad frame would only make the charger reconnect.
    async def _handle_message(self, raw: str | bytes) -> None:
        self._snapshot = replace(self._snapshot, last_seen=self._clock())
        frame = parse_frame(raw)
        if frame is None:
            _LOGGER.warning("Ignoring an unreadable frame from %s", self.cp_id)
            return
        self._listener.on_frame(self.cp_id, SENDER_CHARGER, frame)
        if frame[0] == MSG_CALL:
            await self._handle_call(frame)
        elif frame[0] in (MSG_CALL_RESULT, MSG_CALL_ERROR):
            future = self._pending.get(str(frame[1]))
            if future is not None and not future.done():
                future.set_result(frame)

    # Answer a request from the charger: the handler's result, or an error frame.
    # Every request needs an answer, otherwise the charger stops sending.
    async def _handle_call(self, frame: Frame) -> None:
        unique_id = str(frame[1])
        if len(frame) < 4 or not isinstance(frame[3], dict):
            await self._send(build_call_error(unique_id, ERROR_FORMATION_VIOLATION, "payload must be an object"))
            return
        handler = self._handlers.get(str(frame[2]))
        if handler is None:
            await self._send(build_call_error(unique_id, ERROR_NOT_IMPLEMENTED, f"{frame[2]} is not supported"))
            return
        try:
            result = handler(frame[3])
            if isinstance(result, Awaitable):
                result = await result
        except FormationError as error:
            await self._send(build_call_error(unique_id, ERROR_FORMATION_VIOLATION, str(error)))
        except Exception:  # noqa: BLE001
            # A bug in one handler must not end the connection; the charger gets an error and carries on.
            _LOGGER.exception("Handling %s from %s failed", frame[2], self.cp_id)
            await self._send(build_call_error(unique_id, ERROR_INTERNAL, "internal error"))
        else:
            await self._send(build_call_result(unique_id, result))

    # Replace the snapshot and tell the listener.
    # One place publishes, so listeners never miss a change.
    def _update(self, **changes: Any) -> None:
        self._snapshot = replace(self._snapshot, **changes)
        self._listener.on_snapshot(self._snapshot)

    # Replace one connector in the snapshot.
    # Connector changes build a new mapping; the snapshot itself stays immutable.
    def _update_connector(self, connector: ConnectorState) -> None:
        connectors = dict(self._snapshot.connectors)
        connectors[connector.connector_id] = connector
        self._update(connectors=connectors)

    # Look up a connector, creating an unknown one on first sight.
    # A charger may start a transaction before it ever sent a status for that connector.
    def _connector(self, connector_id: int) -> ConnectorState:
        return self._snapshot.connectors.get(connector_id) or ConnectorState(connector_id)

    # ISO 8601 UTC time as the protocol wants it.
    # The charger sets its clock from the answers to BootNotification and Heartbeat.
    def _now_iso(self) -> str:
        return self._clock().astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # Record the charger's identity, accept it, and start the follow-up setup in the background.
    # Setup waits and sends requests of its own, so it must not block the answer.
    def _boot_notification(self, payload: dict[str, Any]) -> dict[str, Any]:
        info = ChargerInfo(
            vendor=payload.get("chargePointVendor"),
            model=payload.get("chargePointModel"),
            serial=payload.get("chargePointSerialNumber"),
            firmware=payload.get("firmwareVersion"),
        )
        self._update(info=info)
        if self._setup_task is None or self._setup_task.done():
            self._setup_task = self._start_task(self._post_boot_setup())
        return {"status": "Accepted", "currentTime": self._now_iso(), "interval": self._settings.heartbeat_interval_s}

    # Answer a heartbeat with the hub's time.
    # This is how the charger keeps its clock right.
    def _heartbeat(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {"currentTime": self._now_iso()}

    # Store the status of a connector, keeping the transaction that may be running on it.
    # The status is what Home Assistant shows as plugged, charging or available.
    def _status_notification(self, payload: dict[str, Any]) -> dict[str, Any]:
        connector_id = _int_field(payload, "connectorId")
        status = payload.get("status")
        if not isinstance(status, str):
            raise FormationError("status must be a string")
        info = payload.get("info")
        self._update_connector(
            replace(
                self._connector(connector_id),
                status=status,
                error_code=str(payload.get("errorCode", "NoError")),
                info=info if isinstance(info, str) else None,
            )
        )
        return {}

    # Keep the newest meter reading, and the highest power of the charge that is running.
    # A payload without usable values is acknowledged and ignored.
    def _meter_values(self, payload: dict[str, Any]) -> dict[str, Any]:
        sample = parse_meter_values(payload, self._clock())
        if sample is None:
            return {}
        connectors = dict(self._snapshot.connectors)
        running = self._running_connector(payload)
        if running is not None:
            connectors[running.connector_id] = replace(running, stats=running.stats.add(sample))
        self._update(meter=merge_samples(self._snapshot.meter, sample), connectors=connectors)
        return {}

    # The connector a meter reading belongs to: the one it names, or the only one with a charge running.
    def _running_connector(self, payload: dict[str, Any]) -> ConnectorState | None:
        named = payload.get("connectorId")
        if isinstance(named, int) and not isinstance(named, bool):
            connector = self._snapshot.connectors.get(named)
            return connector if connector is not None and connector.transaction_id is not None else None
        running = [c for c in self._snapshot.connectors.values() if c.transaction_id is not None]
        return running[0] if len(running) == 1 else None

    # Hand out a transaction id and remember which connector it runs on.
    # A start on a connector that still has a running charge means the charger lost that charge's stop (a power cut,
    # a firmware that starts again); the old charge is closed first, at the new start, so it is not lost.
    # Every tag is accepted: this is a private charger on a trusted network, and refusing one only strands the owner.
    async def _start_transaction(self, payload: dict[str, Any]) -> dict[str, Any]:
        connector_id = _int_field(payload, "connectorId")
        started_at = parse_timestamp(payload.get("timestamp"), self._clock())
        transaction_id = await self._next_transaction_id()
        connector = self._connector(connector_id)
        if connector.transaction_id is not None:
            interrupted = self._finished_session(
                connector,
                connector.transaction_id,
                {"timestamp": started_at.isoformat(), "reason": "Other", "meterStop": payload.get("meterStart")},
            )
            self._listener.on_session_finished(interrupted)
        self._update_connector(
            replace(
                connector,
                transaction_id=transaction_id,
                id_tag=_text(payload.get("idTag")),
                meter_start_wh=_finite(payload.get("meterStart")),
                started_at=started_at,
                stats=ChargeStats(),
            )
        )
        return {"transactionId": transaction_id, "idTagInfo": {"status": "Accepted"}}

    # Clear the transaction from the connector that ran it, and report the finished charge.
    # The id comes from the charger, so a stop for an unknown id (for example one that started before the hub
    # restarted) is acknowledged without touching other sessions and without inventing a charge.
    def _stop_transaction(self, payload: dict[str, Any]) -> dict[str, Any]:
        transaction_id = _int_field(payload, "transactionId")
        for connector in list(self._snapshot.connectors.values()):
            if connector.transaction_id == transaction_id:
                finished = self._finished_session(connector, transaction_id, payload)
                self._update_connector(
                    replace(
                        connector,
                        transaction_id=None,
                        id_tag=None,
                        meter_start_wh=None,
                        started_at=None,
                        stats=ChargeStats(),
                    )
                )
                self._listener.on_session_finished(finished)
                if self._snapshot.limit_purpose == commands.PURPOSE_TX:
                    # A charge profile ends with its charge; the next charge is not limited by it.
                    self.record_limit(None, None)
                if self._snapshot.vendor_purpose == commands.PURPOSE_TX:
                    self.record_vendor_limit(None, None, None)
                # What the charger said before may have been a profile of this charge; ask again.
                self._update(composite_limit_a=None)
                self.request_effective_limit()
        return {"idTagInfo": {"status": "Accepted"}}

    # The charge that a StopTransaction ends. Its energy is the meter at the stop minus the meter at the start, or, when
    # the charger sent no stop reading, the last meter value seen; None when the start reading is missing.
    def _finished_session(
        self, connector: ConnectorState, transaction_id: int, payload: dict[str, Any]
    ) -> ChargeSession:
        ended_at = parse_timestamp(payload.get("timestamp"), self._clock())
        meter_stop = _finite(payload.get("meterStop"))
        last = self._snapshot.meter.energy_wh if self._snapshot.meter is not None else None
        end_reading = meter_stop if meter_stop is not None else last
        start_reading = connector.meter_start_wh
        energy = (
            max(end_reading - start_reading, 0.0) if start_reading is not None and end_reading is not None else None
        )
        return ChargeSession(
            cp_id=self.cp_id,
            connector_id=connector.connector_id,
            transaction_id=transaction_id,
            id_tag=connector.id_tag,
            started_at=connector.started_at or ended_at,
            ended_at=ended_at,
            meter_start_wh=start_reading,
            meter_stop_wh=meter_stop,
            energy_wh=energy,
            max_power_w=connector.stats.max_power_w,
            average_power_w=connector.stats.average_power_w,
            max_current_a=connector.stats.max_current_a,
            offered_a=connector.stats.offered_a,
            limit_a=self._snapshot.charge_limit_a,
            stop_reason=_text(payload.get("reason")),
        )

    # Accept an authorisation request.
    # See _start_transaction: every tag is accepted.
    def _authorize(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {"idTagInfo": {"status": "Accepted"}}

    # Refuse vendor-specific data transfers politely.
    # The hub knows no vendor extensions, and an explicit answer keeps the charger from retrying.
    def _data_transfer(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {"status": "UnknownVendorId"}

    # Acknowledge notifications that need no action.
    # Firmware and diagnostics progress is for the vendor backend, which sees it through the mirror.
    def _acknowledge(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {}

    # Run a coroutine in the background for as long as the connection lives.
    # Tasks are cancelled when the connection ends, so none can outlive its session.
    def _start_task(self, coroutine: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        task = asyncio.get_running_loop().create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    # Whether the charger has said who it is, in a BootNotification.
    def _has_booted(self) -> bool:
        info = self._snapshot.info
        return info.vendor is not None or info.model is not None

    # Ask the charger for the messages it did not send on its own after connecting: its boot, then its status.
    # A charger that reconnects after the hub restarted usually skips both, so the hub would know neither the model nor
    # the state. TriggerMessage is the standard way for a central system that joined late, and it only asks the charger
    # to report; nothing on the charger changes. The charger ignores requests for a while after connecting, hence the
    # wait, and an unanswered request is repeated like the setup is.
    async def _ask_for_introduction(self) -> None:
        delays = (self._settings.boot_wait_s, *self._settings.setup_retry_delays_s)
        for attempt, delay in enumerate(delays, start=1):
            await asyncio.sleep(delay)
            try:
                if not self._has_booted():
                    await self.call("TriggerMessage", commands.trigger_message("BootNotification"))
                    await asyncio.sleep(self._settings.trigger_settle_s)
                if self._has_booted() and PRIMARY_CONNECTOR not in self._snapshot.connectors:
                    await self.call("TriggerMessage", commands.trigger_message("StatusNotification", PRIMARY_CONNECTOR))
            except CallError as error:
                _LOGGER.warning(
                    "Charger %s did not answer the request to introduce itself (try %s of %s): %s",
                    self.cp_id,
                    attempt,
                    len(delays),
                    error,
                )
                continue
            if self._has_booted() and PRIMARY_CONNECTOR in self._snapshot.connectors:
                _LOGGER.info("Charger %s introduced itself after being asked (try %s)", self.cp_id, attempt)
                return

    # After a charger booted: read its configuration and make sure it sends the meter values the hub needs.
    # A charger that does not answer is asked again a few times; failures are logged, not raised, because the charger
    # works without this and its next boot starts over.
    async def _post_boot_setup(self) -> None:
        delays = (self._settings.setup_delay_s, *self._settings.setup_retry_delays_s)
        for attempt, delay in enumerate(delays, start=1):
            await asyncio.sleep(delay)
            try:
                await self._read_configuration()
                await self._ensure_metering()
                await self._refresh_limit_and_schedule()
            except CallError as error:
                _LOGGER.warning(
                    "Setup of charger %s did not complete (try %s of %s): %s", self.cp_id, attempt, len(delays), error
                )
            else:
                _LOGGER.info("Setup of charger %s completed on try %s", self.cp_id, attempt)
                return
        _LOGGER.warning("Giving up on the setup of charger %s until its next boot", self.cp_id)

    # Read the configuration keys that decide what the hub can do and store them in the snapshot, one key per request.
    # The Raedian NEO never answered a request that names several keys; it answers each key on its own, which is also
    # how the reference implementation asks. SupportedFeatureProfiles tells whether smart charging is offered at all.
    # Only the keys asked for are kept: some chargers answer with every key they have, and the snapshot is shown in
    # diagnostics and to the vendor. A key that gets no answer is skipped; when no key was answered at all, the last
    # error goes up so the whole setup is tried again later.
    async def _read_configuration(self) -> None:
        failure: CallError | None = None
        answered = 0
        for key in CONFIG_KEYS:
            try:
                answer = await self.call("GetConfiguration", commands.get_configuration([key]))
            except CallError as error:
                failure = error
                continue
            answered += 1
            for item in answer.get("configurationKey", []):
                if isinstance(item, dict) and item.get("key") == key:
                    self._store_configuration(key, str(item.get("value", "")))
        if failure is not None and answered == 0:
            raise failure

    # Add one configuration value to the snapshot, and the feature profiles it implies.
    # Updating per key means a charger that answers only some keys still shows what it said.
    def _store_configuration(self, key: str, value: str) -> None:
        changes: dict[str, Any] = {"configuration": {**self._snapshot.configuration, key: value}}
        if key == "SupportedFeatureProfiles":
            changes["features"] = frozenset(part.strip() for part in value.split(",") if part.strip())
        self._update(**changes)

    # Ask the charger to report the measurands the hub uses, at the configured interval, when it does not already.
    # A charger reports only what it is configured to, and the per-phase current is what load balancing needs.
    async def _ensure_metering(self) -> None:
        wanted = {
            "MeterValuesSampledData": self._settings.measurands,
            "MeterValueSampleInterval": str(self._settings.meter_sample_interval_s),
        }
        for key, value in wanted.items():
            if key in self._rejected_keys:
                continue
            if key in self._snapshot.configuration and self._snapshot.configuration[key] != value:
                answer = await self.call("ChangeConfiguration", commands.change_configuration(key, value))
                _LOGGER.info("Charger %s: %s set to %s, answer %s", self.cp_id, key, value, answer.get("status"))
                if answer.get("status") in ("Accepted", "RebootRequired"):
                    self._update(configuration={**self._snapshot.configuration, key: value})
                else:
                    self._rejected_keys.add(key)
