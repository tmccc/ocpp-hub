"""One charger's link to the vendor backend: connects, replays, forwards in order and answers the vendor's commands."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import ssl
import uuid
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, WebSocketException
from websockets.typing import Subprotocol

from . import readings
from .control import ControlPolicy, plain_text
from .frames import (
    ERROR_FORMATION_VIOLATION,
    ERROR_INTERNAL,
    ERROR_NOT_IMPLEMENTED,
    ERROR_NOT_SUPPORTED,
    MSG_CALL,
    MSG_CALL_ERROR,
    MSG_CALL_RESULT,
    Frame,
    build_call,
    build_call_error,
    build_call_result,
    call_action,
    join_websocket_url,
    parse_frame,
)
from .mirror_commands import TriggerRequest, answer_vendor_call
from .mirror_forward import FORWARDED, Forward, plan_forward, profile_limit_a, refusal_for
from .mirror_model import COMMANDS_FORWARD, MirrorSettings, MirrorStatus
from .mirror_queue import (
    ACTION_BOOT,
    ACTION_HEARTBEAT,
    ACTION_START,
    ACTION_STATUS,
    ACTION_STOP,
    Outgoing,
    OutgoingQueue,
    connector_of,
    is_integer,
)
from .mirror_rewrite import StartPairing, translate_for_vendor
from .plan_bridge import (
    PlanHandler,
    PlanQuery,
    PlanUnreadable,
    PlanWrite,
    classify,
    plan_answer,
    planning_switch,
    write_acknowledged,
)
from .raedian import plan_from_exchange
from .session import CallError, CallRejectedError, ChargerSession
from .txid_map import TransactionIdMap

_LOGGER = logging.getLogger(__name__)

# The subprotocol the vendor backend is asked for; the mirror is a virtual OCPP 1.6 charge point.
VENDOR_SUBPROTOCOL = Subprotocol("ocpp1.6")


class _RedactAuthorization(logging.Filter):
    """Hides the value of the Authorization header, which the websockets library logs at debug level."""

    # Replace the header value in a record that logs an Authorization header; pass every other record unchanged.
    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args
        if isinstance(args, tuple) and len(args) == 2 and str(args[0]).lower() == "authorization":
            record.args = (args[0], "[redacted]")
        return True


# The websockets library logs every request header at debug level, the Authorization header with the vendor password
# included. The vendor connection gets this logger instead of the library's own, so a debug log can never leak it.
_WEBSOCKET_LOGGER = logging.getLogger(f"{__name__}.websocket")
_WEBSOCKET_LOGGER.addFilter(_RedactAuthorization())

# The error codes of the charger that are passed on to the vendor as they are; any other becomes an internal error.
# The most vendor commands that may wait for the charger at once; the rest are refused, so a flood cannot hold up the
# owner's own writes, which queue on the same connection.
MAX_FORWARDS = 16
_RELAYED_ERRORS = frozenset({ERROR_NOT_SUPPORTED, ERROR_NOT_IMPLEMENTED, ERROR_FORMATION_VIOLATION, ERROR_INTERNAL})


# A fresh unique id for a frame the mirror makes up itself (a heartbeat or a replay).
# The prefix keeps them apart from the ids the charger chose.
def _new_id() -> str:
    return f"mirror-{uuid.uuid4().hex}"


class _BootRefusedError(Exception):
    """The vendor backend did not accept the BootNotification; try again after the time it asked for."""

    # Keep what the vendor said and how long to wait before the next try.
    def __init__(self, status: str, retry_after_s: float) -> None:
        super().__init__(f"the vendor backend answered BootNotification with {status}")
        self.retry_after_s = retry_after_s


class MirrorLink:
    """Keeps the vendor backend informed about one charger, as one task, for as long as there is something to say."""

    # Keep everything needed to run; nothing connects until start() is called.
    def __init__(
        self,
        session: ChargerSession,
        settings: MirrorSettings,
        transactions: TransactionIdMap,
        ssl_context: ssl.SSLContext | None,
        on_finished: Callable[[MirrorLink], None],
        clock: Callable[[], datetime] | None = None,
        policy: ControlPolicy | None = None,
        plan_handler: PlanHandler | None = None,
    ) -> None:
        self._session = session
        self._plan_handler = plan_handler
        self._policy = policy or ControlPolicy()
        # Commands being passed on to the charger; they run beside the reader, which must keep reading meanwhile.
        self._forwards: set[asyncio.Task[None]] = set()
        self._settings = settings
        self._transactions = transactions
        self._ssl_context = ssl_context
        self._on_finished = on_finished
        self._clock = clock or (lambda: datetime.now(UTC))
        self._queue = OutgoingQueue(settings.max_queue)
        self._pairing = StartPairing(transactions)
        # Charger unique id of a StartTransaction the hub has not answered yet -> the mirror's id for it.
        self._start_tokens: dict[str, str] = {}
        self._replies: dict[str, asyncio.Future[Frame]] = {}
        self._wakeup = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        # What is remembered to start a new vendor connection from.
        self._boot: Outgoing | None = None
        self._statuses: dict[int, Outgoing] = {}
        # The frame that was sent and not answered yet; it goes first again on the next connection.
        self._in_flight: Outgoing | None = None
        self._heartbeat_s = settings.fallback_heartbeat_s
        self._charger_gone = False
        self._closing = False
        self._connected = False
        self._delivered_on_this_connection = False
        self._retry_floor_s = 0.0
        self._failing = False
        self._overflow_reported = False
        self._last_success: datetime | None = None
        self._last_error: str | None = None
        self._dropped = 0
        self._unmapped = 0

    @property
    def cp_id(self) -> str:
        """The charger this link speaks for."""
        return self._session.cp_id

    @property
    def session(self) -> ChargerSession:
        """The charger session the link currently follows."""
        return self._session

    # Start the background task.
    def start(self) -> None:
        self._task = asyncio.get_running_loop().create_task(self._run())

    # Cancel the task and wait until it has closed the vendor connection.
    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)

    # Follow a new session of the same charger. Returns False when the link is already shutting down,
    # so the caller starts a fresh one instead of reviving something that is closing its connection.
    def attach(self, session: ChargerSession) -> bool:
        if self._closing:
            return False
        self._session = session
        self._charger_gone = False
        self._wakeup.set()
        return True

    # The charger left. The link first delivers what is still waiting, then closes the vendor connection.
    def charger_gone(self) -> None:
        self._charger_gone = True
        self._wakeup.set()

    # Report the state of the link for Home Assistant.
    def status(self) -> MirrorStatus:
        waiting = len(self._queue) + (1 if self._in_flight is not None else 0)
        return MirrorStatus(
            self._connected, waiting, self._last_success, self._last_error, self._dropped, self._unmapped
        )

    # Take in a frame the charger sent to the hub. Requests are queued for the vendor; heartbeats are not,
    # because the vendor chose its own heartbeat interval and gets heartbeats from the link itself.
    def charger_frame(self, frame: Frame) -> None:
        action = call_action(frame)
        if action is None or action == ACTION_HEARTBEAT or not isinstance(frame[3], dict):
            return
        item = Outgoing(str(frame[1]), action, frame[3])
        if action == ACTION_BOOT:
            self._boot = item
        elif action == ACTION_STATUS and (connector := connector_of(item)) is not None:
            self._statuses[connector] = item
        elif action == ACTION_START:
            item = self._track_start(item)
        self._enqueue(item)

    # Give a StartTransaction a unique id of the mirror's own and remember which charger id it belongs to.
    # A charger can reuse an id after a restart while an earlier start still waits for an offline vendor, so pairing
    # on the charger's id would mix up the two transactions.
    def _track_start(self, item: Outgoing) -> Outgoing:
        tracked = replace(item, unique_id=_new_id())
        self._start_tokens[item.unique_id] = tracked.unique_id
        self._pairing.expect(tracked.unique_id)
        return tracked

    # Take in a frame the hub sent to the charger. Only the answers to StartTransaction matter: they carry the
    # transaction id the hub handed out, which the vendor's own id has to be paired with. The hub answers every
    # request once, so the id is forgotten here and the table cannot grow.
    def hub_frame(self, frame: Frame) -> None:
        if frame[0] not in (MSG_CALL_RESULT, MSG_CALL_ERROR):
            return
        token = self._start_tokens.pop(str(frame[1]), None)
        if token is None:
            return
        hub_id = frame[2].get("transactionId") if frame[0] == MSG_CALL_RESULT and isinstance(frame[2], dict) else None
        if is_integer(hub_id):
            self._pairing.hub_answered(token, hub_id)
        else:
            self._pairing.abandon(token)

    # Queue a frame, count what the size limit made us drop, and wake the sender.
    def _enqueue(self, item: Outgoing) -> None:
        self._dropped += self._queue.push(item)
        if self._queue.over_capacity and not self._overflow_reported:
            self._overflow_reported = True
            _LOGGER.warning(
                "Mirror queue of %s is over its limit of %s with frames that must not be dropped",
                self.cp_id,
                self._settings.max_queue,
            )
        self._wakeup.set()

    # True when the charger is gone and everything has been delivered.
    def _finished(self) -> bool:
        return self._charger_gone and self._in_flight is None and len(self._queue) == 0

    # Keep a vendor connection up for as long as the link has a reason to, reconnecting with growing delays.
    async def _run(self) -> None:
        attempt = 0
        try:
            while not self._finished():
                self._delivered_on_this_connection = False
                await self._attempt()
                if self._finished():
                    break
                attempt = 0 if self._delivered_on_this_connection else attempt + 1
                await asyncio.sleep(self._backoff(attempt))
        finally:
            self._connected = False
            self._on_finished(self)

    # Connect once and serve until the connection ends, turning every failure into a status instead of an exception.
    # This is the boundary that keeps the mirror alive: whatever goes wrong with the vendor backend, the charger and
    # the hub carry on, and the link tries again.
    async def _attempt(self) -> None:
        try:
            await self._serve_once()
        except _BootRefusedError as refused:
            self._retry_floor_s = refused.retry_after_s
            self._note_failure(str(refused))
        except (OSError, WebSocketException, TimeoutError) as error:
            self._note_failure(f"{type(error).__name__}: {error}")
        except Exception as error:  # noqa: BLE001
            _LOGGER.exception("Mirror link of %s failed unexpectedly", self.cp_id)
            self._note_failure(f"{type(error).__name__}: {error}")

    # The delay before the next connection attempt: doubling from the minimum up to the maximum,
    # or the time the vendor asked for after a refused boot when that is longer.
    def _backoff(self, attempt: int) -> float:
        minimum, maximum = self._settings.backoff_min_s, self._settings.backoff_max_s
        exponent = max(attempt - 1, 0)
        # Compare finite binary exponents before scaling, including subnormal minima and arbitrarily long outages.
        ceiling = math.frexp(maximum)[1] - math.frexp(minimum)[1]
        delay = maximum if exponent > ceiling else min(maximum, math.ldexp(minimum, exponent))
        delay, self._retry_floor_s = max(delay, self._retry_floor_s), 0.0
        return delay

    # Remember a failure. The first one of a streak is a warning, the repeats are debug lines, so a vendor outage
    # of a night does not fill the log.
    def _note_failure(self, text: str) -> None:
        self._last_error = text
        _LOGGER.log(logging.DEBUG if self._failing else logging.WARNING, "Mirror link of %s: %s", self.cp_id, text)
        self._failing = True

    # A real answer from the vendor ends a streak of failures. A connection that is opened and then refused at the boot
    # does not, so a vendor that keeps refusing is warned about once and not at every attempt.
    def _end_failure_streak(self) -> None:
        if self._failing:
            _LOGGER.info("Mirror link of %s delivers to the vendor backend again", self.cp_id)
            self._failing = False

    # Open the vendor connection, prepare the queue, and run until the connection or the link ends.
    async def _serve_once(self) -> None:
        connection = await self._open()
        self._connected = True
        _LOGGER.info("Mirror link of %s connected to the vendor backend", self.cp_id)
        try:
            self._prepare_session()
            await self._pump(connection)
        finally:
            self._connected = False
            await connection.close()

    # Open the websocket to the vendor backend: the charger's identity as the last path segment, OCPP 1.6 as
    # subprotocol, Basic auth only when credentials are set. System proxy settings are ignored on purpose.
    async def _open(self) -> ClientConnection:
        auth = self._settings.auth_header
        options: dict[str, Any] = {"ssl": self._ssl_context} if self._settings.use_tls else {}
        return await connect(
            join_websocket_url(self._settings.url, self.cp_id),
            subprotocols=[VENDOR_SUBPROTOCOL],
            additional_headers=[auth] if auth else None,
            open_timeout=self._settings.connect_timeout_s,
            proxy=None,
            logger=_WEBSOCKET_LOGGER,
            **options,
        )

    # Start a fresh connection from the right place: the unanswered frame first, then a boot and the current state.
    def _prepare_session(self) -> None:
        if self._in_flight is not None:
            self._queue.push_front(self._in_flight)
            self._in_flight = None
        self._queue.begin_session(self._boot, self._statuses, _new_id)

    # Run the reader and the sender side by side until one of them ends, then stop the other.
    # The reader must keep running while the sender waits for an answer, because the vendor's commands arrive on
    # the same connection and each needs a prompt reply.
    async def _pump(self, connection: ClientConnection) -> None:
        reader = asyncio.create_task(self._read_loop(connection))
        sender = asyncio.create_task(self._send_loop(connection))
        try:
            done, _ = await asyncio.wait({reader, sender}, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
            if reader in done:
                raise ConnectionError("the vendor backend closed the connection")
        finally:
            reader.cancel()
            sender.cancel()
            forwards = list(self._forwards)
            for forward in forwards:
                forward.cancel()
            await asyncio.gather(reader, sender, *forwards, return_exceptions=True)

    # Read frames from the vendor: answers resolve the waiting request, commands get an answer at once.
    async def _read_loop(self, connection: ClientConnection) -> None:
        async for message in connection:
            frame = parse_frame(message)
            if frame is None:
                _LOGGER.warning("Ignoring an unreadable frame from the vendor backend for %s", self.cp_id)
            elif frame[0] == MSG_CALL:
                await self._answer_command(connection, frame)
            elif frame[0] in (MSG_CALL_RESULT, MSG_CALL_ERROR):
                future = self._replies.get(str(frame[1]))
                if future is not None and not future.done():
                    future.set_result(frame)

    # Answer a command from the vendor without passing it on, and send any frame it asked to be triggered afterwards.
    # Every refusal is logged, so the owner can see what the vendor tried.
    async def _answer_command(self, connection: ClientConnection, frame: Frame) -> None:
        unique_id = str(frame[1])
        action = call_action(frame)
        if action is None or not isinstance(frame[3], dict):
            await connection.send(build_call_error(unique_id, ERROR_FORMATION_VIOLATION, "malformed request"))
            return
        if self._settings.commands == COMMANDS_FORWARD and action in FORWARDED:
            self._start_forward(connection, unique_id, action, frame[3])
            return
        answer = answer_vendor_call(
            unique_id,
            action,
            frame[3],
            self._session.snapshot.configuration,
            self._can_trigger,
            readings.confirmed_limit_a(self._session.snapshot),
            lambda duration: readings.schedule_from_now(self._session.snapshot, self._clock(), duration),
        )
        _LOGGER.info("Vendor backend sent %s for %s; answered without passing it to the charger", action, self.cp_id)
        _LOGGER.debug("Vendor command %s payload for %s: %s", action, self.cp_id, frame[3])
        await connection.send(answer.frame)
        if answer.trigger is not None:
            self._trigger(answer.trigger)

    # Pass a vendor command on to the charger in a task of its own and send the charger's answer back, so the reader
    # goes on reading while the charger takes its time.
    def _start_forward(
        self, connection: ClientConnection, unique_id: str, action: str, payload: dict[str, Any]
    ) -> None:
        task = asyncio.get_running_loop().create_task(self._forward(connection, unique_id, action, payload))
        self._forwards.add(task)
        task.add_done_callback(self._forwards.discard)

    # Check the command against the ceiling and the transaction map, give it to the charger, and relay the answer.
    # Every outcome is logged with the action, because this is the one way the vendor changes what the charger does.
    async def _forward(
        self, connection: ClientConnection, unique_id: str, action: str, payload: dict[str, Any]
    ) -> None:
        # The planning is answered by the hub itself and never waits for the charger, so it comes before the queue
        # limit: the app opens with a burst of commands and its planning question must not be one that is turned away.
        if (
            action == "DataTransfer"
            and self._plan_handler is not None
            and await self._answer_planning(connection, unique_id, payload)
        ):
            return
        if len(self._forwards) > MAX_FORWARDS:
            _LOGGER.warning("[forward] Vendor %s for %s refused: too many commands waiting", action, self.cp_id)
            await self._send_to_vendor(connection, build_call_result(unique_id, refusal_for(action)))
            return
        plan = plan_forward(action, payload, self._policy, self._transactions)
        if not isinstance(plan, Forward):
            _LOGGER.warning("[forward] Vendor %s for %s refused: %s", action, self.cp_id, plain_text(plan.reason))
            await self._send_to_vendor(connection, build_call_result(unique_id, plan.answer))
            return
        try:
            answer = await self._session.call(plan.action, plan.payload)
        except CallRejectedError as error:
            _LOGGER.info(
                "[forward] Vendor %s for %s: the charger answered %s", action, self.cp_id, plain_text(error.code, 40)
            )
            code = error.code if error.code in _RELAYED_ERRORS else ERROR_INTERNAL
            refusal = build_call_error(unique_id, code, "the charger refused the request")
            await self._send_to_vendor(connection, refusal)
            return
        except CallError as error:
            _LOGGER.warning("[forward] Vendor %s for %s did not reach the charger: %s", action, self.cp_id, error)
            await self._send_to_vendor(
                connection, build_call_error(unique_id, ERROR_INTERNAL, "the charger did not answer")
            )
            return
        _LOGGER.info(
            "[forward] Vendor %s for %s passed on; the charger answered %s", action, self.cp_id, answer.get("status")
        )
        if action == "DataTransfer":
            self._note_plan(plan.payload, answer)
            # A vendor-specific message the hub cannot judge: what was asked and what the charger said are both logged.
            _LOGGER.info("[forward] DataTransfer request for %s: %s", self.cp_id, plain_text(str(plan.payload), 1500))
            _LOGGER.info("[forward] DataTransfer answer of %s: %s", self.cp_id, plain_text(str(answer), 1500))
        else:
            _LOGGER.debug("[forward] the charger's answer to %s: %s", action, plain_text(str(answer), 1500))
        if answer.get("status") == "Accepted":
            self._note_accepted(action, plan.payload)
        await self._send_to_vendor(connection, build_call_result(unique_id, answer))
        await self._note_planning_switch(plan.payload, answer)

    # When the app switched its planning on or off and the charger accepted it, tell the plan, which then writes or
    # clears the profile. This comes after the vendor has its answer, because it talks to the charger again.
    async def _note_planning_switch(self, request: dict[str, Any], answer: dict[str, Any]) -> None:
        on = planning_switch(request) if self._plan_handler is not None else None
        if on is None or answer.get("status") != "Accepted" or self._plan_handler is None:
            return
        _LOGGER.info("[plan] The app switched its planning %s for %s", "on" if on else "off", self.cp_id)
        await self._plan_handler.planning_switched(self.cp_id, on)

    # Answer the app's planning from the hub's own plan instead of passing it on. A write is taken over by the plan (and
    # acknowledged only when that worked), a query is answered from the plan, and a message about the planning that the
    # hub cannot read completely is refused. A query while the hub has no plan yet is passed on, and its answer becomes
    # the plan. Returns True when the message has been answered here.
    async def _answer_planning(self, connection: ClientConnection, unique_id: str, payload: dict[str, Any]) -> bool:
        handler = self._plan_handler
        kind = classify(payload)
        if handler is None or kind is None:
            return False
        if isinstance(kind, PlanUnreadable):
            _LOGGER.warning(
                "[plan] The app's planning message for %s was refused: %s; it said %s",
                self.cp_id,
                kind.reason,
                plain_text(str(payload.get("data")), 600),
            )
            await self._send_to_vendor(connection, build_call_result(unique_id, {"status": "Rejected"}))
            return True
        if isinstance(kind, PlanWrite):
            stored = await handler.app_wrote(self.cp_id, kind.windows)
            _LOGGER.info(
                "[plan] The app wrote %s window(s) for %s; taken over: %s", len(kind.windows), self.cp_id, stored
            )
            reply = write_acknowledged(kind.session) if stored else {"status": "Rejected"}
            await self._send_to_vendor(connection, build_call_result(unique_id, reply))
            return True
        assert isinstance(kind, PlanQuery)
        windows = handler.plan(self.cp_id)
        if windows is None:
            return False
        _LOGGER.info(
            "[plan] The app asked for the planning of %s; answered with %s window(s)", self.cp_id, len(windows)
        )
        await self._send_to_vendor(connection, build_call_result(unique_id, plan_answer(kind.session, windows)))
        return True

    # If this DataTransfer was a planning exchange of the Raedian app that parses completely, keep the plan it shows.
    # The data are JSON with the hex of the protocol frame in `originalData`; anything that does not fit is ignored.
    def _note_plan(self, request: dict[str, Any], answer: dict[str, Any]) -> None:
        try:
            asked = json.loads(request.get("data") or "")["originalData"]
            answered = json.loads(answer.get("data") or "")["originalData"]
            windows = plan_from_exchange(asked, answered)
        except TypeError, ValueError, KeyError:
            return
        if windows is not None:
            self._session.record_app_plan([(w.weekday, w.start_s, w.end_s) for w in windows if w.enabled])
            if self._plan_handler is not None and self._plan_handler.plan(self.cp_id) is None:
                self._plan_handler.imported(self.cp_id, windows)

    # Take over what an accepted command of the vendor changed on the charger: the vendor's limit for a profile it set
    # or cleared, and the hub's own limit when a clear without an id (or by filters) could have removed that as well.
    # Then the charger is asked, in the background, what limit is really in effect now.
    def _note_accepted(self, action: str, payload: dict[str, Any]) -> None:
        snapshot = self._session.snapshot
        if action == "SetChargingProfile":
            profile = payload.get("csChargingProfiles", {})
            self._session.record_vendor_limit(
                profile_limit_a(profile), profile.get("chargingProfileId"), profile.get("chargingProfilePurpose")
            )
        elif action == "ClearChargingProfile":
            if "id" not in payload or payload["id"] == snapshot.vendor_profile_id:
                self._session.record_vendor_limit(None, None, None)
            if set(payload) - {"id"}:
                self._session.record_limit(None, None)
                self._session.record_vendor_limit(None, None, None)
        else:
            return
        self._session.request_effective_limit()

    # Send a frame to the vendor; a vendor that left meanwhile has no use for the answer.
    async def _send_to_vendor(self, connection: ClientConnection, text: str) -> None:
        try:
            await connection.send(text)
        except ConnectionClosed:
            _LOGGER.info("The vendor backend of %s closed before the answer to its command could be sent", self.cp_id)

    # Whether the frame a TriggerMessage asks for is known.
    def _can_trigger(self, request: TriggerRequest) -> bool:
        if request.message == ACTION_BOOT:
            return self._boot is not None
        if request.connector_id is None:
            return bool(self._statuses)
        return request.connector_id in self._statuses

    # Send the remembered Boot or Status frames again, with new unique ids.
    def _trigger(self, request: TriggerRequest) -> None:
        if request.message == ACTION_BOOT and self._boot is not None:
            cached = [self._boot]
        else:
            cached = [
                item for connector, item in sorted(self._statuses.items()) if request.connector_id in (None, connector)
            ]
        for item in cached:
            self._enqueue(replace(item, unique_id=_new_id()))

    # Send queued frames one at a time, each only after the previous one was answered.
    async def _send_loop(self, connection: ClientConnection) -> None:
        while (item := await self._next_item()) is not None:
            await self._deliver(connection, item)

    # The next frame to send: the oldest queued one, or a heartbeat when nothing happened for the vendor's interval.
    # None means the charger is gone and the queue is empty, so the link can close.
    async def _next_item(self) -> Outgoing | None:
        while True:
            queued = self._queue.pop()
            if queued is not None:
                return queued
            if self._charger_gone:
                self._closing = True
                return None
            self._wakeup.clear()
            try:
                await asyncio.wait_for(self._wakeup.wait(), self._heartbeat_s)
            except TimeoutError:
                return Outgoing(_new_id(), ACTION_HEARTBEAT, {})

    # Send one frame and process the answer.
    # The frame stays "in flight" until it is answered, so a connection that dies in between does not lose it.
    async def _deliver(self, connection: ClientConnection, item: Outgoing) -> None:
        if item.action != ACTION_HEARTBEAT:
            self._in_flight = item
        translation = translate_for_vendor(item.action, item.payload, self._transactions)
        if not translation.send:
            self._unmapped += 1
            _LOGGER.warning("Not sending %s of %s to the vendor backend: %s", item.action, self.cp_id, translation.note)
        else:
            if translation.note:
                _LOGGER.warning(
                    "Sending %s of %s without a transaction id: %s", item.action, self.cp_id, translation.note
                )
            self._accept_answer(item, await self._request(connection, item, translation.payload))
            self._delivered_on_this_connection = True
            self._end_failure_streak()
        self._in_flight = None
        if item.action == ACTION_STOP and is_integer(item.payload.get("transactionId")):
            self._transactions.forget(item.payload["transactionId"])

    # Send one request to the vendor and wait for its answer. A vendor that stays silent breaks the connection,
    # because nothing further can be sent while an answer is outstanding.
    async def _request(self, connection: ClientConnection, item: Outgoing, payload: dict[str, Any]) -> Frame:
        future: asyncio.Future[Frame] = asyncio.get_running_loop().create_future()
        self._replies[item.unique_id] = future
        try:
            await connection.send(build_call(item.unique_id, item.action, payload))
            return await asyncio.wait_for(future, self._settings.reply_timeout_s)
        except TimeoutError:
            raise TimeoutError(f"no answer to {item.action} within {self._settings.reply_timeout_s:g} s") from None
        finally:
            self._replies.pop(item.unique_id, None)

    # Use the vendor's answer. The boot answer sets the heartbeat interval, and the start answer carries the vendor's
    # transaction id. All other answers are discarded: the hub, not the vendor, decides what the charger does.
    def _accept_answer(self, item: Outgoing, answer: Frame) -> None:
        if answer[0] == MSG_CALL_ERROR:
            detail = f"{answer[2]}: {answer[3] if len(answer) > 3 else ''}"
            self._last_error = f"the vendor backend refused {item.action} with {detail}"
            _LOGGER.warning("Mirror link of %s: %s", self.cp_id, self._last_error)
            if item.action == ACTION_BOOT:
                raise _BootRefusedError(str(answer[2]), self._settings.backoff_max_s)
            self._pairing.abandon(item.unique_id)
            return
        self._last_success = self._clock()
        result = answer[2] if isinstance(answer[2], dict) else {}
        if item.action == ACTION_BOOT:
            self._accept_boot(result)
        elif item.action == ACTION_START and is_integer(result.get("transactionId")):
            self._pairing.vendor_answered(item.unique_id, result["transactionId"])

    # Take the heartbeat interval from an accepted boot, or wait out the vendor's interval after a refused one.
    def _accept_boot(self, result: dict[str, Any]) -> None:
        interval = result.get("interval")
        seconds = interval if is_integer(interval) and interval > 0 else 0
        if result.get("status") != "Accepted":
            raise _BootRefusedError(str(result.get("status")), float(seconds or self._settings.backoff_max_s))
        self._heartbeat_s = seconds or self._settings.fallback_heartbeat_s
        _LOGGER.info("Vendor backend accepted %s; heartbeat every %s s", self.cp_id, self._heartbeat_s)
