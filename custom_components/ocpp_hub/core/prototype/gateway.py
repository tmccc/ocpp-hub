"""Local authority with durable cloud delivery and explicitly approved vendor passthrough.

This is an offline engine, not a Home Assistant setup path. It reuses OCPP Hub's
frames and charging-profile builders while replacing its volatile mirror boundary.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from ..frames import Frame, build_call_error, build_call_result, parse_frame
from .auth_info import InfoBook, validate_info
from .authorisation import VendorAuthorisation
from .events import validate_observation
from .ics import TransactionQuery, is_transaction_query, unique_object
from .ics_writes import IcsWrite, known_write
from .local_control import LocalControl
from .managed_start_delivery import (
    record_start_denial,
    record_start_error,
    record_start_wire_ack,
    retry_deferred,
    supersede_unknown_start_retry,
)
from .managed_stop_delivery import approved_stop_metadata, record_stop_ack, reject_stop_reply
from .observation_receipts import ObservationReceipts
from .plans import PlanBook
from .policy import DeniedError, Policy, integer, number
from .rpc import InboundCache, Peer, RegistrationDeferredError, Sender, UnavailableError, reply_as
from .security import SecurityError
from .store import ConflictError, Journal, RecoveryRequiredError, encoded, fingerprint
from .vendor_read_recovery import recover_legacy_vendor_read


# Reject malformed or non-finite input before handing it to the upstream frame vocabulary.
def decode(raw: str | bytes) -> Frame:
    if not isinstance(raw, str):
        raise DeniedError("frame must be text")
    try:
        if len(raw.encode()) > 131072:
            raise DeniedError("frame must be bounded text")
        encoded(json.loads(raw, object_pairs_hook=unique_object))
        frame = parse_frame(raw)
    except ValueError, RecursionError:
        raise DeniedError("invalid JSON frame") from None
    if frame is None or type(frame[0]) is not int or not isinstance(frame[1], str) or not 1 <= len(frame[1]) <= 36:
        raise DeniedError("invalid frame identifier")
    if frame[0] == 2 and len(frame) == 4 and isinstance(frame[2], str) and isinstance(frame[3], dict):
        return frame
    if frame[0] == 3 and len(frame) == 3 and isinstance(frame[2], dict):
        return frame
    if (
        frame[0] == 4
        and len(frame) == 5
        and all(isinstance(frame[i], str) for i in (2, 3))
        and isinstance(frame[4], dict)
    ):
        return frame
    raise DeniedError("invalid frame shape")


# Validate timestamps without confusing a replayed event with a current observation.
def timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise DeniedError("timestamp must be a timezone-aware string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise DeniedError("invalid timestamp") from None
    if parsed.tzinfo is None:
        raise DeniedError("timestamp must have a timezone")
    try:
        return parsed.astimezone(UTC)
    except OverflowError, ValueError:
        raise DeniedError("timestamp is outside the supported UTC range") from None


@dataclass(frozen=True)
class NativeStartReport:
    """A held lifecycle report, with an optional verified native source, committed with the actual transaction."""

    native_id: int | None
    plan_id: int | None
    attempt: tuple[str, int] | None
    record: Callable[[int], None] = field(compare=False)


@dataclass(frozen=True)
class VendorProfileWrite:
    """The exact translated profile awaiting a real acceptance, including a new alias reservation."""

    vendor: str
    local: int
    transaction: int | None
    allocated: bool


class Gateway:
    """One admitted charger, independent local control, and one fixed vendor mirror."""

    # Nothing connects here; an offline harness must explicitly supply the peer senders.
    def __init__(self, journal: Journal, policy: Policy | None = None, timeout: float = 1.0) -> None:
        self.journal = journal
        recover_legacy_vendor_read(journal)
        self.plans = PlanBook(journal)
        self.policy = policy or Policy()
        self.charger = Peer("local", timeout)
        self.vendor = Peer("vendor", timeout)
        self.controls = LocalControl(journal, self.charger, self.policy, lambda: self._charger_epoch)
        self.authorisation = VendorAuthorisation(journal, self.policy.vendor_tag_ttl_s)
        self.auth_info = InfoBook(journal, protected_tags=self.policy.allowed_tags)
        self.boot_observer: Callable[[], None] | None = None
        self.status_observer: Callable[[str, dict[str, Any]], None] | None = None
        self.prepare_report_hold: Callable[[dict[str, Any]], Awaitable[None]] | None = None
        self.held_start_report: Callable[[str], bool] | None = None
        self.native_start_report: Callable[[dict[str, Any]], Awaitable[NativeStartReport | None]] | None = None
        self.remote_start_guard: Callable[[], bool] | None = None
        self.tag_guard: Callable[[str], bool] | None = None
        self.defer_command_binding: Callable[[int, str], bool] | None = None
        self.start_observer: Callable[[int, dict[str, Any]], None] | None = None
        if self.policy.strict_cloud_authorization:
            self.controls.authorize = self._local_start_allowed
        self._tasks: set[asyncio.Task[None]] = set()
        self.timer_retry_after: dict[tuple[str, str], float] = {}
        self._flush_lock = asyncio.Lock()
        self._boot_lock = asyncio.Lock()
        self._start_renewals: dict[tuple[str, int, int, str | None], asyncio.Future[bool]] = {}
        self._vendor_ready = False
        self.vendor_interval = 60
        self.vendor_registration = "disconnected"
        self._boot_retry_at = 0.0
        self._charger_epoch = 0
        self._boot_charger_epoch = -1
        self.boot_metadata_revision = 0
        self.boot_observation: str | None = None
        self._boot_seen = asyncio.Event()
        self._vendor_epoch = 0
        self._inbound = InboundCache(journal=journal)
        self._observations = ObservationReceipts(journal)
        self._start_retry_waits: dict[str, tuple[str, float]] = {}
        self.failures: list[str] = []

    # Restrict new transports or changed introductions before they can authorize any charging-capable request.
    def _restrict_trial_introduction(self) -> None:
        if self.journal.get("trial_phase") == "active" and self.journal.get("trial_binding") is not None:
            with self.journal.atomic():
                control = self.journal.get("control", {})
                self.journal.put("control", {**control, "owner": "local", "inhibit": True, "uncertain": True})
                self.journal.put("trial_phase", "recovering")

    # Attach only an already admitted transport; the gateway never opens sockets itself.
    def attach_charger(self, sender: Sender) -> int:
        self._restrict_trial_introduction()
        epoch = self.journal.connection_epoch()
        self.charger.detach()
        self._charger_epoch = epoch
        self._boot_seen.clear()
        self.boot_observation = None
        self._inbound.retire("charger", epoch)
        self.charger.sender = sender
        return epoch

    # Reconnecting the vendor requires a fresh BootNotification before queued traffic.
    def attach_vendor(self, sender: Sender) -> int:
        self.vendor.detach()
        self._vendor_epoch += 1
        self._inbound.retire("vendor", self._vendor_epoch)
        self.vendor.sender = sender
        self._vendor_ready = False
        self.vendor_registration = "connected"
        return self._vendor_epoch

    # A retired reader cannot detach a newer authenticated charger.
    def detach_charger(self, epoch: int) -> None:
        if epoch == self._charger_epoch:
            self.charger.detach()
            self.boot_observation = None
            self._charger_epoch = self.journal.connection_epoch()

    # A vendor outage never closes the local charger or cancels its handlers.
    def detach_vendor(self, epoch: int) -> None:
        if epoch == self._vendor_epoch:
            self.vendor.detach()
            self._vendor_epoch += 1
            self._vendor_ready = False
            self.vendor_registration = "disconnected"

    # A persisted introduction is useful for history but is not proof of the current charger generation.
    def current_boot_seen(self, epoch: int) -> bool:
        return epoch == self._charger_epoch and self._boot_charger_epoch == epoch

    # Waiters still check generation after waking, so a replacement's Boot cannot introduce an old connection.
    async def wait_for_boot(self, epoch: int) -> None:
        await self._boot_seen.wait()
        self._require_epoch("charger", epoch)

    # Stop pending work without delivering another command or discarding durable events.
    async def close(self) -> None:
        self.charger.detach()
        self.vendor.detach()
        await self.controls.close()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    # Drain request handlers in deterministic offline tests; receivers themselves never block on a bridge.
    async def drain(self) -> None:
        while self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            # Python 3.14 can return a completed gather without yielding to the
            # task-done callbacks that remove these tasks from the set.
            await asyncio.sleep(0)

    # Resolve replies immediately; process bounded incoming requests in separate tasks.
    async def receive(self, side: str, raw: str | bytes, epoch: int | None = None) -> None:
        if side not in ("charger", "vendor"):
            raise DeniedError("unknown peer")
        current_epoch = self._charger_epoch if side == "charger" else self._vendor_epoch
        if epoch is not None and epoch != current_epoch:
            raise UnavailableError("retired connection")
        frame = decode(raw)
        peer = self.charger if side == "charger" else self.vendor
        if frame[0] != 2:
            peer.receive(frame)
            return
        if len(self._tasks) >= 32:
            if peer.sender:
                await peer.sender(build_call_error(frame[1], "OccurrenceConstraintViolation", "request queue is full"))
            return
        task = asyncio.create_task(self._answer(side, current_epoch, frame))
        self._tasks.add(task)
        task.add_done_callback(self._finished)

    # Deduplicate a peer's requests before execution and never send old replies onto a replacement connection.
    async def _answer(self, side: str, epoch: int, frame: Frame) -> None:
        current = self._charger_epoch if side == "charger" else self._vendor_epoch
        if epoch != current:
            return

        # Core charger observations have their own durable receipts; heartbeat times are never cached.
        async def handle() -> str:
            return (
                await self._charger_call(frame, epoch) if side == "charger" else await self._vendor_call(frame, epoch)
            )

        text = (
            await handle()
            if side == "charger" and frame[2] != "DataTransfer"
            else await self._inbound.response(side, epoch, frame, handle)
        )
        current = self._charger_epoch if side == "charger" else self._vendor_epoch
        peer = self.charger if side == "charger" else self.vendor
        if epoch == current and peer.sender:
            await peer.sender(text)

    # A retired connection cannot execute queued work on a replacement transport.
    def _require_epoch(self, side: str, epoch: int) -> None:
        current = self._charger_epoch if side == "charger" else self._vendor_epoch
        if current != epoch:
            raise UnavailableError("request belongs to a retired connection")

    # Retrieve task exceptions without logging frame bodies, identities or credentials.
    def _finished(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and (error := task.exception()) is not None:
            self.failures.append(type(error).__name__)

    # Send a local result only after its storage commit; transport failure leaves the receipt available for retry.
    async def _charger_call(self, frame: Frame, epoch: int) -> str:
        token, action = str(frame[1]), str(frame[2])
        payload: dict[str, Any] = frame[3]
        mutation: str | None = None
        vendor_epoch = self._vendor_epoch
        try:
            if action == "DataTransfer":
                if is_transaction_query(payload) or known_write(payload):
                    raise DeniedError("this iCS query is supported only from the cloud")
                self._extension_allowed(payload)
                await self._ensure_vendor_boot()

                # Recheck ownership after waiting for the vendor's outgoing request gate.
                def prepare() -> dict[str, Any]:
                    nonlocal mutation
                    self._require_epoch("charger", epoch)
                    self._require_epoch("vendor", vendor_epoch)
                    self._extension_allowed(payload)
                    if self.policy.kind(action, payload) == "write":
                        mutation = self.journal.mutation_intent("charger", action, payload)
                    return payload

                answer = await self.vendor.call(action, prepare)
                if mutation:
                    self.journal.mutation_finished(mutation, True)
                text = reply_as(answer, token)
            else:
                if action == "Authorize" and self.policy.strict_cloud_authorization:
                    info = await self._authorize_tag(payload.get("idTag"))
                    self._require_epoch("charger", epoch)
                    if info["status"] == "Accepted" and not self._tag_allowed(payload.get("idTag")):
                        info = {"status": "Invalid"}
                    if info["status"] == "Accepted":
                        if (
                            self.authorisation.allows(payload.get("idTag", ""))
                            and self.charger.gate.busy
                            and not self.controls.ready
                        ):
                            raise UnavailableError("nested authorization cannot bypass an unconfirmed ceiling")
                        try:
                            await self.controls.ensure_ceiling()
                        except DeniedError:
                            # A newer Stop still returns the normal denied Authorize result, never a stale grant.
                            if self._tag_allowed(payload.get("idTag")):
                                raise
                            info = {"status": "Invalid"}
                    self._require_epoch("charger", epoch)
                    if info["status"] == "Accepted":
                        latest = self._tag_info(payload["idTag"])
                        info = (
                            latest
                            if self._tag_allowed(payload["idTag"]) or latest["status"] != "Accepted"
                            else {"status": "Invalid"}
                        )
                    return build_call_result(token, {"idTagInfo": info})
                if action == "StartTransaction" and self.prepare_report_hold and not self.journal.known_start(payload):
                    await self.prepare_report_hold(payload)
                    self._require_epoch("charger", epoch)
                ticket = self.journal.get("control", {}).get("ticket")
                if action == "StartTransaction" and self.policy.strict_cloud_authorization:
                    try:
                        await self._renew_direct_start(payload, epoch)
                    except DeniedError, UnavailableError:
                        if not self.journal.known_start(payload):
                            raise
                    self._require_epoch("charger", epoch)
                if action in ("Authorize", "StartTransaction") and self._tag_allowed(payload.get("idTag")):
                    await self.controls.ensure_ceiling()
                    self._require_epoch("charger", epoch)
                if (
                    action == "StartTransaction"
                    and self.journal.get("control", {}).get("ticket") != ticket
                    and not self.journal.known_start(payload)
                ):
                    raise DeniedError("a newer owner operation superseded the start")
                native_report = None
                if (
                    action == "StartTransaction"
                    and self.native_start_report is not None
                    and not self.journal.known_start(payload)
                ):
                    native_report = await self.native_start_report(payload)
                    tag = payload.get("idTag")
                    if (
                        native_report is not None
                        and self.policy.strict_cloud_authorization
                        and isinstance(tag, str)
                        and self._tag_info(tag)["status"] == "Expired"
                        and self.auth_info.accepted_for_delayed_report(
                            tag, timestamp(payload["timestamp"]).timestamp(), payload["timestamp"]
                        )
                        is None
                    ):
                        await self._renew_direct_start(payload, epoch, held_native=True)
                        refreshed = await self.native_start_report(payload)
                        if refreshed is None or refreshed != native_report:
                            raise DeniedError("the native occurrence changed during cloud authorization")
                        native_report = refreshed
                    self._require_epoch("charger", epoch)
                    if self.journal.get("control", {}).get("ticket") != ticket:
                        raise DeniedError("a newer owner operation superseded the reported start")
                receipt = f"{epoch}:{token}"
                text = build_call_result(
                    token, self._local_answer(receipt, action, payload, native_report=native_report)
                )
        except DeniedError, ConflictError:
            text = build_call_error(
                token, "PropertyConstraintViolation", "request conflicts with local policy or state"
            )
        except UnavailableError, RecoveryRequiredError, SecurityError, sqlite3.Error:
            if mutation:
                try:
                    self.journal.mutation_finished(mutation, False)
                except sqlite3.Error:
                    self.failures.append("JournalUnavailable")
            text = build_call_error(token, "InternalError", "outcome unavailable; success was not confirmed")
        return text

    # Accept local lifecycle messages while leaving opaque exchanges to their actual vendor.
    def _local_answer(
        self, token: str, action: str, payload: dict[str, Any], *, native_report: NativeStartReport | None = None
    ) -> dict[str, Any]:
        if action == "BootNotification":
            previous_boot = self.journal.get("boot")
            if previous_boot is not None and previous_boot != payload:
                self._restrict_trial_introduction()
                self.controls.invalidate()
            with self.journal.atomic():
                self._observations.boot(self._charger_epoch)
                self.journal.put("boot", payload)
            if previous_boot != payload:
                # A later return to the same body cannot erase an intervening introduction revocation.
                self.boot_metadata_revision += 1
            self._boot_charger_epoch = self._charger_epoch
            self.boot_observation = uuid.uuid4().hex
            self._boot_seen.set()
            if self.boot_observer is not None:
                self.boot_observer()
            return {
                "status": "Accepted",
                "interval": 60,
                "currentTime": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        if action == "Heartbeat":
            return {"currentTime": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")}
        if action == "Authorize":
            tag = payload.get("idTag")
            allowed = self._tag_allowed(tag)
            return {"idTagInfo": {"status": "Accepted" if allowed else "Invalid"}}
        if action == "StartTransaction":
            integer(payload.get("connectorId"), 1, 1)
            number(payload.get("meterStart"), 1e12)
            occurred = timestamp(payload.get("timestamp"))
            tag = payload.get("idTag")
            if not isinstance(tag, str):
                raise DeniedError("a reported start requires a tag")
            known = self.journal.known_start(payload)
            historical = None
            if (
                not known
                and self.policy.strict_cloud_authorization
                and (native_report is not None or self.held_start_report is not None and self.held_start_report(tag))
            ):
                historical = self.auth_info.accepted_for_delayed_report(tag, occurred.timestamp(), payload["timestamp"])
            if not known and not self._tag_allowed(
                tag, lifecycle=True, historical=historical is not None, native_report=native_report is not None
            ):
                raise DeniedError("tag is not approved for local authorization")

            # Permission binding and the pending flag belong to the very same commit as the new Start fact.
            def accepted(local: int) -> None:
                if native_report is not None:
                    native_report.record(local)
                elif self.defer_command_binding is None or not self.defer_command_binding(local, payload["idTag"]):
                    self.authorisation.consume(payload["idTag"], local)
                    self.controls.bind_start(payload["idTag"], local)
                if self.start_observer is not None:
                    self.start_observer(local, payload)

            info = historical or (self._tag_info(tag) if self.policy.strict_cloud_authorization and not known else None)
            if info is not None:
                if info["status"] != "Accepted":
                    raise DeniedError("authorization changed before accepting the new session")
                if historical is None and not self._local_tag(tag):
                    self.auth_info.require_dynamic(info)
            return self.journal.start(token, payload, accepted, info)
        if action == "StopTransaction":
            integer(payload.get("transactionId"))
            number(payload.get("meterStop"), 1e12)
            timestamp(payload.get("timestamp"))
            tag = payload.get("idTag")
            info = self.auth_info.get(tag) if self.policy.strict_cloud_authorization and isinstance(tag, str) else None
            if info is not None and info["status"] == "Accepted" and isinstance(tag, str) and not self._local_tag(tag):
                try:
                    self.auth_info.require_dynamic(info)
                except RecoveryRequiredError:
                    info = None
            response = {"idTagInfo": info} if info is not None else None
            return self.journal.stop(token, payload, self.authorisation.retire, response)
        if action in (
            "StatusNotification",
            "MeterValues",
            "FirmwareStatusNotification",
            "DiagnosticsStatusNotification",
        ):
            validate_observation(action, payload, self.journal, check_history=False)
            with self.journal.atomic():
                published = self._observations.accept(self._charger_epoch, token.partition(":")[2], action, payload)
                if action == "StatusNotification":
                    self.journal.put("last_status", payload)
                    if self.status_observer is not None:
                        self.status_observer(token, payload)
                elif published and action == "MeterValues" and type(payload.get("transactionId")) is int:
                    rows = payload.get("meterValue", [])
                    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                        observed = timestamp(rows[0].get("timestamp"))
                        if abs((datetime.now(UTC) - observed).total_seconds()) < 60:
                            self.journal.db.execute(
                                "UPDATE transactions SET live=1 WHERE local=? AND stop IS NULL",
                                (payload["transactionId"],),
                            )
            return {}
        raise DeniedError("unsupported charger request")

    # Local stop overrides both the static allowlist and an app-generated trusted vendor start grant.
    def _tag_allowed(
        self, tag: Any, *, lifecycle: bool = False, historical: bool = False, native_report: bool = False
    ) -> bool:
        if not isinstance(tag, str):
            return False
        inhibited = (
            self.journal.get("control", {}).get("inhibit") or self.tag_guard is not None and not self.tag_guard(tag)
        )
        phase = self.journal.get("trial_phase")
        held_report = (
            lifecycle
            and inhibited
            and (native_report or self.held_start_report is not None and self.held_start_report(tag))
        )
        if (
            inhibited
            and not held_report
            or self.journal.get("trial_starting")
            or phase not in (None, "active")
            and not (phase == "recovering" and held_report)
        ):
            return False
        # A confirmed held report consumes its original receipt without reviving a tag for new Starts.
        selected = (
            self._local_tag(tag)
            or self.policy.trust_vendor_start_tags
            and (held_report or self.authorisation.allows(tag))
        )
        if not selected or not self.policy.strict_cloud_authorization:
            return bool(selected)
        if held_report and historical:
            return True
        info = self.auth_info.get(tag)
        if info is not None and info["status"] == "Accepted" and not self._local_tag(tag):
            try:
                self.auth_info.require_dynamic(info)
            except RecoveryRequiredError:
                return False
        return (self._local_tag(tag) and info is None) or (info is not None and info["status"] == "Accepted")

    # Lookup is case-insensitive while journal facts and outbound OCPP preserve their original tag text.
    def _local_tag(self, tag: str) -> bool:
        return any(tag.casefold() == approved.casefold() for approved in self.policy.allowed_tags)

    # An explicit owner Start may release a completed hold, but cannot bypass revocation or recovery mode.
    def _local_start_allowed(self, tag: str) -> bool:
        if (
            not self._local_tag(tag)
            or self.remote_start_guard is not None
            and not self.remote_start_guard()
            or self.tag_guard is not None
            and not self.tag_guard(tag)
            or self.journal.get("trial_phase") not in (None, "active")
            or self.journal.get("trial_starting")
        ):
            return False
        info = self.auth_info.get(tag)
        return info is None or info["status"] == "Accepted"

    # Owner-approved local permissions have explicit local expiry, distinct from actual cloud metadata.
    def _tag_info(self, tag: str) -> dict[str, Any]:
        info = self.auth_info.get(tag)
        if info is not None:
            return info
        return self.auth_info.local() if self._local_tag(tag) else {"status": "Invalid"}

    # A native timer can send Start directly on the next day, after its previous cloud approval expired.
    async def _renew_direct_start(self, payload: dict[str, Any], epoch: int, *, held_native: bool = False) -> None:
        tag = payload.get("idTag")
        if (
            not isinstance(tag, str)
            or not self._local_tag(tag)
            or self.vendor.sender is None
            or self.journal.known_start(payload)
            or self.journal.active() is not None
            or self.journal.get("control", {}).get("inhibit")
            and not held_native
            or self.journal.get("trial_starting")
            or self.journal.get("trial_phase") not in (None, "active")
            and not (held_native and self.journal.get("trial_phase") == "recovering")
            or self.authorisation.allows(tag)
        ):
            return
        stored = self.auth_info.stored(tag)
        if stored is None or stored["status"] != "Accepted" or self._tag_info(tag)["status"] != "Expired":
            return
        integer(payload.get("connectorId"), 1, 1)
        number(payload.get("meterStart"), 1e12)
        timestamp(payload.get("timestamp"))
        ticket = self.journal.get("control", {}).get("ticket")

        # Work from a retired plug connection or superseded owner operation cannot renew current authority.
        def guard() -> None:
            self._require_epoch("charger", epoch)
            if (
                self.journal.get("control", {}).get("ticket") != ticket
                or self.auth_info.stored(tag) != stored
                or held_native
                and (not self.controls.ready or self.controls.desired() != 0)
            ):
                raise DeniedError("scheduled start authority changed during renewal")

        # Share even a timeout or unchanged expired answer across this burst of retries. A later burst may retry.
        key = (tag.casefold(), epoch, self._vendor_epoch, ticket)
        pending = self._start_renewals.get(key)
        if pending is not None:
            if not await asyncio.shield(pending):
                raise UnavailableError("scheduled start renewal was unavailable")
            return
        pending = asyncio.get_running_loop().create_future()
        self._start_renewals[key] = pending
        received = False
        try:
            try:
                await self._cloud_authorize(tag, guard=guard)
            except DeniedError:
                # An ordinary Authorize can publish a fresh decision while this Start waits for the vendor gate.
                self._require_epoch("charger", epoch)
                self._require_epoch("vendor", key[2])
                current = self.auth_info.get(tag)
                if (
                    self.journal.get("control", {}).get("ticket") != ticket
                    or current is None
                    or current["status"] != "Accepted"
                    or not held_native
                    and not self._tag_allowed(tag)
                ):
                    raise
            received = True
        finally:
            pending.set_result(received)
            del self._start_renewals[key]

    # Preserve the actual correlated backend decision, checking generation after every await.
    async def _cloud_authorize(self, tag: str, guard: Callable[[], None] | None = None) -> dict[str, Any]:
        epoch = self._vendor_epoch
        await self._ensure_vendor_boot()
        self._require_epoch("vendor", epoch)

        # Recheck both transports and any owner operation after waiting for the outbound gate.
        def check() -> None:
            self._require_epoch("vendor", epoch)
            if guard is not None:
                guard()

        result = await self.vendor.call("Authorize", {"idTag": tag}, priority=5, check=check)
        check()
        if result[0] != 3 or set(result[2]) != {"idTagInfo"}:
            raise UnavailableError("actual cloud authorization was unavailable")
        info = validate_info(result[2]["idTagInfo"])
        with self.journal.atomic():
            self.auth_info.put(tag, info)
        return info

    # Nested Authorize during a forwarded RemoteStart uses its preflighted grant without acquiring the vendor gate.
    async def _authorize_tag(self, tag: Any) -> dict[str, Any]:
        if (
            not isinstance(tag, str)
            or self.journal.get("control", {}).get("inhibit")
            or self.journal.get("trial_phase") not in (None, "active")
        ):
            return {"status": "Invalid"}
        if self.authorisation.allows(tag):
            if not self._local_tag(tag) and not self._tag_allowed(tag):
                info = self._tag_info(tag)
                return info if info.get("status") != "Accepted" else {"status": "Invalid"}
            return self._tag_info(tag)
        if not self._local_tag(tag):
            return {"status": "Invalid"}
        if self.vendor.sender is not None:
            return await self._cloud_authorize(tag)
        return self._tag_info(tag)

    # Opaque writes obey the same owner lock as standard mutations.
    def _extension_allowed(self, payload: dict[str, Any]) -> None:
        kind = self.policy.kind("DataTransfer", payload)
        if kind == "write" and self.journal.get("control", {}).get("owner") == "local":
            raise DeniedError("local owner blocks vendor extension writes")

    # Forward a real vendor request and its exact result/error instead of inventing successful acknowledgements.
    async def _vendor_call(self, frame: Frame, epoch: int) -> str:
        token, action = str(frame[1]), str(frame[2])
        original: dict[str, Any] = frame[3]
        mutation: str | None = None
        query: TransactionQuery | None = None
        ics_write: IcsWrite | None = None
        grant: str | None = None
        profile_write: VendorProfileWrite | None = None
        dispatched = False
        charger_epoch = self._charger_epoch

        # From this boundary onward even a sender exception can follow a physical transmission.
        def on_dispatch(_action: str) -> None:
            nonlocal dispatched
            dispatched = True

        try:
            self.policy.kind(action, original)
            self.policy.validate(action, original)
            self._check_vendor(action, original, require_cap=False)
            if self.policy.kind(action, original) == "write" and action != "RemoteStopTransaction":
                await self.controls.ensure_ceiling()
            if action == "RemoteStartTransaction" and self.policy.strict_cloud_authorization:
                info = await self._cloud_authorize(original["idTag"])
                if info["status"] != "Accepted":
                    raise DeniedError("cloud refused the trial start tag")
                if not self._local_tag(original["idTag"]):
                    self.auth_info.require_dynamic(info)

            # Allocate profile ids and mutation intent only after the in-gate policy check succeeds.
            def prepare() -> dict[str, Any]:
                nonlocal mutation, query, ics_write, grant, profile_write
                self._require_epoch("vendor", epoch)
                self._require_epoch("charger", charger_epoch)
                self._check_vendor(action, original)
                with self.journal.atomic():
                    if action == "DataTransfer" and is_transaction_query(original):
                        query = TransactionQuery.from_request(original, self.journal.historical_local_id)
                        payload = query.request(original)
                    elif action == "DataTransfer" and known_write(original):
                        ics_write = IcsWrite.prepare(original, self.policy, self.journal, self.plans)
                        payload = ics_write.payload
                    else:
                        old_profiles = self.journal.get("vendor_profiles", {})
                        payload = self._translated_command(action, original)
                        if action == "SetChargingProfile":
                            vendor_profile = str(original["csChargingProfiles"]["chargingProfileId"])
                            actual_profile = payload["csChargingProfiles"]
                            profile_write = VendorProfileWrite(
                                vendor_profile,
                                actual_profile["chargingProfileId"],
                                actual_profile.get("transactionId"),
                                vendor_profile not in old_profiles,
                            )
                    if action == "RemoteStartTransaction":
                        if self.policy.strict_cloud_authorization:
                            info = self.auth_info.get(original["idTag"])
                            if info is None or info["status"] != "Accepted":
                                raise DeniedError("trial authorization expired before forwarding")
                            if not self._local_tag(original["idTag"]):
                                self.auth_info.require_dynamic(info)
                        grant = self.authorisation.reserve(original["idTag"])
                    if self.policy.kind(action, original) == "write":
                        mutation = self.journal.mutation_intent("vendor", action, original)
                        if grant is not None:
                            current_grant = self.journal.get("vendor_start_grant", {})
                            self.journal.put("vendor_start_grant", {**current_grant, "mutation": mutation})
                return payload

            # Native validation awaits HTTP after preparation; revoke stale origin, ownership or tag authority then.
            def check() -> None:
                self._require_epoch("vendor", epoch)
                self._require_epoch("charger", charger_epoch)
                self._check_vendor(action, original, own_mutation=mutation)
                if mutation is not None:
                    row = self.journal.db.execute(
                        "SELECT origin,action,digest,state FROM mutations WHERE token=?", (mutation,)
                    ).fetchone()
                    if row is None or tuple(row) != ("vendor", action, fingerprint(original), "pending"):
                        raise DeniedError("this vendor request no longer owns its pending mutation")
                if action == "RemoteStartTransaction":
                    if self.policy.strict_cloud_authorization:
                        info = self.auth_info.get(original["idTag"])
                        if info is None or info["status"] != "Accepted":
                            raise DeniedError("cloud authorization expired during native validation")
                        if not self._local_tag(original["idTag"]):
                            self.auth_info.require_dynamic(info)
                    if grant is not None:
                        saved = self.journal.get("vendor_start_grant", {})
                        if (
                            saved.get("token") != grant
                            or saved.get("local") is not None
                            or not self.authorisation.allows(original["idTag"])
                            or not self._tag_allowed(original["idTag"])
                        ):
                            raise DeniedError("this vendor start no longer owns its current tag grant")

            answer = await self.charger.call(action, prepare, check=check, on_dispatch=on_dispatch)
            if (
                profile_write is not None
                and answer[0] == 3
                and answer[2]
                not in (
                    {"status": "Accepted"},
                    {"status": "Rejected"},
                    {"status": "NotSupported"},
                )
            ):
                raise UnavailableError("unobserved charging profile response")
            if (
                action == "RemoteStartTransaction"
                and answer[0] == 3
                and answer[2]
                not in (
                    {"status": "Accepted"},
                    {"status": "Rejected"},
                )
            ):
                raise UnavailableError("unobserved remote Start response")
            if query is not None:
                answer = query.response(answer)
            if grant is not None and answer[0] == 3 and answer[2] == {"status": "Rejected"}:
                self.authorisation.revoke(grant)
            with self.journal.atomic():
                if profile_write is not None and answer[0] == 3 and answer[2] == {"status": "Accepted"}:
                    self._accepted_vendor_profile(profile_write)
                if ics_write is not None and ics_write.accepted(answer):
                    self.plans.accepted(ics_write.plans, ics_write.clearing)
                if mutation:
                    confirmed = answer[0] != 4
                    self.journal.mutation_finished(mutation, confirmed)
                    if not confirmed:
                        self.journal.put(
                            "uncertain_vendor_command",
                            {"mutation": mutation, "action": action, "digest": fingerprint(original)},
                        )
            text = reply_as(answer, token)
        except DeniedError, ConflictError:
            text = build_call_error(token, "SecurityError", "request is not allowed by current local policy")
        except UnavailableError, RecoveryRequiredError, SecurityError, sqlite3.Error:
            # Storage may itself be unavailable; the already durable pre-send intent remains the recovery boundary.
            try:
                if mutation and dispatched:
                    self.journal.mutation_finished(mutation, False)
                    self.journal.put(
                        "uncertain_vendor_command",
                        {"mutation": mutation, "action": action, "digest": fingerprint(original)},
                    )
            except sqlite3.Error:
                self.failures.append("JournalUnavailable")
            text = build_call_error(token, "InternalError", "charger outcome is unconfirmed; request was not replayed")
        finally:
            self._finish_vendor_dispatch(action, original, mutation, grant, profile_write, dispatched)
        return text

    # Retire only proven-unsent work; cancellation or any error after dispatch retains physical uncertainty.
    def _finish_vendor_dispatch(
        self,
        action: str,
        original: dict[str, Any],
        mutation: str | None,
        grant: str | None,
        profile: VendorProfileWrite | None,
        dispatched: bool,
    ) -> None:
        if mutation is None and grant is None and profile is None:
            return
        try:
            with self.journal.atomic():
                if not dispatched:
                    if mutation is not None:
                        self.journal.db.execute("DELETE FROM mutations WHERE token=?", (mutation,))
                    if grant is not None:
                        self.authorisation.revoke(grant)
                    if profile is not None and profile.allocated:
                        profiles = self.journal.get("vendor_profiles", {})
                        if profiles.get(profile.vendor) == profile.local:
                            profiles.pop(profile.vendor)
                            self.journal.put("vendor_profiles", profiles)
                elif mutation is not None:
                    row = self.journal.db.execute("SELECT state FROM mutations WHERE token=?", (mutation,)).fetchone()
                    if row is not None and row[0] == "pending":
                        self.journal.mutation_finished(mutation, False)
                        self.journal.put(
                            "uncertain_vendor_command",
                            {"mutation": mutation, "action": action, "digest": fingerprint(original)},
                        )
        except sqlite3.Error:
            self.failures.append("JournalUnavailable")

    # Commit the last accepted definition even if the old transaction stopped while its reply was in flight.
    def _accepted_vendor_profile(self, profile: VendorProfileWrite) -> None:
        profiles = self.journal.get("vendor_profiles", {})
        transactions = self.journal.get("vendor_profile_transactions", {})
        if profile.transaction is None:
            profiles[profile.vendor] = profile.local
            transactions.pop(profile.vendor, None)
        elif self.journal.active() == profile.transaction:
            profiles[profile.vendor] = profile.local
            transactions[profile.vendor] = profile.transaction
        else:
            if profiles.get(profile.vendor) == profile.local:
                profiles.pop(profile.vendor)
                transactions.pop(profile.vendor, None)
        self.journal.put("vendor_profiles", profiles)
        self.journal.put("vendor_profile_transactions", transactions)

    # Recheck priority, owner and current transaction after acquiring the outgoing CALL gate.
    def _check_vendor(
        self, action: str, payload: dict[str, Any], require_cap: bool = True, *, own_mutation: str | None = None
    ) -> None:
        kind = self.policy.kind(action, payload)
        if (
            kind == "write"
            and action != "RemoteStopTransaction"
            and (self.journal.get("trial_phase") not in (None, "active") or self.journal.get("trial_starting"))
        ):
            raise DeniedError("trial vendor controls are frozen")
        control = self.journal.get("control", {})
        if kind == "write" and action != "RemoteStopTransaction":
            if own_mutation is None:
                self.journal.require_vendor_writes_reconciled()
            elif self.journal.db.execute(
                "SELECT 1 FROM mutations WHERE origin='vendor' AND state!='confirmed' AND token!=? LIMIT 1",
                (own_mutation,),
            ).fetchone():
                raise RecoveryRequiredError("another vendor write still requires reconciliation")
            if require_cap and not self.controls.ready:
                raise UnavailableError("station ceiling is not confirmed for this charger connection")
        if (
            kind == "write"
            and action != "RemoteStopTransaction"
            and (control.get("owner") == "local" or control.get("inhibit"))
        ):
            raise DeniedError("explicit local control has priority")
        if action == "RemoteStartTransaction" and self.remote_start_guard is not None and not self.remote_start_guard():
            raise DeniedError("an outstanding native timer must finish before a separate remote Start")
        if action == "RemoteStartTransaction" and (
            self.journal.active() is not None or self.journal.get("control", {}).get("start_pending")
        ):
            raise DeniedError("a transaction is already unresolved on this connector")
        if action == "RemoteStopTransaction" and self.journal.local_id(payload["transactionId"]) is None:
            raise DeniedError("cloud stop no longer names the active transaction")
        if action == "SetChargingProfile":
            vendor = payload["csChargingProfiles"].get("transactionId")
            if vendor is not None and self.journal.local_id(vendor) is None:
                raise DeniedError("cloud profile no longer names the active transaction")

    # A retired alias needs a new live slot before an explicit request can recreate its independently absent profile.
    def _vendor_profile_retired(self, local: int) -> bool:
        if not self.journal.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trial_resources'"
        ).fetchone():
            return False
        row = self.journal.db.execute(
            "SELECT state FROM trial_resources WHERE key=?", ("profile:" + str(local),)
        ).fetchone()
        return bool(row is not None and row[0] == "absent_verified")

    # Retain every alias identity while independently absent trial profiles stop consuming the live namespace bound.
    def _vendor_profile_capacity(self, profiles: dict[str, int]) -> bool:
        if not self.journal.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trial_resources'"
        ).fetchone():
            return len(profiles) < self.journal.capacity
        absent = {
            row[0] for row in self.journal.db.execute("SELECT key FROM trial_resources WHERE state='absent_verified'")
        }
        return sum("profile:" + str(local) not in absent for local in profiles.values()) < self.journal.capacity

    # Translate session ids in both control directions and isolate vendor profile ids from local profile 7001.
    def _translated_command(self, action: str, original: dict[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = json.loads(encoded(original))
        if action == "RemoteStopTransaction":
            local = self.journal.local_id(payload["transactionId"])
            if local is None:
                raise DeniedError("unknown cloud transaction")
            payload["transactionId"] = local
        if action in ("SetChargingProfile", "ClearChargingProfile"):
            profiles = self.journal.get("vendor_profiles", {})
            profile = payload.get("csChargingProfiles", {})
            original_id = profile["chargingProfileId"] if action == "SetChargingProfile" else payload["id"]
            key = str(original_id)
            if key not in profiles:
                if action == "ClearChargingProfile":
                    raise DeniedError("cloud does not own this profile")
                if not self._vendor_profile_capacity(profiles):
                    raise DeniedError("vendor profile namespace is full")
                local_profile = int(self.journal.get("next_vendor_profile", 99999)) + 1
                if local_profile >= 2**31:
                    raise RecoveryRequiredError("vendor profile counter exhausted")
                self.journal.put("next_vendor_profile", local_profile)
                profiles[key] = local_profile
                self.journal.put("vendor_profiles", profiles)
            elif (
                action == "SetChargingProfile"
                and self._vendor_profile_retired(profiles[key])
                and not self._vendor_profile_capacity(profiles)
            ):
                raise DeniedError("vendor profile namespace is full")
            if action == "SetChargingProfile":
                profile["chargingProfileId"] = profiles[key]
                if "transactionId" in profile:
                    local = self.journal.local_id(profile["transactionId"])
                    if local is None:
                        raise DeniedError("profile names an unknown cloud transaction")
                    profile["transactionId"] = local
            else:
                payload["id"] = profiles[key]
        return payload

    # The dedicated local service serializes safety-cap and command sequences under one priority gate.
    async def set_limit(self, value: Any) -> Frame:
        return await self.controls.set_limit(value)

    # An explicit local start keeps any previous lower ceiling and restores zero if its outcome is uncertain.
    async def start(self, tag: str = "LOCAL_TEST") -> Frame:
        if self.policy.strict_cloud_authorization and not self._local_start_allowed(tag):
            raise DeniedError("current authorization does not permit a new local start")
        return await self.controls.start(tag)

    # A bounded management request must keep its grant and the existing static authorization valid at every send.
    async def start_managed(
        self,
        tag: str,
        guard: Callable[[], None],
        *,
        request_id: str | None = None,
        on_prepared: Callable[[], None] | None = None,
    ) -> Frame:
        # Revocation or a newer Pause can invalidate authorization while native evidence is awaited.
        def check() -> None:
            guard()
            if not self._local_start_allowed(tag):
                raise DeniedError("managed test tag authorization is no longer current")

        check()
        return await self.controls.start_managed(tag, check, request_id=request_id, on_prepared=on_prepared)

    # A local stop installs a durable zero-current hold even when there is no known transaction to stop.
    async def stop(self) -> Frame:
        return await self.controls.stop()

    # Releasing ownership confirms the physical resume ceiling before permitting vendor writes again.
    async def release(self, acknowledge_uncertainty: bool = False) -> None:
        await self.controls.release(acknowledge_uncertainty)

    # Introduce the virtual charge point once per cloud connection without blocking the local receiver.
    async def _ensure_vendor_boot(self) -> None:
        epoch = self._vendor_epoch
        async with self._boot_lock:
            self._require_epoch("vendor", epoch)
            if self._vendor_ready:
                return
            remaining = self._boot_retry_at - asyncio.get_running_loop().time()
            if remaining > 0:
                raise RegistrationDeferredError(remaining)
            boot = self.journal.get("boot")
            if boot is None:
                raise UnavailableError("charger has not introduced itself")
            answer = await self.vendor.call("BootNotification", boot, priority=5)
            self._require_epoch("vendor", epoch)
            if answer[0] != 3:
                raise UnavailableError("vendor registration returned a protocol error")
            status, interval = answer[2].get("status"), answer[2].get("interval")
            if (
                status not in ("Accepted", "Pending", "Rejected")
                or type(interval) is not int
                or not 1 <= interval <= 86400
            ):
                raise UnavailableError("vendor registration response is invalid")
            try:
                timestamp(answer[2].get("currentTime"))
            except DeniedError:
                raise UnavailableError("vendor registration time is invalid") from None
            self.vendor_registration = status
            self.vendor.diagnostics.registration(status)
            self.vendor_interval = interval
            if status != "Accepted":
                self._boot_retry_at = asyncio.get_running_loop().time() + interval
                raise RegistrationDeferredError(float(interval))
            self._vendor_ready = True

    # OCPP heartbeat is independent of charger traffic and even an ambiguous retained transaction.
    async def heartbeat(self) -> None:
        epoch = self._vendor_epoch
        await self._ensure_vendor_boot()
        answer = await self.vendor.call("Heartbeat", {}, priority=5, check=lambda: self._require_epoch("vendor", epoch))
        self._require_epoch("vendor", epoch)
        if answer[0] != 3:
            raise UnavailableError("vendor heartbeat failed")
        try:
            timestamp(answer[2].get("currentTime"))
        except DeniedError:
            raise UnavailableError("vendor heartbeat time is invalid") from None

    # Deliver durable events in order; a cloud-accepted Start with a lost reply requires explicit reconciliation.
    async def flush(self) -> None:
        epoch = self._vendor_epoch
        async with self._flush_lock:
            self._require_epoch("vendor", epoch)
            await self._ensure_vendor_boot()
            while (item := self.journal.next_delivery()) is not None:
                if item.state in ("ambiguous", "inflight", "rejected"):
                    if item.action == "StartTransaction" and item.state in ("ambiguous", "inflight"):
                        with self.journal.atomic():
                            self.journal.db.execute("UPDATE outbox SET state='ambiguous' WHERE token=?", (item.token,))
                            current_head = self.journal.next_delivery()
                            assert current_head is not None
                            supersede_unknown_start_retry(self.journal, current_head)
                    raise RecoveryRequiredError("cloud delivery requires explicit reconciliation")
                if retry_deferred(self.journal, item, self._start_retry_waits):
                    return
                payload = dict(item.payload)
                if item.action in ("MeterValues", "StopTransaction") and "transactionId" in payload:
                    vendor = self.journal.vendor_id(integer(payload["transactionId"]))
                    if vendor is None:
                        raise RecoveryRequiredError("transaction mapping is not known; event retained")
                    payload["transactionId"] = vendor
                try:
                    current = item
                    sent = False

                    # Persist possible delivery inside the CALL gate, immediately before send.
                    def before_send() -> None:
                        nonlocal sent
                        self._require_epoch("vendor", epoch)
                        self.journal.mark_sent(current)
                        sent = True

                    answer = await self.vendor.call(item.action, payload, token=item.token, before_send=before_send)
                except UnavailableError, asyncio.CancelledError:
                    if sent:
                        with self.journal.atomic():
                            self.journal.db.execute(
                                "UPDATE outbox SET state=? WHERE token=?",
                                ("ambiguous" if item.action == "StartTransaction" else "queued", item.token),
                            )
                            current_head = self.journal.next_delivery()
                            if current_head is not None:
                                supersede_unknown_start_retry(self.journal, current_head)
                    raise
                if answer[0] != 3:
                    if item.action in (
                        "StatusNotification",
                        "MeterValues",
                        "FirmwareStatusNotification",
                        "DiagnosticsStatusNotification",
                    ):
                        self.journal.reject_observation(item, str(answer[2]))
                        continue
                    if item.action == "StopTransaction":
                        reject_stop_reply(self.journal, item, answer, reason="cloud_call_error")
                        raise RecoveryRequiredError("cloud rejected a retained lifecycle event")
                    if item.action == "StartTransaction" and not record_start_error(self.journal, item, answer):
                        return
                    raise RecoveryRequiredError("cloud rejected a retained lifecycle event")
                metadata: dict[str, Any] | None = None
                ignored_stop_metadata = False
                tag = item.payload.get("idTag")
                if self.policy.strict_cloud_authorization and "idTagInfo" in answer[2]:
                    try:
                        metadata = validate_info(answer[2]["idTagInfo"])
                    except DeniedError:
                        if item.action != "StopTransaction":
                            raise
                        if not approved_stop_metadata(self.journal, item, answer[2]):
                            reject_stop_reply(self.journal, item, answer[2], reason="authorization_metadata_invalid")
                            raise RecoveryRequiredError(
                                "cloud Stop metadata requires exact independent reconciliation"
                            ) from None
                        ignored_stop_metadata = True
                denied = item.action == "StartTransaction" and metadata is not None and metadata["status"] != "Accepted"

                # Cache updates and deletion of this durable event share its actual acknowledgement commit.
                def acknowledged() -> None:
                    if metadata is not None and isinstance(tag, str):
                        self.auth_info.put(tag, metadata)
                    if denied:
                        record_start_denial(self.journal, item, answer[2])
                    if ignored_stop_metadata:
                        row = self.journal.db.execute(
                            "SELECT start FROM transactions WHERE local=?", (item.transaction,)
                        ).fetchone()
                        rejected_tag = tag if isinstance(tag, str) else json.loads(row[0])["idTag"] if row else None
                        if isinstance(rejected_tag, str):
                            self.journal.db.execute(
                                "DELETE FROM auth_info WHERE digest=?", (fingerprint(rejected_tag.casefold()),)
                            )
                    record_stop_ack(self.journal, item, answer[2])
                    record_start_wire_ack(self.journal, item, answer[2])

                try:
                    self.journal.acknowledge(item, answer[2], acknowledged, allow_denied=denied)
                except ConflictError, RecoveryRequiredError:
                    if item.action == "StartTransaction":
                        with self.journal.atomic():
                            self.journal.db.execute("UPDATE outbox SET state='ambiguous' WHERE token=?", (item.token,))
                            current_head = self.journal.next_delivery()
                            assert current_head is not None
                            supersede_unknown_start_retry(self.journal, current_head)
                    raise
                if denied:
                    await self.controls.ensure_ceiling()
                    raise RecoveryRequiredError("cloud denied the mapped local session; zero hold retained")
