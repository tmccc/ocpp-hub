"""Local authority with durable cloud delivery and explicitly approved vendor passthrough.

This is an offline engine, not a Home Assistant setup path. It reuses OCPP Hub's
frames and charging-profile builders while replacing its volatile mirror boundary.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime
from typing import Any

from ..frames import Frame, build_call_error, build_call_result, parse_frame
from .authorisation import VendorAuthorisation
from .events import validate_observation
from .ics import TransactionQuery, is_transaction_query, unique_object
from .ics_writes import IcsWrite, known_write
from .local_control import LocalControl
from .plans import PlanBook
from .policy import DeniedError, Policy, integer, number
from .rpc import InboundCache, Peer, RegistrationDeferredError, Sender, UnavailableError, reply_as
from .store import ConflictError, Journal, RecoveryRequiredError, encoded, fingerprint


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
    return parsed.astimezone(UTC)


class Gateway:
    """One admitted charger, independent local control, and one fixed vendor mirror."""

    # Nothing connects here; an offline harness must explicitly supply the peer senders.
    def __init__(self, journal: Journal, policy: Policy | None = None, timeout: float = 1.0) -> None:
        self.journal = journal
        self.plans = PlanBook(journal)
        self.policy = policy or Policy()
        self.charger = Peer("local", timeout)
        self.vendor = Peer("vendor", timeout)
        self.controls = LocalControl(journal, self.charger, self.policy, lambda: self._charger_epoch)
        self.authorisation = VendorAuthorisation(journal, self.policy.vendor_tag_ttl_s)
        self._tasks: set[asyncio.Task[None]] = set()
        self._flush_lock = asyncio.Lock()
        self._boot_lock = asyncio.Lock()
        self._vendor_ready = False
        self.vendor_interval = 60
        self.vendor_registration = "disconnected"
        self._boot_retry_at = 0.0
        self._charger_epoch = 0
        self._boot_charger_epoch = -1
        self._boot_seen = asyncio.Event()
        self._vendor_epoch = 0
        self._inbound = InboundCache()
        self.failures: list[str] = []

    # Attach only an already admitted transport; the gateway never opens sockets itself.
    def attach_charger(self, sender: Sender) -> int:
        epoch = self.journal.connection_epoch()
        self.charger.detach()
        self._charger_epoch = epoch
        self._boot_seen.clear()
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
                if action in ("Authorize", "StartTransaction") and self._tag_allowed(payload.get("idTag")):
                    await self.controls.ensure_ceiling()
                receipt = f"{epoch}:{token}"
                text = build_call_result(token, self._local_answer(receipt, action, payload))
        except DeniedError, ConflictError:
            text = build_call_error(
                token, "PropertyConstraintViolation", "request conflicts with local policy or state"
            )
        except UnavailableError, RecoveryRequiredError, sqlite3.Error:
            if mutation:
                try:
                    self.journal.mutation_finished(mutation, False)
                except sqlite3.Error:
                    self.failures.append("JournalUnavailable")
            text = build_call_error(token, "InternalError", "outcome unavailable; success was not confirmed")
        return text

    # Accept local lifecycle messages while leaving opaque exchanges to their actual vendor.
    def _local_answer(self, token: str, action: str, payload: dict[str, Any]) -> dict[str, Any]:
        if action == "BootNotification":
            self.journal.put("boot", payload)
            self._boot_charger_epoch = self._charger_epoch
            self._boot_seen.set()
            return {"status": "Accepted", "interval": 60, "currentTime": datetime.now(UTC).isoformat()}
        if action == "Heartbeat":
            return {"currentTime": datetime.now(UTC).isoformat()}
        if action == "Authorize":
            tag = payload.get("idTag")
            allowed = self._tag_allowed(tag)
            return {"idTagInfo": {"status": "Accepted" if allowed else "Invalid"}}
        if action == "StartTransaction":
            integer(payload.get("connectorId"), 1, 1)
            number(payload.get("meterStart"), 1e12)
            timestamp(payload.get("timestamp"))
            known = self.journal.known_start(payload)
            if not known and not self._tag_allowed(payload.get("idTag")):
                raise DeniedError("tag is not approved for local authorization")

            # Permission binding and the pending flag belong to the very same commit as the new Start fact.
            def accepted(local: int) -> None:
                self.authorisation.consume(payload["idTag"], local)
                control = self.journal.get("control", {})
                self.journal.put("control", {**control, "start_pending": False})

            return self.journal.start(token, payload, accepted)
        if action == "StopTransaction":
            integer(payload.get("transactionId"))
            number(payload.get("meterStop"), 1e12)
            timestamp(payload.get("timestamp"))
            return self.journal.stop(token, payload, self.authorisation.retire)
        if action in (
            "StatusNotification",
            "MeterValues",
            "FirmwareStatusNotification",
            "DiagnosticsStatusNotification",
        ):
            validate_observation(action, payload, self.journal)
            with self.journal.atomic():
                self.journal.enqueue(action, payload, payload.get("transactionId"))
                if action == "StatusNotification":
                    self.journal.put("last_status", payload)
                elif action == "MeterValues" and type(payload.get("transactionId")) is int:
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
    def _tag_allowed(self, tag: Any) -> bool:
        if not isinstance(tag, str) or self.journal.get("control", {}).get("inhibit"):
            return False
        return tag in self.policy.allowed_tags or (
            self.policy.trust_vendor_start_tags and self.authorisation.allows(tag)
        )

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
        charger_epoch = self._charger_epoch
        try:
            self.policy.kind(action, original)
            self.policy.validate(action, original)
            self._check_vendor(action, original, require_cap=False)
            if self.policy.kind(action, original) == "write" and action != "RemoteStopTransaction":
                await self.controls.ensure_ceiling()

            # Allocate profile ids and mutation intent only after the in-gate policy check succeeds.
            def prepare() -> dict[str, Any]:
                nonlocal mutation, query, ics_write, grant
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
                        payload = self._translated_command(action, original)
                    if action == "RemoteStartTransaction":
                        grant = self.authorisation.reserve(original["idTag"])
                    if self.policy.kind(action, original) == "write":
                        mutation = self.journal.mutation_intent("vendor", action, original)
                return payload

            answer = await self.charger.call(action, prepare)
            if query is not None:
                answer = query.response(answer)
            if grant is not None and (answer[0] != 3 or answer[2].get("status") != "Accepted"):
                self.authorisation.revoke(grant)
            with self.journal.atomic():
                if ics_write is not None and ics_write.accepted(answer):
                    self.plans.accepted(ics_write.plans, ics_write.clearing)
                if mutation:
                    confirmed = answer[0] != 4
                    self.journal.mutation_finished(mutation, confirmed)
            text = reply_as(answer, token)
        except DeniedError, ConflictError:
            text = build_call_error(token, "SecurityError", "request is not allowed by current local policy")
        except UnavailableError, RecoveryRequiredError, sqlite3.Error:
            # Storage may itself be unavailable; the already durable pre-send intent remains the recovery boundary.
            try:
                if mutation:
                    self.journal.mutation_finished(mutation, False)
                self.journal.put("uncertain_vendor_command", {"action": action, "digest": fingerprint(original)})
            except sqlite3.Error:
                self.failures.append("JournalUnavailable")
            text = build_call_error(token, "InternalError", "charger outcome is unconfirmed; request was not replayed")
        return text

    # Recheck priority, owner and current transaction after acquiring the outgoing CALL gate.
    def _check_vendor(self, action: str, payload: dict[str, Any], require_cap: bool = True) -> None:
        kind = self.policy.kind(action, payload)
        control = self.journal.get("control", {})
        if kind == "write" and action != "RemoteStopTransaction":
            self.journal.require_vendor_writes_reconciled()
            if require_cap and not self.controls.ready:
                raise UnavailableError("station ceiling is not confirmed for this charger connection")
        if (
            kind == "write"
            and action != "RemoteStopTransaction"
            and (control.get("owner") == "local" or control.get("inhibit"))
        ):
            raise DeniedError("explicit local control has priority")
        if action == "RemoteStartTransaction" and self.journal.active() is not None:
            raise DeniedError("a transaction is already unresolved on this connector")
        if action == "RemoteStopTransaction" and self.journal.local_id(payload["transactionId"]) is None:
            raise DeniedError("cloud stop no longer names the active transaction")
        if action == "SetChargingProfile":
            vendor = payload["csChargingProfiles"].get("transactionId")
            if vendor is not None and self.journal.local_id(vendor) is None:
                raise DeniedError("cloud profile no longer names the active transaction")

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
                if len(profiles) >= self.journal.capacity:
                    raise DeniedError("vendor profile namespace is full")
                local_profile = int(self.journal.get("next_vendor_profile", 99999)) + 1
                if local_profile >= 2**31:
                    raise RecoveryRequiredError("vendor profile counter exhausted")
                self.journal.put("next_vendor_profile", local_profile)
                profiles[key] = local_profile
                self.journal.put("vendor_profiles", profiles)
            if action == "SetChargingProfile":
                profile["chargingProfileId"] = profiles[key]
                if "transactionId" in profile:
                    local = self.journal.local_id(profile["transactionId"])
                    if local is None:
                        raise DeniedError("profile names an unknown cloud transaction")
                    profile["transactionId"] = local
                    transactions = self.journal.get("vendor_profile_transactions", {})
                    transactions[key] = local
                    self.journal.put("vendor_profile_transactions", transactions)
            else:
                payload["id"] = profiles[key]
        return payload

    # The dedicated local service serializes safety-cap and command sequences under one priority gate.
    async def set_limit(self, value: Any) -> Frame:
        return await self.controls.set_limit(value)

    # An explicit local start keeps any previous lower ceiling and restores zero if its outcome is uncertain.
    async def start(self, tag: str = "LOCAL_TEST") -> Frame:
        return await self.controls.start(tag)

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
                    raise RecoveryRequiredError("cloud delivery requires explicit reconciliation")
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
                        self.journal.db.execute(
                            "UPDATE outbox SET state=? WHERE token=?",
                            ("ambiguous" if item.action == "StartTransaction" else "queued", item.token),
                        )
                    raise
                if answer[0] != 3:
                    self.journal.db.execute("UPDATE outbox SET state='rejected' WHERE token=?", (item.token,))
                    raise RecoveryRequiredError("cloud rejected a retained lifecycle event")
                self.journal.acknowledge(item, answer[2])
