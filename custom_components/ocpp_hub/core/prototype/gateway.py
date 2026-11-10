"""Local authority with durable cloud delivery and explicitly approved vendor passthrough.

This is an offline engine, not a Home Assistant setup path. It reuses OCPP Hub's
frames and charging-profile builders while replacing its volatile mirror boundary.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from ..frames import Frame, build_call_error, build_call_result, parse_frame
from .auth_info import InfoBook, validate_info
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
        self.auth_info = InfoBook(journal, protected_tags=self.policy.allowed_tags)
        self.start_observer: Callable[[int, dict[str, Any]], None] | None = None
        if self.policy.strict_cloud_authorization:
            self.controls.authorize = self._local_start_allowed
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
            previous_boot = self.journal.get("boot")
            if previous_boot is not None and previous_boot != payload:
                self._restrict_trial_introduction()
                self.controls.invalidate()
            self.journal.put("boot", payload)
            self._boot_charger_epoch = self._charger_epoch
            self._boot_seen.set()
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
            timestamp(payload.get("timestamp"))
            known = self.journal.known_start(payload)
            if not known and not self._tag_allowed(payload.get("idTag")):
                raise DeniedError("tag is not approved for local authorization")

            # Permission binding and the pending flag belong to the very same commit as the new Start fact.
            def accepted(local: int) -> None:
                self.authorisation.consume(payload["idTag"], local)
                control = self.journal.get("control", {})
                self.journal.put("control", {**control, "start_pending": False})
                if self.start_observer is not None:
                    self.start_observer(local, payload)

            info = self._tag_info(payload["idTag"]) if self.policy.strict_cloud_authorization and not known else None
            if info is not None:
                if info["status"] != "Accepted":
                    raise DeniedError("authorization changed before accepting the new session")
                if not self._local_tag(payload["idTag"]):
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
        if (
            not isinstance(tag, str)
            or self.journal.get("trial_starting")
            or self.journal.get("control", {}).get("inhibit")
            or self.journal.get("trial_phase") not in (None, "active")
        ):
            return False
        selected = self._local_tag(tag) or (self.policy.trust_vendor_start_tags and self.authorisation.allows(tag))
        if not selected or not self.policy.strict_cloud_authorization:
            return bool(selected)
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

    # Preserve the actual correlated backend decision, checking generation after every await.
    async def _cloud_authorize(self, tag: str) -> dict[str, Any]:
        epoch = self._vendor_epoch
        await self._ensure_vendor_boot()
        self._require_epoch("vendor", epoch)
        result = await self.vendor.call(
            "Authorize", {"idTag": tag}, priority=5, check=lambda: self._require_epoch("vendor", epoch)
        )
        self._require_epoch("vendor", epoch)
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
        charger_epoch = self._charger_epoch
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
        if (
            kind == "write"
            and action != "RemoteStopTransaction"
            and (self.journal.get("trial_phase") not in (None, "active") or self.journal.get("trial_starting"))
        ):
            raise DeniedError("trial vendor controls are frozen")
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
        if self.policy.strict_cloud_authorization and not self._local_start_allowed(tag):
            raise DeniedError("current authorization does not permit a new local start")
        return await self.controls.start(tag)

    # A bounded management request must keep its grant and the existing static authorization valid at every send.
    async def start_managed(self, tag: str, guard: Callable[[], None]) -> Frame:
        # Revocation or a newer Pause can invalidate authorization while native evidence is awaited.
        def check() -> None:
            guard()
            if not self._local_start_allowed(tag):
                raise DeniedError("managed test tag authorization is no longer current")

        check()
        return await self.controls.start_managed(tag, check)

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
                metadata: dict[str, Any] | None = None
                tag = item.payload.get("idTag")
                if self.policy.strict_cloud_authorization and "idTagInfo" in answer[2]:
                    metadata = validate_info(answer[2]["idTagInfo"])
                    if item.action == "StartTransaction" and metadata["status"] != "Accepted":
                        with self.journal.atomic():
                            if isinstance(tag, str):
                                self.auth_info.put(tag, metadata)
                            self.journal.db.execute("UPDATE outbox SET state='rejected' WHERE token=?", (item.token,))
                            control = self.journal.get("control", {})
                            self.journal.put(
                                "control", {**control, "owner": "local", "inhibit": True, "uncertain": True}
                            )
                        await self.controls.ensure_ceiling()
                        raise RecoveryRequiredError("cloud rejected an already local session; zero hold retained")

                # Cache updates and deletion of this durable event share its actual acknowledgement commit.
                def acknowledged() -> None:
                    if metadata is not None and isinstance(tag, str):
                        self.auth_info.put(tag, metadata)

                self.journal.acknowledge(item, answer[2], acknowledged)
