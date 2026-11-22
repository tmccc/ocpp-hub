"""Journal ownership of managed current selection and fault state."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, cast, overload

from .managed_intent import ControlIntent, decode_intent, finish_request, insert_request, request_pending
from .store import RecoveryRequiredError, encoded

if TYPE_CHECKING:
    from .store import Journal

_CURRENT = "managed_current_change"
_FAULT = "managed_control_fault"
INTERRUPTED_CURRENT = "managed_current_interrupted_fault"


class CurrentFailure(Enum):
    """The next effect allowed after a failed current selection."""

    KEEP_NEWER_CONTROL = "keep_newer_control"
    PROTECT_OPEN_SESSION = "protect_open_session"
    HOLD_IDLE = "hold_idle"


@dataclass(frozen=True)
class TerminalCurrent:
    """Exact current records checked again after the caller obtains physical proof."""

    body: str
    request: tuple[Any, ...]
    target: int

    # Correlate only the original receipt with this proof.
    @property
    def identifier(self) -> str:
        return str(self.request[0])


@dataclass(frozen=True)
class CurrentChange:
    """Read view of the original JSON, including fields retained for exact recovery proofs."""

    value: dict[str, Any]

    # Keep unrecognized states visible so each caller can apply its own policy to them.
    @property
    def state(self) -> str | None:
        return self.value.get("state")

    # Correlate a selection to the original request and any worker still applying it.
    @property
    def identifier(self) -> str | None:
        return self.value.get("id")

    # Pending applies the requested current; recovery retains the previous selection.
    @property
    def selecting(self) -> bool:
        return self.state == "pending"

    # An open recovery can hold zero and reconcile an independent session.
    @property
    def recovering(self) -> bool:
        return self.state == "recovering"

    # A live selection or automatic recovery blocks a new Start, Return or selection.
    @property
    def awaiting_outcome(self) -> bool:
        return self.state in ("pending", "recovering")

    # Release additionally accounts for interrupted or uncertain terminal outcomes.
    @property
    def unresolved(self) -> bool:
        return self.state in ("pending", "recovering", "interrupted", "uncertain")

    # Debt proof accepts only a missing selection or one already settled.
    @property
    def settled_or_missing(self) -> bool:
        return self.state in (None, "completed", "cancelled")

    # Keep the later command in control and protect an independent open session without issuing a Stop.
    def failure_outcome(self, expected: ControlIntent, actual: ControlIntent, session_open: bool) -> CurrentFailure:
        if actual != expected:
            return CurrentFailure.KEEP_NEWER_CONTROL
        if expected.mode == "app" and (session_open or self.value.get("observed_open")):
            return CurrentFailure.PROTECT_OPEN_SESSION
        return CurrentFailure.HOLD_IDLE

    # A pending selection interrupted by replacement must preserve any later owner Pause.
    def replacement_state(self, intent: ControlIntent, owner_pause: bool) -> str:
        if intent.mode == "app":
            return "recovering"
        if self.value.get("mode") == "app" or owner_pause:
            return "cancelled"
        return "uncertain" if self.value.get("mode") == "paused" else "interrupted"


class CurrentChangeJournal:
    """Own current outcomes and their receipts while callers retain the proof and transaction."""

    # Share the existing journal connection and keep reads uncached.
    def __init__(self, journal: Journal) -> None:
        self.journal = journal

    # Ordinary decisions use an empty object when no selection has been recorded.
    def read(self) -> CurrentChange:
        return CurrentChange(self.journal.get(_CURRENT, {}))

    # Exact recovery snapshots distinguish a missing record from an empty object.
    def stored(self) -> dict[str, Any] | None:
        return cast("dict[str, Any] | None", self.journal.get(_CURRENT))

    # Save the selection and bind its arguments and caller revision in the acceptance transaction.
    def accept(
        self, identifier: str, target: int, previous: int, caller: ControlIntent, floor: int, prior_attempts: list[str]
    ) -> None:
        self.replace(
            {
                "id": identifier,
                "current_a": target,
                "previous_a": previous,
                "mode": caller.mode,
                "floor": floor,
                "prior_attempts": prior_attempts,
                "state": "pending",
            }
        )
        self.journal.db.execute("INSERT INTO managed_current_requests VALUES(?,?)", (identifier, target))
        insert_request(self.journal, identifier, "set_current", caller.revision, {"accepted_at": time.time()})

    # Binding is saved with the physical control claim, before station transmission can start.
    def bind(self, identifier: str, ticket: str, boot: str) -> None:
        change = self.read()
        if not change.selecting or change.identifier != identifier or not self.journal.db.in_transaction:
            raise RecoveryRequiredError("current selection no longer owns its station claim")
        self.replace({**change.value, "ticket": ticket, "boot": boot})

    # Publish the verified selection and its receipt together after the caller checks the live profile.
    def complete(self, identifier: str, target: int, revision: int) -> None:
        change = self.read()
        if (
            not change.selecting
            or change.identifier != identifier
            or change.value.get("current_a") != target
            or self._request_target(identifier) != target
            or not request_pending(self.journal, identifier, "set_current", revision)
        ):
            raise RecoveryRequiredError("current verification no longer owns the requested selection")
        saved = change.value
        binding: dict[str, Any] = {key: saved[key] for key in ("ticket", "boot") if key in saved}
        self.replace({"id": identifier, "current_a": target, "state": "completed", **binding})
        finish_request(self.journal, identifier, "completed", {"current_a": target, "charging_started": False})

    # Record the failed selection and retain its evidence whether a newer owner has already taken over.
    def fail(
        self, identifier: str, target: int, previous: int, mode: str, cause: dict[str, Any], outcome: CurrentFailure
    ) -> None:
        self.replace(
            {
                **self.read().value,
                "id": identifier,
                "current_a": target,
                "previous_a": previous,
                "mode": mode,
                "state": "uncertain",
                "failure": cause,
            },
        )
        finish_request(
            self.journal,
            identifier,
            "failed" if outcome is CurrentFailure.KEEP_NEWER_CONTROL else "uncertain",
            self.unconfirmed(target, previous),
        )

    # Preserve the original request while the caller claims a restrictive hold in the same transaction.
    def recover_overlap(
        self,
        identifier: str,
        previous: int,
        reason: str,
        revision: int,
        claim_hold: Callable[[], None],
        failure: dict[str, Any] | None,
    ) -> None:
        change = cast("dict[str, Any]", self.stored())
        self.replace(
            {
                **change,
                "state": "recovering",
                "previous_a": previous,
                "recovery_revision": revision,
                "recovery_reason": reason,
                **({"failure": failure} if failure is not None else {}),
            },
        )
        claim_hold()
        finish_request(self.journal, identifier, "failed", {"classification": reason})

    # Use the same outcome description for recording a failure and matching it during recovery.
    @staticmethod
    def unconfirmed(target: int, previous: int | None) -> dict[str, Any]:
        return {
            "classification": "current_unconfirmed",
            "requested_current_a": target,
            "previous_current_a": previous,
        }

    # A malformed request-specific failure may never borrow an older unbound diagnostic.
    def failure(self, change: dict[str, Any]) -> dict[str, Any] | None:
        if "failure" not in change:
            legacy = self.journal.get("managed_current_failure")
            return legacy if isinstance(legacy, dict) else None
        cause = change["failure"]
        if not isinstance(cause, dict) or set(cause) != {"id", "revision", "current_a", "type", "detail"}:
            return None
        row = self.journal.db.execute(
            "SELECT r.action,r.revision,c.current_a FROM managed_requests r "
            "JOIN managed_current_requests c ON c.id=r.id WHERE r.id=?",
            (change.get("id"),),
        ).fetchone()
        if (
            row is None
            or row["action"] != "set_current"
            or cause["id"] != change.get("id")
            or type(cause["revision"]) is not int
            or cause["revision"] != row["revision"]
            or type(cause["current_a"]) is not int
            or cause["current_a"] != row["current_a"]
            or cause["current_a"] != change.get("current_a")
            or not isinstance(cause["type"], str)
            or not 1 <= len(cause["type"]) <= 128
            or not isinstance(cause["detail"], str)
            or len(cause["detail"]) > 1024
        ):
            return None
        return dict(cause)

    # Keep the raw receipt bytes for rechecking the particular terminal request.
    def _receipt_snapshot(self, identifier: str | None) -> tuple[Any, ...] | None:
        request = self.journal.db.execute("SELECT * FROM managed_requests WHERE id=?", (identifier,)).fetchone()
        return tuple(request) if request is not None else None

    # Read the target separately so changed receipts can be rejected before reading the argument.
    def _request_target(self, identifier: str | None) -> Any:
        target = self.journal.db.execute(
            "SELECT current_a FROM managed_current_requests WHERE id=?", (identifier,)
        ).fetchone()
        return target[0] if target is not None else None

    # Zero, restoration and fault recovery all need the same exact failed selection and immutable argument.
    def _failed_selection(self, change: CurrentChange) -> TerminalCurrent | None:
        request = self._receipt_snapshot(change.identifier)
        target = self._request_target(change.identifier)
        if (
            request is None
            or request[1] != "set_current"
            or request[3] not in ("uncertain", "failed")
            or type(target) is not int
            or type(change.value.get("current_a")) is not int
            or target != change.value["current_a"]
        ):
            return None
        return TerminalCurrent(encoded(change.value), request, target)

    # A local zero keeps the previous selection both as the effective future limit and as the saved rollback value.
    def _previous_at_hold(self, change: dict[str, Any], selected: Callable[[], int]) -> bool:
        previous = change.get("previous_a")
        return type(previous) is int and previous == selected() == self.journal.get("control", {}).get("resume_limit")

    # A failed idle selection can still require its original local hold before its receipt may settle.
    def failed_idle_at_zero(self, selected: Callable[[], int], boot: str | None) -> bool:
        current = self.read()
        change = current.value
        record = self._failed_selection(current)
        return bool(
            current.state == "uncertain"
            and change.get("mode") == "app"
            and change.get("native_open") is False
            and change.get("observed_open") is False
            and "failure" in change
            and self.failure(change) is not None
            and record is not None
            and record.request[3] == "uncertain"
            and json.loads(record.request[4]) == self.unconfirmed(record.target, change.get("previous_a"))
            and decode_intent(self.journal.get("managed_control_intent"))
            == ControlIntent("paused", record.request[2] + 2)
            and self._previous_at_hold(change, selected)
            and boot is not None
            and change.get("source_boot") == boot
        )

    # A fresh zero proof can retire only a finished worker's exact failed selection with its prior limit retained.
    def terminal(self, *, worker_pending: bool, selected: Callable[[], int]) -> TerminalCurrent | None:
        current = self.read()
        change = current.value
        record = self._failed_selection(current)
        if (
            worker_pending
            or change.get("state") not in ("uncertain", "interrupted", "cancelled")
            or change.get("mode") not in ("app", "paused")
            or not self._previous_at_hold(change, selected)
            or record is None
            or (change.get("state") == "cancelled" and record.request[3] != "uncertain")
            or record.request[2] >= decode_intent(self.journal.get("managed_control_intent")).revision
        ):
            return None
        result = json.loads(record.request[4])
        if "failure" in change:
            if self.failure(change) is None or result != self.unconfirmed(change["current_a"], change["previous_a"]):
                return None
        elif result != {"classification": "process_replaced"}:
            return None
        return record

    # A candidate is usable only while its metadata, raw receipt and current argument still match.
    def require_terminal(self, record: TerminalCurrent) -> None:
        if encoded(self.read().value) != record.body or self._receipt_snapshot(record.identifier) != record.request:
            raise RecoveryRequiredError("terminal record changed before settlement")
        if self._request_target(record.identifier) != record.target:
            raise RecoveryRequiredError("terminal current target changed before settlement")

    # Ordinary verified zero ends uncertainty without rewriting the original outcome bytes.
    def settle_terminal(self, record: TerminalCurrent, selected: Callable[[], int]) -> None:
        state = json.loads(record.body)
        if not self._previous_at_hold(state, selected):
            raise RecoveryRequiredError("previous current selection changed before settlement")
        self._retire_failed(record, "previous_selection_at_verified_zero")

    # After verified restoration, retire the abandoned selection and preserve already terminal receipts.
    def restore_previous(
        self,
        selected: Callable[[], int],
        stopping: bool,
        recovery: dict[str, Any] | None,
    ) -> None:
        j = self.journal
        current = self.read()
        change = current.value
        record = self._failed_selection(current)
        if record is None:
            return
        _, _, revision, state, result = record.request
        automatic = recovery is not None
        eligible = change.get("state") in ("uncertain", "interrupted", "cancelled")
        if recovery is not None:
            # Native observations may refresh during awaits; only immutable request/authority fields are compared.
            keys = ("id", "current_a", "previous_a", "mode", "recovery_revision")
            eligible = (
                change.get("state") == "recovering"
                and all(change.get(key) == recovery.get(key) for key in keys)
                and change.get("mode") == "app"
                and not stopping
                and (intent := decode_intent(j.get("managed_control_intent"))).mode == "app"
                and intent.revision == change.get("recovery_revision")
                and revision + 1 == change.get("recovery_revision")
                and (state != "uncertain" or json.loads(result) == {"classification": "process_replaced"})
            )
        if eligible and change.get("previous_a") == selected():
            self._retire_failed(
                record,
                "verified_overlap_recovery" if automatic else "explicit_return_previous_selection",
                {
                    "classification": (
                        "superseded_by_verified_overlap_recovery" if automatic else "superseded_by_verified_return"
                    ),
                    "current_a": selected(),
                },
            )

    # A recognized historical fault can settle its own receipt after an explicit verified Pause.
    def reconcile_at_zero(self, change: dict[str, Any], previous: int) -> None:
        record = self._failed_selection(CurrentChange(change))
        if record is None:
            raise RecoveryRequiredError("the reconciled current no longer owns its original request")
        self._retire_failed(
            record,
            "explicit_pause_verified_zero",
            {**self.unconfirmed(record.target, previous), "reconciled_at_zero": True},
            previous_a=previous,
        )
        self.journal.db.execute("DELETE FROM meta WHERE key=?", (INTERRUPTED_CURRENT,))

    # Retire the exact request once; already-terminal results and unchanged failure bytes remain historical evidence.
    def _retire_failed(
        self, record: TerminalCurrent, reason: str, result: dict[str, Any] | None = None, **details: Any
    ) -> None:
        self.require_terminal(record)
        self.cancel(json.loads(record.body), **details, recovery_reason=reason)
        update = "state='failed'" if result is None else "state='failed',result=:result"
        self.journal.db.execute(
            f"UPDATE managed_requests SET {update} "
            "WHERE id=:identifier AND action='set_current' AND revision=:revision AND state='uncertain'",
            {
                "result": json.dumps(result) if result is not None else None,
                "identifier": record.identifier,
                "revision": record.request[2],
            },
        )

    # Only the caller can establish which exact preimage is still valid.
    def replace(self, value: dict[str, Any]) -> None:
        self.journal.put(_CURRENT, value)

    # Advance the caller's record without losing its original identity or other evidence.
    def cancel(self, value: dict[str, Any], **details: Any) -> None:
        self.replace({**value, "state": "cancelled", **details})


class ManagedFaultJournal:
    """Own the persisted latch. Recovery callers keep responsibility for proof and atomicity."""

    # The latch shares the journal's connection and transaction.
    def __init__(self, journal: Journal) -> None:
        self.journal = journal

    # Proofs may need to distinguish absence from a falsey retained JSON value.
    @overload
    def read(self) -> dict[str, Any] | None: ...

    # A caller expecting a mapping can give its historical missing-row default.
    @overload
    def read(self, default: dict[str, Any]) -> dict[str, Any]: ...

    # Return the original stored representation for equality and fingerprints.
    def read(self, default: dict[str, Any] | None = None) -> dict[str, Any] | None:
        return cast("dict[str, Any] | None", self.journal.get(_FAULT, default))

    # Read the exact classification after the caller proved that the latch is present.
    def classification(self) -> str:
        return cast(str, cast("dict[str, Any]", self.read())["classification"])

    # Most admission checks treat any truthy recorded fault as latched.
    def latched(self) -> bool:
        return bool(self.read())

    # Write the initial fault or a replacement already proved by the caller.
    def replace(self, fault: dict[str, Any]) -> None:
        self.journal.put(_FAULT, fault)

    # Clearing the latch does not itself change phase or grant positive authority.
    def clear(self) -> None:
        self.journal.db.execute("DELETE FROM meta WHERE key=?", (_FAULT,))
