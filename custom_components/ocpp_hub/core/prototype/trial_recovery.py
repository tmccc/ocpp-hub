"""Restricted restart admission establishes zero before known-session Stop and never resumes charging itself."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from ..control import HUB_PROFILE_ID
from .gateway import Gateway
from .policy import number
from .security import SecurityError
from .station import StationChoice
from .store import RecoveryRequiredError, fingerprint
from .trial_evidence import Evidence
from .trial_resources import ResourceBook


# A native open session must be the exact accepted local fact, never merely another positive transaction number.
def require_recovery_state(evidence: Evidence, identity: str, known: int | None) -> None:
    evidence.require_fresh(identity)
    number(evidence.current_a, 1000)
    number(evidence.power_w, 1000000)
    states = {"A": "Unplug", "B": "Plug", "C": "Charging"}
    if states.get(evidence.cp) != evidence.connection:
        raise SecurityError("native connection state is unsupported or contradictory")
    if evidence.cp in ("A", "B") and (evidence.current_a != 0 or evidence.power_w != 0):
        raise SecurityError("idle native connection reports flowing power")
    if evidence.transaction_open:
        if known is None or type(evidence.native_transaction_id) is not int or evidence.native_transaction_id != known:
            raise SecurityError("native open session does not match the saved local transaction")
    elif evidence.native_transaction_id is not None or evidence.cp == "C":
        raise SecurityError("native session and connector observations contradict each other")


class Recovery:
    """Fresh trials stay unplugged; only an existing verified ownership ledger may recover while plugged in."""

    # Keep recovery policy separate from transport startup and ordinary charging controls.
    def __init__(
        self,
        gateway: Gateway,
        book: ResourceBook,
        observe: Callable[[], Awaitable[Evidence]],
        choice: StationChoice,
    ) -> None:
        self.gateway, self.book, self.observe, self.choice = gateway, book, observe, choice
        self.introduced_epoch: int | None = None
        self.introduced_revision: int | None = None

    # Configuration, station selection and physical introduction cannot silently change across process replacement.
    def require_binding(self, evidence: Evidence, boot: bool = False) -> None:
        binding = self.gateway.journal.get("trial_binding")
        if (
            not isinstance(binding, dict)
            or binding.get("configuration_digest") != evidence.configuration_digest
            or binding.get("station") != self.choice.value()
            or boot
            and binding.get("boot_digest") != fingerprint(self.gateway.journal.get("boot"))
        ):
            raise SecurityError("recovery configuration or charger introduction does not match the saved binding")

    # Require actual existing station ownership, without treating a missing or already removed profile as ours.
    def require_owned_station(self, evidence: Evidence) -> None:
        owned = self.book.owned_present(evidence)
        if not any(resource.key == "profile:" + str(HUB_PROFILE_ID) for resource in owned):
            raise SecurityError("recovery requires the independently present owned station profile")
        receipt = self.gateway.journal.db.execute(
            "SELECT state FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
        ).fetchone()
        if receipt is None or receipt["state"] != "confirmed":
            raise SecurityError("recovery station ownership remains uncertain")

    # Persist the hold before binding; native state can authorize safety Stop but never adoption of an unknown Start.
    def admit(self, evidence: Evidence) -> None:
        journal = self.gateway.journal
        require_recovery_state(evidence, journal.identity, journal.active())
        self.require_owned_station(evidence)
        if journal.get("trial_binding") is not None or evidence.cp != "A":
            self.require_binding(evidence)
        with journal.atomic():
            control = journal.get("control", {})
            journal.put("control", {**control, "owner": "local", "inhibit": True, "uncertain": True})
            journal.put("trial_phase", "recovering")

    # Every new transport generation reaudits the native session and returns to a zero hold before cloud writes.
    async def introduce(self, evidence: Evidence, epoch: int) -> None:
        gateway, journal = self.gateway, self.gateway.journal
        gateway._require_epoch("charger", epoch)
        phase = journal.get("trial_phase")
        if phase not in ("active", "recovering", "blocked"):
            raise SecurityError("recovery cannot introduce a charger after cleanup has started")
        if self.introduced_epoch is not None and epoch != self.introduced_epoch and phase == "active":
            self.admit(evidence)
            phase = "recovering"
        self.book.owned_present(evidence)
        binding = journal.get("trial_binding")
        if binding is None and phase == "active":
            evidence.require_unplugged(journal.identity)
            if journal.db.execute("SELECT COUNT(*) FROM trial_resources").fetchone()[0]:
                raise SecurityError("an existing profile cannot acquire a new introduction binding")
            journal.put(
                "trial_binding",
                {
                    "configuration_digest": evidence.configuration_digest,
                    "station": self.choice.value(),
                    "boot_digest": fingerprint(journal.get("boot")),
                },
            )
        elif binding is not None:
            self.require_binding(evidence, boot=True)
        self.introduced_epoch = epoch
        self.introduced_revision = gateway.controls._revision
        if phase == "active":
            journal.put("trial_starting", False)
        if phase in ("recovering", "blocked"):
            require_recovery_state(evidence, journal.identity, journal.active())
            self.require_owned_station(evidence)
            if evidence.transaction_open:
                await gateway.stop()
            else:
                await gateway.controls.ensure_ceiling()
        gateway._require_epoch("charger", epoch)

    # Explicit owner acknowledgement clears recovery only after real closure and delivery, while retaining zero.
    async def resume(self) -> dict[str, Any]:
        gateway, journal = self.gateway, self.gateway.journal
        epoch = gateway._charger_epoch
        ticket = journal.get("control", {}).get("ticket")
        if (
            journal.get("trial_phase") != "recovering"
            or gateway.charger.sender is None
            or not gateway.current_boot_seen(epoch)
            or self.introduced_epoch != epoch
            or self.introduced_revision != gateway.controls._revision
        ):
            raise RecoveryRequiredError("resume requires a connected restricted recovery session")
        evidence = await self.observe()
        gateway._require_epoch("charger", epoch)
        require_recovery_state(evidence, journal.identity, journal.active())
        self.require_binding(evidence, boot=True)
        self.require_owned_station(evidence)
        control = journal.get("control", {})
        if (
            journal.get("trial_phase") != "recovering"
            or not gateway.current_boot_seen(epoch)
            or self.introduced_epoch != epoch
            or self.introduced_revision != gateway.controls._revision
            or control.get("ticket") != ticket
            or not control.get("inhibit")
            or not gateway.controls.ready
            or gateway.controls.desired() != 0
            or evidence.current_a != 0
            or evidence.power_w != 0
            or evidence.transaction_open
            or journal.active() is not None
            or control.get("start_pending")
            or journal.get("uncertain_vendor_command")
            or journal.db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
            or journal.get("vendor_start_grant", {}).get("local") is None
            and journal.get("vendor_start_grant", {})
        ):
            raise RecoveryRequiredError("physical closure, confirmed zero and actual history delivery are required")
        journal.require_vendor_writes_reconciled()
        journal.require_allocation()
        with journal.atomic():
            journal.put("control", {**control, "owner": "local", "inhibit": True, "uncertain": False})
            journal.put("trial_phase", "active")
        return {"recovery_acknowledged": True, "zero_hold_retained": True, "charging_resumed": False}
