"""Schedule decisions on the shared control owner's journal and command queue."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

from ..control import HUB_PROFILE_ID
from .control_state import ControlState
from .current_policy import attempt_key, current_session_decision, original_current_plan
from .managed_intent import ControlIntent
from .managed_sources import ORDINARY_HOLD, ORDINARY_SOURCE, confirmed_vendor_start, known_plan_source
from .recovery_contracts import ALLOW, GateDecision, Pending, Prerequisite
from .rpc import UnavailableError
from .start_handoff import NativeStartReport
from .store import RecoveryRequiredError, fingerprint

if TYPE_CHECKING:
    from .control_decisions import ControlDecisions
    from .gateway import Gateway
    from .managed import ManagedController
    from .managed_charging import ChargeLease, ManagedCharging
    from .ordinary_recovery import OrdinaryRecovery
    from .trial_handover_evidence import HandoverEvidence


class ScheduleControl:
    """Recognize the saved schedule and guard its charging reports and continuation."""

    # Use the existing control owner and journal for every schedule decision.
    def __init__(self, owner: ControlDecisions) -> None:
        self.owner = owner
        self.journal = owner.journal

    # Classify the same ledger ID using a verified native read. A scheduled transaction never spends a command.
    def observe(self, charging: ManagedCharging, n: HandoverEvidence) -> None:
        c, j = charging.controller, self.journal
        gateway, original = c.session.gateway, c.native.spec.saved_schedule_digest
        if n.saved_schedule is None:
            return
        plans = n.saved_schedule["planList"]
        digest = n.saved_schedule_digest
        if digest == original:
            catalogue: dict[str, list[int]] = {}
            for plan in plans:
                tag, identifier = plan.get("idTag"), plan.get("planId")
                if isinstance(tag, str) and type(identifier) is int:
                    catalogue.setdefault(fingerprint(tag.casefold()), []).append(identifier)
            if catalogue != j.get("managed_schedule_tags"):
                j.put("managed_schedule_tags", catalogue)
        self.attest_bound_vendor(n, original)
        self.bind_deferred_start(gateway, charging, n, original)

    # Distinct tags bind at allocation; their first complete native read must pin the same source as deferred tags.
    def attest_bound_vendor(self, n: HandoverEvidence, original: str) -> None:
        j = self.journal
        grant = j.get("vendor_start_grant", {})
        local = grant.get("local")
        if (
            "native_source" in grant
            or local is None
            or local != j.active()
            or local != n.physical.native_transaction_id
            or not n.physical.transaction_open
            or n.saved_schedule_digest != original
            or known_plan_source(j, n)
        ):
            return
        row = j.db.execute("SELECT start FROM transactions WHERE local=?", (local,)).fetchone()
        if row is None:
            return
        start = json.loads(row["start"])
        if not confirmed_vendor_start(j, fingerprint(start["idTag"].casefold()), local, settled=True):
            return
        j.put(
            "vendor_start_grant",
            {
                **grant,
                "native_source": {"marker": n.session_plan_id, "start": fingerprint(start), "schedule": original},
            },
        )

    # Preserve the exact pending requests at allocation, before a later acknowledgement, Pause, or restart.
    def defer_start_binding(self, local: int, tag: str) -> bool:
        j = self.journal
        digest = fingerprint(tag.casefold())
        owner, vendor = j.starts.receipt(), j.get("vendor_start_grant", {})
        local_match = bool(
            j.get("control", {}).get("start_pending")
            and owner.get("local") is None
            and owner.get("stage") in ("issued", "accepted")
            and owner.get("digest") == digest
        )
        cloud_match = bool(
            vendor.get("prepared") is not True
            and vendor.get("local") is None
            and vendor.get("digest") == digest
            and vendor.get("token")
        )
        if not (local_match or cloud_match):
            return False
        catalogue = j.get("managed_schedule_tags")
        if catalogue is not None and digest not in catalogue:
            return False
        j.put(
            "managed_deferred_start",
            {
                "local": local,
                "owner": local_match,
                "ticket": owner.get("ticket"),
                "vendor": vendor.get("token") if cloud_match else None,
            },
        )
        return True

    # Use independent native identity to bind only the command that still owns the actual report.
    def bind_deferred_start(
        self, gateway: Gateway, charging: ManagedCharging, n: HandoverEvidence, original: str
    ) -> None:
        j = self.journal
        pending = j.get("managed_deferred_start", {})
        local = pending.get("local")
        if (
            local is None
            or local != j.active()
            or local != n.physical.native_transaction_id
            or not n.physical.transaction_open
        ):
            return
        start = json.loads(j.db.execute("SELECT start FROM transactions WHERE local=?", (local,)).fetchone()["start"])
        tag = start["idTag"]
        catalogue = j.get("managed_schedule_tags")
        if catalogue is None:
            return
        digest = fingerprint(tag.casefold())
        if known_plan_source(j, n) or digest in catalogue and n.session_plan_id is None:
            return
        if pending["owner"] and pending["vendor"] is not None:
            return
        with j.atomic():
            if pending["owner"] and j.starts.receipt().get("ticket") == pending["ticket"]:
                self.owner.control.bind_start(tag, local)
                self.owner.bind_managed_start(charging, local, start)
            elif pending["vendor"] is not None and j.get("vendor_start_grant", {}).get("token") == pending["vendor"]:
                gateway.authorisation.consume(tag, local)
                grant = j.get("vendor_start_grant", {})
                j.put(
                    "vendor_start_grant",
                    {
                        **grant,
                        "native_source": {
                            "marker": n.session_plan_id,
                            "start": fingerprint(start),
                            "schedule": original,
                        },
                    },
                )
            j.put("managed_deferred_start", {})
        j.outcomes.retain_bound_owner_start()

    # Keep verified source evidence on its actual Boot; an ACK never identifies an unrelated native transaction.
    def record_start_source(self, charging: ManagedCharging, native: HandoverEvidence) -> None:
        g = charging.controller.session.gateway
        charging.source = (native, g.boot_observation, g.controls._revision)
        state, lease = charging.state(), charging.lease
        if state.get("state") == "completed" or state.get("independent_seen"):
            return
        if (
            not (lease is not None and lease.normal and lease.identifier == state.get("id"))
            and not charging.scoped_normal_stop()
        ):
            return
        local = self.journal.active()
        if local is not None and self.independent_daily(charging, native, local):
            self.journal.starts.independent_seen(state)

    # The verified original plan and matching ledger identify the daily even when the owner uses its tag.
    def independent_daily(self, charging: ManagedCharging, native: HandoverEvidence, local: int) -> bool:
        state, j = charging.state(), self.journal
        if (
            state.get("kind") != "normal"
            or not isinstance(state.get("tag_digest"), str)
            or not native.physical.transaction_open
            or native.physical.native_transaction_id != local
            or j.active() != local
            or native.physical.connection == "Unplug"
            or native.saved_schedule is None
            or j.starts.receipt().get("local") == local
        ):
            return False
        row = j.db.execute("SELECT start FROM transactions WHERE local=? AND stop IS NULL", (local,)).fetchone()
        if row is None:
            return False
        tag = json.loads(row["start"])["idTag"].casefold()
        return bool(
            any(
                plan["planId"] == native.session_plan_id
                and isinstance(plan.get("idTag"), str)
                and plan["idTag"].casefold() == tag
                and plan.get("kindType") == "Recurring"
                for plan in native.saved_schedule["planList"]
            )
        )

    # Cached evidence authorizes only this same transaction on the same fresh, authenticated introduction.
    def current_daily(self, charging: ManagedCharging, local: int) -> bool:
        c, g = charging.controller, charging.controller.session.gateway
        if charging.source is None:
            return False
        native, boot, revision = charging.source
        return bool(
            boot is not None
            and boot == g.boot_observation
            and revision == g.controls._revision
            and c.native.last_verified_at == native.physical.observed_at
            and 0 <= c.native.observation_age() <= 5
            and self.independent_daily(charging, native, local)
        )

    # Publish only validated native evidence; a later read must revoke an earlier independent-session identification.
    def record_current_source(self, n: HandoverEvidence, boot: str | None) -> None:
        j = self.journal
        change = j.current_change.read()
        if not change.awaiting_outcome:
            return
        local = n.physical.native_transaction_id
        original = j.get("native_bounded_handover", {}).get("spec", {}).get("saved_schedule_digest")
        plan = original_current_plan(n, original)
        row = j.db.execute("SELECT start FROM transactions WHERE local=? AND stop IS NULL", (local,)).fetchone()
        reported_plan = change.value.get("reported_plan") if change.value.get("reported_local") == local else None
        prior_attempts = set(change.value.get("prior_attempts", []))
        unnumbered = (
            local is None
            and plan is not None
            and any(
                record["transactionId"] == -1
                and record["idTagStart"].casefold() == plan["idTag"].casefold()
                and attempt_key(record) not in prior_attempts
                for record in n.history
            )
        )
        independent = bool(
            n.physical.transaction_open
            and local == j.active()
            and plan is not None
            and reported_plan in (None, plan["planId"])
            and row is not None
            and json.loads(row["start"])["idTag"].casefold() == plan["idTag"].casefold()
        )
        j.current_change.replace(
            {
                **change.value,
                "observed_open": change.value.get("observed_open", False) or n.physical.transaction_open or unnumbered,
                "native_open": n.physical.transaction_open or unnumbered,
                "source_boot": boot,
                "continuing_local": local if independent else None,
            },
        )

    # Only the exact original native plan or a positively accepted command bound to this transaction proves source.
    def ordinary_source(self, ordinary: OrdinaryRecovery, native: HandoverEvidence) -> dict[str, Any] | None:
        c, j = ordinary.c, self.journal
        local = j.active()
        if local is None:
            return {"kind": "idle"} if not native.physical.transaction_open else None
        if not native.physical.transaction_open or native.physical.native_transaction_id != local:
            return None
        row = j.db.execute("SELECT start FROM transactions WHERE local=? AND stop IS NULL", (local,)).fetchone()
        if row is None or native.saved_schedule_digest != c.native.spec.saved_schedule_digest:
            return None
        tag = json.loads(row["start"])["idTag"].casefold()
        digest = fingerprint(tag)
        state = c.charging.state()
        local_source = (
            state.get("native_source")
            if state.get("local_transaction") == local and ordinary.verified_normal()
            else None
        )
        expected_source = {
            "marker": native.session_plan_id,
            "start": fingerprint(json.loads(row["start"])),
            "schedule": c.native.spec.saved_schedule_digest,
        }
        if local_source is not None and local_source != expected_source:
            return None
        grant = j.get("vendor_start_grant", {})
        attested = grant.get("native_source")
        if attested is not None and attested != {
            "marker": native.session_plan_id,
            "start": fingerprint(json.loads(row["start"])),
            "schedule": c.native.spec.saved_schedule_digest,
        }:
            return None
        for plan in (native.saved_schedule or {}).get("planList", []):
            if plan.get("planId") == native.session_plan_id and str(plan.get("idTag", "")).casefold() == tag:
                return {"kind": "daily", "plan": plan["planId"], "tag": digest}
        if (
            local_source == expected_source
            and state.get("local_transaction") == local
            and not known_plan_source(j, native)
        ):
            return {
                "kind": "local",
                "tag": digest,
                "request": state["id"],
                "receipt": fingerprint(j.starts.receipt()),
            }
        if native.session_plan_id is not None and (known_plan_source(j, native) or attested is None):
            return None  # An unknown marker needs the exact command-source attestation made during binding.
        if confirmed_vendor_start(j, digest, local, settled=True):
            return {"kind": "vendor", "tag": digest, "mutation": grant["mutation"]}
        return None

    # Choose the domain's current source proof once, identically at preflight and the final native send gate.
    def continuation_decision(self, c: ManagedController, native: HandoverEvidence, local: int) -> GateDecision:
        j = self.journal
        if j.outcomes.awaits_stop_report(local):
            return Pending(Prerequisite.LIFECYCLE, "continuation awaits the Stop's lifecycle report")
        if j.current_change.read().recovering:
            return current_session_decision(c.session.gateway, native, local)
        elif c.ordinary.active():
            return c.ordinary.session_decision(native, local)
        elif not (c.charging.scoped_normal_stop() and self.independent_daily(c.charging, native, local)):
            return Pending(Prerequisite.SOURCE, "continuation requires an identified daily or owner session")
        return ALLOW

    # Use the owning hold to validate a delayed original-plan report.
    async def prepare_native_report(self, c: ManagedController, payload: dict[str, Any]) -> NativeStartReport | None:
        if self.journal.current_change.read().recovering:
            return await c.current.prepare_native_report(payload)
        if c.ordinary.active():
            return await self.prepare_ordinary_report(c.ordinary, payload)
        return None

    # A real daily Start can need its OCPP number before native HTTP publishes that number; zero remains mandatory.
    async def prepare_ordinary_report(
        self, ordinary: OrdinaryRecovery, payload: dict[str, Any]
    ) -> NativeStartReport | None:
        c, j = ordinary.c, self.journal
        g = c.session.gateway
        tag = payload.get("idTag")
        hold = j.get(ORDINARY_HOLD, {})
        if (
            not ordinary.ordinary()
            or not ordinary.active()
            or hold.get("local") is not None
            or hold.get("start") is not None
            or j.active() is not None
            or payload.get("connectorId") != 1
            or not isinstance(tag, str)
        ):
            return None
        epoch, boot, revision = g._charger_epoch, g.boot_observation, g.controls._revision
        ticket = j.get("control", {}).get("ticket")
        try:
            native = await c.native.read(closed=True)
            if not any(
                str(plan.get("idTag", "")).casefold() == tag.casefold()
                for plan in (native.saved_schedule or {}).get("planList", [])
            ):
                return None
            ordinary.confirm_zero_introduction(native, None)
            ordinary.require_authority(None)
        except (RecoveryRequiredError, UnavailableError, TimeoutError) as error:
            raise UnavailableError("ordinary daily report awaits fresh closed native zero") from error
        source = j.get(ORDINARY_SOURCE)
        control = j.get("control", {})
        station = j.db.execute(
            "SELECT state,confirmed FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
        ).fetchone()
        receipt = tuple(station) if station is not None else None

        # This is rechecked atomically after allocation, before its Start fact and binding can commit.
        def guard(identifier: int | None) -> None:
            current = j.get("control", {})
            current_station = j.db.execute(
                "SELECT state,confirmed FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
            ).fetchone()
            c.native.spec.require_boot(j.get("boot"))
            c.native.require(native, closed=True)
            if (
                not ordinary.ordinary()
                or not ordinary.active()
                or j.get(ORDINARY_HOLD) != hold
                or j.get(ORDINARY_SOURCE) != source
                or j.active() != identifier
                or g._charger_epoch != epoch
                or g.boot_observation != boot
                or g.controls._revision != revision
                or not g.current_boot_seen(epoch)
                or current != control
                or current.get("ticket") != ticket
                or not ControlState(current).explicit_local_hold
                or not g.controls.ready
                or g.controls.desired() != 0
                or (tuple(current_station) if current_station is not None else None) != receipt
                or identifier is not None
                and ordinary.start_digest(identifier) != fingerprint(payload)
            ):
                raise RecoveryRequiredError("ordinary daily report was superseded before allocation")

        guard(None)

        # Binding the actual accepted wire report grants no current; later exact native plan/source is still required.
        def accepted(identifier: int) -> None:
            guard(identifier)
            j.put(ORDINARY_HOLD, {**hold, "local": identifier, "start": ordinary.start_digest(identifier)})
            j.put(
                ORDINARY_SOURCE,
                {**source, "local": identifier, "start": ordinary.start_digest(identifier), "source": None},
            )

        return NativeStartReport(None, None, None, accepted)

    # An independently identified daily remains with the app when an unrelated test expires.
    def retire_test_beside_daily(
        self, charging: ManagedCharging, lease: ChargeLease, state: dict[str, Any], target: int | None
    ) -> bool:
        c, j = charging.controller, self.journal
        active, current = j.active(), charging.source
        if (
            lease.normal
            or active is None
            or active == target
            or current is None
            or c.requests.intent() != ControlIntent("app", lease.revision)
            or j.get("control", {}).get("start_pending")
            or j.get("vendor_start_grant")
        ):
            return False
        native, boot, revision = current
        g = c.session.gateway
        if (
            (boot, revision) != (g.boot_observation, g.controls._revision)
            or not 0 <= time.time() - native.physical.observed_at <= 15
            or (self.ordinary_source(c.ordinary, native) or {}).get("kind") != "daily"
        ):
            return False
        j.starts.retire_independent_test(lease, state)
        j.starts.set_lease(None)
        charging.changed.set()
        return True
