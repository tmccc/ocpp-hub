"""Restore only positively identified ordinary owner authority after an admitted station-zero recovery."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

from ..control import HUB_PROFILE_ID
from .gateway import NativeStartReport
from .local_control import local_start_receipt
from .managed_cutoffs import supervised_timer
from .managed_occurrences import occurrences
from .managed_schedule import recorded_station_profile
from .rpc import UnavailableError
from .store import RecoveryRequiredError, ReleaseDeferredError, fingerprint
from .trial_rollback import require_lifecycle_idle

if TYPE_CHECKING:
    from .managed import ManagedController
    from .trial_handover_evidence import HandoverEvidence

KEY = "managed_ordinary_recovery"
SOURCE = "managed_ordinary_source"


class OrdinaryRecovery:
    """A station hold never creates a Start, guesses a Stop, or borrows another lifecycle's continuation receipt."""

    # The existing controller retains all native/resource/zero and outgoing ownership guards.
    def __init__(self, controller: ManagedController) -> None:
        self.c = controller

    # Timer/current/test state machines retain exclusive ownership of their own restoration obligations.
    def ordinary(self) -> bool:
        c, j = self.c, self.c.session.journal
        return (
            not c.stopping
            and not j.get("managed_control_fault")
            and j.get("trial_phase") in ("active", "recovering")
            and c.requests.intent().mode == "app"
            and not any(
                supervised_timer(lease) or lease.get("state") == "requested" for lease in occurrences(j).values()
            )
            and not c.current.pending()
            and c.tests.lease is None
            and c.tests.state().get("state") in (None, "completed")
            and j.get("managed_current_change", {}).get("state") not in ("interrupted", "uncertain")
        )

    # A newer explicit Pause or control revision immediately invalidates the saved positive recovery authority.
    def active(self) -> bool:
        saved = self.c.session.journal.get(KEY, {})
        intent = self.c.requests.intent()
        return saved.get("state") == "holding" and intent.mode == "app" and saved.get("revision") == intent.revision

    # The immutable accepted Start binds identity; a connector number or native session number alone cannot do so.
    def start_digest(self, local: int | None) -> str | None:
        if local is None:
            return None
        row = self.c.session.journal.db.execute(
            "SELECT start FROM transactions WHERE local=? AND stop IS NULL", (local,)
        ).fetchone()
        return fingerprint(json.loads(row["start"])) if row is not None else None

    # Only the exact original native plan or a positively accepted command bound to this transaction proves source.
    def source(self, native: HandoverEvidence) -> dict[str, Any] | None:
        j = self.c.session.journal
        local = j.active()
        if local is None:
            return {"kind": "idle"} if not native.physical.transaction_open else None
        if not native.physical.transaction_open or native.physical.native_transaction_id != local:
            return None
        row = j.db.execute("SELECT start FROM transactions WHERE local=? AND stop IS NULL", (local,)).fetchone()
        if row is None or native.saved_schedule_digest != self.c.native.spec.saved_schedule_digest:
            return None
        tag = json.loads(row["start"])["idTag"].casefold()
        digest = fingerprint(tag)
        for plan in (native.saved_schedule or {}).get("planList", []):
            if plan.get("planId") == native.session_plan_id and str(plan.get("idTag", "")).casefold() == tag:
                return {"kind": "daily", "plan": plan["planId"], "tag": digest}
        if native.session_plan_id is not None:
            return None  # A claimed foreign plan or original plan with the wrong tag contradicts manual source.
        grant = j.get("vendor_start_grant", {})
        if (
            grant.get("local") == local
            and grant.get("digest") == digest
            and j.db.execute(
                "SELECT 1 FROM mutations WHERE token=? AND origin='vendor' "
                "AND action='RemoteStartTransaction' AND state='confirmed'",
                (grant.get("mutation"),),
            ).fetchone()
        ):
            return {"kind": "vendor", "tag": digest, "mutation": grant["mutation"]}
        return None

    # Publish or revoke source after every successful full native audit; an older proof cannot survive contradiction.
    def observe(self, native: HandoverEvidence) -> None:
        c, j, g = self.c, self.c.session.journal, self.c.session.gateway
        if not self.ordinary() or not g.current_boot_seen(g._charger_epoch):
            return
        j.put(
            SOURCE,
            {
                "revision": c.requests.intent().revision,
                "local": j.active(),
                "start": self.start_digest(j.active()),
                "source": self.source(native),
                "boot": g.boot_observation,
                "epoch": g._charger_epoch,
                "observed_at": native.physical.observed_at,
                "native_digest": native.digest,
            },
        )

    # The shared zero-only recovery works after reconnect as well as after a bounded ordinary telemetry outage.
    def needed(self) -> bool:
        j = self.c.session.journal
        return self.ordinary() and (self.active() or j.get("trial_phase") == "recovering")

    # Existing commissioned ownership and a current matching Boot permit only zero while native HTTP is unavailable.
    async def hold(self, reason: str) -> None:
        c, j, g = self.c, self.c.session.journal, self.c.session.gateway
        if not self.ordinary() or not g.current_boot_seen(g._charger_epoch):
            raise RecoveryRequiredError("ordinary recovery awaits admitted current owner authority")
        c.native.spec.require_boot(j.get("boot"))
        if not recorded_station_profile(j, c.native.spec.digest, c.native.spec.boot_digest):
            raise RecoveryRequiredError("ordinary zero hold requires the existing admitted station receipt")
        current = j.get(KEY, {})
        local = j.active()
        if not self.active():
            current = {
                "state": "holding",
                "revision": c.requests.intent().revision,
                "local": local,
                "start": self.start_digest(local),
                "selected_a": c.current.selected(),
                "reason": reason,
                "at_epoch": time.time(),
            }
            j.put(KEY, current)
        elif current.get("local") is not None and local is None:
            old = j.db.execute("SELECT stop FROM transactions WHERE local=?", (current["local"],)).fetchone()
            if old is not None and old["stop"] is not None:
                current = {**current, "local": None, "start": None}
                j.put(KEY, current)
        epoch, boot = g._charger_epoch, g.boot_observation
        if not j.get("control", {}).get("inhibit") or j.get("control", {}).get("owner") != "local":
            g.controls._claim(True, stopping=True)
        j.put("trial_phase", "recovering")
        c.released_epoch = None

        # A new owner intent, source epoch or overlapping state machine cannot inherit this station write.
        def guard() -> None:
            c.native.spec.require_boot(j.get("boot"))
            if (
                not self.ordinary()
                or not self.active()
                or j.get(KEY) != current
                or g._charger_epoch != epoch
                or g.boot_observation != boot
                or not g.current_boot_seen(epoch)
                or not recorded_station_profile(j, c.native.spec.digest, c.native.spec.boot_digest)
                or g.controls.desired() != 0
            ):
                raise RecoveryRequiredError("ordinary zero hold was superseded")

        await g.controls.ensure_ceiling(guard=guard)

    # This guard also runs inside the real positive RPC gate after all native HTTP waits.
    def require_authority(self, local: int | None, *, introduced: bool = True) -> None:
        c, j, g = self.c, self.c.session.journal, self.c.session.gateway
        hold, source = j.get(KEY, {}), j.get(SOURCE, {})
        grant, local_start = j.get("vendor_start_grant", {}), local_start_receipt(j)
        if (
            not self.ordinary()
            or not self.active()
            or hold.get("local") != local
            or j.active() != local
            or hold.get("start") != self.start_digest(local)
            or hold.get("selected_a") != c.current.selected()
            or source.get("revision") != c.requests.intent().revision
            or source.get("local") != local
            or source.get("start") != hold.get("start")
            or not source.get("source")
            or source.get("boot") != g.boot_observation
            or source.get("epoch") != g._charger_epoch
            or not g.current_boot_seen(g._charger_epoch)
            or g.vendor.sender is None
            or g.vendor_registration != "Accepted"
            or not c.session.runtime.status.vendor_connected
            or local is not None
            and j.vendor_id(local) is None
            or introduced
            and c.native.introduced_epoch != g._charger_epoch
            or introduced
            and c.native.introduced_revision != g.controls._revision
            or not 0 <= time.time() - source.get("observed_at", 0) <= 5
            or j.get("trial_preservation_fault")
            or j.get("trial_inventory_fault")
            or j.get("uncertain_vendor_command")
            or j.get("control", {}).get("start_pending")
            or grant
            and (local is None or grant.get("local") != local)
            or local_start.get("local") is None
            and local_start.get("stage") in ("prepared", "issued", "accepted")
            or j.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed' LIMIT 1").fetchone()
            or j.db.execute(
                "SELECT 1 FROM transactions WHERE vendor IS NULL OR (stop IS NULL AND local IS NOT ?) LIMIT 1",
                (local,),
            ).fetchone()
        ):
            raise ReleaseDeferredError("ordinary continuation awaits exact fresh source and reconciled owner authority")
        c.native.spec.require_boot(j.get("boot"))

    # Repair a superseded Boot's zero introduction only after the full current zero and source proof.
    def confirm_zero_introduction(self, native: HandoverEvidence, local: int | None) -> None:
        c, j, g = self.c, self.c.session.journal, self.c.session.gateway
        self.require_authority(local, introduced=False)
        c.native.require(native, closed=local is None)
        c.native.require_known_session(native)
        control = j.get("control", {})
        epoch, boot, revision, ticket = (
            g._charger_epoch,
            g.boot_observation,
            g.controls._revision,
            control.get("ticket"),
        )
        present = c.session.book.owned_present(native.physical)
        station = [r for r in present if r.kind == "profile" and r.identifier == HUB_PROFILE_ID]
        row = j.db.execute(
            "SELECT state,confirmed FROM trial_resources WHERE key=?", ("profile:" + str(HUB_PROFILE_ID),)
        ).fetchone()
        require_lifecycle_idle(g, continuing_local=local)
        if (
            any(v != 0 for v in native.currents_a + native.powers_w)
            or control.get("owner") != "local"
            or control.get("inhibit") is not True
            or not g.controls.ready
            or g.controls.desired() != 0
            or len(station) != 1
            or station[0].canonical.get("limit") != 0
            or row is None
            or row["state"] != "confirmed"
            or row["confirmed"] is None
            or json.loads(row["confirmed"]) != station[0].canonical
            or j.get("trial_binding")
            != {
                "configuration_digest": c.native.binding_digest(native),
                "station": c.native.choice.value(),
                "boot_digest": c.native.spec.boot_digest,
            }
            or j.db.execute("SELECT 1 FROM outbox LIMIT 1").fetchone()
            or g.controls._operations
            or g.charger._pending
            or g.vendor._pending
        ):
            raise ReleaseDeferredError("ordinary introduction awaits confirmed zero and reconciled binding")
        with j.atomic():
            self.require_authority(local, introduced=False)
            if (g._charger_epoch, g.boot_observation, g.controls._revision, j.get("control", {}).get("ticket")) != (
                epoch,
                boot,
                revision,
                ticket,
            ):
                raise ReleaseDeferredError("ordinary zero introduction was superseded")
            j.put(
                "managed_ordinary_zero_introduction",
                {
                    "native_digest": native.digest,
                    "boot": boot,
                    "epoch": epoch,
                    "revision": revision,
                    "at_epoch": time.time(),
                },
            )
            j.put("trial_starting", False)
        c.native.introduced_epoch, c.native.introduced_revision = epoch, revision

    # A native reread must still prove the exact known session that is about to receive its original cap.
    def require_session(self, native: HandoverEvidence, local: int) -> None:
        self.require_authority(local)
        if (
            not native.physical.transaction_open
            or native.physical.native_transaction_id != local
            or self.source(native) != self.c.session.journal.get(SOURCE, {}).get("source")
        ):
            raise ReleaseDeferredError("ordinary continuation source changed during verification")

    # A real daily Start can need its OCPP number before native HTTP publishes that number; zero remains mandatory.
    async def prepare_native_report(self, payload: dict[str, Any]) -> NativeStartReport | None:
        c, j, g = self.c, self.c.session.journal, self.c.session.gateway
        tag = payload.get("idTag")
        hold = j.get(KEY, {})
        if (
            not self.ordinary()
            or not self.active()
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
            self.confirm_zero_introduction(native, None)
            self.require_authority(None)
        except (RecoveryRequiredError, UnavailableError, TimeoutError) as error:
            raise UnavailableError("ordinary daily report awaits fresh closed native zero") from error
        source = j.get(SOURCE)
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
                not self.ordinary()
                or not self.active()
                or j.get(KEY) != hold
                or j.get(SOURCE) != source
                or j.active() != identifier
                or g._charger_epoch != epoch
                or g.boot_observation != boot
                or g.controls._revision != revision
                or not g.current_boot_seen(epoch)
                or current != control
                or current.get("ticket") != ticket
                or current.get("owner") != "local"
                or current.get("inhibit") is not True
                or not g.controls.ready
                or g.controls.desired() != 0
                or (tuple(current_station) if current_station is not None else None) != receipt
                or identifier is not None
                and self.start_digest(identifier) != fingerprint(payload)
            ):
                raise RecoveryRequiredError("ordinary daily report was superseded before allocation")

        guard(None)

        # Binding the actual accepted wire report grants no current; later exact native plan/source is still required.
        def accepted(identifier: int) -> None:
            guard(identifier)
            j.put(KEY, {**hold, "local": identifier, "start": self.start_digest(identifier)})
            j.put(SOURCE, {**source, "local": identifier, "start": self.start_digest(identifier), "source": None})

        return NativeStartReport(None, None, None, accepted)

    # Fresh zero/source proof restores only the original intent and never emits a Start or Stop.
    async def recover(self) -> None:
        c, j = self.c, self.c.session.journal
        await self.hold("restricted_introduction")
        native = await c.native.read()
        local = j.active()
        self.confirm_zero_introduction(native, local)
        self.require_authority(local)
        if local is not None:
            self.require_session(native, local)
        elif native.physical.transaction_open:
            raise ReleaseDeferredError("ordinary idle recovery cannot adopt a new native transaction")
        await c.restore_owner_control(continuing_local=local)
        j.put(KEY, {**j.get(KEY), "state": "complete", "completed_at": time.time()})
