"""One real managed process, journal and independently driven charger per event sequence."""

import asyncio
import json
import sqlite3
import uuid
from contextlib import closing
from types import SimpleNamespace

from tests.managed_rig import starts
from tests.model_tls_support import tls_for_case
from tests.peer_mutation_helpers import observe_device_replies
from tests.process_helpers import process_rig
from tests.prototype_rig import START
from tests.runtime_rig import until

CASE_SECONDS = 25


# Run each proposed sequence from a fresh journal and independent simulated peers.
async def simulate(directory, order, model_tls, ports, monkeypatch, observations, timeout):
    directory.mkdir()
    async with asyncio.timeout(timeout):
        async with process_rig(
            directory, tls_for_case(directory, model_tls), ports(), ports(), monkeypatch, timing="fast-poll"
        ) as child:
            case = Overlap(child, monkeypatch, observations)
            try:
                await case.prefix()
                for event in order:
                    await case.step(event)
                await case.finish()
            except BaseException:
                # A failing test must retire its actual process before another replay can use fresh peers.
                if child.process is not None and child.process.returncode is None:
                    await child.kill()
                raise
            finally:
                case.release.set()
                child.rig.bench.before_reply = None


class ViolationError(AssertionError):
    """An independently observed rule failure with a stable identity for replay and shortening."""

    # Retain the failed rule and action without copying raw SQLite rows or charger frames into reports.
    def __init__(self, rule, event):
        self.rule, self.event = rule, event
        super().__init__(f"{rule} after {event}")


class Overlap:
    """Use only public requests, actual wire effects and independent committed SQLite snapshots."""

    # Names used in saved event orders refer to fresh, per-case management UUIDs.
    def __init__(self, child, monkeypatch, observations):
        self.child = child
        self.bench, self.peer = child.rig.bench, child.rig.peer
        self.monkeypatch = monkeypatch
        self.ids = {name: uuid.uuid4().hex for name in ("original", "start", "pause", "return", "newer_pause", "final")}
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.paused = "original"
        self.terminal = {}
        self.observations = observations
        self.event = "prefix"
        self.return_revision = None
        self.exact_reply = None

    # Report only a real invariant failure, distinct from a missing setup witness or test timeout.
    def expect(self, value, rule):
        if not value:
            self.record(*self.snapshot())
            raise ViolationError(rule, self.event)

    # This snapshot bypasses the service's request projection and samples all relevant committed facts together.
    def snapshot(self):
        snapshot = self.child.durable()
        rows = {row["id"]: row for row in snapshot["requests"]}
        return snapshot["metadata"], {name: rows.get(identifier) for name, identifier in self.ids.items()}

    # Hold the actual response after the independent firmware has executed the original Start.
    async def hold(self, action, _payload):
        if action == "RemoteStartTransaction" and not self.entered.is_set():
            self.entered.set()
            await self.release.wait()

    # A real positive Start offer exists before any explored newer request.
    async def prefix(self):
        c = self.child
        await c.start()
        ready = await c.status()
        first = await c.request("pause", id=self.ids["original"], revision=ready["control_revision"])
        assert first["ok"], first
        await c.completed_control(self.ids["original"])
        ready = await c.status(lambda value: value.get("local_hold_verified") is True)
        self.revision = ready["control_revision"]
        self.bench.before_reply = self.hold
        self.replies = observe_device_replies(SimpleNamespace(sock=c.socket), self.monkeypatch)
        start = await c.request("start", id=self.ids["start"], revision=self.revision)
        assert start["ok"], start
        await asyncio.wait_for(self.entered.wait(), 4)
        (self.command,) = starts(c.rig)
        self.exact_reply = [3, self.command[1], {"status": "Accepted"}]
        self.mark = len(self.bench.device_calls)
        meta, rows = self.snapshot()
        assert self.exact_reply not in self.replies
        receipt = meta["local_start_receipt"]
        assert receipt["stage"] == "issued"
        assert receipt["request_id"] == self.ids["start"] and receipt["local"] is None
        self.start_identity = {key: receipt[key] for key in ("request_id", "ticket", "digest", "local")}
        self.claim = meta["managed_owner_pause"]
        self.terminal["original"] = rows["original"]
        self.check()

    # Only a committed newer Pause can supersede a pending owner decision.
    async def pause(self, name):
        prior, rows = self.snapshot()
        accepted = await self.child.request("pause", id=self.ids[name], revision=self.revision)
        self.expect(accepted["ok"], "pause_not_admitted")
        self.revision += 1
        after, current = self.snapshot()
        self.claim = after["managed_owner_pause"]
        self.expect(
            self.claim["revision"] == self.revision and self.claim != prior["managed_owner_pause"],
            "pause_claim_not_replaced",
        )
        self.expect(
            current[name] is not None
            and current[name]["state"] == "accepted"
            and current[name]["revision"] == self.revision - 1,
            "pause_receipt_not_admitted",
        )
        old_pause = self.paused
        if rows[old_pause]["state"] == "accepted":
            settled = current[old_pause] or {}
            self.expect(
                settled.get("state") == "failed"
                and json.loads(settled["result"])
                == {"classification": "superseded_by_pause", "superseded_by": self.ids[name]},
                "older_pause_not_superseded",
            )
            self.terminal[old_pause] = settled
        if rows["return"] is not None and rows["return"]["state"] in ("accepted", "running", "uncertain"):
            settled = current["return"] or {}
            self.expect(
                settled.get("state") == "failed"
                and json.loads(settled["result"]) == {"classification": "superseded_by_pause"},
                "older_return_not_superseded",
            )
            self.terminal["return"] = settled
        self.paused = name

    # The known older Start has no lifecycle yet, so this Return may be admitted but cannot publish app mode.
    async def request_return(self):
        result = await self.child.request("app_control", id=self.ids["return"], revision=self.revision)
        self.expect(result["ok"] and result["result"]["state"] == "accepted", "overlapping_return_not_admitted")
        self.return_revision = self.revision

    # A late retry of the exact old UUID cannot acquire a new generation.
    async def replay_return(self):
        before, rows = self.snapshot()
        result = await self.child.request("app_control", id=self.ids["return"], revision=self.return_revision)
        self.expect(
            result["ok"]
            and result["result"]["state"] == rows["return"]["state"]
            and result["result"]["result"] == json.loads(rows["return"]["result"]),
            "return_replay_changed_outcome",
        )
        after, current = self.snapshot()
        self.expect(
            before["managed_control_intent"] == after["managed_control_intent"] and rows["return"] == current["return"],
            "return_replay_changed_record",
        )

    # Refuse the last actual INSERT in the Pause transaction, after earlier intent and receipt writes were attempted.
    async def write_failure(self):
        path = self.child.rig.prepared.config.journal
        identifier = self.ids["newer_pause"]
        before, rows = self.snapshot()
        with closing(sqlite3.connect(path, isolation_level=None)) as db:
            db.execute(
                "CREATE TRIGGER discovery_refuse_pause BEFORE INSERT ON managed_requests "
                f"WHEN NEW.id='{identifier}' AND NEW.action='pause' "
                "BEGIN SELECT RAISE(ABORT,'fixture Pause receipt write refused'); END"
            )
            try:
                result = await self.child.request("pause", id=identifier, revision=self.revision)
            finally:
                db.execute("DROP TRIGGER discovery_refuse_pause")
        self.expect(not result["ok"] and result.get("error") == "not_ready", "failed_pause_reported_admitted")
        after, current = self.snapshot()
        self.expect(
            all(after[key] == before[key] for key in ("managed_control_intent", "managed_owner_pause", "control"))
            and all(current[name] == rows[name] for name in ("pause", "return", "newer_pause")),
            "failed_pause_changed_committed_state",
        )

    # Observe a genuine Accepted frame before requiring its separate committed Start record.
    async def reply(self):
        self.release.set()
        await until(lambda: self.exact_reply in self.replies, timeout=3)
        try:
            await until(lambda: self.snapshot()[0].get("local_start_receipt", {}).get("stage") == "accepted", timeout=3)
        except TimeoutError:
            self.expect(False, "matched_reply_not_saved")

    # The old reply can have been sent, or be lost when the original socket dies.
    async def restart(self):
        before, rows = self.snapshot()
        actual = before["local_start_receipt"]
        delivered = self.exact_reply in self.replies
        assert actual["stage"] == ("accepted" if delivered else "issued")
        await self.child.kill()
        self.release.set()
        self.bench.before_reply = None
        await self.child.start()
        await self.child.status(
            lambda value: value.get("charger_connected") is True and value.get("boot_verified") is True
        )
        after, current = self.snapshot()
        self.expect(after["local_start_receipt"] == actual, "restart_changed_original_start_outcome")
        self.expect((self.exact_reply in self.replies) is delivered, "retired_socket_delivered_reply")
        if rows["return"] is not None and rows["return"]["state"] == "accepted":
            self.expect(
                current["return"] is not None
                and current["return"]["state"] == "uncertain"
                and json.loads(current["return"]["result"]) == {"classification": "process_replaced"},
                "restart_lost_unsettled_return",
            )

    # After every explored action, keep actual wire paths and the original journal evidence independent.
    def check(self):
        meta, rows = self.snapshot()
        self.expect(
            meta["managed_control_intent"] == {"mode": "paused", "revision": self.revision}, "latest_pause_lost"
        )
        self.expect(meta["managed_owner_pause"] == self.claim, "latest_pause_claim_lost")
        self.expect(
            (rows[self.paused] or {}).get("state") == ("completed" if self.paused == "original" else "accepted"),
            "pause_finished_before_lifecycle",
        )
        self.expect(all(rows[name] == row for name, row in self.terminal.items()), "terminal_receipt_changed")
        self.expect(rows["start"] is not None, "original_start_receipt_lost")
        self.expect(rows["start"]["state"] != "completed", "start_finished_before_lifecycle")
        original = meta.get("local_start_receipt") or {}
        self.expect(
            all(key in original and original[key] == value for key, value in self.start_identity.items()),
            "original_start_identity_changed",
        )
        expected_stage = "accepted" if self.exact_reply in self.replies else "issued"
        self.expect(original.get("stage") == expected_stage, "start_stage_disagrees_with_reply")
        self.expect(not rows["return"] or rows["return"]["state"] != "completed", "return_published_before_lifecycle")
        self.check_wire()
        self.record(meta, rows)

    # Permit physical Stop only once an exact local session exists. Positive offers stay revoked until Return.
    def check_wire(self, local=None):
        self.expect(starts(self.child.rig) == [self.command], "duplicate_start")
        frames = self.bench.device_calls[self.mark :]
        self.expect(
            all(
                local is not None and frame[3]["transactionId"] == local
                for frame in frames
                if frame[2] == "RemoteStopTransaction"
            ),
            "stop_outside_exact_session",
        )
        self.expect(
            not any(
                frame[2] == "SetChargingProfile"
                and any(
                    period["limit"] > 0
                    for period in frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"]
                )
                for frame in frames
            ),
            "stale_positive_profile",
        )

    # Keep sanitized committed outcomes available at successful and failed boundaries.
    def record(self, meta, rows):
        intent = meta.get("managed_control_intent") or {}
        self.observations.append(
            {
                "event": self.event,
                "intent": {key: intent.get(key) for key in ("mode", "revision")},
                "expected_pause_revision": self.revision,
                "old_start_stage": (meta.get("local_start_receipt") or {}).get("stage"),
                "old_reply_seen": self.exact_reply in self.replies,
                "start_commands_seen": len(starts(self.child.rig)),
                "receipts": {name: row["state"] for name, row in rows.items() if row is not None},
            }
        )

    # Execute only declared external actions. Oracle observations run immediately after each boundary.
    async def step(self, event):
        self.event = event
        if event in ("pause", "newer_pause"):
            await self.pause(event)
        elif event == "return":
            await self.request_return()
        elif event == "replay_return":
            await self.replay_return()
        elif event == "write_failure":
            await self.write_failure()
        elif event == "reply":
            await self.reply()
        elif event == "restart":
            await self.restart()
        else:
            raise ValueError("unsupported discovery action")
        self.check()

    # Use genuine reports for both lifecycles, with fresh protection and then a separate Return.
    async def finish(self):
        self.event = "settle_lifecycle"
        c = self.child
        if not self.release.is_set():
            await self.reply()
            self.check()
        original = await self.bench.call(
            c.socket, uuid.uuid4().hex, "StartTransaction", START | {"idTag": self.command[3]["idTag"]}
        )
        assert original[0] == 3 and original[2]["idTagInfo"]["status"] == "Accepted", original
        local = original[2]["transactionId"]
        self.peer.plan_session = (local, 987654)
        self.peer.permission_enabled = False
        closed = await self.bench.call(
            c.socket,
            uuid.uuid4().hex,
            "StopTransaction",
            {"transactionId": local, "meterStop": 1100, "timestamp": "2026-01-01T00:01:00Z"},
        )
        assert closed[0] == 3, closed
        history = await c.delivered(local, closed=True)
        try:
            async with asyncio.timeout(7):
                await c.completed_control(self.ids[self.paused])
                await c.status(lambda value: value.get("local_hold_verified") is True)
        except TimeoutError, AssertionError:
            self.expect(False, "pause_unsettled_after_actual_closure")
        meta, rows = self.snapshot()
        self.expect(
            meta["managed_control_intent"] == {"mode": "paused", "revision": self.revision}, "closure_changed_pause"
        )
        self.expect((rows["start"] or {}).get("state") == "completed", "closed_original_start_unsettled")
        self.check_wire(local)
        self.expect(self.bench.firmware.current == self.bench.firmware.effective_limit() == 0, "settled_pause_not_zero")
        self.terminal.update({self.paused: rows[self.paused], "start": rows["start"]})
        self.record(meta, rows)

        self.event = "later_charge"
        result = await c.request("app_control", id=self.ids["final"], revision=self.revision)
        self.expect(result["ok"], "return_after_closure_refused")
        try:
            async with asyncio.timeout(7):
                await c.completed_control(self.ids["final"])
        except TimeoutError, AssertionError:
            self.expect(False, "return_unsettled_after_actual_closure")
        self.expect(
            self.snapshot()[0]["managed_control_intent"] == {"mode": "app", "revision": self.revision + 1},
            "return_published_wrong_revision",
        )
        following = await self.bench.call(
            c.socket,
            uuid.uuid4().hex,
            "StartTransaction",
            START | {"idTag": "NATIVE_TIMER", "meterStart": 2000, "timestamp": "2026-01-01T00:02:00Z"},
        )
        assert following[0] == 3 and following[2]["idTagInfo"]["status"] == "Accepted", following
        later = following[2]["transactionId"]
        self.expect(later != local, "later_charge_reused_local_id")
        self.peer.plan_session = (later, 9)
        self.peer.paused, self.peer.permission_enabled = False, True
        self.bench.cp, self.bench.connection = "C", "Charging"
        self.bench.firmware.fire_plan(9)
        await c.status(lambda value: value.get("current_a") == 6 and value.get("power_w", 0) > 0)
        closed = await self.bench.call(
            c.socket,
            uuid.uuid4().hex,
            "StopTransaction",
            {"transactionId": later, "meterStop": 2100, "timestamp": "2026-01-01T00:03:00Z"},
        )
        assert closed[0] == 3, closed
        self.peer.permission_enabled = False
        await c.delivered(later, closed=True)
        self.expect(history in c.durable()["transactions"], "original_charge_history_changed")
        self.expect(starts(c.rig) == [self.command], "duplicate_start")
        meta, rows = self.snapshot()
        self.expect(all(rows[name] == row for name, row in self.terminal.items()), "terminal_receipt_changed")
        self.record(meta, rows)
