"""Real peer checks for owned release publication and completion lifetime.

Each test uses real owner Pause/Return admission, TLS/OCPP, native HTTP and the real SQLite journal.
Only independent peer timing, actual SQL refusal and read-only observations are injected.
The final Boot test requires its precise scheduling witnesses; failure to reach them is not a policy red.
"""

import asyncio
import copy
import json
import sqlite3
import sys
import uuid
from types import SimpleNamespace

import pytest
from ocpp_hub_core.prototype.connection_scope import current_scope
from ocpp_hub_core.prototype.local_control import ControlSupersededError, LocalControl
from ocpp_hub_core.prototype.managed_view import refresh_for_read
from ocpp_hub_core.prototype.recovery_contracts import FailureKind, Prerequisite, RestoreState, WireReply
from ocpp_hub_core.prototype.trial_resources import ResourceBook

from tests.charge_rig import owner_rig
from tests.managed_rig import healthy, starts
from tests.peer_mutation_helpers import observe_device_replies
from tests.prototype_rig import BOOT
from tests.recovery_barriers import completed_control
from tests.runtime_rig import until
from tests.tls_support import tls as tls

pytestmark = pytest.mark.asyncio


# Intent: Return can settle an unsent failure when its own preparation write is refused.
# Why: a refused request record must leave the verified hold available for polling and a new Return.
async def test_return_binding_write_refusal_finishes_without_sending(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        await pause_prefix(b)
        c, j = b.controller, b.session.journal
        mark = len(b.rig.bench.device_calls)
        identifier, revision = uuid.uuid4().hex, c.requests.intent().revision
        j.db.execute(
            "CREATE TEMP TRIGGER refuse_return_binding BEFORE UPDATE ON managed_requests "
            "WHEN NEW.id='" + identifier + "' AND json_type(NEW.result,'$._return')='object' "
            "BEGIN SELECT RAISE(ABORT,'Return preparation write refused'); END"
        )
        try:
            c.command(identifier, "app_control", revision)
            assert (await healthy(c))["local_hold_verified"]
            result = c.requests.receipt(identifier)
            assert result["state"] == "failed" and result["result"] == {"classification": "return_not_sent"}
            assert not positive_calls(b, mark) and not j.fault.latched()
        finally:
            j.db.execute("DROP TRIGGER refuse_return_binding")
        retry = uuid.uuid4().hex
        c.command(retry, "app_control", revision)
        await completed_control(b, retry, mode="app", revision=revision + 1, ceiling=6)


# Count actual positive station calls independently of the controller's result classification.
def positive_calls(b, mark):
    return [
        frame
        for frame in b.rig.bench.device_calls[mark:]
        if frame[2] == "SetChargingProfile"
        and frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] > 0
    ]


# Establish genuine closed Pause and retain the immutable pre-Return public/durable inputs.
async def pause_prefix(b):
    c, g, j = b.controller, b.session.gateway, b.session.journal
    await healthy(c)
    pause, revision = uuid.uuid4().hex, c.requests.intent().revision
    accepted = c.command(pause, "pause", revision)
    assert accepted["state"] == "accepted"
    await completed_control(b, pause, mode="paused", revision=revision + 1, ceiling=0)
    await until(lambda: not g.controls._operations and not g.controls._completions and not g.charger._pending, 8)
    assert j.active() is None and b.rig.bench.firmware.effective_limit() == 0
    assert not starts(b.rig) and not any(frame[2] == "RemoteStopTransaction" for frame in b.rig.bench.device_calls)
    return {
        "intent": c.requests.intent(),
        "scope": current_scope(g),
        "owner_epoch": copy.deepcopy(j.get("managed_owner_epoch")),
        "owner_pause": copy.deepcopy(j.get("managed_owner_pause")),
        "pause_receipt": copy.deepcopy(c.requests.receipt(pause)),
        "released_epoch": c.released_epoch,
        "plans": copy.deepcopy(b.rig.bench.firmware.plans),
        "configuration": copy.deepcopy(b.rig.peer.configuration),
        "wire": len(b.rig.bench.device_calls),
    }


# Admit exactly one real Return without bypassing the owner's request/intent transaction.
def accepted_return(b, before):
    identifier = uuid.uuid4().hex
    accepted = b.controller.command(identifier, "app_control", before["intent"].revision)
    assert accepted["id"] == identifier and accepted["state"] == "accepted"
    assert b.controller.requests.pending_return() == (identifier, before["intent"].revision)
    return identifier


# Observe one actual applied positive profile, optionally holding only its independent peer acknowledgement.
def capture_positive(b, *, entered=None, release=None):
    previous, witness = b.rig.bench.before_reply, {}

    # Keep all controller guards and peer reply bodies real; record facts before introducing response latency.
    async def before_reply(action, payload):
        if (
            not witness
            and action == "SetChargingProfile"
            and payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] > 0
        ):
            g, c = b.session.gateway, b.controller
            attempt = g.controls.release_attempt
            witness.update(
                attempt=attempt,
                completion=c.recovery._task,
                children=tuple(g.controls._operations),
                scope=current_scope(g),
                physical=b.rig.bench.firmware.effective_limit(),
                profiles=copy.deepcopy(b.rig.bench.firmware.profiles),
                payload=copy.deepcopy(payload),
                live=attempt is not None and not attempt.finished,
                positive_dispatched=attempt is not None and attempt.positive_may_have_dispatched,
                positive_reply=attempt.positive_reply if attempt is not None else None,
            )
            if entered is not None:
                entered.set()
            if release is not None:
                async with asyncio.timeout(8):
                    await release.wait()
        if previous is not None:
            await previous(action, payload)

    b.rig.bench.before_reply = before_reply
    return witness, previous


# Inspect only the current real child/SQL stack; observer errors remain explicit test failures.
def sql_observer(b, observations, errors):
    c, g, j = b.controller, b.session.gateway, b.session.journal

    # SQLite calls this observer synchronously; it never changes an engine result, guard or authority.
    def observed(label, value):
        frame = sys._getframe(1)
        resource, child = None, None
        try:
            while frame is not None:
                if frame.f_code is ResourceBook.after_result.__code__ and frame.f_locals.get("self") is b.session.book:
                    resource = {
                        "action": frame.f_locals.get("action"),
                        "answer": copy.deepcopy(frame.f_locals.get("answer")),
                    }
                if frame.f_code is LocalControl._release.__code__ and frame.f_locals.get("self") is g.controls:
                    child = asyncio.current_task()
                frame = frame.f_back
            attempt = g.controls.release_attempt
            observations.append(
                {
                    "label": label,
                    "candidate": json.loads(value),
                    "attempt": attempt,
                    "child": child,
                    "resource": resource,
                    "in_transaction": j.db.in_transaction,
                    "physical": b.rig.bench.firmware.effective_limit(),
                    "positive_verified": attempt is not None and attempt.positive_confirmed,
                    "positive_reply": attempt.positive_reply if attempt is not None else None,
                    "intent": c.requests.intent(),
                    "owner_epoch": copy.deepcopy(j.get("managed_owner_epoch")),
                }
            )
        except Exception as error:
            errors.append(type(error).__name__)  # A read-only observer must never replace the tested SQL failure.
        finally:
            del frame
        return 0

    return observed


# Preserve a real storage exception if the public poll boundary exposes it; other setup/runtime errors still fail.
async def tick_with_storage_outcome(c):
    try:
        return await c.tick()
    except sqlite3.Error as error:
        return error


# Check genuine applied/received facts before consuming the completed operation's classification.
async def finished_attempt(b, witness, replies, before):
    c, g = b.controller, b.session.gateway
    attempt = witness["attempt"]
    assert witness["live"] and witness["positive_dispatched"] and witness["positive_reply"] is WireReply.MISSING
    assert witness["scope"] == before["scope"] and witness["physical"] == 6
    profile = witness["payload"]["csChargingProfiles"]
    assert witness["profiles"][profile["chargingProfileId"]] == profile
    assert witness["completion"] is not None and len(witness["children"]) == 1
    await until(lambda: attempt.finished and not g.controls._operations and not g.charger._pending, 8)
    completion = witness["completion"]
    await asyncio.wait_for(asyncio.gather(completion, return_exceptions=True), 8)
    await until(lambda: not g.controls._completions, 8)
    calls = positive_calls(b, before["wire"])
    assert len(calls) == 1 and [3, calls[0][1], {"status": "Accepted"}] in replies
    assert not starts(b.rig) and not any(frame[2] == "RemoteStopTransaction" for frame in b.rig.bench.device_calls)
    result = attempt.result()
    assert result.claimed and result.effects.positive_dispatched and result.effects.positive_reply is WireReply.ACCEPTED
    assert result.authority.scope == before["scope"] and result.authority.ticket == attempt.ticket
    if result.effects.zero_verified:
        suffix = b.rig.bench.device_calls[before["wire"] :]
        zeros = [
            frame
            for frame in suffix
            if frame[2] == "SetChargingProfile"
            and frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"] == 0
        ]
        assert zeros and [3, zeros[-1][1], {"status": "Accepted"}] in replies
        zero_composites = [
            answer
            for answer in replies
            if answer[0] == 3
            and any(call[1] == answer[1] and call[2] == "GetCompositeSchedule" for call in suffix)
            and answer[2].get("chargingSchedule", {}).get("chargingSchedulePeriod") == [{"startPeriod": 0, "limit": 0}]
        ]
        assert zero_composites
    assert c.current.selected() == 6
    return result


# Retain a failed Return, then prove recovery through a new actual Pause and separate new Return, without replay.
async def recover_failed_return(b, failed, before):
    c, j = b.controller, b.session.journal
    receipt, fault = copy.deepcopy(c.requests.receipt(failed)), copy.deepcopy(j.get("managed_control_fault"))
    assert receipt["state"] == "uncertain" and fault["classification"] == "owner_release_unconfirmed"
    for _ in range(2):
        assert (await c.tick())["state"] == "attention"
        assert j.get("managed_control_fault") == fault and b.rig.bench.firmware.effective_limit() == 0
    pause, revision = uuid.uuid4().hex, c.requests.intent().revision
    c.command(pause, "pause", revision)
    if c.pause_task is not None:
        await asyncio.shield(c.pause_task)
    async with asyncio.timeout(10):
        while j.get("managed_control_fault"):
            status = await c.tick()
            assert b.rig.bench.firmware.effective_limit() == 0
            if j.get("managed_control_fault"):
                assert j.get("managed_control_fault") == fault and status["state"] == "attention"
                assert c.requests.receipt(pause)["state"] in ("accepted", "completed")
                await asyncio.sleep(0)
    await completed_control(b, pause, mode="paused", revision=revision + 1, ceiling=0)
    assert not j.get("managed_control_fault")
    superseded = c.requests.receipt(failed)
    assert superseded == {
        "id": failed,
        "action": "app_control",
        "state": "failed",
        "result": {"classification": "superseded_by_pause"},
    }
    assert c.command(failed, "app_control", before["intent"].revision) == superseded
    returning, revision = uuid.uuid4().hex, c.requests.intent().revision
    c.command(returning, "app_control", revision)
    await completed_control(b, returning, mode="app", revision=revision + 1, ceiling=6)
    assert c.requests.receipt(failed) == superseded and len(positive_calls(b, before["wire"])) == 2
    assert b.rig.bench.firmware.plans == before["plans"] and b.rig.peer.configuration == before["configuration"]
    assert not starts(b.rig) and not any(frame[2] == "RemoteStopTransaction" for frame in b.rig.bench.device_calls)


# Intent: real resource ACK storage refusal preserves actual positive Accepted knowledge and confirms owned zero.
# Why: after_result failure must not erase the reply recorded by on_reply or permit a replay of the same Return.
@pytest.mark.parametrize("diagnostic_available", [True, False], ids=["diagnostic-available", "diagnostic-refused"])
async def test_actual_positive_reply_survives_resource_ack_statement_refusal(
    tmp_path, tls, unused_tcp_port, monkeypatch, diagnostic_available
):
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, g, j = b.controller, b.session.gateway, b.session.journal
        before = await pause_prefix(b)
        replies = observe_device_replies(SimpleNamespace(sock=b.charger), monkeypatch)
        witness, previous = capture_positive(b)
        observations, errors = [], []
        j.db.create_function("release_sql_observed", 2, sql_observer(b, observations, errors))
        j.db.execute(
            "CREATE TEMP TRIGGER refuse_positive_resource_ack BEFORE UPDATE ON trial_resources "
            "WHEN NEW.key='profile:7001' AND NEW.state='confirmed' "
            "AND json_extract(NEW.desired,'$.limit')=6 "
            "BEGIN SELECT release_sql_observed('resource-ack',NEW.desired); "
            "SELECT RAISE(ABORT,'independent positive resource ACK unavailable'); END"
        )
        diagnostic_writes = []
        if not diagnostic_available:
            j.db.create_function("observe_refused_release_diagnostic", 1, diagnostic_writes.append)
            j.db.execute(
                "CREATE TEMP TRIGGER refuse_release_diagnostic BEFORE INSERT ON meta "
                "WHEN NEW.key='managed_release_failure' BEGIN SELECT observe_refused_release_diagnostic(NEW.key); "
                "SELECT RAISE(ABORT,'independent optional diagnostic unavailable'); END"
            )
        try:
            returning = accepted_return(b, before)
            poll = await tick_with_storage_outcome(c)
            assert not errors and len(observations) == 1
            cut = observations[0]
            assert cut["label"] == "resource-ack" and cut["resource"]["action"] == "SetChargingProfile"
            assert cut["resource"]["answer"][2] == {"status": "Accepted"}
            assert cut["attempt"] is witness["attempt"] and cut["child"] in witness["children"]
            assert cut["physical"] == 6 and cut["positive_reply"] is WireReply.ACCEPTED
            assert not cut["positive_verified"]
            result = await finished_attempt(b, witness, replies, before)
            assert result.initial_failure_kind is result.failure_kind is FailureKind.STORAGE
            assert isinstance(result.failure, sqlite3.IntegrityError) and not result.cleanup_failed
            assert not result.effects.positive_verified
            assert result.effects.zero_dispatched and result.effects.zero_verified
            assert not result.published and result.published_authority is None
            assert current_scope(g) == before["scope"]
            assert c.requests.intent() == before["intent"] and c.released_epoch == before["released_epoch"]
            assert c.requests.receipt(returning)["state"] == "uncertain"
            assert j.get("control")["inhibit"] and j.get("control")["uncertain"]
            assert b.rig.bench.firmware.effective_limit() == 0
            assert j.fault.classification() == "owner_release_unconfirmed"
            assert poll is result.failure
            if not diagnostic_available:
                assert diagnostic_writes == ["managed_release_failure"]
                assert j.get("managed_release_failure") is None
        finally:
            b.rig.bench.before_reply = previous
            j.db.execute("DROP TRIGGER refuse_positive_resource_ack")
            j.db.create_function("release_sql_observed", 2, None)
            if not diagnostic_available:
                j.db.execute("DROP TRIGGER refuse_release_diagnostic")
                j.db.create_function("observe_refused_release_diagnostic", 1, None)
        await recover_failed_return(b, returning, before)


# Intent: verified positive output cannot half-publish owner epoch, intent, control or a completed Return receipt.
# Why: a statement or deferred COMMIT failure must roll the entire handoff back before owned zero and fault settle.
@pytest.mark.parametrize("boundary", ["owner-epoch-statement", "receipt-commit"])
async def test_actual_release_publication_failure_rolls_back_all_authority(
    tmp_path, tls, unused_tcp_port, monkeypatch, boundary
):
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, g, j = b.controller, b.session.gateway, b.session.journal
        before = await pause_prefix(b)
        replies = observe_device_replies(SimpleNamespace(sock=b.charger), monkeypatch)
        witness, previous = capture_positive(b)
        observations, errors = [], []
        returning = accepted_return(b, before)
        j.db.create_function("release_sql_observed", 2, sql_observer(b, observations, errors))
        if boundary == "receipt-commit":
            j.db.execute("CREATE TABLE release_commit_parent(id INTEGER PRIMARY KEY)")
            j.db.execute(
                "CREATE TABLE release_commit_child("
                "id REFERENCES release_commit_parent(id) DEFERRABLE INITIALLY DEFERRED)"
            )
            j.db.execute(
                "CREATE TEMP TRIGGER refuse_release_commit AFTER UPDATE ON managed_requests "
                "WHEN NEW.id='" + returning + "' AND NEW.state='completed' "
                "BEGIN SELECT release_sql_observed('receipt-completed',NEW.result); "
                "INSERT INTO release_commit_child VALUES(123); END"
            )
            j.db.execute(
                "CREATE TEMP TRIGGER observe_release_control BEFORE INSERT ON meta "
                "WHEN NEW.key='control' AND json_extract(NEW.value,'$.owner')='shared' "
                "BEGIN SELECT release_sql_observed('shared-control',NEW.value); END"
            )
        else:
            j.db.execute(
                "CREATE TEMP TRIGGER refuse_release_epoch BEFORE INSERT ON meta "
                "WHEN NEW.key='managed_owner_epoch' "
                "BEGIN SELECT release_sql_observed('owner-epoch',NEW.value); "
                "SELECT RAISE(ABORT,'independent release publication unavailable'); END"
            )
        try:
            await tick_with_storage_outcome(c)
            assert not errors and observations
            assert {cut["label"] for cut in observations} == (
                {"receipt-completed", "shared-control"} if boundary == "receipt-commit" else {"owner-epoch"}
            )
            assert all(cut["in_transaction"] and cut["physical"] == 6 for cut in observations)
            assert all(cut["attempt"] is witness["attempt"] and cut["positive_verified"] for cut in observations)
            assert all(cut["positive_reply"] is WireReply.ACCEPTED for cut in observations)
            if boundary == "receipt-commit":
                assert all(cut["intent"].mode == "app" for cut in observations)
                assert j.db.execute("SELECT COUNT(*) FROM release_commit_child").fetchone()[0] == 0
            result = await finished_attempt(b, witness, replies, before)
            assert result.effects.positive_verified and result.effects.zero_dispatched and result.effects.zero_verified
            assert result.initial_failure_kind is result.failure_kind is FailureKind.STORAGE
            assert isinstance(result.failure, sqlite3.IntegrityError) and not result.published
            if boundary == "receipt-commit":
                assert result.failure.sqlite_errorcode == sqlite3.SQLITE_CONSTRAINT_FOREIGNKEY
            assert result.published_authority is None and not j.db.in_transaction
            assert current_scope(g) == before["scope"]
            assert c.requests.intent() == before["intent"] and j.get("managed_owner_epoch") == before["owner_epoch"]
            assert j.get("managed_owner_pause") == before["owner_pause"]
            assert c.requests.receipt(returning)["state"] == "uncertain"
            assert c.requests.receipt(before["pause_receipt"]["id"]) == before["pause_receipt"]
            assert c.released_epoch == before["released_epoch"]
            control = j.get("control")
            assert control["ticket"] == witness["attempt"].ticket and control["owner"] == "local"
            assert control["inhibit"] and control["uncertain"] and b.rig.bench.firmware.effective_limit() == 0
        finally:
            b.rig.bench.before_reply = previous
            if boundary == "receipt-commit":
                j.db.execute("DROP TRIGGER refuse_release_commit")
                j.db.execute("DROP TRIGGER observe_release_control")
            else:
                j.db.execute("DROP TRIGGER refuse_release_epoch")
            j.db.create_function("release_sql_observed", 2, None)
        await recover_failed_return(b, returning, before)


# Intent: cancelling a poll during a genuine positive RPC cannot abandon final Return publication or its receipt.
# Why: running Return invalidates the old zero reading and retains one completion after its original waiter leaves.
async def test_cancelled_poll_cannot_abandon_owned_return_publication(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, g, j = b.controller, b.session.gateway, b.session.journal
        before = await pause_prefix(b)
        published = dict(await c.tick(), running=True, certificate_remaining_seconds=7200)
        assert published["power_w"] == 0 and published["local_hold_verified"]
        replies = observe_device_replies(SimpleNamespace(sock=b.charger), monkeypatch)
        entered, release = asyncio.Event(), asyncio.Event()
        witness, previous = capture_positive(b, entered=entered, release=release)
        returning = accepted_return(b, before)
        polling = asyncio.create_task(c.tick())
        try:
            await asyncio.wait_for(entered.wait(), 5)
            attempt, completion = witness["attempt"], witness["completion"]
            assert witness["live"] and witness["physical"] == 6 and len(witness["children"]) == 1
            child = witness["children"][0]
            assert child in g.controls._operations and completion in g.controls._completions
            assert not child.done() and not completion.done() and g.charger._pending
            assert c.requests.intent() == before["intent"] and c.requests.receipt(returning)["state"] == "running"
            reading = refresh_for_read(c, published)
            assert reading["return_pending"] and not reading["local_hold_verified"]
            assert reading["fault"] == "owner_release_pending"
            assert reading["power_w"] is reading["current_a"] is reading["native_age_seconds"] is None
            assert reading["charging_session_open"] is reading["station_ceiling_a"] is None
            polling.cancel()
            await asyncio.gather(polling, return_exceptions=True)
            assert polling.cancelled() and not child.cancelled() and not completion.cancelled()
            assert not attempt.finished and completion in g.controls._completions
            assert b.session.book.before_positive_release is attempt.positive_guard
            # A second real poll observes ownership; it cannot prepare another zero/release while this ACK is held.
            mark, ticket = len(b.rig.bench.device_calls), j.get("control")["ticket"]
            pending = await asyncio.wait_for(c.tick(), 1)
            assert pending["state"] == "reconciling" and pending["fault"] == "owner_release_pending"
            assert not pending["owner_control_enabled"] and pending["station_ceiling_a"] is None
            assert len(b.rig.bench.device_calls) == mark and j.get("control")["ticket"] == ticket
            assert c.requests.receipt(returning)["state"] == "running" and not completion.done()
            release.set()
            progress = await asyncio.wait_for(asyncio.shield(completion), 8)
            assert progress.state is RestoreState.SHARED and progress.shared
            result = await finished_attempt(b, witness, replies, before)
            assert progress.operation == result and result.published and result.published_authority is not None
            assert result.failure is None and not result.cancelled and not result.cleanup_failed
            assert result.effects.positive_verified and not result.effects.zero_dispatched
            assert not j.get("managed_control_fault") and c.requests.intent().mode == "app"
            assert c.requests.intent().revision == before["intent"].revision + 1
            receipt = copy.deepcopy(c.requests.receipt(returning))
            assert receipt["state"] == "completed" and g.controls.ready and b.rig.bench.firmware.effective_limit() == 6
            settled = refresh_for_read(c, pending)
            assert settled["control_mode"] == "waiting" and not settled["return_pending"]
            assert settled.get("fault") is None and settled["station_ceiling_a"] is None
            assert not settled["owner_control_enabled"] and not settled["can_start"]
            assert c.command(returning, "app_control", before["intent"].revision) == receipt
            assert len(positive_calls(b, before["wire"])) == 1
        finally:
            release.set()
            b.rig.bench.before_reply = previous
            if not polling.done():
                polling.cancel()
            await asyncio.gather(polling, return_exceptions=True)
        pause, revision = uuid.uuid4().hex, c.requests.intent().revision
        c.command(pause, "pause", revision)
        await completed_control(b, pause, mode="paused", revision=revision + 1, ceiling=0)
        assert c.requests.receipt(returning) == receipt


# Intent: a genuine positive Return reply arriving after a newer Pause cannot publish app control.
# Why: the late reply matters for the physical outcome, while only the newer request can settle the resulting hold.
async def test_late_return_reply_keeps_the_newer_pause(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        before = await pause_prefix(b)
        replies = observe_device_replies(SimpleNamespace(sock=b.charger), monkeypatch)
        entered, release = asyncio.Event(), asyncio.Event()
        witness, previous = capture_positive(b, entered=entered, release=release)
        returning = accepted_return(b, before)
        consumer = asyncio.create_task(
            c.recovery.restore(before["intent"], return_request=returning, claim_return=True)
        )
        try:
            await asyncio.wait_for(entered.wait(), 5)
            attempt = witness["attempt"]
            assert witness["live"] and witness["physical"] == 6 and witness["positive_dispatched"]
            positive = positive_calls(b, before["wire"])
            assert len(positive) == 1 and not any(reply[1] == positive[0][1] for reply in replies)

            pause, revision = uuid.uuid4().hex, c.requests.intent().revision
            c.command(pause, "pause", revision)
            expected = c.requests.intent()
            claim = copy.deepcopy(j.get("managed_owner_pause"))
            old_outcome = c.requests.receipt(returning)
            assert old_outcome["state"] == "failed"
            assert old_outcome["result"] == {"classification": "superseded_by_pause"}
            release.set()

            with pytest.raises(ControlSupersededError):
                await asyncio.wait_for(asyncio.shield(consumer), 8)
            assert [3, positive[0][1], {"status": "Accepted"}] in replies
            assert attempt.finished and not attempt.published
            assert attempt.positive_reply is WireReply.ACCEPTED
            assert j.get("managed_owner_epoch") == before["owner_epoch"]
            assert c.requests.intent() == expected and j.get("managed_owner_pause") == claim
            assert j.get("control")["ticket"] == claim["ticket"]
            assert c.requests.receipt(returning) == old_outcome

            await completed_control(b, pause, mode="paused", revision=revision + 1, ceiling=0)
            assert c.requests.receipt(returning) == old_outcome
            assert len(positive_calls(b, before["wire"])) == 1 and not starts(b.rig)
        finally:
            release.set()
            b.rig.bench.before_reply = previous
            await asyncio.gather(consumer, return_exceptions=True)


# Intent: a real matching Boot after child COMMIT retires successful result A without faulting current authority B.
# Why: a late observer must preserve the completed Return and verify the ceiling under the newer Boot.
async def test_matching_boot_after_child_publication_retires_only_result(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, g, j = b.controller, b.session.gateway, b.session.journal
        before = await pause_prefix(b)
        replies = observe_device_replies(SimpleNamespace(sock=b.charger), monkeypatch)
        witness, previous_reply = capture_positive(b)
        published, resume = asyncio.Event(), asyncio.Event()
        release = g.controls.release_result

        # Wait only after the actual child has committed, before the coordinator consumes its real result.
        async def wait_after_commit(*args, **kwargs):
            result = await release(*args, **kwargs)
            if result.published and not published.is_set():
                published.set()
                await resume.wait()
            return result

        monkeypatch.setattr(g.controls, "release_result", wait_after_commit)
        returning = accepted_return(b, before)
        poll = asyncio.create_task(c.tick())
        try:
            await asyncio.wait_for(published.wait(), 5)
            original = c.requests.receipt(returning)
            assert original["state"] == "completed"
            assert c.requests.intent().mode == "app" and g.controls.ready
            assert witness["attempt"].finished and witness["attempt"].published
            assert witness["children"][0].done() and not witness["completion"].done()
            assert current_scope(g) == before["scope"]

            assert (await b.rig.bench.call(b.charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
            current = current_scope(g)
            assert current.lineage == before["scope"].lineage and current != before["scope"]
            assert not g.controls.ready and b.rig.bench.firmware.effective_limit() == 6
            assert c.requests.receipt(returning) == original
            resume.set()

            await poll
            result = await finished_attempt(b, witness, replies, before)
            progress = witness["completion"].result()
            assert progress.state is RestoreState.YIELDED and not progress.shared
            assert progress.operation.published_authority.scope == before["scope"]
            assert result.failure is None and result.published and result.effects.positive_verified
            assert not result.effects.zero_dispatched and not j.fault.latched()
            assert c.requests.receipt(returning) == original and b.rig.bench.firmware.effective_limit() == 6
        finally:
            resume.set()
            await asyncio.gather(poll, return_exceptions=True)
            b.rig.bench.before_reply = previous_reply

        pause, revision = uuid.uuid4().hex, c.requests.intent().revision
        c.command(pause, "pause", revision)
        await completed_control(b, pause, mode="paused", revision=revision + 1, ceiling=0)
        assert c.requests.receipt(returning) == original


# Intent: a newer real Pause after owned SHARED completion must retire the awaiting consumer's old result.
# Why: another task can change authority after the owned policy returns but before shield wakes its caller.
async def test_new_pause_after_owned_completion_retires_restore_consumer(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with owner_rig(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, j = b.controller, b.session.journal
        before = await pause_prefix(b)
        returning = accepted_return(b, before)
        replies = observe_device_replies(SimpleNamespace(sock=b.charger), monkeypatch)
        entered, release = asyncio.Event(), asyncio.Event()
        witness, previous = capture_positive(b, entered=entered, release=release)
        event = asyncio.get_running_loop().create_future()
        pause = uuid.uuid4().hex

        # A real admitted UUID is consumed by the same production restore API; no SQL seeds a running result.
        consumer = asyncio.create_task(c.recovery.restore(before["intent"], return_request=returning))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            completion, attempt = witness["completion"], witness["attempt"]
            assert not completion.done() and not consumer.done() and attempt.positive_may_have_dispatched
            assert not attempt.published and c.requests.receipt(returning)["state"] == "accepted"

            # Registered after shield: its callback is already in the ready queue when shield schedules the waiter.
            # This is an external owner API action in an event-loop callback, never an engine/SQL callback override.
            def newer_owner(done):
                try:
                    progress = done.result()
                    operation = progress.operation
                    observed = {
                        "owned_progress": progress,
                        "consumer_done": consumer.done(),
                        "before_authority": c.authority(),
                        "return_receipt": copy.deepcopy(c.requests.receipt(returning)),
                        "physical": b.rig.bench.firmware.effective_limit(),
                    }
                    assert progress.state is RestoreState.SHARED and operation is not None
                    assert operation.published and operation.failure is None and operation.effects.positive_verified
                    assert not operation.effects.zero_dispatched and not observed["consumer_done"]
                    assert observed["before_authority"] == operation.published_authority
                    assert observed["return_receipt"]["state"] == "completed"
                    accepted = c.command(pause, "pause", observed["before_authority"].intent.revision)
                    assert accepted["state"] == "accepted"
                    observed["after_authority"] = c.authority()
                    observed["new_ticket"] = j.get("control")["ticket"]
                    assert observed["after_authority"] != operation.published_authority
                    event.set_result(observed)
                except BaseException as error:
                    event.set_exception(error)

            completion.add_done_callback(newer_owner)
            release.set()
            observed = await asyncio.wait_for(asyncio.shield(event), 8)
            progress = await asyncio.wait_for(asyncio.shield(consumer), 8)

            # Establish real Accepted and Composite effect evidence before asserting the consumer classification.
            calls = positive_calls(b, before["wire"])
            assert len(calls) == 1 and [3, calls[0][1], {"status": "Accepted"}] in replies
            composites = [
                frame for frame in b.rig.bench.device_calls[before["wire"] :] if frame[2] == "GetCompositeSchedule"
            ]
            assert any(
                reply[0] == 3
                and any(call[1] == reply[1] for call in composites)
                and reply[2].get("chargingSchedule", {}).get("chargingSchedulePeriod")
                == [{"startPeriod": 0, "limit": 6}]
                for reply in replies
            )
            assert observed["physical"] == 6 and attempt.finished and attempt.published
            assert observed["owned_progress"].shared
            assert not progress.shared and progress.state is RestoreState.YIELDED
            assert progress.pending.reason is Prerequisite.SUPERSEDED
            assert progress.operation == observed["owned_progress"].operation
            assert j.get("control")["ticket"] == observed["new_ticket"]
            assert c.requests.intent().mode == "paused" and not j.get("managed_control_fault")
            receipt = observed["return_receipt"]
            assert c.requests.receipt(returning) == receipt
            assert c.command(returning, "app_control", before["intent"].revision) == receipt

            # The new request alone owns zero; the old completed receipt cannot replay or complete that request.
            revision = observed["after_authority"].intent.revision
            await completed_control(b, pause, mode="paused", revision=revision, ceiling=0)
            assert c.requests.receipt(returning) == receipt and len(positive_calls(b, before["wire"])) == 1
            assert b.rig.bench.firmware.plans == before["plans"]
            assert b.rig.peer.configuration == before["configuration"]
            assert not starts(b.rig)
            assert not any(frame[2] == "RemoteStopTransaction" for frame in b.rig.bench.device_calls)
        finally:
            release.set()
            b.rig.bench.before_reply = previous
            await asyncio.gather(consumer, return_exceptions=True)
