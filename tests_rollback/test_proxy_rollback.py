"""Two real standalone builds use one advancing journal and independent simulated peers."""

import asyncio
import copy
import json
import shutil
import sqlite3
import uuid
from contextlib import asynccontextmanager, closing
from pathlib import Path
from types import SimpleNamespace

import pytest
from ocpp_hub_core.prototype.trial_session import save_private

from tests.connected_rig import strategy_file
from tests.managed_rig import managed_bench, starts
from tests.model_tls_support import model_tls as model_tls
from tests.model_tls_support import tls_for_case
from tests.native_peer_failures import InterruptedNativePeer
from tests.process_helpers import ServiceProcess
from tests.prototype_rig import START
from tests.recovery_barriers import require_zero_held
from tests.runtime_rig import until
from tools.check_proxy_rollback import check_rollback, main


# Let the first rehearsed build create its journal while the parent owns only independent device peers.
@asynccontextmanager
async def rollback_rig(directory, tls, listener, management):
    async with managed_bench(directory, tls, listener, native_peer=InterruptedNativePeer, cli_route=True) as rig:
        config = directory / "trial.json"
        value = json.loads(config.read_text())
        value["local_tags"] = ["APP_OWNER", "NATIVE_TIMER"]
        save_private(config, value)
        strategy = strategy_file(rig, directory)
        save_private(directory / "process-strategy.json", json.loads(strategy.read_text()))
        child = ServiceProcess(rig, directory, management, "fast-poll")
        try:
            yield child
        finally:
            await child.close()


# Intent: a refused rollback identifies each source only if its bytes match the rehearsed build.
# Why: the operator must know which of the two supplied paths failed verification.
def test_unrehearsed_build_reports_only_verified_commit(tmp_path, builds):
    current, previous = builds
    config = SimpleNamespace(journal=tmp_path / "unused.sqlite3")
    wrong_previous = check_rollback(config, current, current)
    assert wrong_previous.reason == "build_pair_unrehearsed"
    assert wrong_previous.current_commit is not None and wrong_previous.previous_commit is None
    wrong_current = check_rollback(config, previous, previous)
    assert wrong_current.reason == "build_pair_unrehearsed"
    assert wrong_current.current_commit is None and wrong_current.previous_commit is not None


# Intent: a pinned rollback pair must match the service's actual installation requirements as well as its code.
# Why: testing another WebSocket version gives no rollback evidence for a source tree with changed service pins.
@pytest.mark.parametrize("side", ["current", "previous"])
def test_rollback_rejects_changed_service_pin(tmp_path, builds, side):
    current, previous = builds
    changed = tmp_path / "changed"
    shutil.copytree(current if side == "current" else previous, changed)
    (changed / "requirements-trial.txt").write_text("websockets==16.0\n")
    config = SimpleNamespace(journal=tmp_path / "unused.sqlite3")
    result = check_rollback(
        config, changed if side == "current" else current, changed if side == "previous" else previous
    )
    assert result.reason == "build_pair_unrehearsed"
    assert (result.current_commit is None) == (side == "current")
    assert (result.previous_commit is None) == (side == "previous")


# Select only actual lifecycle commands from the simulated cloud's transcript.
def lifecycle_frames(child):
    return [
        frame
        for frame in child.rig.bench.calls
        if frame[0] == 2 and frame[2] in ("StartTransaction", "StopTransaction")
    ]


# Switch only a stopped journal that passed the same reader used by the operator check.
async def checked_switch(child, current, previous):
    evidence = check_rollback(child.rig.prepared.config, current, previous)
    if evidence.allowed:
        await child.start(source=previous)
    return evidence


# Drive a real managed Start, measured power, exact Stop, delivered history and completed Pause.
async def completed_charge_and_pause(child):
    ready = await child.status()
    identifier = uuid.uuid4().hex
    assert (await child.request("start", id=identifier, revision=ready["control_revision"]))["ok"]
    await until(lambda: bool(starts(child.rig)))
    [command] = starts(child.rig)
    bench, peer = child.rig.bench, child.rig.peer
    message = await bench.call(
        child.socket, uuid.uuid4().hex, "StartTransaction", START | {"idTag": command[3]["idTag"]}
    )
    assert message[0] == 3 and message[2]["idTagInfo"]["status"] == "Accepted"
    local = message[2]["transactionId"]
    peer.plan_session = (local, 987654)
    peer.paused, peer.permission_enabled = False, True
    bench.cp, bench.connection = "C", "Charging"
    bench.firmware.fire_plan(9)
    active = await child.status(lambda status: status.get("current_a") == 6 and status.get("power_w", 0) > 0)
    started = await child.completed_control(identifier)
    pause = uuid.uuid4().hex
    assert (await child.request("pause", id=pause, revision=active["control_revision"]))["ok"]
    await asyncio.wait_for(bench.remote_stop.wait(), 5)
    targets = [frame[3]["transactionId"] for frame in bench.device_calls if frame[2] == "RemoteStopTransaction"]
    assert targets and set(targets) == {local}
    closed = await bench.call(
        child.socket,
        uuid.uuid4().hex,
        "StopTransaction",
        {"transactionId": local, "meterStop": 1100, "timestamp": "2026-01-01T00:01:00Z"},
    )
    assert closed[0] == 3
    peer.permission_enabled = False
    row = await child.delivered(local, closed=True)
    paused = await child.completed_control(pause)
    status = await child.status(lambda value: value.get("local_hold_verified") is True)
    assert status["desired_control"] == "paused" and status["charging_session_open"] is False
    assert status["power_w"] == bench.firmware.current == bench.firmware.effective_limit() == 0
    return started, paused, row


# Intent: a previous release resumes the exact newer Pause and uses history written by the newer release.
# Why: restarting an older build on a copied pre-upgrade journal would conceal lost work and ownership.
async def test_previous_build_keeps_advanced_journal(tmp_path, model_tls, unused_tcp_port_factory, builds, capsys):
    current, previous = builds
    port = unused_tcp_port_factory
    async with rollback_rig(tmp_path, tls_for_case(tmp_path, model_tls), port(), port()) as child:
        # Open the journal under the old build first so schema provenance also belongs to that release.
        await child.start(source=previous, commission_route=True)
        await child.status()
        original_journal = child.rig.prepared.config.journal.stat()
        await child.close()
        assert child.process.returncode == 0
        before_candidate = child.durable()

        await child.start(source=current)
        assert Path(f"/proc/{child.process.pid}/cwd").resolve() == current
        started, pause, charge = await completed_charge_and_pause(child)
        candidate = child.durable()
        intent = candidate["metadata"]["managed_control_intent"]
        owner = candidate["metadata"]["managed_owner_pause"]
        assert intent["revision"] > before_candidate["metadata"]["managed_control_intent"]["revision"]
        assert intent == {"mode": "paused", "revision": owner["revision"]}
        assert len(candidate["transactions"]) == len(before_candidate["transactions"]) + 1
        assert charge in candidate["transactions"]
        wire = copy.deepcopy(lifecycle_frames(child))
        assert [frame[2] for frame in wire] == ["StartTransaction", "StopTransaction"]
        assert wire[-1][3]["transactionId"] == charge["vendor"]
        [start] = copy.deepcopy(starts(child.rig))
        mark = len(child.rig.bench.device_calls)
        candidate_pid = child.process.pid
        assert check_rollback(child.rig.prepared.config, current, previous).reason == "journal_owner_running"
        await child.close()
        assert child.process.returncode == 0
        stopped = child.durable()
        assert stopped["metadata"]["managed_control_intent"] == intent
        assert stopped["metadata"]["managed_owner_pause"] == owner

        assert (
            main(
                [
                    "--config",
                    str(tmp_path / "trial.json"),
                    "--current-root",
                    str(current),
                    "--previous-root",
                    str(previous),
                ]
            )
            == 0
        )
        snapshot = json.loads(capsys.readouterr().out)
        assert snapshot["allowed"] is True and snapshot["pause_revision"] == intent["revision"]
        assert child.durable() == stopped
        proof = await checked_switch(child, current, previous)
        assert proof.allowed and proof.pause_revision == intent["revision"]
        assert child.process.pid != candidate_pid and Path(f"/proc/{child.process.pid}/cwd").resolve() == previous
        resumed_journal = child.rig.prepared.config.journal.stat()
        assert (resumed_journal.st_dev, resumed_journal.st_ino) == (original_journal.st_dev, original_journal.st_ino)
        status = await child.status(lambda value: value.get("local_hold_verified") is True)
        assert status["control_revision"] == intent["revision"] and status["power_w"] == 0
        recovered = child.durable()
        assert recovered["metadata"]["managed_control_intent"] == intent
        assert recovered["metadata"]["managed_owner_pause"] == owner
        assert recovered["transactions"] == stopped["transactions"]
        assert (await child.request("receipt", request_id=pause["id"]))["result"] == pause

        assert (await child.request("receipt", request_id=started["id"]))["result"] == started
        assert starts(child.rig) == [start] and lifecycle_frames(child) == wire
        require_zero_held(child.rig, mark)

        returned = await child.request("app_control", revision=intent["revision"])
        assert returned["ok"]
        await child.completed_control(returned["result"]["id"])
        await child.scheduled_sessions(6, True, start_minute=2)
        later = lifecycle_frames(child)
        assert starts(child.rig) == [start] and later[: len(wire)] == wire
        assert len(later) == len(wire) + 4
        final = child.durable()
        assert charge in final["transactions"] and not final["outbox"]
        assert len(final["transactions"]) == len(stopped["transactions"]) + 2
        assert (await child.request("receipt", request_id=pause["id"]))["result"] == pause


# Intent: an actually sent Start whose reply stayed unknown prevents the older build from launching.
# Why: a process exit and physical inactivity cannot settle the original command's possible outcome.
async def test_unresolved_start_blocks_previous_build(tmp_path, model_tls, unused_tcp_port_factory, builds):
    current, previous = builds
    port = unused_tcp_port_factory
    async with rollback_rig(tmp_path, tls_for_case(tmp_path, model_tls), port(), port()) as child:
        await child.start(source=current, commission_route=True)
        status = await child.status()
        seen, release = asyncio.Event(), asyncio.Event()
        bench = child.rig.bench

        # Hold the independent charger's actual reply after the Start frame reaches it.
        async def withhold(action, *_):
            if action == "RemoteStartTransaction":
                seen.set()
                await release.wait()

        bench.before_reply = withhold
        identifier = uuid.uuid4().hex
        try:
            assert (await child.request("start", id=identifier, revision=status["control_revision"]))["ok"]
            await asyncio.wait_for(seen.wait(), 4)
            accepted = child.durable()["metadata"]["local_start_receipt"]
            assert accepted["request_id"] == identifier and accepted["stage"] == "issued"
            [start] = starts(child.rig)
            await child.kill()
        finally:
            release.set()
            bench.before_reply = None
        generation, snapshot = child.generation, child.durable()
        proof = await checked_switch(child, current, previous)
        assert not proof.allowed and proof.reason == "unresolved_start"
        assert child.generation == generation and child.process.returncode == -9
        assert child.durable() == snapshot and starts(child.rig) == [start]


# Intent: refuse an unknown schema or unsupported target before inspecting a stopped eligible journal.
# Why: SQLite being readable and a target path existing are weaker conditions than rehearsed compatibility.
async def test_unrehearsed_state_or_build_blocks_switch(tmp_path, model_tls, unused_tcp_port_factory, builds):
    current, previous = builds
    port = unused_tcp_port_factory
    async with rollback_rig(tmp_path, tls_for_case(tmp_path, model_tls), port(), port()) as child:
        await child.start(source=current, commission_route=True)
        status = await child.status()
        paused = await child.request("pause", revision=status["control_revision"])
        assert paused["ok"]
        await child.completed_control(paused["result"]["id"])
        await child.status(lambda value: value.get("local_hold_verified") is True)
        await child.close()
        assert child.process.returncode == 0
        config = child.rig.prepared.config
        assert check_rollback(config, current, previous).allowed
        generation, snapshot = child.generation, child.durable()
        proof = await checked_switch(child, current, current)
        assert not proof.allowed and proof.reason == "build_pair_unrehearsed"
        assert child.generation == generation and child.durable() == snapshot

        with closing(sqlite3.connect(config.journal)) as db, db:
            db.execute(
                "INSERT INTO meta VALUES ('managed_current_prepare_intent', ?)",
                (json.dumps({"stage": "native_write_intent"}),),
            )
        changed = child.durable()
        proof = await checked_switch(child, current, previous)
        assert not proof.allowed and proof.reason == "unresolved_current_preparation"
        assert child.generation == generation and child.durable() == changed

        with closing(sqlite3.connect(config.journal)) as db, db:
            db.execute("DELETE FROM meta WHERE key='managed_current_prepare_intent'")
            profile = db.execute("SELECT desired FROM trial_resources WHERE key='profile:7001'").fetchone()[0]
            nonzero = json.dumps({**json.loads(profile), "limit": 6})
            db.execute("UPDATE trial_resources SET desired=?,confirmed=? WHERE key='profile:7001'", (nonzero, nonzero))
        proof = await checked_switch(child, current, previous)
        assert not proof.allowed and proof.reason == "saved_station_zero_required"
        assert child.generation == generation

        with closing(sqlite3.connect(config.journal)) as db, db:
            db.execute("UPDATE trial_resources SET desired=?,confirmed=? WHERE key='profile:7001'", (profile, profile))
            db.execute("ALTER TABLE transactions ADD COLUMN future_state TEXT")
        changed = child.durable()
        proof = await checked_switch(child, current, previous)
        assert not proof.allowed and proof.reason == "journal_schema_unrehearsed"
        assert child.generation == generation and child.durable() == changed
