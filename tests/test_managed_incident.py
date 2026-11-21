"""Independent process cuts and a read-only report used to select the matching replay."""

import asyncio
import hashlib
import json
import socket
import uuid
from types import SimpleNamespace

import pytest
from ocpp_hub_core.prototype.managed_intent import ManagedRequests
from ocpp_hub_core.prototype.store import Journal
from ocpp_hub_core.prototype.trial_session import save_private

from tests.managed_rig import starts
from tests.model_tls_support import model_tls as model_tls
from tests.model_tls_support import tls_for_case
from tests.peer_mutation_helpers import observe_device_replies
from tests.process_helpers import process_rig
from tests.runtime_rig import until
from tests.tls_support import tls as tls
from tests.trial_helpers import config_files
from tools import managed_proxy
from tools.managed_incident import MAX_REPORT_EVENTS, incident_report


# Obtain the exact same read-only report as the CLI for this independent process fixture.
def report(child):
    configuration = child.rig.prepared.config
    return incident_report(configuration.journal, configuration.incident_events)


# Use only the report's evidence to choose how to reproduce an interrupted Start on fresh peers.
def reconstruct(report_body):
    journal, wire = report_body["journal"], report_body["wire"]
    assert journal["request"]["request"] == wire["request"] and journal["start"]["ticket"] == wire["ticket"]
    assert journal["start"]["authority_published"] is False
    assert wire["send"] in ("send_returned", "matching_reply")
    saved = journal["start"]["saved_reply"]
    if saved == "accepted":
        assert journal["start"]["stage"] == "accepted"
        boundary = "after-accepted-commit"
    else:
        assert journal["start"]["stage"] == "issued" and saved == wire["observed_reply"] == "unknown"
        boundary = "charger-holds-reply"
    return boundary, journal["request"]["selected_current_a"]


# Kill at a proven committed SQLite boundary or after a real receiving charger withholds its matching reply.
async def cut_and_restart(tmp, model_tls, ports, monkeypatch, boundary, selected, *, output_fails=False):
    tmp.mkdir()
    async with process_rig(
        tmp, tls_for_case(tmp, model_tls), ports(), ports(), monkeypatch, selected, timing="fast-poll"
    ) as child:
        if output_fails:
            child.rig.prepared.config.incident_events.mkdir()
        await child.start()
        ready = await child.status()
        seen, release = asyncio.Event(), asyncio.Event()
        bench = child.rig.bench
        replies = observe_device_replies(SimpleNamespace(sock=child.socket), monkeypatch)
        identifier = uuid.uuid4().hex
        held = boundary == "charger-holds-reply"

        # The independent charger receives and handles the Start frame before withholding its actual reply.
        async def hold(action, *_):
            if action == "RemoteStartTransaction":
                seen.set()
                await release.wait()

        if held:
            bench.before_reply = hold
        else:
            await child.arm(boundary, identifier)
        request = asyncio.create_task(child.request("start", id=identifier, revision=ready["control_revision"]))
        try:
            if held:
                await asyncio.wait_for(seen.wait(), 4)
                await until(lambda: report(child)["wire"]["send"] == "send_returned", timeout=2)
            else:
                gate = await child.gate(boundary, identifier)
                assert gate["transaction_open"] is (boundary == "before-accepted-commit")
                if boundary == "before-accepted-commit":
                    await until(lambda: report(child)["wire"]["observed_reply"] == "accepted", timeout=2)
            frames = starts(child.rig)
            before = child.durable()["metadata"]["local_start_receipt"]
            assert before["request_id"] == identifier and before["local"] is None
            if boundary == "after-issued-commit":
                assert not frames and before["stage"] == "issued"
            else:
                assert len(frames) == 1
                accepted = [3, frames[0][1], {"status": "Accepted"}] in replies
                assert accepted is not held
                assert before["stage"] == ("accepted" if boundary == "after-accepted-commit" else "issued")
            await child.kill()
        finally:
            release.set()
            bench.before_reply = None
            if not request.done():
                request.cancel()
            await asyncio.gather(request, return_exceptions=True)
        await child.start()
        await child.status(lambda value: value.get("charger_connected") is True)
        if not output_fails:
            await until(
                lambda: len([row for row in report(child)["observations"] if row["event"] == "process_started"]) == 2
            )
        before_report = child.durable()
        learned = report(child)
        assert child.durable()["metadata"]["local_start_receipt"] == before
        assert child.durable()["requests"] == before_report["requests"]
        assert starts(child.rig) == frames
        assert len(child.kills) == 1
        assert learned["journal"]["request"]["request"] != identifier
        assert identifier not in json.dumps(learned) and before["ticket"] not in json.dumps(learned)
        save_private(tmp / "incident-report.json", learned)
        return learned


# Intent: a report alone selects an actual repeatable crash and the original selected current on fresh peers.
# Why: durable acceptance and an unknown reply must not lead a reviewer to reproduce the same interruption.
@pytest.mark.parametrize("boundary,selected", [("after-accepted-commit", 6), ("charger-holds-reply", 16)])
async def test_incident_recreates_interrupted_start(
    tmp_path, model_tls, unused_tcp_port_factory, monkeypatch, boundary, selected
):
    args = (model_tls, unused_tcp_port_factory, monkeypatch)
    original = await cut_and_restart(tmp_path / "observed", *args, boundary, selected)
    exported = json.loads((tmp_path / "observed" / "incident-report.json").read_text())
    assert exported == original
    replay = reconstruct(exported)
    assert replay == (boundary, selected)
    recreated = await cut_and_restart(tmp_path / "recreated", *args, *replay)
    assert reconstruct(recreated) == replay
    for result in (original, recreated):
        assert result["journal"]["start"]["lifecycle"] == "unbound"
        assert result["waiting_for_evidence"] == [
            "start_lifecycle" if boundary == "after-accepted-commit" else "start_outcome_or_lifecycle"
        ]
        runs = [row["run"] for row in result["observations"] if row["event"] == "process_started"]
        sent = [row["run"] for row in result["observations"] if row["event"] == "start_sent"]
        assert len(set(runs)) == 2
        assert sent == [runs[0]]


# Intent: issued is a journal fact even when the process never sends the frame.
# Why: a cut can lie exactly between the receipt commit and completion of the actual send.
async def test_incident_issued_leaves_transmission_unknown(tmp_path, model_tls, unused_tcp_port_factory, monkeypatch):
    learned = await cut_and_restart(
        tmp_path / "issued", model_tls, unused_tcp_port_factory, monkeypatch, "after-issued-commit", 6
    )
    assert learned["journal"]["start"]["stage"] == "issued"
    assert learned["wire"]["send"] == learned["wire"]["observed_reply"] == "unknown"


# Intent: a real matched Accepted can be observed while process death rolls back its journal write.
# Why: the optional report cannot upgrade an observed reply into committed recovery authority.
async def test_incident_observed_reply_survives_failed_commit(
    tmp_path, model_tls, unused_tcp_port_factory, monkeypatch
):
    learned = await cut_and_restart(
        tmp_path / "before-commit", model_tls, unused_tcp_port_factory, monkeypatch, "before-accepted-commit", 6
    )
    assert learned["wire"]["observed_reply"] == "accepted"
    assert learned["journal"]["start"]["stage"] == "issued"
    assert learned["journal"]["start"]["saved_reply"] == "unknown"
    assert learned["waiting_for_evidence"] == ["start_outcome_or_lifecycle"]


# Intent: a genuine late Accepted is correlated with the exact newer Pause through a process replacement.
# Why: a stale client revision and a real older reply can coexist with a different current request.
async def test_incident_retains_late_reply_and_newer_pause(tmp_path, model_tls, unused_tcp_port_factory, monkeypatch):
    ports = unused_tcp_port_factory
    async with process_rig(
        tmp_path, tls_for_case(tmp_path, model_tls), ports(), ports(), monkeypatch, timing="fast-poll"
    ) as child:
        await child.start()
        ready = await child.status()
        first = (await child.request("pause", revision=ready["control_revision"]))["result"]["id"]
        await child.completed_control(first)
        paused = await child.status(lambda status: status.get("local_hold_verified") is True)
        replies = observe_device_replies(SimpleNamespace(sock=child.socket), monkeypatch)
        seen, release = asyncio.Event(), asyncio.Event()
        bench = child.rig.bench

        # Keep the actual old response awaiting delivery until this newer owner request commits.
        async def hold(action, *_):
            if action == "RemoteStartTransaction":
                seen.set()
                await release.wait()

        bench.before_reply = hold
        start, pause = uuid.uuid4().hex, uuid.uuid4().hex
        try:
            assert (await child.request("start", id=start, revision=paused["control_revision"]))["ok"]
            await asyncio.wait_for(seen.wait(), 4)
            frames = starts(child.rig)
            assert len(frames) == 1 and paused["control_revision"] > 0
            assert (await child.request("pause", id=pause, revision=0))["ok"]
            expected = [3, frames[0][1], {"status": "Accepted"}]
            assert expected not in replies
            release.set()
            await until(lambda: (report(child)["journal"]["start"] or {}).get("saved_reply") == "accepted")
            await until(lambda: report(child)["wire"]["observed_reply"] == "accepted")
            assert expected in replies
        finally:
            release.set()
            bench.before_reply = None
        await child.kill()
        await child.start()
        await child.status(lambda status: status.get("charger_connected") is True)
        learned = report(child)
        intent = learned["journal"]["intent"]
        assert intent["mode"] == "paused" and intent["revision"] == paused["control_revision"] + 1
        assert intent["pause_request"] == hashlib.sha256(pause.encode()).hexdigest()[:32]
        sent, superseding, reply = [
            row
            for row in learned["observations"]
            if row["event"] in ("start_sent", "start_reply") or row.get("request") == intent["pause_request"]
        ]
        assert [row["event"] for row in (sent, superseding, reply)] == ["start_sent", "pause_committed", "start_reply"]
        assert sent["sequence"] < superseding["sequence"] < reply["sequence"]
        assert sent["intent"]["revision"] == paused["control_revision"]
        assert reply["intent"]["revision"] == intent["revision"] and reply["pause_ticket"] == superseding["ticket"]
        assert learned["journal"]["start"]["saved_reply"] == "accepted"
        assert learned["journal"]["start"]["authority_published"] is False
        assert starts(child.rig) == frames
        save_private(tmp_path / "late-incident-report.json", learned)


# Intent: failed optional event writes cannot prevent a genuine reply from being saved.
# Why: the recovery journal remains useful when the reporting destination is unavailable.
async def test_incident_output_failure_keeps_committed_reply(tmp_path, model_tls, unused_tcp_port_factory, monkeypatch):
    learned = await cut_and_restart(
        tmp_path / "failed-output",
        model_tls,
        unused_tcp_port_factory,
        monkeypatch,
        "after-accepted-commit",
        6,
        output_fails=True,
    )
    assert learned["observations"] == []
    assert learned["wire"]["send"] == "matching_reply" and learned["wire"]["observed_reply"] == "unknown"
    assert learned["journal"]["start"]["saved_reply"] == "accepted"


# Intent: an incident export can read a private configuration without networking or creating a new journal.
# Why: an absent installation should be reported as unknown without changing its state.
def test_incident_cli_uses_local_read_only_sources(tmp_path, tls, capsys, monkeypatch):
    config, file = config_files(tmp_path, 21010, 21011)

    # Fail immediately if incident constructs a charger transport or resolves any connection.
    def forbidden(*args, **kwargs):
        raise AssertionError("incident attempted networking")

    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    before = sorted(tmp_path.iterdir())
    assert managed_proxy.main(["incident", "--config", str(file)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert not result["journal"]["available"] and result["wire"]["send"] == "unknown"
    assert not config.journal.exists() and sorted(tmp_path.iterdir()) == before


# Intent: a newer unsent local Start remains the incident selected after cloud test metadata replaces it.
# Why: the report must never attach an older command's physical reply to the latest owner request.
def test_incident_uses_latest_local_request_even_without_wire_receipt(tmp_path):
    path = tmp_path / "journal.db"
    event_path = tmp_path / "journal.start-events.jsonl"
    old, new, cloud, ticket = (uuid.uuid4().hex for _ in range(4))
    journal = Journal(path, "OFFLINE001")
    try:
        requests = ManagedRequests(journal)
        requests.accept(old, "start", 0)
        requests.finish(old, "completed", {"local_session_verified": True})
        journal.put("local_start_receipt", {"request_id": old, "ticket": ticket, "stage": "accepted"})
        requests.accept(new, "start", 1)
        requests.finish(new, "failed", {"classification": "preflight_refused_unsent"})
        requests.accept(cloud, "arm_cloud_test", 1)
        journal.put("managed_test_charge", {"id": cloud, "source": "cloud", "state": "armed"})
        learned = incident_report(path, event_path)
        saved = learned["journal"]
        assert saved["request"] == {
            "request": hashlib.sha256(new.encode()).hexdigest()[:32],
            "action": "start",
            "revision": 1,
            "state": "failed",
        }
        assert saved["start"] is None
        assert learned["wire"]["ticket"] is None and learned["wire"]["observed_reply"] == "unknown"
    finally:
        journal.close()


# Intent: project only exact retained reply and lifecycle records, with a bounded and redacted event excerpt.
# Why: saved cloud reply settlement, unknown cloud delivery and unrelated stored observations mean different things.
def test_incident_binds_only_exact_bounded_records(tmp_path):
    path = tmp_path / "journal.db"
    event_path = tmp_path / "journal.start-events.jsonl"
    token, start, ticket = "private-token", uuid.uuid4().hex, uuid.uuid4().hex
    journal = Journal(path, "OFFLINE001")
    try:
        ManagedRequests(journal)
        journal.db.execute("INSERT INTO managed_requests VALUES(?,'start',0,'uncertain','{}')", (start,))
        journal.put("local_start_receipt", {"request_id": start, "ticket": ticket, "stage": "accepted", "local": 42})
        journal.db.execute(
            "INSERT INTO transactions(local,connector,start,stop,vendor) VALUES(42,1,'private-start','private-stop',99)"
        )
        journal.db.execute(
            "INSERT INTO outbox(token,action,payload,local,state) "
            "VALUES(?,'StopTransaction','private-body',42,'inflight')",
            (token,),
        )
        journal.db.execute(
            "INSERT INTO delivery_replies VALUES(?,'StopTransaction','different-body',42,'private-reply')", (token,)
        )
        assert incident_report(path, event_path)["waiting_for_evidence"] == ["cloud_delivery"]
        journal.db.execute("UPDATE delivery_replies SET payload='private-body' WHERE token=?", (token,))

        row = {
            "run": uuid.uuid4().hex,
            "at": 1,
            "event": "start_sent",
            "request": hashlib.sha256(start.encode()).hexdigest()[:32],
            "ticket": hashlib.sha256(ticket.encode()).hexdigest()[:32],
            "free_text": "private-endpoint",
        }
        for n in range(MAX_REPORT_EVENTS + 3):
            managed_proxy.event(event_path, {**row, "sequence": n + 1})
        managed_proxy.event(
            event_path,
            {
                **row,
                "sequence": MAX_REPORT_EVENTS + 4,
                "event": "start_reply",
                "ticket": uuid.uuid4().hex,
                "reply": "rejected",
            },
        )
        with event_path.open("ab") as stream:
            stream.write(b'{"private-torn-field":')
        learned = incident_report(path, event_path)
        assert len(learned["observations"]) == MAX_REPORT_EVENTS
        assert learned["wire"]["observed_reply"] == "unknown"
        assert learned["journal"]["start"]["pending_delivery"] == [
            {"event": "StopTransaction", "state": "inflight", "reply_saved": True}
        ]
        assert learned["journal"]["start"]["lifecycle"] == "closed"
        assert learned["waiting_for_evidence"] == ["saved_cloud_reply_settlement"]
        assert "private-" not in json.dumps(learned)
        managed_proxy.event(
            event_path,
            {**row, "sequence": MAX_REPORT_EVENTS + 5, "event": "start_reply", "reply": "accepted"},
        )
        learned = incident_report(path, event_path)
        assert learned["wire"]["observed_reply"] == "accepted"
        assert "private-" not in json.dumps(learned)
        journal.db.execute("DELETE FROM delivery_replies WHERE token=?", (token,))
        journal.db.execute("UPDATE outbox SET state='rejected' WHERE token=?", (token,))
        rejected = incident_report(path, event_path)["journal"]["start"]["pending_delivery"]
        assert rejected == [{"event": "StopTransaction", "state": "rejected", "reply_saved": False}]
    finally:
        journal.close()
