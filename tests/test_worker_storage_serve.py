"""Actual service publications and authenticated API recover current completion failures without manual ticks."""

import asyncio
import json
import signal
import sqlite3
import uuid
from contextlib import closing

import pytest
from ocpp_hub_core.frames import basic_auth_header
from ocpp_hub_core.prototype.native_http import NativeTransportUnavailableError, read_native
from ocpp_hub_core.prototype.trial_session import TrialSession, save_private
from websockets.asyncio.client import connect

from tests import test_native_bounded_session as native_bench
from tests.handover_helpers import crash
from tests.test_managed_api import TOKEN, call
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_prototype import BOOT, START
from tests.test_prototype_runtime import FAST, until
from tests.test_prototype_tls import tls as tls
from tests.test_worker_storage_recovery import fail_boundary, failing_cleanup
from tools import managed_proxy


# Observe the actual service's awaited poll stack; this cannot advance the controller or manufacture an outcome.
def waits_for_current_worker(task):
    coroutine = task.get_coro()
    while coroutine is not None:
        frame = getattr(coroutine, "cr_frame", None)
        if (
            frame is not None
            and frame.f_code.co_name == "settle"
            and type(frame.f_locals.get("self")).__name__ == "ManagedCurrentLimits"
        ):
            return True
        coroutine = getattr(coroutine, "cr_await", None)
    return False


# Intent: persistent completion and diagnostic failures restrict zero while the real service still supervises HTTP.
# Why: later SQL repair cannot excuse positive offers or a frozen status/native poll during the failure interval.
@pytest.mark.parametrize("first_failure", ["statement", "commit"], ids=["statement-first", "commit-first"])
async def test_actual_serve_recovers_current_storage_and_two_later_sessions(
    tmp_path, tls, unused_tcp_port_factory, monkeypatch, first_failure
):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    async with commissioned(tmp_path, tls, unused_tcp_port_factory(), monkeypatch, 6, cli_route=True) as original:
        await original.controller.current.close()
        await original.controller.tests.close()
        await crash(original.session)
        rig = original.rig
        sessions, callbacks = [], {}
        loop = asyncio.get_running_loop()

        # Keep service logic intact; supply only loopback fixture addresses and real signal-callback capture.
        def create_session(prepared, **kwargs):
            session = TrialSession(prepared, FAST, **kwargs)
            sessions.append(session)
            rig.sessions.append(session)
            return session

        monkeypatch.setattr(managed_proxy, "preflight", lambda _: rig.prepared)
        monkeypatch.setattr(managed_proxy, "TrialSession", create_session)
        monkeypatch.setattr(managed_proxy, "notify", lambda _: None)
        monkeypatch.setattr(loop, "add_signal_handler", lambda sig, callback: callbacks.update({sig: callback}))
        monkeypatch.setattr(loop, "remove_signal_handler", lambda _: True)
        spec, settings, secret = (
            tmp_path / "serve-native.json",
            tmp_path / "serve-management.json",
            tmp_path / "serve-token.json",
        )
        save_private(spec, rig.spec.value())
        save_private(secret, {"token": TOKEN})
        port = unused_tcp_port_factory()
        save_private(settings, {"host": "127.0.0.1", "port": port, "token_file": str(secret)})
        status_path = rig.prepared.config.journal.with_suffix(".status.json")
        service = asyncio.create_task(managed_proxy.serve(tmp_path / "trial.json", spec, settings))

        # Await a genuinely new healthy service publication without advancing its control loop.
        async def published_after(previous=0, *, power=False, desired=None):
            async with asyncio.timeout(16):
                while True:
                    if service.done():
                        await service
                        raise AssertionError("service exited instead of recovering")
                    if status_path.exists():
                        value = json.loads(status_path.read_text())
                        if value.get("state") == "healthy" and value.get("checked_at", 0) > previous:
                            if (not power or value.get("power_w", 0) > 0) and (
                                desired is None or value.get("desired_control") == desired
                            ):
                                return value
                    await asyncio.sleep(0.02)

        # Read only the named durable command receipt until the service confirms its physical result.
        async def completed(api, identifier):
            async with asyncio.timeout(14):
                while True:
                    response = await call(api, "receipt", request_id=identifier)
                    assert response["ok"]
                    if response["result"]["state"] == "completed":
                        return response["result"]
                    assert response["result"]["state"] in ("accepted", "running", "uncertain")
                    await asyncio.sleep(0.05)

        try:
            await until(lambda: sessions and sessions[0].started)
            session = sessions[0]
            async with rig.charger(session) as charger:
                assert (await rig.bench.call(charger, "real-serve-storage-Boot", "BootNotification", BOOT))[0] == 3
                async with connect(
                    f"ws://127.0.0.1:{port}/management",
                    additional_headers={"Authorization": "Bearer " + TOKEN},
                    subprotocols=["ics-proxy.v1"],
                    proxy=None,
                ) as api:
                    local_ids = []
                    for cycle, failure in enumerate(
                        (first_failure, "commit" if first_failure == "statement" else "statement")
                    ):
                        first = await published_after()
                        current = (await call(api, "status"))["result"]
                        if current["desired_control"] == "paused":
                            response = await call(api, "app_control", revision=current["control_revision"])
                            assert response["ok"]
                            await completed(api, response["result"]["id"])
                            first = await published_after(first["checked_at"], desired="app")
                        assert first["station_ceiling_a"] == 6
                        j = session.journal
                        fail_boundary(j, "current", failure)
                        _reached, attempts = failing_cleanup(j, "managed_current_failure")
                        identifier = uuid.uuid4().hex
                        revision = first["control_revision"]
                        before_limits = len(rig.bench.received_limits)
                        accepted = await call(api, "set_current", id=identifier, revision=revision, current_a=16)
                        assert accepted["ok"] and accepted["result"]["id"] == identifier
                        assert accepted["result"]["state"] == "accepted", accepted
                        await until(lambda: bool(attempts))
                        assert rig.bench.received_limits[before_limits:].count(16) == 1
                        change = j.get("managed_current_change")
                        assert change["id"] == identifier and change["current_a"] == 16 and change["previous_a"] == 6
                        assert (await call(api, "receipt", request_id=identifier))["result"]["state"] != "completed"
                        assert rig.bench.firmware.current == 0 and j.active() is None

                        # Prove actual HTTP503 independently; this identified observer probe is not engine progress.
                        rig.peer.unavailable = True
                        probe = {"review_probe": identifier}
                        with pytest.raises(NativeTransportUnavailableError):
                            await read_native(
                                "127.0.0.1",
                                rig.prepared.observer.port,
                                basic_auth_header("admin", "NATIVE_TEST_SECRET")[1],
                                "getConfig",
                                probe,
                            )
                        engine_failed_reads = []
                        probe_reads = []

                        # Separate controller requests from the explicit observer probe using their real HTTP payloads.
                        def take_failed_reads():
                            while not rig.peer.failed_reads.empty():
                                request = rig.peer.failed_reads.get_nowait()
                                if request.get("data") == probe:
                                    probe_reads.append(request)
                                else:
                                    engine_failed_reads.append(request)

                        take_failed_reads()
                        assert len(probe_reads) == 1 and probe_reads[0]["action"] == "getConfig"

                        # Save physical and receipt facts before teardown can remove SQL failures or send zero.
                        def witness(stage):
                            take_failed_reads()
                            row = j.db.execute(
                                "SELECT id,action,revision,state FROM managed_requests WHERE id=?", (identifier,)
                            ).fetchone()
                            value = {
                                "stage": stage,
                                "failure": failure,
                                "selected_before_a": 6,
                                "target_a": 16,
                                "effective_offer_a": rig.bench.firmware.effective_limit(),
                                "physical_current_a": rig.bench.firmware.current,
                                "native_meter": rig.peer.native("METER"),
                                "request": dict(row),
                                "change": j.get("managed_current_change"),
                                "control": j.get("control"),
                                "completion_and_diagnostic_triggers": [
                                    r[0]
                                    for r in j.db.execute(
                                        "SELECT name FROM sqlite_temp_master WHERE type='trigger' AND name IN "
                                        "('review_worker_boundary','review_cleanup_unavailable') ORDER BY name"
                                    )
                                ],
                                "diagnostic_refusals": len(attempts),
                                "engine_failed_http": engine_failed_reads,
                                "observer_failed_http": probe_reads,
                                "previous_checked_at": first["checked_at"],
                                "published_status": json.loads(status_path.read_text()),
                                "service_waits_for_worker": waits_for_current_worker(service),
                            }
                            save_private(tmp_path / ("current-cleanup-" + failure + "-" + stage + ".json"), value)
                            return value

                        # Only autonomous service/worker tasks may reach zero; neither trigger is removed to help them.
                        zero = False
                        zero_deadline = asyncio.get_running_loop().time() + 7.5
                        async with asyncio.timeout(8):
                            while not zero:
                                zero = (
                                    rig.bench.firmware.effective_limit() == 0
                                    and session.gateway.controls.ready
                                    and session.gateway.controls.desired() == 0
                                )
                                if zero:
                                    break
                                if service.done():
                                    await service
                                await asyncio.sleep(0.02)
                                if asyncio.get_running_loop().time() >= zero_deadline:
                                    break
                        state = witness("before-diagnostic-repair")
                        assert (
                            len(state["completion_and_diagnostic_triggers"]) == 2 and state["diagnostic_refusals"] > 0
                        )
                        assert state["request"]["id"] == identifier and state["request"]["action"] == "set_current"
                        assert state["request"]["revision"] == revision
                        assert zero, ("persistent diagnostic storage refusal must not delay protective zero", state)
                        assert rig.bench.firmware.current == 0
                        assert j.get("control")["resume_limit"] == 6

                        # Require three engine HTTP failures and a fresh honest status with SQL still refused.
                        async with asyncio.timeout(18):
                            while True:
                                take_failed_reads()
                                status = json.loads(status_path.read_text())
                                if (
                                    len(engine_failed_reads) >= 3
                                    and status.get("checked_at", 0) > first["checked_at"]
                                    and status.get("state") not in ("healthy", "local_only")
                                ):
                                    break
                                await asyncio.sleep(0.02)
                        assert all(request["action"] == "getConfig" for request in engine_failed_reads)
                        response = await call(api, "status")
                        assert response["ok"] and response["result"]["healthy"] is False
                        witness("native-unavailable-still-refused")

                        # Recover HTTP while diagnostic storage still refuses writes, before repair or owner controls.
                        rig.peer.unavailable = False
                        await read_native(
                            "127.0.0.1",
                            rig.prepared.observer.port,
                            basic_auth_header("admin", "NATIVE_TEST_SECRET")[1],
                            "getConfig",
                            probe,
                        )
                        reads = rig.peer.requests.count("getConfig")
                        unavailable_checked = status["checked_at"]
                        async with asyncio.timeout(12):
                            while (
                                rig.peer.requests.count("getConfig") <= reads
                                or json.loads(status_path.read_text()).get("checked_at", 0) <= unavailable_checked
                            ):
                                await asyncio.sleep(0.02)
                        assert rig.bench.firmware.effective_limit() == 0 and rig.bench.firmware.current == 0
                        assert rig.bench.received_limits[before_limits:].count(16) == 1
                        assert (await call(api, "receipt", request_id=identifier))["result"]["state"] != "completed"
                        witness("native-recovered-still-refused")
                        j.db.execute("DROP TRIGGER review_worker_boundary")
                        j.db.execute("DROP TRIGGER review_cleanup_unavailable")
                        healed = await published_after(first["checked_at"])
                        assert healed["station_ceiling_a"] == 0 and rig.bench.firmware.effective_limit() == 0
                        response = await call(api, "pause", revision=healed["control_revision"])
                        assert response["ok"]
                        await completed(api, response["result"]["id"])
                        state = (await call(api, "status"))["result"]
                        returned = await call(api, "app_control", revision=state["control_revision"])
                        assert returned["ok"]
                        await completed(api, returned["result"]["id"])
                        assert (await call(api, "receipt", request_id=identifier))["result"]["state"] == "failed"
                        assert rig.bench.firmware.effective_limit() == 6

                        # Independent peers now supply a real ordinary session, measured energy and both closures.
                        authorized = await rig.bench.call(
                            charger, uuid.uuid4().hex, "Authorize", {"idTag": "NATIVE_TIMER"}
                        )
                        assert authorized[0] == 3 and authorized[2]["idTagInfo"]["status"] == "Accepted"
                        start = START | {
                            "idTag": "NATIVE_TIMER",
                            "meterStart": 1400 + cycle * 100,
                            "timestamp": f"2026-01-01T01:0{cycle * 2}:00Z",
                        }
                        response = await rig.bench.call(charger, uuid.uuid4().hex, "StartTransaction", start)
                        assert response[0] == 3 and response[2]["idTagInfo"]["status"] == "Accepted"
                        local = response[2]["transactionId"]
                        local_ids.append(local)
                        rig.peer.plan_session, rig.peer.permission_enabled, rig.peer.paused = (local, 9), True, False
                        rig.bench.cp, rig.bench.connection = "C", "Charging"
                        rig.bench.firmware.fire_plan(9)
                        running = await published_after(healed["checked_at"], power=True)
                        assert running["current_a"] == 6 and rig.bench.firmware.current == 6
                        meter = {
                            "transactionId": local,
                            "connectorId": 1,
                            "meterValue": [
                                {
                                    "timestamp": f"2026-01-01T01:0{cycle * 2}:30Z",
                                    "sampledValue": [
                                        {
                                            "value": str(1450 + cycle * 100),
                                            "measurand": "Energy.Active.Import.Register",
                                            "unit": "Wh",
                                        }
                                    ],
                                }
                            ],
                        }
                        assert (await rig.bench.call(charger, uuid.uuid4().hex, "MeterValues", meter))[0] == 3
                        await until(lambda: session.journal.next_delivery() is None)
                        rig.bench.remote_stop.clear()
                        response = await call(api, "pause", revision=running["control_revision"])
                        assert response["ok"]
                        await asyncio.wait_for(rig.bench.remote_stop.wait(), 8)
                        assert rig.bench.firmware.effective_limit() == 0
                        rig.peer.permission_enabled, rig.peer.plan_session = False, None
                        rig.bench.firmware.requested_current = 0
                        stop = {
                            "transactionId": local,
                            "meterStop": 1500 + cycle * 100,
                            "timestamp": f"2026-01-01T01:0{cycle * 2 + 1}:00Z",
                        }
                        assert (await rig.bench.call(charger, uuid.uuid4().hex, "StopTransaction", stop))[0] == 3
                        await until(lambda: j.active() is None and j.next_delivery() is None)
                        await completed(api, response["result"]["id"])
                        with closing(sqlite3.connect(j.path.as_uri() + "?mode=ro", uri=True)) as db:
                            row = db.execute(
                                "SELECT vendor,start,stop FROM transactions WHERE local=?", (local,)
                            ).fetchone()
                            assert row[0] is not None and json.loads(row[1]) == start and json.loads(row[2]) == stop
                            assert db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 0
                        wires = [
                            frame
                            for frame in rig.bench.calls
                            if frame[0] == 2
                            and (
                                (frame[2] == "StartTransaction" and frame[3] == start)
                                or (frame[2] == "StopTransaction" and frame[3] == stop | {"transactionId": row[0]})
                            )
                        ]
                        assert len(wires) == 2 and {frame[2] for frame in wires} == {
                            "StartTransaction",
                            "StopTransaction",
                        }
                        assert all(
                            any(reply[1] == frame[1] for reply in rig.bench.sent_vendor_replies) for frame in wires
                        )
                        assert len(sessions) == 1 and not service.done() and not session.closed
                    assert len(set(local_ids)) == 2
        finally:
            rig.peer.unavailable = False
            if sessions and not sessions[0].closed:
                sessions[0].journal.db.execute("DROP TRIGGER IF EXISTS review_worker_boundary")
                sessions[0].journal.db.execute("DROP TRIGGER IF EXISTS review_cleanup_unavailable")
            if not service.done():
                callbacks[signal.SIGTERM]()
            await asyncio.wait_for(service, 12)
