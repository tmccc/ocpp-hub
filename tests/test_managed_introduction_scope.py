"""Exact introduction origin and acknowledgement survive retries and real storage refusals."""

from __future__ import annotations

import asyncio
import socket
import sqlite3
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from urllib.parse import urlsplit, urlunsplit

import pytest
from ocpp_hub_core.prototype.connection_scope import current_scope
from ocpp_hub_core.prototype.managed_intent import OFFLINE_PAUSE
from ocpp_hub_core.prototype.native_bounded_handover import AuditSupersededError

from tests.current_rig import selected_ready
from tests.handover_helpers import NativeVehiclePeer
from tests.managed_rig import OwnerBench, healthy, introduced, starts
from tests.native_peer_failures import InterruptedNativePeer
from tests.prototype_rig import BOOT
from tests.runtime_rig import until
from tests.test_managed_current_tick_race import OneNativeResponseGate
from tests.tls_support import tls as tls

pytestmark = pytest.mark.asyncio


# Reconnect an already commissioned service and stop before the controller acknowledges its real zero introduction.
@asynccontextmanager
async def pending_introduction(
    tmp_path,
    tls,
    port,
    monkeypatch,
    *,
    native_peer: type[NativeVehiclePeer] = NativeVehiclePeer,
    bench_type: type[OwnerBench] = OwnerBench,
):
    async with selected_ready(
        tmp_path, tls, port, monkeypatch, 32, native_peer=native_peer, bench_type=bench_type
    ) as b:
        session, g, j = b.session, b.session.gateway, b.session.journal
        await b.charger.close()
        await until(lambda: not session.runtime.status.charger_connected)
        async with b.rig.charger(session) as charger:
            b.charger = charger
            await introduced(b.rig, session, charger, uuid.uuid4().hex)
            await until(
                lambda: (
                    not g.controls._operations
                    and not g.charger._pending
                    and not g.vendor._pending
                    and j.next_delivery() is None
                )
            )
            scope = current_scope(g)
            assert scope is not None
            assert j.get("trial_phase") == "recovering"
            assert session.native_bounded.introduction_lineage == scope.lineage
            assert session.native_bounded.acknowledged_lineage != scope.lineage
            assert b.rig.bench.firmware.effective_limit() == 0
            yield b


# Close the actual cloud and reserve a non-listening endpoint until its captured current-scope attempt really fails.
@asynccontextmanager
async def failed_cloud(b):
    runtime, g = b.session.runtime, b.session.gateway
    scope, count = current_scope(g), runtime.status.vendor_fault.count
    assert scope is not None and runtime.status.vendor_connected and g.vendor_registration == "Accepted"
    transport, original = runtime.transport, runtime.transport.config
    with socket.socket() as unavailable:
        unavailable.bind(("127.0.0.1", 0))
        url = urlsplit(original.vendor_url)
        transport.config = replace(
            original,
            vendor_url=urlunsplit(url._replace(netloc="127.0.0.1:" + str(unavailable.getsockname()[1]))),
        )
        try:
            await b.rig.bench.cloud.close()
            await until(
                lambda: (
                    runtime.status.vendor_fault.count > count
                    and runtime.status.vendor_failed_scope == scope
                    and not runtime.status.vendor_connected
                    and g.vendor_registration != "Accepted"
                )
            )
            yield scope
        finally:
            transport.config = original


# Read only the independent peer's recorded failed requests; ticks after the first hold need not reread HTTP.
def failed_requests(peer):
    requests = []
    while not peer.failed_reads.empty():
        requests.append(peer.failed_reads.get_nowait())
    return requests


# Check actual charger command history without manufacturing transaction or control receipts.
def no_start_or_stop(b):
    assert not starts(b.rig)
    assert not any(frame[2] == "RemoteStopTransaction" for frame in b.rig.bench.device_calls)


# Intent: acknowledged reconnect provenance stays retired across a matching Boot and a later ordinary outage.
# Why: reusing recovering phase or a changed Boot UUID must not turn an ordinary app hold into a replacement Pause.
async def test_acknowledged_lineage_survives_matching_boot_and_ordinary_outage(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    async with pending_introduction(
        tmp_path, tls, unused_tcp_port, monkeypatch, native_peer=InterruptedNativePeer
    ) as b:
        c, g, j, peer = b.controller, b.session.gateway, b.session.journal, b.rig.peer
        native, intent = c.native, c.requests.intent()
        initial = current_scope(g)
        assert initial is not None
        assert (await healthy(c))["station_ceiling_a"] == 32
        assert native.introduction_lineage == native.acknowledged_lineage == initial.lineage
        assert (await b.rig.bench.call(b.charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
        current = current_scope(g)
        assert current is not None and current.boot != initial.boot and current.lineage == initial.lineage
        await healthy(c)
        failed_requests(peer)
        try:
            async with failed_cloud(b) as failed:
                assert failed == current
                peer.unavailable = True
                for _ in range(3):
                    status = await c.tick()
                    assert status["state"] != "attention", status
                requests = failed_requests(peer)
                assert requests and all(request["action"] == "getConfig" for request in requests)
                assert j.get("trial_phase") == "recovering"
                assert b.rig.bench.firmware.effective_limit() == 0
                assert current_scope(g) == current and c.requests.intent() == intent
                assert native.introduction_lineage == native.acknowledged_lineage == current.lineage
                assert not j.get("managed_control_fault")
                peer.unavailable = False
                assert (await c.tick())["state"] == "waiting_for_cloud"
                assert c.requests.intent() == intent
        finally:
            peer.unavailable = False
        await until(lambda: b.session.runtime.status.vendor_connected and g.vendor_registration == "Accepted")
        assert (await healthy(c))["station_ceiling_a"] == 32
        assert c.requests.intent() == intent and current_scope(g) == current
        no_start_or_stop(b)


# Intent: a real resume COMMIT refusal cannot retire origin, while a fresh successful retry can retire it.
# Why: publishing acknowledgement before the atomic phase/control transaction commits would suppress required Pause.
async def test_resume_acknowledges_lineage_only_after_real_commit(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with pending_introduction(tmp_path, tls, unused_tcp_port, monkeypatch) as b:
        c, g, j = b.controller, b.session.gateway, b.session.journal
        native, scope = c.native, current_scope(g)
        assert scope is not None
        acknowledged, control = native.acknowledged_lineage, j.get("control")
        attempts = []

        # Observe the actual phase write inside SQLite; the missing deferred parent makes COMMIT itself fail.
        def observed():
            attempts.append(1)
            return 0

        j.db.execute("CREATE TABLE cf01_resume_parent(id INTEGER PRIMARY KEY)")
        j.db.execute(
            "CREATE TABLE cf01_resume_child(id REFERENCES cf01_resume_parent(id) DEFERRABLE INITIALLY DEFERRED)"
        )
        j.db.create_function("cf01_resume_commit_reached", 0, observed)
        j.db.execute(
            "CREATE TEMP TRIGGER cf01_resume_commit AFTER INSERT ON meta "
            "WHEN NEW.key='trial_phase' AND json_extract(NEW.value,'$')='active' "
            "BEGIN SELECT cf01_resume_commit_reached(); INSERT INTO cf01_resume_child VALUES(123); END"
        )
        try:
            with pytest.raises(sqlite3.IntegrityError) as raised:
                await native.resume()
            assert raised.value.sqlite_errorcode == sqlite3.SQLITE_CONSTRAINT_FOREIGNKEY
            assert attempts == [1] and not j.db.in_transaction
            assert j.db.execute("SELECT COUNT(*) FROM cf01_resume_child").fetchone()[0] == 0
            assert j.get("trial_phase") == "recovering" and j.get("control") == control
            assert native.introduction_lineage == scope.lineage
            assert native.acknowledged_lineage == acknowledged != scope.lineage
            assert current_scope(g) == scope and b.rig.bench.firmware.effective_limit() == 0
            no_start_or_stop(b)
        finally:
            j.db.execute("DROP TRIGGER cf01_resume_commit")
            j.db.create_function("cf01_resume_commit_reached", 0, None)
        result = await native.resume()
        assert result["recovery_acknowledged"] is True and j.get("trial_phase") == "active"
        assert native.introduction_lineage == native.acknowledged_lineage == scope.lineage
        assert (await healthy(c))["station_ceiling_a"] == 32
        no_start_or_stop(b)


# Intent: matching Boot supersedes stale proof and failure, then the current Boot can recover after its cloud returns.
# Why: only the current Boot can impose its offline hold or prove that the saved app setting can resume.
async def test_matching_boot_fences_proof_and_failed_attempt_without_erasing_origin(
    tmp_path, tls, unused_tcp_port, monkeypatch
):
    async with pending_introduction(
        tmp_path, tls, unused_tcp_port, monkeypatch, native_peer=OneNativeResponseGate
    ) as b:
        c, g, j, peer = b.controller, b.session.gateway, b.session.journal, b.rig.peer
        # Change the environment's backoff after setup; the accepted A connection will adopt it when closed.
        monkeypatch.setattr(
            b.session.runtime, "settings", replace(b.session.runtime.settings, retry_min=5, retry_max=5)
        )
        native, initial, intent = c.native, current_scope(g), c.requests.intent()
        assert initial is not None
        acknowledged = native.acknowledged_lineage
        peer.arm = True
        proof = asyncio.create_task(native.resume())
        try:
            await asyncio.wait_for(peer.entered.wait(), 3)
            assert peer.gated_request["action"] == "getConfig"
            async with failed_cloud(b) as failed:
                assert failed == initial
                faults = b.session.runtime.status.vendor_fault.count
                assert (await b.rig.bench.call(b.charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
                current = current_scope(g)
                assert current is not None and current.boot != initial.boot and current.lineage == initial.lineage
                assert native.introduction_lineage == current.lineage
                assert native.acknowledged_lineage == acknowledged != current.lineage
                status = await c.tick()
                assert status["state"] == "waiting_for_cloud", status
                # This assertion rejects an accidental later B failure before the negative-control tick.
                assert b.session.runtime.status.vendor_failed_scope == initial
                assert c.requests.intent() == intent and j.get("trial_phase") == "recovering"
                assert b.rig.bench.firmware.effective_limit() == 0
                peer.release.set()
                with pytest.raises(AuditSupersededError):
                    await asyncio.wait_for(proof, 3)
                assert native.acknowledged_lineage == acknowledged
                assert native.introduction_lineage == current.lineage and j.get("trial_phase") == "recovering"
                # A real next transport attempt against the still-reserved endpoint must capture B for itself.
                await until(
                    lambda: (
                        b.session.runtime.status.vendor_fault.count > faults
                        and b.session.runtime.status.vendor_failed_scope == current
                    ),
                    timeout=12,
                )
                status = await c.tick()
                paused = c.requests.intent()
                assert paused.mode == "paused" and paused.revision == intent.revision + 1
                assert j.get(OFFLINE_PAUSE) == paused.revision
                assert current_scope(g) == current and b.rig.bench.firmware.effective_limit() == 0
                assert not j.get("managed_control_fault"), status
                no_start_or_stop(b)
        finally:
            peer.release.set()
            if not proof.done():
                proof.cancel()
            await asyncio.gather(proof, return_exceptions=True)
        await until(
            lambda: (
                b.session.runtime.status.vendor_connected
                and g.vendor_registration == "Accepted"
                and j.next_delivery() is None
                and not g.vendor._pending
            ),
            timeout=12,
        )
        async with asyncio.timeout(5):
            while (status := await healthy(c))["desired_control"] != "app":
                await asyncio.sleep(0.02)
        assert c.requests.intent() == paused.next("app") and status["station_ceiling_a"] == 32
        assert j.get(OFFLINE_PAUSE) is None and current_scope(g) == current
        assert native.acknowledged_lineage == current.lineage
        assert b.rig.bench.firmware.effective_limit() == 32
        no_start_or_stop(b)
