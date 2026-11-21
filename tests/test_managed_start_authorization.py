"""Normal owner Start can renew aged approval through an actual guarded upstream TLS exchange."""

import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from functools import partial

import pytest
from ocpp_hub_core.frames import build_call_error, build_call_result
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests.charge_rig import actual_stop, owner_rig, positive_flow
from tests.current_rig import commissioned
from tests.managed_rig import healthy, starts
from tests.prototype_rig import BOOT
from tests.runtime_rig import until
from tests.test_managed_cached_authorization import AuthorizationPeer
from tests.tls_support import tls as tls
from tests.trial_peer import Bench


class RenewalPeer(AuthorizationPeer):
    """Gate only the independent cloud's real correlated reply after commissioning finishes."""

    # Existing setup traffic is unchanged; the test explicitly arms its next actual Authorize reply.
    def __init__(self, connection, outcome):
        super().__init__(connection, outcome)
        self.armed = False
        self.entered, self.release = asyncio.Event(), asyncio.Event()

    # Actual wire refusal, loss or fresh metadata is the oracle, never a replacement engine decision.
    async def send(self, raw):
        frame = json.loads(raw)
        if self.armed and frame[0] == 3 and frame[1] in self.authorizations:
            self.entered.set()
            await self.release.wait()
            if self.outcome == "timeout":
                return
            if self.outcome == "callerror":
                raw = build_call_error(frame[1], "InternalError", "synthetic authorization refusal")
            elif self.outcome == "malformed":
                raw = build_call_result(frame[1], {"unexpected": True})
            else:
                lifetime = timedelta(hours=-1 if self.outcome == "already_expired" else 1)
                info = {"status": "Blocked" if self.outcome == "Blocked" else "Accepted"}
                info["expiryDate"] = (datetime.now(UTC) + lifetime).isoformat()
                raw = build_call_result(frame[1], {"idTagInfo": info})
        await self.connection.send(raw)


# Preserve all real commissioning, native, journal and TLS behavior while changing only the independent peer reply.
@asynccontextmanager
async def renewal_rig(tmp_path, tls, port, monkeypatch, current, outcome="Accepted"):
    peers, original = [], Bench.vendor

    # Every actual replacement cloud connection gets its own correlation and gate state.
    async def vendor(bench, connection):
        peer = RenewalPeer(connection, outcome)
        peers.append(peer)
        await original(bench, peer)

    monkeypatch.setattr(Bench, "vendor", vendor)
    factory = owner_rig if current == 6 else partial(commissioned, current=32, owner_tag="APP_OWNER")
    async with factory(tmp_path, tls, port, monkeypatch) as test:
        g = test.session.gateway
        expired = {"status": "Accepted", "expiryDate": (datetime.now(UTC) - timedelta(hours=1)).isoformat()}
        for tag in g.policy.allowed_tags:
            g.auth_info.put(tag, expired)
        peer = peers[-1]
        peer.authorizations.clear()
        peer.armed = True
        try:
            yield test, peer, expired
        finally:
            peer.release.set()


# Intent: every expired configured tag can be renewed once online before any charger effect at6A or32A.
# Why: the old readiness gate made the owner unable to request the very approval needed to enable Start.
@pytest.mark.parametrize("current", [6, 32])
@pytest.mark.parametrize("initial", ["app", "paused"])
async def test_normal_start_renews_expired_approval_before_effects(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, initial
):
    async with renewal_rig(tmp_path, tls, unused_tcp_port, monkeypatch, current) as (test, peer, expired):
        c, g = test.controller, test.session.gateway
        if initial == "paused":
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            assert (await healthy(c))["local_hold_verified"]
        assert c.charging.authorization_status() == "renewal_required"
        assert c.charging.available(c.latest_native, normal=True)
        assert not c.charging.available(c.latest_native)
        first_call, intent = len(test.rig.bench.device_calls), c.requests.intent()
        identifier = uuid.uuid4().hex
        c.command(identifier, "start", intent.revision)
        await asyncio.wait_for(peer.entered.wait(), 2)
        assert len(peer.authorizations) == 1 and not starts(test.rig)
        c.command(identifier, "start", intent.revision)
        assert len(peer.authorizations) == 1 and not starts(test.rig)
        assert test.rig.bench.device_calls[first_call:] == []
        assert all(g.auth_info.stored(tag) == expired for tag in g.policy.allowed_tags)
        peer.release.set()
        await c.charging.settle()
        assert len(starts(test.rig)) == 1 and len(peer.authorizations) == 1
        local = await positive_flow(test)
        status = await healthy(c)
        assert status["charging_session_open"] and c.requests.receipt(identifier)["state"] == "completed"
        assert status["station_ceiling_a"] == current and status["power_w"] > 0
        c.command(identifier, "start", intent.revision)
        assert len(peer.authorizations) == len(starts(test.rig)) == 1
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert (await actual_stop(test, local))["local_hold_verified"]


# Intent: a fresh refusal, stale Accepted, lost reply or malformed reply never reaches any charger mutation.
# Why: online renewal is a request for actual permission, not a fallback that discards expiry or cloud denial.
@pytest.mark.parametrize("current", [6, 32])
@pytest.mark.parametrize("outcome", ["Blocked", "already_expired", "timeout", "callerror", "malformed"])
async def test_failed_renewal_is_a_no_effects_start_failure(
    tmp_path, tls, unused_tcp_port, monkeypatch, current, outcome
):
    async with renewal_rig(tmp_path, tls, unused_tcp_port, monkeypatch, current, outcome) as (test, peer, expired):
        c, g = test.controller, test.session.gateway
        g.vendor.timeout = 0.08
        before, first_call = c.requests.intent(), len(test.rig.bench.device_calls)
        identifier = uuid.uuid4().hex
        c.command(identifier, "start", before.revision)
        await asyncio.wait_for(peer.entered.wait(), 2)
        peer.release.set()
        await c.charging.settle()
        assert c.requests.receipt(identifier)["state"] == "failed"
        assert c.requests.intent() == before and not starts(test.rig)
        assert test.rig.bench.device_calls[first_call:] == []
        selected = sorted(g.policy.allowed_tags)[0]
        actual = g.auth_info.stored(selected)
        if outcome == "Blocked":
            assert actual["status"] == "Blocked" and c.charging.authorization_status() == "renewal_required"
        elif outcome == "already_expired":
            assert g.auth_info.get(selected)["status"] == "Expired" and actual != expired
        else:
            assert actual == expired
        assert test.rig.bench.firmware.effective_limit() == current
        assert (await healthy(c))["state"] == "healthy"


# Intent: explicit cached denials and offline aged approvals cannot silently use the renewal exception.
# Why: renewal must preserve revocation and existing offline authorization policy.
@pytest.mark.parametrize("cached", ["Blocked", "Invalid", "Expired", "ConcurrentTx", "offline"])
async def test_unrenewable_approval_refuses_admission(tmp_path, tls, unused_tcp_port, monkeypatch, cached):
    async with renewal_rig(tmp_path, tls, unused_tcp_port, monkeypatch, 6) as (test, peer, expired):
        c, g = test.controller, test.session.gateway
        if cached == "offline":
            test.session.runtime.status.vendor_connected = False
        else:
            for tag in g.policy.allowed_tags:
                g.auth_info.put(tag, {"status": cached})
        assert not c.charging.available(c.latest_native, normal=True)
        assert c.charging.authorization_status() == ("expired_cloud_unavailable" if cached == "offline" else "denied")
        with pytest.raises(RecoveryRequiredError):
            c.command(uuid.uuid4().hex, "start", c.requests.intent().revision)
        assert not peer.authorizations and not starts(test.rig)


# Intent: a newer Pause, Boot or tag decision blocks a late approval, while a real denial still belongs to its tag.
# Why: the settled tag and permission to use it for this Start are separate facts.
@pytest.mark.parametrize("superseding", ["pause", "pause-denial", "boot", "metadata", "vendor"])
async def test_renewal_reply_cannot_overtake_newer_authority(tmp_path, tls, unused_tcp_port, monkeypatch, superseding):
    outcome = "Blocked" if superseding == "pause-denial" else "Accepted"
    async with renewal_rig(tmp_path, tls, unused_tcp_port, monkeypatch, 32, outcome) as (test, peer, expired):
        c, g = test.controller, test.session.gateway
        identifier = uuid.uuid4().hex
        c.command(identifier, "start", c.requests.intent().revision)
        await asyncio.wait_for(peer.entered.wait(), 2)
        tag = sorted(g.policy.allowed_tags)[0]
        if superseding in ("pause", "pause-denial"):
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        elif superseding == "boot":
            before = c.authority()
            assert (await test.rig.bench.call(test.charger, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
            assert c.authority() != before
        elif superseding == "vendor":
            epoch = g._vendor_epoch
            await peer.connection.close()
            await until(lambda: g._vendor_epoch != epoch)
        else:
            g.auth_info.put(tag, {"status": "Blocked"})
        peer.release.set()
        await c.charging.settle()
        assert c.requests.receipt(identifier)["state"] == "failed" and not starts(test.rig)
        if superseding == "pause-denial":
            assert g.auth_info.stored(tag)["status"] == "Blocked"
        else:
            assert g.auth_info.stored(tag) == ({"status": "Blocked"} if superseding == "metadata" else expired)
        if superseding in ("pause", "pause-denial"):
            assert (await healthy(c))["local_hold_verified"]


# Intent: malformed optional approval metadata cannot prevent an accepted Pause from enforcing physical zero.
# Why: a diagnostic added to the pre-supervision status shape must not run before required restrictive work.
async def test_authorization_diagnostic_does_not_preempt_pending_pause(tmp_path, tls, unused_tcp_port, monkeypatch):
    async with renewal_rig(tmp_path, tls, unused_tcp_port, monkeypatch, 32) as (test, peer, expired):
        c, j = test.controller, test.session.journal
        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
        assert j.phase.read().active and test.rig.bench.firmware.effective_limit() == 32
        j.db.execute("UPDATE auth_info SET info='{' ")
        try:
            await c.tick()
        except ValueError:
            pass  # The damaged row remains invalid; physical Pause must precede its optional diagnostic read.
        assert test.rig.bench.firmware.effective_limit() == 0
        assert not peer.authorizations and not starts(test.rig)
