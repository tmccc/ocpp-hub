"""A real cloud decision can replace its cached denial for an approved native timer tag."""

import json

import pytest
from ocpp_hub_core.frames import build_call_error, build_call_result
from ocpp_hub_core.prototype.managed import ManagedController

from tests.test_managed_proxy import healthy, introduced, managed_bench, managed_session, starts
from tests.test_prototype import START
from tests.test_prototype_tls import tls as tls
from tests.test_trial_session import Bench


class AuthorizationPeer:
    """Only correlated Authorize replies differ from the normal independent cloud."""

    # Keep the actual wire tokens so unrelated cloud delivery remains unchanged.
    def __init__(self, connection, outcome):
        self.connection, self.outcome = connection, outcome
        self.authorizations = []

    # Observe actual cloud requests instead of replacing the engine's authorisation decision.
    def __aiter__(self):
        # Forward every frame to the existing fake cloud implementation.
        async def frames():
            async for raw in self.connection:
                frame = json.loads(raw)
                if frame[0] == 2 and frame[2] == "Authorize":
                    self.authorizations.append(frame[1])
                yield raw

        return frames()

    # Return a fresh correlated decision, or independently lose/fail only its response.
    async def send(self, raw):
        frame = json.loads(raw)
        if frame[0] == 3 and frame[1] in self.authorizations:
            if self.outcome == "timeout":
                return
            if self.outcome == "callerror":
                raw = build_call_error(frame[1], "InternalError", "synthetic cloud refusal")
            else:
                raw = build_call_result(frame[1], {"idTagInfo": {"status": self.outcome}})
        await self.connection.send(raw)


# Intent: direct approved timer Starts refresh every cached denial using a real correlated cloud exchange.
# Why: an old denial must not stick forever, and a fresh denial or unavailable reply must still prevent Start.
@pytest.mark.parametrize("cached", ["Blocked", "Invalid", "Expired", "ConcurrentTx"])
@pytest.mark.parametrize("outcome", ["Accepted", "Blocked", "timeout", "callerror"])
async def test_cached_denial_uses_fresh_cloud_decision(tmp_path, tls, unused_tcp_port, monkeypatch, cached, outcome):
    peers = []
    actual = Bench.vendor

    # Change only the independent cloud transport and retain the real TLS gateway and transaction ledger.
    async def vendor(bench, connection):
        peer = AuthorizationPeer(connection, outcome)
        peers.append(peer)
        await actual(bench, peer)

    monkeypatch.setattr(Bench, "vendor", vendor)
    async with managed_bench(tmp_path, tls, unused_tcp_port, monkeypatch) as rig:
        session = managed_session(rig)
        await session.start()
        controller = ManagedController(session)
        rig.route_to_trial()
        async with rig.charger(session) as charger:
            await introduced(rig, session, charger, "cache-boot")
            await healthy(controller)
            session.gateway.vendor.timeout = 0.08
            session.gateway.auth_info.put("NATIVE_TIMER", {"status": cached})
            result = await rig.bench.call(charger, "cache-start", "StartTransaction", START | {"idTag": "NATIVE_TIMER"})
            assert len(peers[-1].authorizations) == 1
            if outcome == "Accepted":
                assert result[0] == 3 and result[2]["idTagInfo"]["status"] == "Accepted"
                assert session.journal.active() == result[2]["transactionId"]
            else:
                assert result[0] == 4 and session.journal.active() is None
                expected = outcome if outcome == "Blocked" else cached
                assert session.gateway.auth_info.stored("NATIVE_TIMER")["status"] == expected
            assert not starts(rig)
