"""Ambiguous replies and storage faults at the physical dispatch boundary."""

import asyncio
from contextlib import asynccontextmanager

import pytest
from ocpp_hub_core import commands
from ocpp_hub_core.frames import build_call, build_call_error, build_call_result
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.security import TRANSPORT_LOG
from websockets.asyncio.server import serve

from tests.test_prototype_runtime import Bench, until
from tests.test_prototype_tls import admission as admission
from tests.test_prototype_tls import tls as tls


# Keep all effects and replies on actual independent peers, with a configurable charger result body.
@asynccontextmanager
async def connected(tmp_path, tls, admission):
    b = Bench(tmp_path / "journal.db", tls, admission)
    reply = {}
    async with serve(
        b.vendor, "127.0.0.1", 0, ssl=b.server_tls, subprotocols=["ocpp1.6"], logger=TRANSPORT_LOG
    ) as cloud:
        await b.start(cloud.sockets[0].getsockname()[1])
        try:
            async with b.charger() as charger:
                await b.boot(charger)
                await until(lambda: b.gateway.vendor_registration == "Accepted")
                calls = []

                # Answer from independent firmware state while controlling only the peer's wire reply.
                async def read():
                    async for raw in charger:
                        frame = decode(raw)
                        if frame[0] == 2:
                            calls.append(frame)
                            actual = b.firmware.answer(frame[2], frame[3])
                            body = reply.get(frame[2], actual)
                            if body == "CALLERROR":
                                await charger.send(
                                    build_call_error(frame[1], "InternalError", "physical outcome unknown")
                                )
                            else:
                                await charger.send(build_call_result(frame[1], body))

                reader = asyncio.create_task(read())

                # Wait for the actual correlated cloud reply rather than inspecting engine decisions.
                async def vendor(token, action, payload):
                    await b.connections[-1].send(build_call(token, action, payload))
                    await until(lambda: any(f[0] != 2 and f[1] == token for f in b.calls))
                    return next(f for f in b.calls if f[0] != 2 and f[1] == token)

                try:
                    yield b, calls, reply, vendor
                finally:
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        finally:
            await b.close()


# Intent: malformed RemoteStart results retain uncertainty after the charger received a real physical command.
# Why: lack of a recognized refusal cannot justify a second Start while the first may execute late.
@pytest.mark.parametrize("body", [{}, {"status": "Unknown"}, {"status": "Accepted", "data": ""}, "CALLERROR"])
async def test_malformed_remote_start_retains_dispatch(tmp_path, tls, admission, body):
    async with connected(tmp_path, tls, admission) as (b, calls, reply, vendor):
        reply["RemoteStartTransaction"] = body
        first = await vendor("ambiguous-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": "LOCAL_TEST"})
        if body == "CALLERROR":
            assert first[2:] == ["InternalError", "physical outcome unknown", {}]
        assert len([f for f in calls if f[2] == "RemoteStartTransaction"]) == 1
        second = await vendor("later-start", "RemoteStartTransaction", {"connectorId": 1, "idTag": "LOCAL_TEST"})
        physical_starts = len([f for f in calls if f[2] == "RemoteStartTransaction"])
        rows = [row[0] for row in b.store.db.execute("SELECT state FROM mutations WHERE origin='vendor'")]
        assert rows == ["uncertain"], {
            "states": rows,
            "reply": first,
            "second": second,
            "physical_starts": physical_starts,
            "grant": b.store.get("vendor_start_grant"),
        }
        assert b.store.get("vendor_start_grant")
        assert second[0] == 4
        assert physical_starts == 1


# Intent: SQL refusal during prepare never leaks a new vendor profile reservation or physical uncertainty.
# Why: all prepared-only ownership belongs to the same durable atomic write boundary.
async def test_unsent_sql_failure_drops_reservation(tmp_path, tls, admission):
    async with connected(tmp_path, tls, admission) as (b, calls, reply, vendor):
        b.store.db.execute(
            "CREATE TRIGGER fail_mutation BEFORE INSERT ON mutations BEGIN SELECT RAISE(ABORT,'test'); END"
        )
        body = commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 111)
        refused = await vendor("sql-refused", "SetChargingProfile", body)
        assert refused[0] == 4 and not calls
        assert not b.store.get("vendor_profiles") and not b.store.get("uncertain_vendor_command")
        assert not b.store.db.execute("SELECT 1 FROM mutations").fetchone()
        b.store.db.execute("DROP TRIGGER fail_mutation")
        assert (await vendor("after-sql-refusal", "SetChargingProfile", body))[2] == {"status": "Accepted"}


# Intent: a storage failure after a genuine profile ACK keeps global uncertainty and the accepted alias reserved.
# Why: physical acceptance must never be reported as durably owned when the ownership commit failed.
async def test_profile_metadata_commit_failure_keeps_uncertainty(tmp_path, tls, admission):
    async with connected(tmp_path, tls, admission) as (b, calls, reply, vendor):
        b.store.put("vendor_profile_transactions", {})
        b.store.db.execute(
            "CREATE TRIGGER fail_profile_metadata BEFORE INSERT ON meta "
            "WHEN NEW.key='vendor_profile_transactions' BEGIN SELECT RAISE(ABORT,'test'); END"
        )
        body = commands.set_charging_profile(1, commands.PURPOSE_TX_DEFAULT, 6, 111)
        result = await vendor("metadata-commit-failed", "SetChargingProfile", body)
        assert result[0] == 4
        assert [row[0] for row in b.store.db.execute("SELECT state FROM mutations WHERE origin='vendor'")] == [
            "uncertain"
        ]
        mapped = b.store.get("vendor_profiles")["111"]
        assert mapped in b.firmware.profiles
        before = len(calls)
        assert (await vendor("blocked-after-commit", "SetChargingProfile", body))[0] == 4
        assert len(calls) == before


# Intent: exact Start acceptance retains its single-use grant, while exact refusal permits a separately requested retry.
# Why: definite Rejected differs from an ambiguous reply, and Accepted cannot permit a second unresolved physical Start.
@pytest.mark.parametrize("status", ["Accepted", "Rejected"])
async def test_exact_remote_start_response_preserves_grant_boundary(tmp_path, tls, admission, status):
    async with connected(tmp_path, tls, admission) as (b, calls, reply, vendor):
        reply["RemoteStartTransaction"] = {"status": status}
        body = {"connectorId": 1, "idTag": "LOCAL_TEST"}
        assert (await vendor("first-start", "RemoteStartTransaction", body))[2] == {"status": status}
        assert bool(b.store.get("vendor_start_grant")) is (status == "Accepted")
        reply["RemoteStartTransaction"] = {"status": "Accepted"}
        result = await vendor("second-start", "RemoteStartTransaction", body)
        assert result[0] == (4 if status == "Accepted" else 3)
        assert len([f for f in calls if f[2] == "RemoteStartTransaction"]) == (1 if status == "Accepted" else 2)
        assert not b.store.db.execute("SELECT 1 FROM mutations WHERE state!='confirmed'").fetchone()
