"""Routine cloud history cannot consume transaction admission and closure reservations."""

import ssl
from pathlib import Path

import pytest
from ocpp_hub_core.frames import build_call
from ocpp_hub_core.prototype.gateway import Gateway
from ocpp_hub_core.prototype.security import TRANSPORT_LOG, Admission, offline_server
from ocpp_hub_core.prototype.store import Journal
from websockets.asyncio.client import connect

from tests.prototype_firmware import Firmware
from tests.test_prototype import BOOT, START
from tests.test_prototype_tls import admission as admission
from tests.test_prototype_tls import charger_reply
from tests.test_prototype_tls import tls as tls

METER = {"connectorId": 1, "meterValue": [{"timestamp": "2026-01-01T00:01:00Z", "sampledValue": [{"value": "1001"}]}]}


# Intent: full routine queues still accept one Start and its Stop, retain all bytes/order, and enforce both budgets.
# Why: bounded storage must reject new work before ACK without stranding a transaction already accepted locally.
@pytest.mark.parametrize("capacity", [2, 10000])
async def test_real_tls_lifecycle_slots_survive_full_observation_queue(
    tmp_path: Path, tls: tuple[ssl.SSLContext, ssl.SSLContext, Path], admission: Admission, capacity: int
) -> None:
    server_tls, client_tls, _ = tls
    journal, firmware = Journal(tmp_path / "journal.db", admission.identity, capacity=capacity), Firmware()
    gateway = Gateway(journal, timeout=1)

    # TLS admission precedes every frame reaching the unmodified production gateway.
    async def charger_handler(connection):
        gateway.attach_charger(connection.send)
        async for raw in connection:
            await gateway.receive("charger", raw)

    try:
        async with offline_server(charger_handler, admission, server_tls) as server:
            async with connect(
                f"wss://127.0.0.1:{server.sockets[0].getsockname()[1]}/charger/{admission.identity}",
                ssl=client_tls,
                additional_headers=[("Authorization", admission.authorization)],
                subprotocols=["ocpp1.6"],
                logger=TRANSPORT_LOG,
                proxy=None,
            ) as charger:
                await charger.send(build_call("boot", "BootNotification", BOOT))
                assert (await charger_reply(charger, "boot", firmware))[2]["status"] == "Accepted"
                with journal.atomic():
                    for _ in range(capacity):
                        journal.enqueue("MeterValues", METER)
                preserved = [tuple(row) for row in journal.db.execute("SELECT * FROM outbox ORDER BY seq")]
                await charger.send(build_call("extra-meter", "MeterValues", METER))
                assert (await charger_reply(charger, "extra-meter", firmware))[:3] == [
                    4,
                    "extra-meter",
                    "InternalError",
                ]
                await charger.send(build_call("start-1", "StartTransaction", START))
                started = await charger_reply(charger, "start-1", firmware)
                assert started[0] == 3 and started[2]["transactionId"] == 1001
                stop = {"transactionId": 1001, "meterStop": 1100, "timestamp": "2026-01-01T00:02:00Z"}
                await charger.send(build_call("stop-1", "StopTransaction", stop))
                assert (await charger_reply(charger, "stop-1", firmware))[0] == 3
                assert journal.active() is None
                rows = [tuple(row) for row in journal.db.execute("SELECT * FROM outbox ORDER BY seq")]
                assert rows[:capacity] == preserved and len(rows) == capacity + 2
                await charger.send(build_call("stop-retry", "StopTransaction", stop))
                assert (await charger_reply(charger, "stop-retry", firmware))[0] == 3
                assert [tuple(row) for row in journal.db.execute("SELECT * FROM outbox ORDER BY seq")] == rows
                if capacity == 2:
                    counter = journal.get("next_transaction")
                    later = START | {"timestamp": "2026-01-02T00:00:00Z"}
                    await charger.send(build_call("start-full", "StartTransaction", later))
                    assert (await charger_reply(charger, "start-full", firmware))[:3] == [
                        4,
                        "start-full",
                        "InternalError",
                    ]
                    assert journal.active() is None and journal.get("next_transaction") == counter
    finally:
        await gateway.close()
        journal.close()
