"""Long vendor outages retain the real legacy link and its queued charger traffic."""

import asyncio
import math

import pytest
from ocpp_hub_core.mirror_model import MirrorSettings, MirrorSettingsError

from tests.fake_vendor import FakeVendor, eventually


# Intent: retry and timeout settings reject nonfinite inputs before an asynchronous retry starts.
# Why: finite exponent saturation cannot give meaningful retry guarantees for NaN or infinite delays.
@pytest.mark.parametrize("name", ["reply_timeout_s", "connect_timeout_s", "backoff_min_s", "backoff_max_s"])
@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_nonfinite_delays_are_rejected(name, value):
    with pytest.raises(MirrorSettingsError, match="finite and positive"):
        MirrorSettings("ws://127.0.0.1/ocpp", **{name: value})


# Intent: capped backoff remains finite for arbitrary attempts and preserves the ordinary early sequence.
# Why: applying min after an overflowing exponent kills the link before a recovered vendor can receive it.
async def test_backoff_saturates_before_overflow(start_rig):
    rig = await start_rig(backoff_min_s=1.0, backoff_max_s=30.0)
    charger = await rig.hub.charger()
    await charger.boot()
    await eventually(lambda: bool(rig.mirror._links))
    link = rig.mirror._links["TEST0001"]
    assert [link._backoff(i) for i in (0, 1, 2, 3, 4, 5, 6)] == [1, 1, 2, 4, 8, 16, 30]
    assert [link._backoff(i) for i in (1024, 1025, 10**100)] == [30, 30, 30]
    link._retry_floor_s = 90
    assert link._backoff(10**100) == 90
    assert link._backoff(10**100) == 30


# Intent: even the smallest positive delay reaches its cap without overflowing or losing early doubling.
# Why: a fixed exponent cutoff or a maximum/minimum ratio breaks supported subnormal retry settings.
@pytest.mark.parametrize("minimum,maximum", [(math.ulp(0.0), 1.0), (1e-9, 1e-9), (0.5, 1.0)])
async def test_backoff_extreme_positive_ranges(start_rig, minimum, maximum):
    rig = await start_rig(backoff_min_s=minimum, backoff_max_s=maximum)
    charger = await rig.hub.charger()
    await charger.boot()
    await eventually(lambda: bool(rig.mirror._links))
    link = rig.mirror._links["TEST0001"]
    assert link._backoff(1) == minimum
    assert link._backoff(2) == min(maximum, 2 * minimum)
    assert link._backoff(10**100) == maximum


# Intent: more than1024 real failed websocket connections still recover on the same charger link.
# Why: arithmetic checks alone do not prove that the run loop retains and later forwards queued Boot/Status.
async def test_vendor_recovery_after_more_than1024_failures(start_hub, start_mirror):
    refused = 0

    # Close the independent TCP peer before any websocket handshake can succeed.
    async def refuse(reader, writer):
        nonlocal refused
        refused += 1
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(refuse, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    vendor = FakeVendor()
    try:
        mirror = await start_mirror(f"ws://127.0.0.1:{port}/ocpp", backoff_min_s=1e-9, backoff_max_s=1e-9)
        hub = await start_hub(mirror)
        charger = await hub.charger()
        await charger.boot()
        await eventually(lambda: refused >= 1030, timeout=20)
        link = mirror._links["TEST0001"]
        assert link._task is not None and not link._task.done()
        server.close()
        await server.wait_closed()
        vendor._port = port
        await vendor.start()
        await eventually(lambda: len(vendor.calls_of("BootNotification")) == 1)
        await charger.call("StatusNotification", {"connectorId": 1, "status": "Available", "errorCode": "NoError"})
        await eventually(lambda: len(vendor.calls_of("StatusNotification")) == 1)
        assert mirror._links["TEST0001"] is link and not link._task.done()
        assert (await charger.call("Heartbeat", {}))[0] == 3
    finally:
        server.close()
        await server.wait_closed()
        await vendor.stop()
