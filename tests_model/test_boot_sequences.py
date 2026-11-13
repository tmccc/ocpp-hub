"""Actual Boot, Pause and independent reply/native-recovery orders preserve fresh restrictive proof."""

import asyncio
import copy
import os
import uuid
from contextlib import AsyncExitStack

import pytest
from ocpp_hub_core.prototype.gateway import decode

from tests import conftest as _engine  # noqa: F401 - initialize the standalone engine before importing it.
from tests import test_native_bounded_session as native_bench
from tests.test_managed_introduction_retry import InterruptedNativePeer
from tests.test_managed_proxy import healthy, starts
from tests.test_managed_start_refusal_native32 import commissioned
from tests.test_managed_test_charge import ChargeRig
from tests.test_prototype import BOOT
from tests.test_prototype_runtime import until
from tests_model.catalog import input_digest
from tests_model.control_cases import boot_reply_candidates, boot_retry_candidates
from tests_model.control_helpers import ordinary_recovery, station_limit
from tests_model.test_schedule_sequences import listener_port, tls_for_case
from tests_model.test_schedule_sequences import model_tls as model_tls


# Explicit development selection never changes the registry or the full qualification's declared denominator.
def cases(values):
    return values[::8] if os.environ.get("MODEL_SMOKE") else values


# A fresh full zero/composite sequence must follow the latest Boot, rather than borrowing the delayed old proof.
def fresh_zero(calls):
    zero = next(i for i, frame in enumerate(calls) if station_limit(frame, 0))
    assert any(frame[2] == "GetCompositeSchedule" for frame in calls[zero + 1 :])


# Drive either real reply withholding or a real HTTP503 prefix, then run every declared owner/Boot order.
async def sequence(tmp_path, model_tls, monkeypatch, record_property, case, family):
    monkeypatch.setattr(native_bench, "NativeVehiclePeer", InterruptedNativePeer)
    events, suffix_complete = [], False
    async with commissioned(
        tmp_path, tls_for_case(tmp_path, model_tls), listener_port(), monkeypatch, case.selected
    ) as b:
        rig, c, s = b.rig, b.controller, b.session
        plan, config = copy.deepcopy(rig.bench.firmware.plans), copy.deepcopy(rig.peer.configuration)
        if case.intent == "paused":
            c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
            await healthy(c)
        await b.charger.close()
        await until(lambda: not s.runtime.status.charger_connected)
        if case.boundary == "native503":
            rig.peer.unavailable = True
        async with AsyncExitStack() as stack:
            sock = await stack.enter_async_context(rig.charger(s))
            send, held, entered, forwarded = sock.send, [], asyncio.Event(), []

            # Retain actual correlated peer response delivery independently from the controller's cached readiness.
            async def forward(raw):
                frame = decode(raw)
                call = next((row for row in rig.bench.device_calls if row[1] == frame[1]), None)
                await send(raw)
                if frame[0] == 3 and call is not None and frame[2].get("status") == "Accepted":
                    forwarded.append(call)

            # Delay one real correlated reply after the independently modelled device already applied its zero.
            async def gate(raw):
                frame = decode(raw)
                call = next((row for row in rig.bench.device_calls if row[1] == frame[1]), None)
                match = call is not None and (
                    case.boundary == "profile"
                    and station_limit(call, 0)
                    or case.boundary == "composite"
                    and call[2] == "GetCompositeSchedule"
                )
                if frame[0] == 3 and match and not held:
                    held.append(raw)
                    entered.set()
                else:
                    await forward(raw)

            monkeypatch.setattr(sock, "send", gate)
            after_boot = len(rig.bench.device_calls)
            after_boot_replies, reusable_zero = 0, False
            await rig.bench.call(sock, uuid.uuid4().hex, "BootNotification", BOOT)
            if case.boundary == "native503":
                async with asyncio.timeout(3):
                    assert (await rig.peer.failed_reads.get())["action"] == "getConfig"
            else:
                await asyncio.wait_for(entered.wait(), 3)
            try:
                for event in case.events:
                    if event == "reply":
                        await forward(held[0])
                    elif event == "boot":
                        after_boot = len(rig.bench.device_calls)
                        after_boot_replies = len(forwarded)
                        reusable_zero = any(station_limit(row, 0) for row in forwarded) and any(
                            row[2] == "GetCompositeSchedule" for row in forwarded
                        )
                        assert (await rig.bench.call(sock, uuid.uuid4().hex, "BootNotification", BOOT))[0] == 3
                    elif event == "pause":
                        c.command(uuid.uuid4().hex, "pause", c.requests.intent().revision)
                    else:
                        rig.peer.unavailable = False
                    await asyncio.sleep(0)
                    events.append(event)
                    assert not starts(rig) and 0 <= rig.bench.firmware.effective_limit() <= case.selected
                assert (await healthy(c))["local_hold_verified"]
                if not reusable_zero:
                    fresh_zero(rig.bench.device_calls[after_boot:])
                    fresh_zero(forwarded[after_boot_replies:])
                await ordinary_recovery(ChargeRig(rig, s, c, sock), case.selected, case.demand)
                assert rig.bench.firmware.plans == plan and rig.peer.configuration == config
                assert events == list(case.events)
                suffix_complete = True
            finally:
                rig.peer.unavailable = False
                if held:
                    # A duplicate real reply is harmless and ensures no independent gate survives failed assertions.
                    await send(held[0])
                record_property("model_case", case.identifier)
                record_property("model_family", family)
                record_property("model_input_sha256", input_digest(family, case))
                record_property("completed_events", events)
                record_property("suffix_complete", suffix_complete)
                record_property("boundary_entered", bool(held) or case.boundary == "native503")


# Intent: all delayed old-proof reply/Boot/Pause orders require new proof and recover two ordinary sessions.
# Why: an identical Boot can revoke an in-flight proof without changing hardware or authority intent.
@pytest.mark.parametrize("case", cases(boot_reply_candidates()), ids=lambda case: case.identifier)
async def test_boot_reply_sequences(tmp_path, model_tls, monkeypatch, record_property, case):
    await sequence(tmp_path, model_tls, monkeypatch, record_property, case, "boot_reply")


# Intent: all real outage/heal/Boot/Pause orders eventually recover with the selected limit and fresh zero proof.
# Why: temporary HTTP loss must neither grant positive authority nor strand ordinary introduction forever.
@pytest.mark.parametrize("case", cases(boot_retry_candidates()), ids=lambda case: case.identifier)
async def test_boot_retry_sequences(tmp_path, model_tls, monkeypatch, record_property, case):
    await sequence(tmp_path, model_tls, monkeypatch, record_property, case, "boot_retry")
