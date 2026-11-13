"""Explicit native32 preparation retains a real managed Pause, owned zero and immutable admission records."""

from __future__ import annotations

import copy
import json
import ssl
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.prototype.managed_current import capability_value, managed_current_ceiling
from ocpp_hub_core.prototype.managed_current_prepare import prepare_current_capability
from ocpp_hub_core.prototype.rpc import UnavailableError
from ocpp_hub_core.prototype.security import SecurityError
from ocpp_hub_core.prototype.store import RecoveryRequiredError

from tests.test_managed_proxy import healthy, starts
from tests.test_managed_test_charge import ChargeRig, actual_stop, positive_flow, ready
from tests.test_prototype_runtime import until
from tests.test_prototype_tls import tls as tls


# Establish Pause through the actual command, TLS profile effect and independent native readback before yielding.
@asynccontextmanager
async def paused_ready(
    directory: Path,
    contexts: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    port: int,
    monkeypatch: pytest.MonkeyPatch,
    *,
    cli_route: bool = False,
) -> AsyncIterator[ChargeRig]:
    async with ready(directory, contexts, port, monkeypatch, cli_route=cli_route) as test:
        test.controller.command(uuid.uuid4().hex, "pause", 0)
        status = await healthy(test.controller)
        assert status["local_hold_verified"] and status["station_ceiling_a"] == 0
        assert test.rig.bench.firmware.effective_limit() == 0
        yield test


# The only injected native writer changes the independently held USER scalar, never journal authority or OCPP state.
def native_writer(test: ChargeRig, writes: list[dict[str, str]]) -> Callable[[], Awaitable[None]]:
    # Model only the authorized native USER scalar and its runtime readback, leaving the independent zero intact.
    async def patch() -> None:
        writes.append({"AmpLmt": "32"})
        test.rig.peer.configuration.update(writes[-1])
        test.rig.peer.runtime_user = 320

    return patch


# Preserve every admission binding and historical policy rather than replacing the original six-ampere specification.
def immutable_metadata(test: ChargeRig) -> dict[str, Any]:
    return {
        key: copy.deepcopy(test.session.journal.get(key))
        for key in (
            "native_bounded_handover",
            "trial_binding",
            "trial_allocation",
            "next_transaction",
            "managed_mode",
            "managed_control_intent",
            "native_history_policy",
            "native_history_policy_acknowledgement",
        )
    }


# Intent: software startup and a verified Pause do not grant adjustable current before an explicit preparation call.
# Why: the existing installation must retain USER6 and its old authority boundary until the owner prepares USER32.
async def test_default_managed_pause_does_not_acquire_current_capability(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with paused_ready(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        assert managed_current_ceiling(test.session.journal, test.rig.spec) == 6
        assert test.session.journal.get("managed_current_capability") is None
        assert test.session.journal.get("managed_current_prepare_intent") is None
        assert test.rig.peer.configuration["AmpLmt"] == "6" and test.rig.peer.runtime_user == 60
        assert test.session.gateway.policy.max_current_a == 6 and not starts(test.rig)


# Intent: explicit USER32 preparation occurs exactly once while real closed zero protects the connected car.
# Why: permission for a higher future ceiling cannot alter the old admission, history, intent or actual output.
async def test_explicit_preparation_changes_only_user_limit_and_binds_capability(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with paused_ready(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        journal = test.session.journal
        before = immutable_metadata(test)
        configuration = copy.deepcopy(test.rig.peer.configuration)
        device_calls = copy.deepcopy(test.rig.bench.device_calls)
        writes: list[dict[str, str]] = []
        result = await prepare_current_capability(
            journal,
            test.session.book,
            test.rig.spec,
            test.rig.prepared.observer.observe_handover,
            native_writer(test, writes),
        )
        assert result == {
            "prepared": True,
            "native_ceiling_a": 32,
            "already_prepared": False,
            "charging_started": False,
        }
        assert writes == [{"AmpLmt": "32"}]
        assert test.rig.peer.configuration == configuration | {"AmpLmt": "32"}
        assert test.rig.peer.runtime_user == test.rig.peer.hardware == 320
        assert journal.get("managed_current_capability") == capability_value(journal, test.rig.spec)
        assert managed_current_ceiling(journal, test.rig.spec) == 32
        assert journal.get("managed_current_prepare_intent")["stage"] == "complete"
        assert immutable_metadata(test) == before
        assert test.rig.bench.device_calls == device_calls and not starts(test.rig)
        assert test.rig.bench.firmware.effective_limit() == 0 and journal.active() is None
        assert journal.get("control")["inhibit"] and journal.get("control")["owner"] == "local"


# Intent: a lost reply after actual USER32 is reconciled by another read and never repeats the native write.
# Why: neither process loss nor an exception before capability commit proves that the original write did not apply.
async def test_applied_user32_with_lost_reply_reconciles_without_another_patch(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with paused_ready(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        journal = test.session.journal
        before = immutable_metadata(test)
        writes: list[dict[str, str]] = []
        actual = native_writer(test, writes)

        # Preserve the independently applied physical USER32 effect while withholding its successful acknowledgement.
        async def lost_reply() -> None:
            await actual()
            raise UnavailableError("synthetic loss after actual native USER32")

        with pytest.raises(UnavailableError):
            await prepare_current_capability(
                journal, test.session.book, test.rig.spec, test.rig.prepared.observer.observe_handover, lost_reply
            )
        assert journal.get("managed_current_capability") is None
        assert journal.get("managed_current_prepare_intent")["stage"] == "native_write_intent"
        assert test.rig.peer.runtime_user == 320 and test.rig.bench.firmware.effective_limit() == 0
        assert managed_current_ceiling(journal, test.rig.spec) == 6
        result = await prepare_current_capability(
            journal, test.session.book, test.rig.spec, test.rig.prepared.observer.observe_handover, actual
        )
        assert result["prepared"] and not result["charging_started"]
        again = await prepare_current_capability(
            journal, test.session.book, test.rig.spec, test.rig.prepared.observer.observe_handover, actual
        )
        assert again["already_prepared"] and writes == [{"AmpLmt": "32"}]
        assert immutable_metadata(test) == before and not starts(test.rig)
        assert test.rig.bench.firmware.effective_limit() == 0


# Add only the specified unresolved durable condition, independently from the valid native zero observation.
def unresolved(test: ChargeRig, fault: str) -> None:
    journal = test.session.journal
    if fault == "unpaused":
        journal.put("managed_control_intent", journal.get("managed_control_intent") | {"mode": "app"})
    elif fault == "control-uncertain":
        journal.put("control", journal.get("control") | {"uncertain": True})
    elif fault == "start-pending":
        journal.put("control", journal.get("control") | {"start_pending": True})
    elif fault == "mutation":
        journal.mutation_intent("vendor", "DataTransfer", {"unresolved": True})
    elif fault == "outbox":
        journal.enqueue("Heartbeat", {})
    elif fault == "vendor-start":
        journal.put("vendor_start_grant", {"local": None, "token": "unresolved"})
    elif fault in ("current-pending", "current-uncertain"):
        journal.put("managed_current_change", {"state": fault.removeprefix("current-")})
    elif fault == "boot-changed":
        journal.put("boot", journal.get("boot") | {"firmwareVersion": "uncommissioned"})
    elif fault == "binding-changed":
        journal.put("trial_binding", journal.get("trial_binding") | {"configuration_digest": "0" * 64})
    elif fault == "confirmed-receipt-mismatch":
        receipt = journal.db.execute("SELECT confirmed FROM trial_resources WHERE key='profile:7001'").fetchone()
        changed = json.loads(receipt[0]) | {"limit": 1}
        journal.db.execute("UPDATE trial_resources SET confirmed=? WHERE key='profile:7001'", (json.dumps(changed),))
    elif fault == "test-pending":
        journal.put("managed_test_charge", {"state": "armed"})
    elif fault in ("request-accepted", "request-running"):
        journal.db.execute(
            "INSERT INTO managed_requests VALUES(?,?,?,?,?)",
            (uuid.uuid4().hex, "app_control", 1, fault.removeprefix("request-"), "{}"),
        )
    elif fault == "missing-owned-zero":
        test.rig.bench.firmware.profiles.pop(7001)
    elif fault == "configuration":
        test.rig.peer.configuration["untouched"] = "external change"
    elif fault == "native32-without-intent":
        test.rig.peer.configuration["AmpLmt"], test.rig.peer.runtime_user = "32", 320
    else:
        raise AssertionError(fault)


# Intent: incomplete authority, pending work, missing zero and changed native settings block before USER32 write.
# Why: the preparation path can acknowledge only its narrowly verified paused installation, never other debt.
@pytest.mark.parametrize(
    "fault",
    [
        "unpaused",
        "control-uncertain",
        "start-pending",
        "mutation",
        "outbox",
        "vendor-start",
        "current-pending",
        "current-uncertain",
        "boot-changed",
        "binding-changed",
        "confirmed-receipt-mismatch",
        "test-pending",
        "request-accepted",
        "request-running",
        "missing-owned-zero",
        "configuration",
        "native32-without-intent",
    ],
)
async def test_unresolved_or_unpreserved_preparation_is_refused_before_native_write(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    async with paused_ready(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        if fault == "outbox":
            await test.session.runtime.stop_vendor()
        unresolved(test, fault)
        writes: list[dict[str, str]] = []
        with pytest.raises((RecoveryRequiredError, SecurityError)):
            await prepare_current_capability(
                test.session.journal,
                test.session.book,
                test.rig.spec,
                test.rig.prepared.observer.observe_handover,
                native_writer(test, writes),
            )
        assert not writes and test.session.journal.get("managed_current_capability") is None
        assert test.session.journal.get("managed_current_prepare_intent") is None
        assert not starts(test.rig)


# Intent: a real active session remains open after a Stop ACK and forbids preparation until actual StopTransaction.
# Why: actual zero output alone cannot manufacture closure or justify changing the native user limit.
async def test_active_session_blocks_preparation_until_real_stop_is_observed(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with ready(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        local = await positive_flow(test, "daily")
        test.controller.command(uuid.uuid4().hex, "pause", 0)
        await until(lambda: test.rig.bench.remote_stop.is_set())
        writes: list[dict[str, str]] = []
        try:
            with pytest.raises((RecoveryRequiredError, SecurityError)):
                await prepare_current_capability(
                    test.session.journal,
                    test.session.book,
                    test.rig.spec,
                    test.rig.prepared.observer.observe_handover,
                    native_writer(test, writes),
                )
            assert not writes and test.session.journal.active() == local
            assert test.session.journal.get("managed_current_capability") is None
        finally:
            assert (await actual_stop(test, local))["local_hold_verified"]


# Intent: an old uncertain test receipt survives preparation when real closure/zero and all current debt are proved.
# Why: a historical acknowledgement gap is not an active permission and must not require deleting old receipts.
async def test_historical_uncertain_test_receipt_is_preserved_without_blocking_preparation(
    tmp_path: Path,
    tls: tuple[ssl.SSLContext, ssl.SSLContext, Path],
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with paused_ready(tmp_path, tls, unused_tcp_port, monkeypatch) as test:
        identifier = uuid.uuid4().hex
        journal = test.session.journal
        journal.db.execute(
            "INSERT INTO managed_requests VALUES(?,?,?,'uncertain',?)", (identifier, "start_test", 0, "{}")
        )
        receipt = tuple(journal.db.execute("SELECT * FROM managed_requests WHERE id=?", (identifier,)).fetchone())
        writes: list[dict[str, str]] = []
        assert (
            await prepare_current_capability(
                journal,
                test.session.book,
                test.rig.spec,
                test.rig.prepared.observer.observe_handover,
                native_writer(test, writes),
            )
        )["prepared"]
        assert (
            tuple(journal.db.execute("SELECT * FROM managed_requests WHERE id=?", (identifier,)).fetchone()) == receipt
        )
        assert writes == [{"AmpLmt": "32"}] and not starts(test.rig)
