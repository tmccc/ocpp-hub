"""Expiry, immutable lifecycle replies and nested authorization against the actual gateway."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from ocpp_hub_core.frames import build_call, build_call_result
from ocpp_hub_core.prototype.auth_info import InfoBook, tag_key, validate_info
from ocpp_hub_core.prototype.gateway import decode
from ocpp_hub_core.prototype.policy import DeniedError, Policy
from ocpp_hub_core.prototype.store import Journal, RecoveryRequiredError

from tests.test_prototype import START, Rig


# Produce fresh actual-looking metadata without treating fixture-shifted 2030 dates as the test clock.
def info(**changes: Any) -> dict[str, Any]:
    return {
        "status": "Accepted",
        "expiryDate": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        "parentIdTag": "PARENT",
        **changes,
    }


class StrictRig(Rig):
    """Independent peers can ask nested questions before acknowledging a RemoteStart."""

    # Keep all gateway/journal behavior real while controlling only the synthetic cloud's actual replies.
    def __init__(self, path: Path) -> None:
        super().__init__(path, Policy(strict_cloud_authorization=True, trust_vendor_start_tags=True))
        self.actual_info = info()
        self.start_info: dict[str, Any] | None = None
        self.nested = False
        self.nested_replies: dict[str, asyncio.Future[list[Any]]] = {}
        self.authorization_seen = asyncio.Event()
        self.authorization_release: asyncio.Event | None = None

    # Return correlated metadata, retaining ordinary Boot and lifecycle behavior from the independent peer.
    async def vendor_send(self, raw: str) -> None:
        frame = decode(raw)
        if frame[0] == 2 and frame[2] == "Authorize":
            self.vendor_frames.append(frame)
            self.authorization_seen.set()
            if self.authorization_release is not None:
                await self.authorization_release.wait()
            await self.gateway.receive("vendor", build_call_result(frame[1], {"idTagInfo": self.actual_info}))
        elif frame[0] == 2 and frame[2] == "StartTransaction" and self.start_info is not None:
            self.vendor_frames.append(frame)
            self.next_vendor += 1
            await self.gateway.receive(
                "vendor", build_call_result(frame[1], {"transactionId": self.next_vendor, "idTagInfo": self.start_info})
            )
        else:
            await super().vendor_send(raw)

    # Force the difficult ordering: nested Authorize and Start both arrive before RemoteStart's result.
    async def charger_send(self, raw: str) -> None:
        frame = decode(raw)
        if frame[0] != 2 and frame[1] in self.nested_replies:
            self.charger_frames.append(frame)
            self.nested_replies[frame[1]].set_result(frame)
            return
        if frame[0] == 2 and frame[2] == "RemoteStartTransaction" and self.nested:
            self.charger_frames.append(frame)
            for token, action, payload in (
                ("nested-auth", "Authorize", {"idTag": frame[3]["idTag"]}),
                ("nested-start", "StartTransaction", {**START, "idTag": frame[3]["idTag"]}),
            ):
                future: asyncio.Future[list[Any]] = asyncio.get_running_loop().create_future()
                self.nested_replies[token] = future
                await self.gateway.receive("charger", build_call(token, action, payload))
                answer = await asyncio.wait_for(future, 0.3)
                assert answer[0] == 3 and answer[2]["idTagInfo"] == self.actual_info
            await self.gateway.receive("charger", build_call_result(frame[1], {"status": "Accepted"}))
        else:
            await super().charger_send(raw)


# Intent: actual cloud metadata is available before a nested charger authorization, without a gate cycle.
# Why: waiting for the cloud from inside the charger gate can deadlock RemoteStart against its own Authorize.
async def test_preflight_authorization_before_nested_start(tmp_path: Path) -> None:
    rig = StrictRig(tmp_path / "state.db")
    await rig.boot()
    await rig.cloud()
    rig.nested = True
    result = await rig.vendor("remote", "RemoteStartTransaction", {"connectorId": 1, "idTag": "DYNAMIC"})
    assert result[2]["status"] == "Accepted"
    assert rig.store.active() == 1001
    assert len([f for f in rig.vendor_frames if f[0] == 2 and f[2] == "Authorize"]) == 1
    assert not rig.gateway.failures
    await rig.close()


# Intent: expiry/revocation changes new permission but never rewrites the original accepted lifecycle reply.
# Why: retransmitted facts must retain their exact local id, parent metadata and initial expiry across restart.
async def test_original_start_metadata_survives_expiry_restart_and_retries(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    rig = StrictRig(path)
    await rig.boot()
    await rig.cloud()
    authorized = await rig.charger("auth", "Authorize", {"idTag": "local_test"})
    assert authorized[2] == {"idTagInfo": rig.actual_info}
    first = await rig.charger("start", "StartTransaction", START)
    expected = first[2]
    rig.gateway.auth_info.put("LOCAL_TEST", info(status="Blocked"))
    assert (await rig.charger("retry-new-id", "StartTransaction", START))[2] == expected
    await rig.close()
    rig = StrictRig(path)
    assert (await rig.charger("restart-retry", "StartTransaction", START))[2] == expected
    assert (await rig.charger("blocked-auth", "Authorize", {"idTag": "LOCAL_TEST"}))[2]["idTagInfo"][
        "status"
    ] == "Blocked"
    rig.gateway.auth_info.put("LOCAL_TEST", info(expiryDate=(datetime.now(UTC) - timedelta(seconds=1)).isoformat()))
    assert (await rig.charger("expired-auth", "Authorize", {"idTag": "LOCAL_TEST"}))[2]["idTagInfo"][
        "status"
    ] == "Expired"
    assert (await rig.charger("still-original", "StartTransaction", START))[2] == expected
    await rig.close()


# Intent: binding to an active session cannot override cloud expiry, and parent metadata grants no sibling permission.
# Why: a single-start grant must not become an indefinite tag/group allowlist.
async def test_bound_grant_expiry_parent_and_case_rules(tmp_path: Path) -> None:
    rig = StrictRig(tmp_path / "state.db")
    rig.gateway.auth_info.put("Dynamic", rig.actual_info)
    rig.gateway.authorisation.reserve("Dynamic")
    answer = await rig.charger("start", "StartTransaction", {**START, "idTag": "dynamic"})
    assert answer[2]["transactionId"] == 1001
    assert rig.gateway.authorisation.allows("DYNAMIC")
    assert (await rig.charger("sibling", "Authorize", {"idTag": "PARENT"}))[2] == {"idTagInfo": {"status": "Invalid"}}
    rig.gateway.auth_info.put("DYNAMIC", info(expiryDate=(datetime.now(UTC) - timedelta(seconds=1)).isoformat()))
    assert (await rig.charger("expired", "Authorize", {"idTag": "dynamic"}))[2]["idTagInfo"]["status"] == "Expired"
    await rig.close()


# Intent: a missing expiry is not invented; narrow dynamic trials reject it before forwarding a start.
# Why: an unbounded device-side authorization cache would outlive the proxy's bounded pending grant.
async def test_dynamic_start_requires_actual_future_expiry(tmp_path: Path) -> None:
    rig = StrictRig(tmp_path / "state.db")
    rig.actual_info = {"status": "Accepted"}
    await rig.boot()
    await rig.cloud()
    answer = await rig.vendor("remote", "RemoteStartTransaction", {"connectorId": 1, "idTag": "DYNAMIC"})
    assert answer[0] == 4
    assert not any(f[0] == 2 and f[2] == "RemoteStartTransaction" for f in rig.charger_frames)
    assert not rig.store.get("vendor_start_grant")
    await rig.close()


# Intent: local stop wins while upstream authorization is in progress.
# Why: an earlier cloud permission cannot release the hold after the stop has changed local authority.
async def test_stop_during_upstream_authorization(tmp_path: Path) -> None:
    rig = StrictRig(tmp_path / "state.db")
    await rig.boot()
    await rig.cloud()
    rig.authorization_release = asyncio.Event()
    remote = asyncio.create_task(rig.vendor("remote", "RemoteStartTransaction", {"connectorId": 1, "idTag": "DYNAMIC"}))
    await rig.authorization_seen.wait()
    await rig.gateway.stop()
    rig.authorization_release.set()
    assert (await remote)[0] == 4
    assert rig.firmware.effective_limit() == 0 and rig.store.get("control")["inhibit"]
    assert not any(f[0] == 2 and f[2] == "RemoteStartTransaction" for f in rig.charger_frames)
    await rig.close()


# Intent: Stop without a tag cannot update an arbitrary starting tag from optional upstream metadata.
# Why: a cloud Stop reply does not identify whose authorization cache entry it describes in that case.
async def test_stop_without_tag_does_not_guess_cache_subject(tmp_path: Path) -> None:
    rig = StrictRig(tmp_path / "state.db")
    rig.gateway.auth_info.put("LOCAL_TEST", rig.actual_info)
    await rig.boot()
    await rig.charger("start", "StartTransaction", START)
    await rig.cloud()
    before = rig.gateway.auth_info.get("LOCAL_TEST")
    rig.vendor_result = {"idTagInfo": info(status="Blocked")}
    payload = {"transactionId": 1001, "timestamp": START["timestamp"], "meterStop": 1000}
    assert (await rig.charger("stop", "StopTransaction", payload))[2] == {}
    await rig.gateway.flush()
    assert rig.gateway.auth_info.get("LOCAL_TEST") == before
    assert (await rig.charger("retry-stop", "StopTransaction", payload))[2] == {}
    await rig.close()


# Intent: a late actual cloud rejection retains the history fact and installs a physical zero hold.
# Why: a locally accepted offline session cannot be silently relabelled Accepted by a negative cloud response.
async def test_late_cloud_rejection_holds_zero_and_retains_original_receipt(tmp_path: Path) -> None:
    rig = StrictRig(tmp_path / "state.db")
    await rig.boot()
    original = (await rig.charger("start", "StartTransaction", START))[2]
    rig.start_info = info(status="Blocked")
    with pytest.raises(RecoveryRequiredError):
        await rig.cloud()
    assert rig.firmware.effective_limit() == 0 and rig.store.get("control")["inhibit"]
    assert rig.store.next_delivery().state == "rejected"
    assert (await rig.charger("retry", "StartTransaction", START))[2] == original
    await rig.close()


# Intent: protected revocations cannot be evicted into a local fallback permission when the book is full.
# Why: bounded storage must fail closed rather than silently forget the current owner's denied tag.
async def test_metadata_capacity_protects_configured_revocations(tmp_path: Path) -> None:
    rig = StrictRig(tmp_path / "state.db")
    book = InfoBook(rig.store, capacity=1, protected_tags=frozenset({"LOCAL_TEST"}))
    book.put("LOCAL_TEST", info(status="Blocked"))
    with pytest.raises(RecoveryRequiredError):
        book.put("OTHER", info())
    assert book.get("local_test")["status"] == "Blocked"
    await rig.close()


# Intent: a current dynamic revocation survives eviction while an older unrelated decision can leave the bounded book.
# Why: protecting only configured owner tags would silently forget the current app grant's denial under cache pressure.
def test_metadata_capacity_protects_current_dynamic_grant(tmp_path: Path) -> None:
    journal = Journal(tmp_path / "state.db", "OFFLINE001")
    try:
        book = InfoBook(journal, capacity=2)
        blocked = info(status="Blocked")
        assert book.put("CURRENT", blocked) == blocked
        journal.put("vendor_start_grant", {"digest": tag_key("current")})
        book.put("OLD", info())
        book.put("NEW", info())
        assert book.get("CURRENT") == blocked
        assert book.get("OLD") is None
        assert book.get("NEW")["status"] == "Accepted"
    finally:
        journal.close()


# Intent: an explicitly approved owner can authorize without cloud, with only a fresh bounded local permission.
# Why: local control must remain usable offline without inventing cloud metadata or an indefinite cached grant.
async def test_local_owner_offline_authorization_has_bounded_local_expiry(tmp_path: Path) -> None:
    rig = StrictRig(tmp_path / "state.db")
    try:
        await rig.boot()
        before = datetime.now(UTC)
        answer = await rig.charger("local-auth", "Authorize", {"idTag": "local_test"})
        metadata = answer[2]["idTagInfo"]
        assert set(metadata) == {"status", "expiryDate"}
        assert metadata["status"] == "Accepted"
        expiry_at = datetime.fromisoformat(metadata["expiryDate"])
        assert before + timedelta(seconds=59) <= expiry_at <= datetime.now(UTC) + timedelta(seconds=60)
        assert not rig.vendor_frames
    finally:
        await rig.close()


# Intent: an ordinary Authorize cannot return Accepted after a newer local Stop claimed the station.
# Why: metadata fetched before the hold must not release permission after the awaited cloud reply.
async def test_ordinary_authorize_rechecks_stop_after_cloud_wait(tmp_path: Path) -> None:
    rig = StrictRig(tmp_path / "state.db")
    await rig.boot()
    await rig.cloud()
    rig.authorization_release = asyncio.Event()
    pending = asyncio.create_task(rig.charger("auth", "Authorize", {"idTag": "LOCAL_TEST"}))
    await rig.authorization_seen.wait()
    await rig.gateway.stop()
    rig.authorization_release.set()
    assert (await pending)[2]["idTagInfo"]["status"] == "Invalid"
    assert rig.firmware.effective_limit() == 0
    await rig.close()


# Intent: a tagged Stop retains its original optional metadata even after cache revocation and a new-id retry.
# Why: late retransmission must not rewrite the reply originally accepted with the completed physical session.
async def test_tagged_stop_metadata_is_an_immutable_fact(tmp_path: Path) -> None:
    rig = StrictRig(tmp_path / "state.db")
    rig.gateway.auth_info.put("LOCAL_TEST", rig.actual_info)
    await rig.charger("start", "StartTransaction", START)
    payload = {"transactionId": 1001, "timestamp": START["timestamp"], "meterStop": 1000, "idTag": "local_test"}
    first = (await rig.charger("stop", "StopTransaction", payload))[2]
    assert first == {"idTagInfo": rig.actual_info}
    rig.gateway.auth_info.put("LOCAL_TEST", info(status="Blocked"))
    assert (await rig.charger("new-stop-id", "StopTransaction", payload))[2] == first
    await rig.close()


# Intent: a Stop, revocation or expiry during composite readback changes the final Authorize answer.
# Why: the last permission check must follow every await, including physical-ceiling verification.
@pytest.mark.parametrize("change", ["stop", "revoked", "expired"])
async def test_authorize_rechecks_permission_after_cap_readback(tmp_path: Path, change: str) -> None:
    from tests.test_prototype_runtime import until

    rig = StrictRig(tmp_path / "state.db")
    await rig.boot()
    await rig.cloud()
    seen, release = asyncio.Event(), asyncio.Event()
    normal = rig.charger_send
    paused = False

    # Delay one actual device response after the positive station profile has already been installed.
    async def pause(raw: str) -> None:
        nonlocal paused
        frame = decode(raw)
        if frame[0] == 2 and frame[2] == "GetCompositeSchedule" and not paused:
            paused = True
            seen.set()
            await release.wait()
        await normal(raw)

    rig.gateway.attach_charger(pause)
    pending = asyncio.create_task(rig.charger("auth", "Authorize", {"idTag": "LOCAL_TEST"}))
    await seen.wait()
    stop = None
    if change == "stop":
        stop = asyncio.create_task(rig.gateway.stop())
        await until(lambda: rig.store.get("control", {}).get("inhibit", False))
    elif change == "revoked":
        rig.gateway.auth_info.put("LOCAL_TEST", info(status="Blocked"))
    else:
        rig.gateway.auth_info.put("LOCAL_TEST", info(expiryDate=(datetime.now(UTC) - timedelta(seconds=1)).isoformat()))
    release.set()
    assert (await pending)[2]["idTagInfo"]["status"] != "Accepted"
    if stop is not None:
        await stop
    await rig.close()


# Intent: optional Stop metadata cannot give a dynamic tag an unbounded cache permission.
# Why: the expiry restriction applies to all authorization emissions, even while retiring the grant.
async def test_tagged_dynamic_stop_omits_unbounded_positive_metadata(tmp_path: Path) -> None:
    rig = StrictRig(tmp_path / "state.db")
    rig.gateway.auth_info.put("DYNAMIC", rig.actual_info)
    rig.gateway.authorisation.reserve("DYNAMIC")
    await rig.charger("start", "StartTransaction", {**START, "idTag": "DYNAMIC"})
    rig.gateway.auth_info.put("DYNAMIC", {"status": "Accepted"})
    payload = {"transactionId": 1001, "timestamp": START["timestamp"], "meterStop": 1000, "idTag": "DYNAMIC"}
    assert (await rig.charger("stop", "StopTransaction", payload))[2] == {}
    assert not rig.gateway.authorisation.allows("DYNAMIC")
    await rig.close()


# Intent: a later actual ACK without expiry cannot widen an already bound dynamic grant into unlimited permission.
# Why: the trial's finite dynamic-tag policy applies on every emission, not only the initial preflight.
async def test_bound_dynamic_grant_cannot_lose_expiry_restriction(tmp_path: Path) -> None:
    rig = StrictRig(tmp_path / "state.db")
    rig.gateway.auth_info.put("DYNAMIC", rig.actual_info)
    rig.gateway.authorisation.reserve("DYNAMIC")
    await rig.charger("start", "StartTransaction", {**START, "idTag": "DYNAMIC"})
    await rig.boot()
    await rig.cloud()  # The ordinary rig's real synthetic Start ACK contains no expiry metadata.
    assert (await rig.charger("auth", "Authorize", {"idTag": "DYNAMIC"}))[2]["idTagInfo"]["status"] == "Invalid"
    assert not rig.gateway._tag_allowed("DYNAMIC")
    await rig.close()


# Intent: both initial revocation and revocation during cap readback prevent a local RemoteStart.
# Why: local API calls must enforce the same metadata decision as a fresh charger Start/Authorize.
@pytest.mark.parametrize("during_readback", [False, True])
async def test_local_start_rechecks_revocation_in_sequence(tmp_path: Path, during_readback: bool) -> None:
    rig = StrictRig(tmp_path / "state.db")
    if not during_readback:
        rig.gateway.auth_info.put("LOCAL_TEST", info(status="Blocked"))
    else:
        normal = rig.charger_send

        # Revoke in the independent peer exactly when the station's positive cap has just been installed.
        async def revoke(raw: str) -> None:
            frame = decode(raw)
            if frame[0] == 2 and frame[2] == "GetCompositeSchedule":
                rig.gateway.auth_info.put("LOCAL_TEST", info(status="Blocked"))
            await normal(raw)

        rig.gateway.attach_charger(revoke)
    with pytest.raises(DeniedError):
        await rig.gateway.start()
    assert not any(frame[0] == 2 and frame[2] == "RemoteStartTransaction" for frame in rig.charger_frames)
    if during_readback:
        assert rig.firmware.effective_limit() == 0
    await rig.close()


# Intent: all unobserved fields and malformed expiry/parent types fail before they become cached authority.
# Why: optional authorization metadata is security-relevant input rather than an opaque successful response.
@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        {"status": True},
        {"status": "Unknown"},
        {"status": "Accepted", "expiryDate": "2030-01-01"},
        {"status": "Accepted", "parentIdTag": "X" * 21},
        {"status": "Accepted", "parentIdTag": "A\x1fB"},
        {"status": "Accepted", "parentIdTag": "A\x7fB"},
        {"status": "Accepted", "extra": 1},
    ],
)
def test_metadata_validation(value: Any) -> None:
    with pytest.raises(DeniedError):
        validate_info(value)
