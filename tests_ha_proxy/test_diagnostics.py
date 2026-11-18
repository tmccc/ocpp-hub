"""The real HA diagnostic surfaces explain cached evidence without reading the charger or exposing private data."""

import copy
import json
from types import SimpleNamespace

import pytest

from tests_ha_proxy.test_client import TOKEN, setup
from tests_ha_proxy.test_client import peer as peer

PRIVATE = "SYNTHETIC_PRIVATE_DIAGNOSTIC_VALUE"


# Include useful bounded evidence and deliberate private extras at every nested projection boundary.
def detail():
    return {
        "schema_version": 1,
        "available": True,
        "sampled_at": 10000,
        "original_fault": None,
        "recovery": "not_assessed",
        "blockers": [],
        "pending_commands": [
            {
                "action": "pause",
                "state": "uncertain",
                "accepted_at": None,
                "age_seconds": None,
                "confirmation": "unknown",
                "result": {"private": PRIVATE},
                "id": PRIVATE,
            }
        ],
        "pending_command_count": 1,
        "commands_truncated": False,
        "workers": {"start": "cancelled", "current": "absent", "owned_io_pending": True, "private": PRIVATE},
        "protocol": {
            "cloud": {
                "rpc": {"action": "StopTransaction", "phase": "awaiting_reply", "private": PRIVATE},
                "fault": {"count": 1, "received_close_code": 1008, "detail": PRIVATE},
            }
        },
        "timeline": [
            {
                "state": "attention",
                "phase": "blocked",
                "at_epoch": 10000,
                "blockers": ["command_outcome_unknown", PRIVATE],
                "private": PRIVATE,
            }
        ],
        "timeline_scope": "current_process",
        "private": PRIVATE,
    }


# Intent: local Start approval explanations remain separate from health and never render arbitrary peer text.
# Why: an expired approval used to leave Start unavailable without telling the owner how the online path differs.
@pytest.mark.parametrize(
    "authorization,expected",
    [
        ("renewal_required", "request fresh approval"),
        ("expired_cloud_unavailable", "needs fresh approval"),
        ("denied", "has not approved"),
        ("no_approved_tag", "No local Start tag"),
    ],
)
async def test_start_authorization_explanation_is_allowlisted(actual_hass, authorization, expected):
    from custom_components.ics_proxy.diagnostic_view import explanation

    data = {"healthy": True, "diagnostics": detail(), "start_authorization": authorization}
    assert expected in explanation(data, fresh=True, native_fresh=True)
    data["start_authorization"] = PRIVATE
    assert PRIVATE not in explanation(data, fresh=True, native_fresh=True)
    data["start_authorization"] = authorization
    assert "stale" in explanation(data, fresh=False, native_fresh=True)
    assert "reading is stale" in explanation(data, fresh=True, native_fresh=False)
    if authorization == "expired_cloud_unavailable":
        data["health_blockers"] = ["cloud_disconnected"]
        assert expected in explanation(data, fresh=True, native_fresh=True)


# Intent: the real HA control-mode entity explains an expired approval without making the service unhealthy.
# Why: a core-only status field would still leave the owner-facing diagnostic surface silent.
async def test_actual_control_mode_sensor_explains_start_renewal(actual_hass, peer):
    peer["state"]["diagnostics"] = detail()
    peer["state"]["start_authorization"] = "renewal_required"
    entry = await setup(actual_hass, peer)
    entity = actual_hass.states.get("sensor.ics_proxy_control_mode")
    assert entity is not None
    assert "request fresh approval" in entity.attributes["diagnostic_summary"]
    assert entry.runtime_data.data["healthy"] is True


# Intent: English explanations distinguish unknown/strict outcomes, storage waits, measurement faults and Pause.
# Why: a fault label must not suggest ordinary Pause/Return can waive an interrupted or strict recovery audit.
@pytest.mark.parametrize(
    "recovery,blocker,fault,expected",
    [
        ("strict_refusal", None, None, "operator review is required"),
        ("audit_outcome_unknown", None, None, "ordinary recovery remains blocked"),
        ("verification_pending", "delivery_storage_pending", None, "required journal storage"),
        ("not_assessed", "pause_verification_pending", None, "physical zero output has not yet been verified"),
        ("not_assessed", "command_outcome_unknown", None, "unresolved outcome"),
        ("not_assessed", "cloud_start_pending", None, "cloud Start is awaiting its matching session"),
        ("verification_pending", "fault_latched", "measured_output_limit_exceeded", "validated measurements exceeded"),
    ],
)
async def test_diagnostic_explanations_keep_evidence_categories_distinct(
    actual_hass, recovery, blocker, fault, expected
):
    from custom_components.ics_proxy.diagnostic_view import diagnostics, explanation

    value = detail() | {
        "recovery": recovery,
        "blockers": [blocker] if blocker else [],
        "original_fault": {"code": fault, "first_at": 9990} if fault else None,
    }
    data = {"healthy": False, "diagnostics": value}
    before = copy.deepcopy(data)
    assert expected in explanation(data, fresh=True, native_fresh=True)
    assert PRIVATE not in json.dumps(diagnostics(value, timeline=True))
    assert data == before


# Intent: unsupported or malformed optional diagnostic fields do not leak arbitrary data or create positive authority.
# Why: a compatible older proxy remains usable, and diagnostic parsing must handle real bounded JSON edge shapes.
@pytest.mark.parametrize(
    "value", [None, [], {"schema_version": True}, {"schema_version": 2}, {"schema_version": 1, "available": False}]
)
async def test_unsupported_diagnostic_details_are_honestly_unavailable(actual_hass, value):
    from custom_components.ics_proxy.diagnostic_view import diagnostics, explanation

    assert diagnostics(value)["available"] is False
    assert "unavailable" in explanation({"diagnostics": value, "healthy": True}, fresh=True, native_fresh=True)


# Intent: hostile nested strings, nonfinite numbers and oversized lists cannot bypass reconstruction or size bounds.
# Why: support downloads must be useful even when optional peer data is malformed and must remain safe to share.
async def test_diagnostic_allowlist_reconstructs_nested_data_and_bounds_arrays(actual_hass):
    from custom_components.ics_proxy.diagnostic_view import diagnostics

    value = detail()
    value.update(timeline=value["timeline"] * 100)
    result = diagnostics(value, timeline=True)
    assert len(result["timeline"]) == 20 and PRIVATE not in json.dumps(result)
    result["timeline"][0]["blockers"].clear()
    assert value["timeline"][0]["blockers"] == ["command_outcome_unknown", PRIVATE]
    for change in (
        {"sampled_at": 10**1000},
        {"sampled_at": float("nan")},
        {"original_fault": {"code": PRIVATE}},
        {"recovery": PRIVATE},
        {"blockers": [PRIVATE, {}, [PRIVATE]]},
        {"pending_commands": [{"action": PRIVATE}]},
    ):
        assert diagnostics(value | change)["available"] is False


# Intent: unsupported faults and contradictory pending summaries cannot produce a reassuring healthy explanation.
# Why: dropping unrecognized evidence must not conceal an unresolved operation behind otherwise green base health.
@pytest.mark.parametrize(
    "change",
    [
        {"current_fault": PRIVATE},
        {"pending_command_count": 0},
        {"pending_command_count": 1.5},
        {"pending_commands": [{"action": "pause", "state": "uncertain"}]},
        {"commands_truncated": True},
    ],
)
async def test_inconsistent_optional_evidence_never_becomes_no_problem(actual_hass, change):
    from custom_components.ics_proxy.diagnostic_view import diagnostics, explanation

    value = detail() | change
    assert diagnostics(value)["available"] is False
    summary = explanation({"diagnostics": value, "healthy": True}, fresh=True, native_fresh=True)
    assert "unavailable" in summary and "No problem" not in summary


# Intent: real HA exports cached redacted evidence and public versions without another status or mutation request.
# Why: collecting support information must neither repeat a lost control command nor change the live snapshot.
@pytest.mark.parametrize("stale,failed", [(False, False), (True, False), (False, True)])
async def test_real_ha_diagnostic_download_is_passive_redacted_and_truthful(
    actual_hass, peer, monkeypatch, stale, failed
):
    peer["state"]["diagnostics"] = detail()
    peer["state"]["private"] = PRIVATE
    peer["state"]["software"] = {"proxy_python": "3.14.4", "proxy_source_sha256": "c" * 64, "token": TOKEN}
    entry = await setup(actual_hass, peer)
    from homeassistant.loader import async_get_integration

    from custom_components.ics_proxy.diagnostics import async_get_config_entry_diagnostics

    integration = await async_get_integration(actual_hass, "ics_proxy")
    coordinator = entry.runtime_data
    if stale:
        coordinator.received_at -= 30
    if failed:
        coordinator.last_update_success = False
    before = copy.deepcopy(coordinator.data), list(peer["requests"])

    # Export is forbidden from refreshing or querying the management peer, even for a supposedly harmless receipt.
    async def deny(*args, **kwargs):
        raise AssertionError("diagnostic download attempted networking")

    monkeypatch.setattr(coordinator.client, "request", deny)
    report = await async_get_config_entry_diagnostics(actual_hass, entry)
    assert report["snapshot_fresh"] is (not stale and not failed)
    assert report["software"]["client_version"] == str(integration.version)
    assert len(report["software"]["client_source_sha256"]) == 64
    assert report["software"]["proxy_source_sha256"] == "c" * 64
    assert report["diagnostics"]["pending_commands"][0]["confirmation"] == "unknown"
    if stale or failed:
        assert all(value is None for value in report["measurements"].values())
        assert "last known" in report["summary"]
    else:
        assert report["measurements"]["power_w"] == 0
    assert PRIVATE not in json.dumps(report) and TOKEN not in json.dumps(report)
    assert (coordinator.data, peer["requests"]) == before
    report["diagnostics"]["timeline"][0]["blockers"].clear()
    assert coordinator.data == before[0]


# Intent: failed first setup produces a bounded missing-snapshot download without needing runtime_data or a token.
# Why: an unavailable integration is precisely when collecting diagnostics is most useful.
async def test_diagnostic_download_handles_failed_first_setup(actual_hass):
    from custom_components.ics_proxy.diagnostics import async_get_config_entry_diagnostics

    report = await async_get_config_entry_diagnostics(actual_hass, SimpleNamespace(data={"token": PRIVATE}))
    assert report["snapshot_available"] is False and report["snapshot_fresh"] is False
    assert PRIVATE not in json.dumps(report) and "last known" in report["summary"]


# Intent: dashboard and delayed notice use safe explanations while existing mode IDs and control gating stay intact.
# Why: a better fault report must not imply that unknown output is zero or restart a debounce deadline.
async def test_diagnostic_explanation_reaches_real_entities_and_notice(actual_hass, peer, monkeypatch):
    peer["state"].update(healthy=False, state="attention", diagnostics=detail())
    peer["state"]["diagnostics"].update(recovery="audit_outcome_unknown", blockers=["native_audit_outcome_unknown"])
    entry = await setup(actual_hass, peer)
    coordinator = entry.runtime_data
    state = actual_hass.states.get("sensor.ics_proxy_control_mode")
    assert state.state == "Needs attention"
    assert "ordinary recovery remains blocked" in state.attributes["diagnostic_summary"]
    assert "timeline" not in state.attributes["diagnostic_details"]
    coordinator.problem_since -= 61
    coordinator.update_notification(False)
    await actual_hass.async_block_till_done()
    from tests_ha_proxy.test_freshness_recovery import _async_get_or_create_notifications

    notice = _async_get_or_create_notifications(actual_hass)[coordinator.notification_id]
    assert coordinator.notified and "ordinary recovery remains blocked" in coordinator.notice.detail
    assert "ordinary recovery remains blocked" in str(notice)
    assert PRIVATE not in json.dumps(dict(state.attributes)) and PRIVATE not in str(notice)
    from homeassistant.components import persistent_notification

    created = []
    original = persistent_notification.async_create

    # Observe actual notice updates while retaining HA's real persistence and the same notification identifier.
    def record(*args, **kwargs):
        created.append(kwargs["notification_id"])
        return original(*args, **kwargs)

    monkeypatch.setattr(persistent_notification, "async_create", record)
    since = coordinator.problem_since
    peer["state"]["diagnostics"].update(recovery="strict_refusal", blockers=["strict_native_refusal"])
    await coordinator.async_refresh()
    await actual_hass.async_block_till_done()
    notice = _async_get_or_create_notifications(actual_hass)[coordinator.notification_id]
    assert "operator review is required" in str(notice)
    assert coordinator.problem_since == since and created == [coordinator.notification_id]
    await coordinator.async_refresh()
    assert created == [coordinator.notification_id]
    assert set(peer["commands"]) == {"status"}
