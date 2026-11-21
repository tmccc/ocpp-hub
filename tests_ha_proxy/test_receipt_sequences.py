"""Actual HA client and loopback responses must preserve the identity of every command receipt."""

import sys

import pytest
from homeassistant.exceptions import HomeAssistantError

from tests_ha_proxy.test_client import peer as peer
from tests_ha_proxy.test_client import setup
from tests_ha_proxy.test_control_completion_progress import control_peer as control_peer
from tests_model.catalog import input_digest
from tests_model.ha_cases import receipt_candidates, receipt_exclusion


# Intent: unrelated, missing or malformed receipt identities never confirm another accepted physical command.
# Why: the websocket envelope identifies this read, while result.id must still identify the original mutation.
@pytest.mark.parametrize(
    "case",
    tuple(case for case in receipt_candidates() if receipt_exclusion(case) is None),
    ids=lambda case: case.identifier,
)
async def test_receipt_identity_sequences(actual_hass, peer, control_peer, case, record_property):
    peer = control_peer if case.action == "set_current" else peer
    fields = {} if case.current_a is None else {"current_a": case.current_a}
    entry = await setup(actual_hass, peer)
    client = entry.runtime_data.client
    error = sys.modules["custom_components.ics_proxy.api"].ProxyError
    events, polls, identifier = [], 0, None
    suffix_complete = False

    # Corrupt only actual peer result bodies; preserve genuine HTTP/websocket framing and exchange identifiers.
    def result_filter(command, request, result):
        nonlocal polls, identifier
        if command == case.action:
            assert request.get("current_a") == case.current_a
            assert ("current_a" in request) == (case.current_a is not None)
            identifier = request["id"]
            events.append("submit")
            position = 0
        elif command == "receipt":
            polls += 1
            events.append(f"poll-{polls}")
            assert request["request_id"] == identifier
            position = polls
        else:
            return result
        if position < case.boundary:
            return {"id": identifier, "action": case.action, "state": "running" if position else "accepted"}
        value = {"id": identifier, "action": case.action, "state": case.terminal}
        if case.identity == "different":
            value["id"] = "f" * 32 if identifier != "f" * 32 else "e" * 32
        elif case.identity == "missing":
            value.pop("id")
        elif case.identity == "null":
            value["id"] = None
        elif case.identity == "malformed":
            value["id"] = "invalid"
        events.append("response")
        return value

    peer["result_filter"] = result_filter
    try:
        if case.identity == "matching" and case.terminal == "completed":
            await client.command(case.action, peer["state"]["control_revision"], **fields)
        else:
            message = (
                "invalid command receipt"
                if case.boundary == 0 and case.identity != "matching"
                else "returned another command's receipt"
                if case.identity != "matching"
                else "could not confirm the requested control state"
            )
            with pytest.raises(error, match=message):
                await client.command(case.action, peer["state"]["control_revision"], **fields)
        assert peer["commands"].count(case.action) == 1 and polls == case.boundary
        assert (await client.request("status"))["api_version"] == 1
        suffix_complete = True
    finally:
        assert events == list(case.events)
        record_property("model_case", case.identifier)
        record_property("model_family", "ha_receipt")
        record_property("model_input_sha256", input_digest("ha_receipt", case))
        record_property("completed_events", events)
        record_property("suffix_complete", suffix_complete)


# Intent: a matching uncertain Start receipt acknowledges admission through each response path.
# Why: after a lost reply only the original request's recognized receipt can prove it was recorded.
@pytest.mark.parametrize(
    ("boundary", "receipt_override"),
    [
        ("first-reply", {}),
        ("poll", {}),
        ("lost-reply", {}),
        ("lost-reply", {"id": "wrong"}),
        ("lost-reply", {"action": "pause"}),
        ("lost-reply", {"action": None}),
        ("lost-reply", {"state": "unrecognized"}),
    ],
    ids=["first-reply", "poll", "lost-reply", "wrong-id", "wrong-action", "missing-action", "unrecognized-state"],
)
async def test_uncertain_start_receipt_reports_recorded_outcome(actual_hass, peer, boundary, receipt_override):
    await setup(actual_hass, peer)
    peer["lose_reply"] = boundary == "lost-reply"
    peer["pending_receipt"] = boundary == "poll"

    # Override receipt evidence without altering the independently reported charger status.
    def result_filter(command, request, result):
        if (command == "start" and boundary == "first-reply") or command == "receipt":
            receipt = {**result, "state": "uncertain", **receipt_override}
            if receipt.get("action") is None:
                receipt.pop("action", None)
            return receipt
        return result

    peer["result_filter"] = result_filter
    message = (
        "The request may have been sent; its outcome is unknown"
        if receipt_override
        else "The proxy recorded the request. The charger outcome is uncertain"
    )
    with pytest.raises(HomeAssistantError) as error:
        await actual_hass.services.async_call("button", "press", {"entity_id": "button.ics_proxy_start"}, blocking=True)
    assert str(error.value).startswith(message)
    (submission,) = (request for request in peer["requests"] if request["command"] == "start")
    lookups = [request["request_id"] for request in peer["requests"] if request["command"] == "receipt"]
    assert lookups == ([] if boundary == "first-reply" else [submission["id"]])


@pytest.mark.parametrize(
    ("binding", "matched"),
    [
        ({"revision": 7, "current_a": 24}, True),
        ({"revision": 6, "current_a": 24}, False),
        ({"revision": 7, "current_a": 16}, False),
        ({}, False),
    ],
)
async def test_lost_reply_requires_exact_binding_for_supplied_request_id(actual_hass, control_peer, binding, matched):
    control_peer["state"]["control_revision"] = 7
    client = (await setup(actual_hass, control_peer)).runtime_data.client
    identifier = "c" * 32
    control_peer["lose_current_before_effect"] = True

    def result_filter(command, request, result):
        if command == "receipt":
            return {"id": identifier, "action": "set_current", "state": "completed", **binding}
        return result

    control_peer["result_filter"] = result_filter
    error = sys.modules["custom_components.ics_proxy.api"].UnknownCommandOutcomeError
    if matched:
        await client.command("set_current", 7, id=identifier, current_a=24)
    else:
        with pytest.raises(error, match="outcome is unknown"):
            await client.command("set_current", 7, id=identifier, current_a=24)
    assert control_peer["commands"].count("set_current") == 1
