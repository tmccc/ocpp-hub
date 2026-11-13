"""Actual HA client and loopback responses must preserve the identity of every command receipt."""

import sys

import pytest

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
            return {"id": identifier, "state": "running" if position else "accepted"}
        value = {"id": identifier, "state": case.terminal}
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
                else "physical confirmation is still pending"
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
