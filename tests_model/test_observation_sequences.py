"""Observation retries cross real TLS result loss, cloud outcomes and socket/process/Boot replacement."""

import pytest

from tests.observation_helpers import ObservationTLS, observation_payload, observation_suffix
from tests.test_prototype import START
from tests_model.catalog import input_digest
from tests_model.review_cases import observation_candidates
from tests_model.test_schedule_sequences import model_tls as model_tls
from tests_model.test_schedule_sequences import tls_for_case


# Intent: retain one publication for an identifiable retry and keep genuinely new post-Boot untimed reports.
# Why: message-body uniqueness would erase recurring statuses while volatile receipts duplicate accepted history.
@pytest.mark.parametrize("case", observation_candidates(), ids=lambda case: case.identifier)
async def test_observation_retry_sequences(tmp_path, model_tls, record_property, case):
    events, complete = [], False
    try:
        async with ObservationTLS(tmp_path / "observation.db", tls_for_case(tmp_path, model_tls)) as peer:
            opened = await peer.call("start", "StartTransaction", START)
            local = opened[2]["transactionId"]
            action, payload = observation_payload(case.payload, local)
            if case.cut == "reply-lost":
                await peer.lose_reply("observation", action, payload)
                assert peer.g.failures == ["OSError"]
            else:
                assert (await peer.call("observation", action, payload))[0] == 3
            events.append("accept")
            if case.cut in ("cloud-ack", "cloud-error"):
                peer.reject_report = case.cut == "cloud-error"
                await peer.flush()
            if case.boundary != "same":
                await peer.replace(restart=case.boundary.startswith("restart"), boot=case.boundary.endswith("-boot"))
            events.append("replace")
            assert (await peer.call("observation", action, payload))[0] == 3
            events.append("retry")
            await peer.flush()
            publications = [frame for frame in peer.cloud_calls if frame[2] == action]
            timed = case.payload in ("meter-transaction", "meter-station", "status-timed")
            expected = 2 if case.boundary.endswith("-boot") and not timed else 1
            assert len(publications) == expected
            assert len({f[1] for f in publications}) == expected
            if case.payload == "meter-transaction":
                expected_id = peer.cloud_ledger[0]["reply"]["transactionId"]
                assert all(frame[3]["transactionId"] == expected_id for frame in publications)
            events.append("drain")
            await observation_suffix(peer, local)
            events.append("two-lifecycles")
            complete = True
    finally:
        record_property("model_case", case.identifier)
        record_property("model_family", "observation_retry")
        record_property("model_input_sha256", input_digest("observation_retry", case))
        record_property("completed_events", events)
        record_property("suffix_complete", complete)
