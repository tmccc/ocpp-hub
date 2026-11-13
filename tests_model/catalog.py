"""Source-pinned finite families and risk links shared by collection, manifest generation and raw audits."""

import hashlib
import json
from collections.abc import Callable
from dataclasses import asdict, dataclass

from tests_model.control_cases import (
    boot_reply_candidates,
    boot_retry_candidates,
    current_candidates,
    current_replacement_candidates,
    no_exclusion,
    stop_candidates,
    vendor_candidates,
)
from tests_model.deep_cases import (
    current_domain_candidates,
    process_current_candidates,
    process_overlap_candidates,
    process_start_candidates,
)
from tests_model.ha_cases import outage_candidates, outage_exclusion, receipt_candidates, receipt_exclusion
from tests_model.recovery_cases import start_candidates, start_exclusion
from tests_model.review_cases import cloud_start_candidates, issued_start_candidates, observation_candidates
from tests_model.schedule_cases import candidates, exclusion, source_candidates, source_exclusion
from tests_model.storage_debt_cases import storage_fifo_candidates

SCHEMA = 2


@dataclass(frozen=True)
class Family:
    """A declared input universe has a fixed scope, causal exclusions, denominator and owning test function."""

    name: str
    suites: tuple[str, ...]
    test_function: str
    inputs: Callable
    excluding: Callable
    raw: int
    admitted: int
    risks: tuple[str, ...]
    required_evidence: tuple[str, ...] = ()


FAMILIES = (
    Family(
        "issued_start_proof",
        ("engine-model",),
        "tests_model/test_issued_start_sequences.py::test_issued_start_proof_sequences",
        issued_start_candidates,
        no_exclusion,
        36,
        36,
        ("START-DISPATCH", "PROCESS-DEATH", "DURABILITY", "NEXT-SESSION"),
        ("actual_sigkill", "independent_durable_read", "peers_survived", "independent_mapping"),
    ),
    Family(
        "observation_retry",
        ("engine-model",),
        "tests_model/test_observation_sequences.py::test_observation_retry_sequences",
        observation_candidates,
        no_exclusion,
        120,
        120,
        ("OBSERVATION-RETRY", "DURABILITY", "NEXT-SESSION"),
    ),
    Family(
        "cloud_start_recovery",
        ("engine-model",),
        "tests_model/test_cloud_start_sequences.py::test_cloud_start_recovery_sequences",
        cloud_start_candidates,
        no_exclusion,
        432,
        432,
        ("CLOUD-START-REFUSAL", "RESTART", "NEXT-SESSION"),
    ),
    Family(
        "process_overlap",
        ("engine-model",),
        "tests/test_real_process_death.py::test_process_sigkill_automatic_current_overlap",
        process_overlap_candidates,
        no_exclusion,
        8,
        8,
        ("PROCESS-DEATH", "CURRENT-ROLLBACK", "CURRENT-IDEMPOTENCY", "NEXT-SESSION"),
        (
            "actual_sigkill",
            "independent_mapping",
            "automatic_restore",
            "receipt_retired_before_new_selection",
            "later_selection_verified",
            "peers_survived",
        ),
    ),
    Family(
        "storage_fifo",
        ("engine-model",),
        "tests_model/test_storage_fifo_sequences.py::test_storage_fifo_sequences",
        storage_fifo_candidates,
        no_exclusion,
        192,
        192,
        ("STORAGE-COMMIT", "STOP-ACK", "FIFO-DEBT", "REPEATED-FAILURE", "NEXT-SESSION"),
        (
            "storage_failures_observed",
            "queued_pairs_verified",
            "changed_reply_refused",
            "two_fresh_lifecycles_complete",
        ),
    ),
    Family(
        "process_current",
        ("engine-model",),
        "tests/test_real_process_death.py::test_process_sigkill_current_selection",
        process_current_candidates,
        no_exclusion,
        36,
        36,
        ("PROCESS-DEATH", "CURRENT-ROLLBACK", "DURABILITY", "NEXT-SESSION"),
        ("actual_sigkill", "independent_durable_read", "peers_survived"),
    ),
    Family(
        "process_start",
        ("engine-model",),
        "tests/test_real_process_death.py::test_process_sigkill_before_start_dispatch",
        process_start_candidates,
        no_exclusion,
        36,
        36,
        ("PROCESS-DEATH", "START-DISPATCH", "DURABILITY", "NEXT-SESSION"),
        ("actual_sigkill", "independent_durable_read", "peers_survived"),
    ),
    Family(
        "current_domain",
        ("engine-model",),
        "tests_model/test_current_domain_sequences.py::test_current_domain_sequences",
        current_domain_candidates,
        no_exclusion,
        2916,
        2916,
        ("CURRENT-DOMAIN", "CURRENT-IDEMPOTENCY", "NEXT-SESSION"),
    ),
    Family(
        "two_receipt",
        ("engine-model",),
        "tests_model/test_schedule_sequences.py::test_two_receipt_event_sequences",
        candidates,
        exclusion,
        11520,
        6240,
        ("TIMER-ORDER", "SOURCE-OWNERSHIP"),
    ),
    Family(
        "source_proof",
        ("engine-model",),
        "tests_model/test_source_sequences.py::test_source_proof_event_sequences",
        source_candidates,
        source_exclusion,
        1920,
        960,
        ("SOURCE-REVOCATION", "RESTART"),
    ),
    Family(
        "start_recovery",
        ("engine-model",),
        "tests_model/test_start_recovery_sequences.py::test_start_failure_recovery_sequences",
        start_candidates,
        start_exclusion,
        25920,
        4320,
        ("START-DISPATCH", "RESTART", "NEXT-SESSION"),
    ),
    Family(
        "boot_reply",
        ("engine-model",),
        "tests_model/test_boot_sequences.py::test_boot_reply_sequences",
        boot_reply_candidates,
        no_exclusion,
        192,
        192,
        ("BOOT-PROOF", "PAUSE-PRECEDENCE", "NEXT-SESSION"),
        ("boundary_entered",),
    ),
    Family(
        "boot_retry",
        ("engine-model",),
        "tests_model/test_boot_sequences.py::test_boot_retry_sequences",
        boot_retry_candidates,
        no_exclusion,
        72,
        72,
        ("BOOT-RETRY", "PAUSE-PRECEDENCE", "NEXT-SESSION"),
        ("boundary_entered",),
    ),
    Family(
        "current_resolution",
        ("engine-model",),
        "tests_model/test_current_sequences.py::test_current_resolution_sequences",
        current_candidates,
        no_exclusion,
        1368,
        1368,
        ("CURRENT-ROLLBACK", "BOOT-PROOF", "PAUSE-PRECEDENCE"),
        ("boundary_entered",),
    ),
    Family(
        "stop_delivery",
        ("engine-model",),
        "tests_model/test_stop_sequences.py::test_stop_delivery_sequences",
        stop_candidates,
        no_exclusion,
        288,
        288,
        ("STOP-ACK", "STOP-PROOF", "RESTART", "NEXT-SESSION"),
    ),
    Family(
        "current_replacement",
        ("engine-model",),
        "tests_model/test_current_replacement_sequences.py::test_current_replacement_sequences",
        current_replacement_candidates,
        no_exclusion,
        72,
        72,
        ("CURRENT-ROLLBACK", "RESTART", "NEXT-SESSION"),
        ("boundary_entered",),
    ),
    Family(
        "vendor_debt",
        ("engine-model",),
        "tests_model/test_vendor_sequences.py::test_vendor_debt_sequences",
        vendor_candidates,
        no_exclusion,
        24,
        24,
        ("VENDOR-DEBT", "STOP-PROOF", "RESTART", "NEXT-SESSION"),
    ),
    Family(
        "ha_receipt",
        ("ha-proxy", "ha-proxy-legacy"),
        "tests_ha_proxy/test_receipt_sequences.py::test_receipt_identity_sequences",
        receipt_candidates,
        receipt_exclusion,
        160,
        160,
        ("HA-RECEIPT-IDENTITY",),
    ),
    Family(
        "ha_outage_lifecycle",
        ("ha-proxy", "ha-proxy-legacy"),
        "tests_ha_proxy/test_outage_sequences.py::test_outage_lifecycle_sequences",
        outage_candidates,
        outage_exclusion,
        384,
        240,
        ("HA-SETUP", "HA-LIFETIME", "HA-STALE-CALLBACK", "HA-NOTICE"),
    ),
)


# Record input identity rather than any engine result; the real test must independently establish its suffix.
def case_record(family: Family, case):
    row = {
        "schema": SCHEMA,
        "family": family.name,
        "id": case.identifier,
        "node_id": family.test_function + "[" + case.identifier + "]",
        "required_evidence": list(family.required_evidence),
        **asdict(case),
        "events": list(case.events),
        "exclusion": family.excluding(case),
    }
    row["input_sha256"] = hashlib.sha256(json.dumps(row, sort_keys=True).encode()).hexdigest()
    return row


# Tests attest which declared input ran; the auditor separately verifies exact events and completed recovery.
def input_digest(family_name, case):
    family = next(family for family in FAMILIES if family.name == family_name)
    return case_record(family, case)["input_sha256"]
