"""Long-lived risk contracts link finite histories and focused failure tests without claiming universal coverage."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Risk:
    """Safety and eventual recovery are separate obligations; remaining limits are explicit review inputs."""

    identifier: str
    safety: str
    recovery: str
    focused: tuple[str, ...]
    limits: str


RISKS = (
    Risk(
        "OBSERVATION-RETRY",
        "One identifiable observation retry creates one durable cloud publication without erasing a new report.",
        "Actual local reply loss, cloud ACK/refusal and replacement retain delivery and two later mapped lifecycles.",
        ("tests/test_observation_receipts.py", "tests/test_transaction_mapping_reuse.py"),
        "Bounded recent receipts; untimed reused IDs after a new Boot cannot establish historical event identity.",
    ),
    Risk(
        "CLOUD-START-REFUSAL",
        "Explicit errors have a bounded persisted retry budget; denied authorization retains zero and real mapping.",
        "Genuine Stop ACK, new Pause and separate Return recover later sessions; unknown outcomes need exact evidence.",
        (
            "tests/test_managed_start_delivery_recovery.py",
            "tests/test_managed_issued_start_recovery.py",
            "tests/test_start_delivery_fault.py",
            "tests/test_bound_owner_retirement.py",
            "tests/test_cloud_denial_zero_boundary.py",
            "tests/test_cloud_ack_storage.py",
            "tests/test_cloud_ack_exhausted_storage.py",
            "tests/test_combined_recovery_proofs.py",
            "tests/test_denial_receipt_contracts.py",
        ),
        "Named reply forms and repeated recovery failures; operator statements are an explicit independent input.",
    ),
    Risk(
        "PROCESS-DEATH",
        "Actual SIGKILL cannot replay one-shot Start or promote an uncommitted selection.",
        "Fresh Boot recovers through Pause/Return or exact automatic original-session restoration and later outages.",
        ("tests/test_real_process_death.py",),
        "Named SQL/COMMIT boundaries; independent peers survive. Process death is not hardware power loss.",
    ),
    Risk(
        "DURABILITY",
        "Independent read-only WAL observations distinguish staged from actual committed facts.",
        "Reopening the real journal rolls back partial transactions and retains exact completed receipts.",
        (
            "tests/test_real_process_death.py",
            "tests/test_journal_fault_recovery.py",
            "tests/test_process_durable_reader.py",
        ),
        "No disk cache, filesystem corruption or physical fsync-failure claim.",
    ),
    Risk(
        "CURRENT-DOMAIN",
        "Every advertised integer choice retains its exact observed physical ceiling or Pause zero.",
        "All729 directed literal pairs reach authenticated API/wire/receipt/replay under both intents; "
        "131 pairs retain both demands and two later outages.",
        (
            "tests_model/test_current_transport_sequences.py",
            "tests/test_current_recovery_cover.py",
            "tests_ha_proxy/test_current.py",
        ),
        "Recovery suffixes use a declared obligation cover, not all729 pair-specific recovery histories; "
        "failure/process families remain unchanged.",
    ),
    Risk(
        "CURRENT-IDEMPOTENCY",
        "Replaying an exact current UUID cannot repeat physical changes.",
        "Same-value controls, abandoned-request retirement and later lifecycle/outage recovery remain functional.",
        (
            "tests_model/test_current_transport_sequences.py",
            "tests_model/test_current_domain_sequences.py",
            "tests/test_managed_current_retirement.py",
        ),
        "Exact same UUID/body; unrelated and changed-body conflicts retain focused coverage.",
    ),
    Risk(
        "STORAGE-COMMIT",
        "Repeated real statement/COMMIT failures preserve proof, FIFO and immutable lifecycle rows.",
        "Repair plus exact independent retry evidence, real ACK and new Pause/Return recover two later sessions.",
        (
            "tests/test_journal_fault_recovery.py",
            "tests/test_managed_current_retirement.py",
            "tests/test_cloud_ack_storage.py",
            "tests/test_storage_fault_recovery.py",
            "tests/test_worker_storage_recovery.py",
            "tests/test_worker_storage_supersession.py",
            "tests/test_worker_storage_serve.py",
            "tests/test_current_poll_storage.py",
            "tests/test_start_diagnostic_progress.py",
            "tests/test_start_cancel_hardening.py",
            "tests/test_management_storage.py",
            "tests/test_management_read_storage.py",
            "tests/test_disconnect_storage_recovery.py",
        ),
        "ABORT and deferred-FK integration cuts, actual SQL read refusal, FULL and writer-BUSY controls; "
        "unavailable physical proof stays unknown rather than becoming a cached zero claim.",
    ),
    Risk(
        "FIFO-DEBT",
        "A head proof cannot authorize later queued Start/Stop pairs or a changed Stop reply.",
        "One/three real queued pairs acquire their own exact proof and actual wire ACK in FIFO order.",
        ("tests_model/test_storage_fifo_sequences.py", "tests/test_cloud_ack_storage.py"),
        "Bounded closed pairs and two changed-response forms; no arbitrary corrupt-tail recovery claim.",
    ),
    Risk(
        "REPEATED-FAILURE",
        "Two storage refusals and later Stop rejection retain restrictive state and old evidence.",
        "Fresh proofs replace superseded ones; two later independent lifecycle/outage epochs recover.",
        (
            "tests_model/test_storage_fifo_sequences.py",
            "tests/test_cloud_ack_storage.py",
            "tests/test_worker_storage_recovery.py",
            "tests/test_disconnect_storage_recovery.py",
        ),
        "Fixed two-failure prefix plus bounded queued-tail failures; unbounded fault histories remain outside scope.",
    ),
    Risk(
        "WIRE-VALIDATION",
        "Invalid HTTP versions/chunk sizes and unrepresentable UTC timestamps grant no evidence.",
        "All valid byte splits, incomplete-prefix rejection and later real lifecycle delivery are checked.",
        ("tests/test_native_http_adversarial.py", "tests/test_protocol_adversarial.py"),
        "Named grammar boundaries and truncation properties; no exhaustive HTTP/OCPP fuzzing claim.",
    ),
    Risk(
        "HA-MALFORMED-REPLY",
        "Recursive JSON and huge numeric fields cannot bypass outage handling or freeze entities.",
        "True first/reload setup, raw failures at three response positions and later fresh reads recover.",
        ("tests_ha_proxy/test_adversarial_responses.py", "tests_ha_proxy/test_adversarial_setup.py"),
        "Eleven raw shapes, selected numeric fields; arbitrary input combinations remain outside scope.",
    ),
    Risk(
        "TIMER-ORDER",
        "Cutoff/cancel keeps zero and preserves original daily plans.",
        "Cleanup and a later genuine daily occurrence recover without replayed Start.",
        ("tests/test_managed_schedule_model_recovery.py",),
        "Finite two-receipt grammar; not arbitrary repeated timers.",
    ),
    Risk(
        "SOURCE-OWNERSHIP",
        "A matching tag alone cannot bind a local request or grant Stop authority.",
        "Actual immutable lifecycle and fresh native source evidence establish the exact owner.",
        ("tests/test_managed_start_binding.py", "tests/test_managed_normal_start.py"),
        "Representative daily/local/cloud sources; no arbitrary firmware source identifiers.",
    ),
    Risk(
        "SOURCE-REVOCATION",
        "Lost or contradicted source cannot authorize continuation.",
        "Eligible source-loss recovery uses a fresh source and current Boot; strict or unknown audits remain blocked.",
        (
            "tests/test_ordinary_daily_report_recovery.py",
            "tests/test_measured_current_recovery.py",
            "tests/test_native_measurement_mixed.py",
            "tests/test_native_repair_strict_conflict.py",
            "tests/test_native_repair_denial_lineage.py",
        ),
        "Finite source-loss and exact fault-repair histories; no ordinary Pause waiver for interrupted unknown audits.",
    ),
    Risk(
        "START-DISPATCH",
        "Prepared/unsent/rejected differs from issued/accepted unknown debt; zero stays restrictive.",
        "Request-bound unsent proof recovers via fresh Pause/Return; unknown debt requires exact reconciliation.",
        (
            "tests/test_managed_start_cap_failure.py",
            "tests/test_managed_start_scheduling_loss.py",
            "tests/test_managed_start_cancelled_outer.py",
            "tests/test_start_worker_outcomes.py",
            "tests/test_start_cancel_hardening.py",
            "tests/test_managed_start_debt_recovery.py",
            "tests/test_managed_stop_replacement.py",
        ),
        "Start model includes successful restart only after native restoration; refused startup is focused coverage.",
    ),
    Risk(
        "BOOT-PROOF",
        "Incomplete old proof cannot cross Boot; a completed zero pair can survive identical metadata.",
        "A fresh correlated proof or valid completed pair permits restrictive convergence then explicit Return.",
        ("tests/test_managed_boot_supersession.py", "tests/test_native_repair_authority.py"),
        "Identical Boot races; foreign identities/configuration retain separate refusal tests.",
    ),
    Risk(
        "BOOT-RETRY",
        "A real failed native observation grants no positive introduction authority.",
        "Actual HTTP restoration and full zero introduction recover without another Start.",
        ("tests/test_managed_introduction_retry.py",),
        "503 retry permutations; other HTTP forms are focused/start/current tests.",
    ),
    Risk(
        "PAUSE-PRECEDENCE",
        "A newer Pause revokes pending positive work and retains physical zero.",
        "Only separate fresh Return restores the confirmed future current.",
        ("tests/test_managed_paused_current_rollback.py", "tests/test_managed_current_rollback.py"),
        "Bounded event orders; no claim about every Python scheduling point.",
    ),
    Risk(
        "CURRENT-ROLLBACK",
        "Refused, malformed, uncertain or superseded target cannot become future resume authority.",
        "Restoration retains prior current; failed settlement retries from zero and later sessions stay supervised.",
        (
            "tests/test_managed_current_interrupted_recovery.py",
            "tests/test_managed_current_legacy_recovery.py",
            "tests/test_managed_current_retirement.py",
            "tests/test_managed_current_overlap.py",
            "tests/test_current_failure_binding.py",
        ),
        "Directed6/16/32 pairs; process loss at native boundaries, not arbitrary in-flight shielded RPC kills.",
    ),
    Risk(
        "STOP-ACK",
        "Operator proof and pre-ACK Pause never fabricate cloud delivery or delete retained Stop.",
        "Actual matching wire ACK, new Pause and distinct Return complete recovery.",
        ("tests/test_managed_stop_reply_recovery.py", "tests/test_managed_stop_replacement.py"),
        "One retained head Stop; queued tails and repeated rejection have focused tests, not all FIFO permutations.",
    ),
    Risk(
        "STOP-PROOF",
        "Exact candidate/proof rejects changed identity, payload, fault, clock and unrelated debt.",
        "Exclusive stopped-service evidence enables only its supported retry or settlement.",
        ("tests/test_managed_debt_reconciliation.py", "tests/test_managed_vendor_start_reconciliation.py"),
        "Attestation is synthetic and independent; offline tests cannot establish a real operator's knowledge.",
    ),
    Risk(
        "VENDOR-DEBT",
        "Accepted unbound and uncertain cloud commands are never retired by zero or replayed.",
        "Distinct exact proof obligations, a new Pause and separate Return recover the next ordinary session.",
        (
            "tests/test_vendor_reply_uncertainty.py",
            "tests/test_managed_vendor_start_reconciliation.py",
            "tests/test_combined_command_mapping.py",
            "tests/test_combined_mapping_contracts.py",
        ),
        "One cloud Start with three-event permutations; not arbitrary multi-command cloud histories.",
    ),
    Risk(
        "RESTART",
        "Replacement retains durable uncertainty and rejects old process/transport evidence.",
        "Successful native startup, genuine Boot and scoped fresh recovery restore only confirmed authority.",
        ("tests/test_native_bounded_recovery.py", "tests/test_managed_start_cap_failure.py"),
        "Task/runtime loss plus named actual SIGKILL boundaries; not OS power loss or filesystem corruption.",
    ),
    Risk(
        "NEXT-SESSION",
        "Old failed metadata cannot disable zero protection for a later genuine original-plan session.",
        "Both closure orders, later real lifecycle, native outage and reconnect remain independently checked.",
        (
            "tests/test_ordinary_recovery.py",
            "tests/test_managed_normal_start.py",
            "tests/test_normal_source_recovery.py",
            "tests/test_autonomous_cross_feature_recovery.py",
            "tests/test_worker_storage_serve.py",
            "tests/test_native_suffix_witness.py",
            "tests/test_suffix_barrier_regression.py",
        ),
        "Start grammar reconnects open daily; Boot/current suffixes exercise two later sessions and native loss.",
    ),
    Risk(
        "STORAGE",
        "Failed writes publish neither half of proof/ACK/dequeue or exact receipt/metadata completion.",
        "Removing a real SQLite trigger permits supported retry while all retained evidence survives.",
        (
            "tests/test_storage_fifo_witness.py",
            "tests_model/test_storage_fifo_sequences.py",
            "tests/test_managed_stop_reply_recovery.py",
            "tests/test_managed_stop_replacement.py",
            "tests/test_vendor_reply_uncertainty.py",
            "tests/test_managed_debt_reconciliation.py",
            "tests/test_native_repair_storage.py",
            "tests/test_native_repair_wait_controls.py",
            "tests/test_native_repair_sources.py",
        ),
        "Named SQLite commit boundaries; no exhaustive disk-full, WAL corruption or power-failure simulation.",
    ),
    Risk(
        "HA-RECEIPT-IDENTITY",
        "Every initial/polled receipt must match the exact submitted UUID; never retry mutation.",
        "Readable status remains available; physical recovery belongs to engine tests.",
        (
            "tests_ha_proxy/test_client.py",
            "tests_ha_proxy/test_receipt_sequences.py",
            "tests_ha_proxy/test_control_completion_progress.py",
        ),
        "Five controls, positions0..3 and five identity shapes; both matching terminals; "
        "invalid IDs reject before terminal read and retain completed false-success sentinels.",
    ),
    Risk(
        "HA-SETUP",
        "Failed first setup/reload cannot expose stale actionable controls or mutate charger state.",
        "Actual HA setup/retry and a second outage recover on both supported runtimes.",
        ("tests_ha_proxy/test_setup_outage_notice.py",),
        "Two setup origins; no all-core-HA lifecycle claim.",
    ),
    Risk(
        "HA-LIFETIME",
        "Unload/removal retires timers; an obsolete entry cannot affect a replacement entry.",
        "Public reload/setup or genuinely new entry recovers; failed unload retains its real working owner.",
        ("tests_ha_proxy/test_setup_outage_notice.py", "tests_ha_proxy/test_inflight_lifecycle.py"),
        "Public disable/enable and real HA stop have focused held-read controls; no cross-process HA restart claim.",
    ),
    Risk(
        "HA-STALE-CALLBACK",
        "Old coordinator/deadline cannot create or dismiss a current entry's notice.",
        "New fault remains visible until its own fresh success.",
        ("tests_ha_proxy/test_setup_outage_notice.py",),
        "Declared four-event lifecycle grammar; no fabricated concurrent private refreshes.",
    ),
    Risk(
        "HA-NOTICE",
        "Retries preserve the original60s deadline; no early or orphan notification.",
        "Fresh recovery dismisses once and the next independent outage can notify again.",
        (
            "tests_ha_proxy/test_freshness_recovery.py",
            "tests_ha_proxy/test_setup_outage_notice.py",
            "tests_ha_proxy/test_periodic_progress.py",
        ),
        "Controlled deadline tests plus actual periodic loss/recovery twice, including the production5s interval; "
        "no system-clock manipulation.",
    ),
    Risk(
        "AUTONOMOUS-PROGRESS",
        "Pending controls and protective zero retain exact receipt ownership during combined failures.",
        "The service and HA's own scheduled work observe repair and recover without helper ticks or refreshes.",
        (
            "tests/test_autonomous_cross_feature_recovery.py",
            "tests/test_worker_storage_serve.py",
            "tests_ha_proxy/test_periodic_progress.py",
            "tests_ha_proxy/test_control_completion_progress.py",
        ),
        "Declared combined6/32A service histories, real current-cleanup supervision and repeated HA polling; "
        "not arbitrary fault combinations "
        "or an end-to-end HA-to-charger test.",
    ),
    Risk(
        "QUALIFICATION",
        "Dropped family/input/event/suffix, changed source/runtime and partial attempts cannot pass.",
        "Four whole20-worker phases are freshly audited against exact source, collection and raw phases.",
        ("tests/test_qualification_evidence.py", "tests/test_qualification_full_mode.py"),
        "Synthetic auditor controls are never counted as actual qualification executions.",
    ),
    Risk(
        "LEGACY",
        "Retained old requests/mirror failures cannot silently expand authority or hot-loop.",
        "Supported old-state recovery and real HA10/11/legacy environments retain ordinary control.",
        ("tests/test_ordinary_legacy_transport_recovery.py", "tests/test_managed_current_legacy_recovery.py"),
        "Selected historical formats and fault classifications; no arbitrary corrupted database migration claim.",
    ),
)
