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
        "Restoration uses a fresh source and current Boot with the same verified ceiling.",
        ("tests/test_ordinary_daily_report_recovery.py",),
        "Finite source-loss grammar; no unbounded outage durations.",
    ),
    Risk(
        "START-DISPATCH",
        "Prepared/unsent/rejected differs from issued/accepted unknown debt; zero stays restrictive.",
        "Request-bound unsent proof recovers via fresh Pause/Return; unknown debt requires exact reconciliation.",
        (
            "tests/test_managed_start_cap_failure.py",
            "tests/test_managed_start_scheduling_loss.py",
            "tests/test_managed_start_debt_recovery.py",
            "tests/test_managed_stop_replacement.py",
        ),
        "Start model includes successful restart only after native restoration; refused startup is focused coverage.",
    ),
    Risk(
        "BOOT-PROOF",
        "Incomplete old proof cannot cross Boot; a completed zero pair can survive identical metadata.",
        "A fresh correlated proof or valid completed pair permits restrictive convergence then explicit Return.",
        ("tests/test_managed_boot_supersession.py",),
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
        "Fresh zero/Pause/Return retains prior verified current and later ordinary sessions remain supervised.",
        ("tests/test_managed_current_interrupted_recovery.py", "tests/test_managed_current_legacy_recovery.py"),
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
        ("tests/test_vendor_reply_uncertainty.py", "tests/test_managed_vendor_start_reconciliation.py"),
        "One cloud Start with three-event permutations; not arbitrary multi-command cloud histories.",
    ),
    Risk(
        "RESTART",
        "Replacement retains durable uncertainty and rejects old process/transport evidence.",
        "Successful native startup, genuine Boot and scoped fresh recovery restore only confirmed authority.",
        ("tests/test_native_bounded_recovery.py", "tests/test_managed_start_cap_failure.py"),
        "Controlled task/runtime loss; not OS power loss at every instruction or physical filesystem corruption.",
    ),
    Risk(
        "NEXT-SESSION",
        "Old failed metadata cannot disable zero protection for a later genuine original-plan session.",
        "Both closure orders, later real lifecycle, native outage and reconnect remain independently checked.",
        (
            "tests/test_ordinary_recovery.py",
            "tests/test_managed_normal_start.py",
            "tests/test_normal_source_recovery.py",
        ),
        "Start grammar reconnects open daily; Boot/current suffixes exercise two later sessions and native loss.",
    ),
    Risk(
        "STORAGE",
        "Failed writes publish neither half of proof/ACK/dequeue or exact receipt/metadata completion.",
        "Removing a real SQLite trigger permits supported retry while all retained evidence survives.",
        (
            "tests/test_managed_stop_reply_recovery.py",
            "tests/test_managed_stop_replacement.py",
            "tests/test_vendor_reply_uncertainty.py",
            "tests/test_managed_debt_reconciliation.py",
        ),
        "Named SQLite commit boundaries; no exhaustive disk-full, WAL corruption or power-failure simulation.",
    ),
    Risk(
        "HA-RECEIPT-IDENTITY",
        "Every initial/polled receipt must match the exact submitted UUID; never retry mutation.",
        "Readable status remains available; physical recovery belongs to engine tests.",
        ("tests_ha_proxy/test_client.py",),
        "Four controls, response positions0..3, five identity shapes, two terminals.",
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
        ("tests_ha_proxy/test_setup_outage_notice.py",),
        "Disable and HA-stop interleavings remain focused/future coverage.",
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
        ("tests_ha_proxy/test_freshness_recovery.py", "tests_ha_proxy/test_setup_outage_notice.py"),
        "Controlled integration clock plus a real short scheduler control; no system-clock manipulation.",
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
