# Roadmap and status

The one place that says how far the project is. Update it in the same change that moves an item. `[x]` is done and verified, `[~]` is built but not proven live, `[ ]` is open. Dates are when it was last confirmed.

## Goal

The repository has two deployment domains: the standalone iCS proxy with its thin `ics_proxy` HA client, and the
older Raedian NEO OCPP backend inside the `ocpp_hub` HA integration. They share core modules, not a live listener.
Read [the navigation guide](code-navigation.md) before interpreting the historical milestones below.

## Automatic recovery after a charger outage — 22 November 2026

- [~] A clean idle recovery can restore the saved app/schedule ceiling after the charger and cloud verify
  closed zero and settle the reports. An owner Pause or unfinished charging work at interruption retains
  the explicit Return path. The change is covered offline.

## Shared Stop deployment — 22 November 2026

- [x] The standalone iCS proxy is running `98c47e1`. Tests sent local Pause and vendor-app Stop in both orders.
  Each order sent one Stop to the charger and waited for the closing reports.
- [x] All 14,962 release tests and the recovery search passed. The simulated installation week passed against
  `11b1515`. After restart, the charger was idle in app/schedule mode at its 32 A ceiling, the cloud was connected,
  and all 11 charging records were preserved.

## Replacement deployment and midnight check — 22 November 2026

- [x] The standalone proxy and thin Home Assistant client were deployed at `11b1515`. All 14,845 release
  cases passed, along with the recovery search and installation-week rehearsal against the previous
  build, `f40f7ed`.
- [x] Both updates were installed with the charger idle on 21 November. Checks after installation
  confirmed app/schedule mode, 32 A selected, zero draw, cloud connectivity and all 11 recorded
  transactions.
- [ ] Verify the next scheduled charge with a vehicle connected. The charger confirmed that the
  midnight schedule was still saved on 22 November. It reported the vehicle unplugged at midnight
  and again at 00:20, with no charge session recorded.

## System interaction coverage and deployment — 21 November 2026

- [x] The standalone proxy and Home Assistant client were deployed at `f40f7ed`. All 14,790 release cases,
  the recovery search and the installation-week rehearsal passed. Live checks confirmed app/schedule mode,
  a connected cloud, zero draw, the selected 32 A ceiling and all 11 recorded transactions.
- [x] The [interaction tests](recovery-coverage.md) cover timed charging, installation and Home Assistant
  alongside the command and report queue. Both software versions opened a copy of the live journal after
  rehearsal of the complete gateway startup. Its schema matched the running proxy's journal.

## Vendor catch-up and overnight observations — 19 November 2026

- [x] Archived charger, proxy and Home Assistant histories agree for three midnight sessions on November 10, 12
  and 13. The sessions used 5.614, 0.000 and 32.508 kWh. On two nights the car stopped drawing power before the
  session closed. The zero-energy session stayed open until the scheduled end. Charger records show a positive
  offer during zero draw. The offline tests cover zero throughout a session, a brief zero followed by resumed draw,
  and lower reported offer before the scheduled Stop. The cause of the drops remains unknown.
- [x] Three timer sessions catch up after a vendor outage, interrupted delivery and a proxy restart. The
  [tests](recovery-coverage.md#risk-matrix) also cover the case where the vendor accepts a Start but its reply goes
  missing. The proxy retains the reports and keeps the charger at zero while the outcome is uncertain.
- [x] The offline repair tool confirms a vendor-accepted Start while preserving later queued charge reports.
  In the simulator, a second lost Start reply and a later rejected Stop can each be resolved with their own
  evidence. Delivery, a fresh Pause and an explicit Return precede the next scheduled charge.
- [x] Actual Home Assistant tests cover delayed Return, interrupted current selection and a newer Pause across proxy
  replacement. They also check both sides of Return's final saved result at an applied 16 A ceiling.
- [x] The [rollback check](release-process.md#rehearsed-standalone-software-rollback) accepts the settled migrated
  journal used in the offline rehearsal. The older build kept its history, saved current and latest Pause. A
  separate Return allowed the next scheduled charge to complete.
- [x] Vendor recovery, rollback and diagnostics share the same proof for a resolved historical timed Start.
  The catch-up regression keeps that receipt and verifies successive accepted Start, rejected Stop and unaccepted
  Start repairs on the same migrated journal before a fresh Pause and Return.

## Current iCS implementation — 18 November 2026

- [x] Backend `bb10464` implements the five simplification stages below. The four full suites passed with
  20 workers each and identical test IDs: 12,990 engine/model, 618 HA 2026.11.0 client, 618 HA 2026.10.4
  client and 57 older-integration tests. The normal validator and independent evidence audit passed.
- [x] The standalone proxy was replaced once and independently verified healthy in app/schedule mode at
  revision 62 with the 32 A ceiling, a closed session and zero output. The journal, history, native configuration
  and daily schedule were preserved. The 16 files of the HA client match the candidate, and HA retained its
  running process, BUILD `8f73bf9`, settings and automation states.

## Previous findings deployment — 18 November 2026

- [x] Implementation `8f73bf9` addresses the six findings from the earlier full review: introduction storage
  retry and diagnostics, guarded approval renewal for normal Start, legacy planning/history handling and release
  tooling. The thin `ics_proxy` HA client is installed from the same implementation with BUILD `8f73bf9`.
  The backend and changed client are deployed and independently verified. App/schedule authority, the selected,
  native and owned 32 A ceilings, closed zero output, original configuration and daily schedule are preserved.
  Protected journal/history, HA settings, dashboard, entity IDs and automation states are retained. Deployment
  issued no Start or dedicated charging trial.
- [x] Implementation `8f73bf9` passed all four complete suites with 20 workers each: 12,990 engine/model tests,
  618 HA 2026.11.0 client tests, 618 HA 2026.10.4 client tests and 57 older-integration tests, 14,283 tests total.
  The normal validator and independent evidence audit passed. Documentation-only closure does not change that
  tested and deployed implementation or the installed client BUILD.
- [x] Claude's scoped fix review and amended-candidate addendum found no unresolved defect in this batch.
  They supplement the earlier full-source review below; the separate pre-existing legacy race remains open below.

## Previous lifecycle deployment and full review — 18 November 2026

- [x] Version 0.2.3 implements shared recovery coordination, explicit permission/result/effect contracts, atomic
  Start/Return authority publication and owned shutdown. Reconnect completion remains separate from admission
  auditing and verified zero. Backend implementation `de2d74b`, including the simplifications below and the
  installation lifecycle owner, is committed, pushed, deployed and independently verified. The unchanged HA client
  remains at implementation `8013d2f`; its files and running process were retained without installation or restart.
  Settings, history, the dashboard, 16 entities and all 25 automation states (including five disabled) are preserved.
  Deployment checks preserved healthy paused zero at control revision 59, the selected/native 32 A ceiling,
  original schedule/configuration and a closed session with no delivery or mutation debt. Home Assistant remains
  on core 2026.11.1. The backend was activated once. A transient outbox snapshot interrupted one schema check;
  its failure is retained alongside two subsequent unchanged strict passes and the completed independent audit.
- [x] Implementation `de2d74b` passed all four complete suites with 20 workers each: 12,946 engine/model tests,
  613 HA 2026.11.0 client tests, 613 HA 2026.10.4 client tests and 48 older-integration tests, 14,220 tests total.
  The normal validator and independent evidence audit passed. Documentation-only follow-ups do not change
  that tested and deployed implementation.
- [x] After deployment, a separately authorised Return restored app/schedule control at revision 60 and the
  verified 32 A ceiling. Subsequent ordinary charging was observed at 3.5 kW and 14.2 A; no Start command was sent
  by that deployment work. The separately authorised 06:00 Europe/London Stop for 18 November was independently
  verified closed at zero. A later explicit Return restored app/schedule control at revision 62 and the 32 A ceiling.
- [x] Claude cleared its context and reviewed the exact deployed source, all code/tests and normal documentation.
  The independent visible-read audit accounts for all 520 normal text files and four limited recorded-data views
  with their complete consumers. Full raw capture bodies and six branding PNGs were not visually reviewed.
  Findings are static, not runtime reproductions: one iCS retry defect, an authorisation renewal/diagnostic gap,
  two legacy-integration issues and two release-tooling weaknesses. The six corrections are recorded below.

## Architectural simplification — 17 November 2026

The completed changes below are included in the qualified deployment above. Line counts describe individual
changes rather than a cumulative total. Further simplification remains open.

- [x] Extract the pure Start failure selector, then consolidate the normal Start lifecycle under `Journal.starts`
  using existing persisted formats. Receipt, pending-bit, managed-charge metadata and lease-slot writes now have
  one owner, with checked typed correlation for local closure and recovery. Shared timed/cloud projections pass
  through that owner; generic process-replaced request normalization remains separate.
- [x] Remove temporary runtime observer plumbing and move the pilot grammar to test references while preserving
  its original assertions. Add fast stored-state and atomic-failure checks; retain real peer, same-tag, cancellation,
  expiry, operator and actual process-death coverage for the coherent lifecycle milestone.
- [x] Consolidate Start retirement eligibility, outcome selection and scoped preimages into one pure production
  policy and owner execution path. Preserve different legacy and proof contracts; add fast pure and SQLite checks.
  This consolidation adds physical runtime lines; moving record types alone does not reduce the implementation.
- [x] Consolidate durable control intent and Pause/Return request transitions in `ManagedRequests`. Start and
  Return share the current-intent transaction check; one completion owns Return disposition, including prelaunch
  queue refusal. Retain the legacy status completion and distinct physical/claim proofs. Remove unused adapters
  and duplicate reads/disposition. This change removes two physical runtime lines; larger reductions come from
  the retired paths below.
- [x] Keep shared restore sequencing directly in its owned driver and terminal mapping in the recovery policy.
  Retain live proof, publication and task boundaries, share wire zero rollback and remove obsolete adapters.
- [x] Retire the experimental vehicle-pause handover, SEAT checker and physical-pause verifier with owner approval.
  Keep native-bounded commissioning and daily controls. Reject unfinished old handovers before
  startup, retain completed history, remove the restore phase interpreter and share seven station-zero predicates.
  Runtime is 1,558 physical lines smaller, including new guards and shared checks; test/doc deletions are excluded.
  Fast physical/terminal checks and the exact collection difference retain the active test scope.
- [x] Retire the remaining plain commissioning lifecycle and require the native specification for session startup.
  Preserve native commissioning Start/Release, cleanup/restoration and daily controls. Refuse unfinished plain
  journals without changing their evidence; retain shared physical/resource tests on the native path. Runtime is
  480 physical lines smaller, including new guards and a repeated-cleanup inventory correction. Fast checks and
  independent semantic review cover the implementation.
- [x] Make daily control prerequisites explicit, centralise status projection, and give the current selection and
  fault latch their own journal owners. Route charger-reported Start through one handler and move shared rigs into
  test support modules with explicit peer selection.
- [ ] Continue broader control/persistence simplification where it removes overlapping responsibilities. Native
  admission, current-selection authority, connection and task lifetimes, cloud delivery and physical evidence remain
  separate duties.
- [x] Give current selection one owner for acceptance, completion and recovery outcomes. The command worker, Pause
  and Return pass their evidence to that owner. The current record and its receipt stay in the same transactions.
- [x] Centralize physical control writes in `Journal.control`, replacing 27 sites across 11 modules with named
  transitions. Share authority predicates and the pure health decision; make the live poll's stages explicit and
  name its production charging owner `ManagedCharging`. Preserve raw stored formats, public status and actual
  effect/commit/task boundaries. The first grouped check passed 242 tests; the full gate above provides broader
  verification of the deployed implementation.
  This improves ownership and navigation but adds runtime lines; no physical code-size reduction is claimed.
- [x] Retire proxy one-off temporary schedules with owner approval. Remove their API, deadline task, native-plan
  write capability and recovery branches while retaining daily schedules and managed Start/Pause/Return/current.
  Read-only startup checks refuse unfinished old obligations before journal normalization; completed history and
  reserved IDs remain passive. Runtime is 2,689 physical lines smaller, including the new guard and shared helpers.
  Retire only the two one-off model families; preserve all 97 process and 72 current-replacement cases. Focused
  checks on both HA client runtimes retain daily control and reconnect behaviour.
- [x] Share exact Boot/control identity checks at seven live boundaries and rebuild complete ownership through
  `ManagedController.authority()`. Keep narrower Stop/fault/current guards, receipt reads and claim/publication
  ordering. Remove three unused exception adapters; retain all charging features and persisted formats. The gain is
  consistent authority vocabulary rather than another large line-count reduction.

## Installation lifecycle refactor — 18 November 2026

- [x] Give installation phase and introduction-flag writes one owner, `Journal.phase`, with named transitions
  and shared phase predicates. Preserve raw stored values, lazy reads, separate fault policies, physical-control
  preimages and original transaction/write order. Focused verification passes 341 unique cases, including 80 new
  lifecycle cases, diagnostic contracts and two actual SIGKILL boundaries. All 17 generated owner mutants are
  caught; static checks and Claude's independent runtime review pass. Runtime grows by 115 physical lines to
  centralise ownership. The exact implementation is included in the full qualification and verified `de2d74b`
  deployment above.

## Follow-ups from the fresh bug review — 18 November 2026

- [x] Retry replay-safe post-read introduction metadata and zero preparation after SQLite failure. Diagnose later
  storage failures without replaying Stop-capable work; cover the retry with real post-read SQLite write failures.
- [x] Explain local-tag approval readiness and let normal Start request actual fresh cloud approval for an aged
  Accepted tag before charger effects. Preserve cached denials, offline expiry and newer authority guards.
- [x] In the separate older integration, expose persistent failed secondary profile synchronization independently
  of the accepted native planning flag; clear it after successful explicit reapply. Guard malformed session-history
  containers while retaining valid sibling records.
- [x] Select release notes by the requested numeric version and fail on missing or empty notes. Compare versions
  without collapsing dotted components. These tooling fixes do not change a charger session.
- [x] Complete Claude's scoped fix review and addendum, all four full 20-worker suites, and verified deployment
  of the backend and thin HA client from `8f73bf9`. Focused regressions remain separate development evidence.
- [x] In the separate older Raedian integration, read the latest saved windows when applying a native planning
  switch. Retire the saved plan when an accepted external profile write replaces it during a plan edit.

## iCS implementation history — 15–17 November 2026

- [x] Previous deployment `8013d2f` passed all four complete suites with 20 workers each: 20,360 engine/model tests,
  620 HA 2026.11.0 client tests, 620 HA 2026.10.4 client tests and 48 older-integration tests. Its deployment and
  preservation checks passed before the simplifications above. These counts describe that historical implementation.

- [~] Introduction readiness and RPC deadline corrections distinguish retryable closed-zero/lifecycle waits from
  strict content refusals, and unsent validation from dispatched uncertainty. Focused regressions and all HA suites
  provide this change's verification; the owner's restriction excludes a fresh full engine qualification and live
  charging tests.
- [x] The four runtime-review corrections below were committed, pushed and deployed as `3105e8b`; their verification
  combined focused offline checks with all three HA suites on that commit, without a new full engine qualification.
- [~] Four runtime-review corrections cover normal Start after paused recovery, failed Stop claim settlement,
  local authorization during incomplete cloud registration and HA local-only presentation. New regressions
  reproduce the old failures. Verification uses focused offline recovery checks and fresh HA suites under the
  owner's restriction on rerunning engine qualification; this is not a fresh full four-suite qualification.
- [x] Recovery and diagnostic corrections through implementation `71710af` are committed, pushed and deployed to
  the standalone service and HA client. Coverage includes interrupted work, native/storage recovery, saved Stop
  acknowledgements, Boot binding and truthful pending/unknown diagnostics. See the changelog and coverage matrix.
- [x] Qualification evidence is recorded with its actual provenance: retained engine results at `52ce581` and fresh
  HA results at `71710af`, under the owner's instruction not to rerun engine qualification. The latter commit changes
  one HA test body; this is composed evidence, not a new four-suite same-commit qualification.
- [x] This installation's 32 A commissioning handover is closed. Managed Start/Pause/Return and idle-only current
  selection are implemented. The earlier trial notes do not redefine today's supported control path.
- [ ] Identify the cause of the vendor cloud connection failure first observed on 14 November. Live connections
  are healthy again in the 18 November deployment checks. In the 15 November investigation, local control remained
  healthy at paused zero while cloud openings were refused on both configured server nodes, including through a
  different public IP. No internal vendor cause or wider outage was established.

The dated entries below retain historical observations and physical limits. The Raedian-specific open items remain
separate from the delivered iCS installation.

## Verified Raedian milestones

- [x] Engine in `core/`: frames, charger session, central system, commands, transaction-id map. No Home Assistant imports, `mypy --strict`, tested over a real websocket with a fake charger (PR 2).
- [x] Vendor mirror in `core/`: store-and-forward queue, replay of boot and status, transaction-id pairing, vendor commands refused, Basic auth only when configured (PR 2).
- [x] Home Assistant layer: setup and unload, config flow, options flow, sensors and binary sensor, diagnostics with redaction, translations (English, Dutch) (PR 3).
- [x] Notification when a charger connects, with vendor, model, firmware, serial, OCPP version and mirror state (2026-10-20).
- [x] Deployed with `tools/deploy_ha.py`; `lbbrhzn/ocpp` removed; OCPP Hub listens on port 9000 and the NEO connects (2026-10-20).
- [x] Reconnect after a reload of the integration: the NEO is back in 14 seconds, because the hub drops connections instead of closing them (2026-10-20).
- [x] Vendor mirror against the real Raedian backend: `wss://cloud-protocol.raedian.com:9043/ocpp/`, no credentials; the mirror shows connected and the Raedian app shows the charger online (2026-10-21).
- [x] Independent review and audit of the engine and of the Home Assistant layer, P0 and P1 findings fixed with a test each.

- [x] First real charge with a car (2026-10-21): the NEO obeyed a 6 A station profile (5.9 A per phase, about 4.05 kW), reported preparing, charging, suspended by the car, finishing and available, and the charge was stored with its numbers (5.0 kWh in 84 minutes, stopped by the car being unplugged).

## Historical implementation and commissioning milestones

These dated checkboxes retain the evidence state recorded at each milestone. The current iCS summary above
supersedes older deployment restrictions; `[~]` here does not mean that the current service is awaiting deployment.

- [~] Broader offline failure/recovery qualification (2026-11-13): source-linked matrix; complete finite
  Start/Boot/current/Stop/cloud-debt and actual HA lifecycle/receipt families; explicit exclusions, real failure
  injection and mandatory later recovery. Require all four complete20-worker suites on one clean commit.
  This change performs no live charging tests or deployment.

- [~] External-review recovery corrections (2026-11-12): preserve ordinary supervision after unsent local Start
  refusals and interrupted zero cleanup; complete safe introduction after identical overlapping Boot; retain HA
  outage notices through setup retries and failed unload. Add exact independent recovery for accepted unbound
  cloud Starts and Stop-reply delivery faults, keeping zero and requiring actual Stop acknowledgment, a new Pause
  and separate Return. Legacy mirror backoff and full20-worker evidence validation are hardened. These corrections
  are offline work; deployment and additional live charging tests are outside this change.

- [~] Standalone iCS coexistence proxy (2026-11-08): second unplugged trial accepted an Absolute stack1 6 A station
  profile, reported a matching composite schedule and retained an accepted manufacturer-cloud session. A one-second
  verification window is corrected. Later unplugged trials verified Stop0/Release6 and profile-free cleanup. Exact
  instrumentation identified a 154 ms future startup reply; bounded read-only retries are tested offline and the corrected
  source confirmed automatically on three live introductions. Three real sessions verified app Start/Stop/restart,
  local Stop/release and nominal 6 A limiting (reported peak 6.2 A). Every lifecycle was acknowledged; all old history
  remains alongside the new charges, and full fresh-baseline restoration is independently verified.
  A sixth unplugged trial also verifies a local zero hold with the cloud link retired, followed by successful
  reconnection/release and complete restoration. A seventh verifies real local Start/charge/Stop with the vendor
  runner retired and actual acknowledgement of queued history after reconnect, including elapsed local authorization.
  All prior history remains plus the new 38 Wh session, and full restoration is verified. A private tested bootstrap
  reserves fresh session numbers above native history. Automatic CLI allocation, first-handoff history revalidation,
  connected restricted restart, explicit zero-hold acknowledgement and bounded stale-snapshot rejection are now
  implemented and tested offline. A subsequent normal-backend car-pause trial proves zero draw through a charger restart
  and fresh 6 A Start offer, with restoration while the lead stays attached. Connected-handover checks now have their
  own fresh provider/vehicle evidence, native USER6/closed-zero gates, journalled cleanup continuation and staged
  restoration. This separate path is tested offline and remains disabled without a real current-state provider;
  the earlier pilot cannot authorise it. An automatic read-only SEAT checker now reads the real charging endpoint
  and blocks preflight before mutation. A live read confirms manual mode/connected with optional power absent.
  The endpoint provides no established vehicle observation timestamp or durable pause lease, so the checker reports
  that uncertainty and cannot grant handover authority. No live Home Assistant or permanent service deployment has
  been made.
  Fresh physical verification is now live-validated (2026-11-09): genuine6A charging, two sampled zero windows over
  a minute each and actual charging between them, with a fresh process-local source and explicit result expiry.
  Normal USER32 overnight charging was independently confirmed after restoration, with the lead attached and all
  pre-existing history retained. This proves current reversible behaviour; the durable permission/reconnection
  guarantee required for unattended connected handover remains open.
  A separate supervised connected strategy now uses the previously proven persistent native6A limit instead of
  vehicle-pause authority. Explicit connected admission, actual Boot binding, per-send native guards, exact closed-zero
  cleanup and staged original-backend/32A restoration are implemented and tested offline. A supervised connected live
  trial passed on 2026-11-09 with the lead attached throughout: real Boot/cloud registration, twelve zero observations
  spanning 60.22 seconds, exact trial-profile cleanup, native original-backend reconnection and full 32 A restoration.
  All 20 history bodies and saved plans remain unchanged; no lifecycle was fabricated. Home Assistant and permanent
  service deployment remain unchanged, and wider unattended compatibility remains open.
  Explicit native-mode recovery acknowledgement now retains zero until a separate Start/release, with actual known
  Stop/replay reconciliation and offline-tested CLI admission. A further connected trial passed on 2026-11-09:
  local Start/Stop with and without the cloud, actual queued lifecycle upload/ACKs, real process replacement using
  the same journal, zero recovery and blocked Start until explicit acknowledgement, then a separate Start/Stop.
  Charger reconnection required no reboot. All 20 old history bodies remain plus three genuine acknowledged sessions;
  final original 32 A/cloud/plans restoration and absence of trial profiles were independently verified. All new
  sessions drew zero power, so those were control/recovery proofs.
  A subsequent connected load trial passed on 2026-11-09: 50 Wh over 138 seconds with 16 positive native samples at 6.1 A
  and 1447–1449 W. The same acknowledged session continued through deliberate active vendor disconnection; local Stop
  offline established closure/all-phase zero, followed by real cloud ACKs and an empty outbox. Native6/owned6 remained
  verified. Full original 32 A/cloud/configuration/plans restoration, no trial profiles and preservation of the fresh 22
  history bodies plus the new charge were independently verified. One older record was already absent before this
  baseline and its cause is unknown; the raw archive remains. The lead stayed attached. Longer unattended operation
  and permanent Home Assistant/service deployment remain separate work.
  Active process-loss recovery is now live-verified on 2026-11-09: the old proxy process was terminated during real
  acknowledged 6 A charging; the same native transaction remained bounded at 6.1 A with USER6/owned6 while absent.
  Replacement launch took 0.651 seconds, actual station0 was requested 21.618 seconds after the crash, and closed/all-phase
  zero with real history ACKs was confirmed 30.854 seconds after the last pre-crash sample. The real trace proves zero
  then exact-known RemoteStop. Explicit recovery acknowledgement stayed at zero before a different real charge.
  One monotonic 180-second budget spanned both processes; invalid authority blocked positive control. Main sessions
  delivered 17 Wh/8 Wh, plus 58 Wh in a preparatory attempt whose late restart was correctly refused and safely stopped.
  Full original 32 A/cloud/configuration/plans and all 23 initial history bodies plus three genuine acknowledged sessions
  were preserved. The lead stayed attached; no Home Assistant or permanent service deployment was performed.
  Persistent bounded app/schedule supervision is now implemented with automatic zero-proof/recovery acknowledgement
  and release6, disabled local Start, durable mode restrictions, native/cloud health and process watchdog reporting.
  Owner app/timetable authority remains available; no Start is generated by service recovery. The managed service
  is now installed with boot linger/restart/watchdog and a separate minute health timer. A real idle process kill
  recovered automatically to verified USER6/owned6/shared control with zero power, no Start/transaction/outbox and
  all26 history bodies/plans preserved; the lead stayed attached. The first release had required manual independent
  zero reconciliation after an unconfirmed outcome whose cause was not captured; future errors retain private
  diagnostics. The first overnight schedule is now verified below; longer endurance remains open. Lead removal exposed the
  managed monitor's connected-only check: it latched inhibition after three samples and remained blocked after
  reconnection. Explicit closed/all-phase-zero unplugged monitoring/recovery is now implemented without weakening
  original connected admission, unknown-session rejection, resource/history protection or32A restoration.
  The correction is deployed after1088 offline cases and12 condition mutations; unplug/replug and absent-vehicle
  restart were tested offline. Independent zero/owned0/history/empty-ledger proof preceded fault acknowledgement;
  real connected replacement recovered shared6A/Boot/cloud with zero power, no Start and all26 history bodies/plans
  intact. The physical lead stayed connected. Home Assistant remains unchanged.
  The first managed overnight schedule completed on 2026-11-10: 00:00:00 start, 06:00:02 Stop and 5,614 Wh, with
  cloud acknowledgements, an empty outbox and all 26 original records preserved alongside the new session. Power
  was zero from around 03:52; its cause is unconfirmed. All 445 minute checks before 07:25 were healthy, with three alerts
  clearing automatically and no restart/persistent fault. The owner accepts overnight scheduling as complete and
  waives another active-load 06:00 cutoff test.
  Authenticated local management and a thin HA adapter now provide status, fault notifications, intentional local
  Stop and explicit return to app control. Desired ownership survives supervision/replacement. Keep 6 A;
  normal higher-current operation requires separate policy/native/recovery work. The older HA engine is not this
  adapter, and deliberately disabled EV/reserve automations remain disabled.
  The local management API and thin HA client are implemented: loopback-only authenticated websocket, atomic
  paused intent/hold claim, restart-persistent Stop and revision-bound explicit Return with actual zero/release
  proof. No Start endpoint or command retry is added; native6A and the app/timetable path remain. Fresh local
  charger history replaces optional cloud-cache claims, entity expiry advances between polls, and sustained faults
  create a debounced HA notification. Isolated actual HA2026.11 tests cover passive lifecycle, controls and stale
  readings. Deployed on2026-11-10 after1117 offline and6 actual-HA tests. Real HA Pause, service loss/restart with
  the pause retained, sustained-fault alert/recovery and explicit HA Return passed at zero power with the lead
  attached. Native6/owned6, unchanged saved plans/all27 records and no new transaction were independently verified.
  The dashboard now uses fresh local readings/history and supported Pause/Return, removing obsolete cloud controls,
  the slider and optional cached schedule/history. Actual template rendering and readback passed; all21 functional
  automation states, including5 disabled EV/reserve automations, are preserved. The final mode-label freshness
  regression passed before the corrected HA client was deployed.
  Managed journals can explicitly tolerate missing historical records generally: full originals and completed
  versions are archived privately; missing counts are informational, while changed admitted bodies and all
  electrical/session/resource faults still block. An exclusive offline acknowledgement atomically records the
  policy/archive/operator receipt and retires only the independently verified history fault into inhibited
  recovery. Actual Boot/reconciliation/release remain required, allocation is unchanged, ordinary trials stay
  strict, and the same journal's eventual original-backend restoration honors the history policy.
  Deployed and independently verified:27 archived/25 returned/2 absent, fresh Boot/cloud/native recovery to shared6A,
  Leon connected at0W, no new transaction or pending delivery, and unchanged admission/allocation/intent/plans.
  HA status is healthy; all21 functional automation states are preserved. Validation:1168 full-suite cases,
  54 final focused cases and40 caught condition mutations.
  A dedicated fixed6A/two-minute HA test action now adds scoped local Start while retaining the generic Start
  prohibition. Fresh native admission, per-request generation guards, independent deadline Stop and interrupted
  restart-to-pause apply; no caller-selected tag/current/duration or Start replay is exposed. Cloud arming performs
  no Start and requires actual consumed-grant/mutation/native-session proof. Both test routes finish paused before
  an explicit Return. Both physical two-minute tests passed: HA/local53Wh and Android iCS/cloud50Wh, actualStops
  about2seconds after theirtargets, cloudacknowledged/emptyqueue/verified0. ExplicitReturn restoredshared6A with
  Leonconnected at0W, unchangedsettings/plans/all21functionalautomations and29archivedrecords(threeolderomissions).
  The expired-first-tag selection bug was fixed without clearing cache evidence; final31 focused cases pass,
  alongside1200 full-suite and25 actual-HA cases. The timed test control/progress is published on the dashboard.
  Normal6A local **Start charging** is implemented from app idle or verified pause, with no two-minute cutoff after
  real session confirmation. It atomically binds the first actual transaction, distinguishes0Wsession from measured
  charging, preserves Pause/preemption/restart-to-zero and ends genuine short sessions without stopping later app or
  native-timer activity. Start/Return reserve Pause capacity, and existing authorization/history/electrical gates and
  timed diagnostics remain. The regular dashboard control is deployed: HA Start from verified pause charged beyond
  two minutes at 6 A, delivering 65 Wh with a 1464 W peak. HA Pause closed the actual session at 0 W, cloud acknowledgement
  completed and the queue emptied. Explicit Return restored app/schedule control at 6 A with the lead connected.
  Configuration, saved plans and all 21 functional automation states remain unchanged. All 26 admitted history bodies
  are retained within 30 archived records; 27 are currently returned and three older omissions remain tolerated.
  The current extension implements explicit zero-protected USER32 commissioning, an idle-only 6–32 A current selector
  and normal local Start during cloud absence. It preserves old admission/history bytes, current/revision/Pause
  guards and genuine cloud backfill. All 27 integer settings passed actual idle readback without Start, and paused
  16/32 A selections retained zero. Both one-minute HA local charging tests passed with a 32 A offer: cloud connected
  and deliberately disconnected, 61/64 Wh delivered, real Stop/zero and genuine offline acknowledgement afterward.
  The Leon drew up to 14.4 A, so full 32 A vehicle load was not established. Both tests ended before 14:38 GMT.
  HA Return restored app/schedule control at 32 A with the lead connected at 0 W; original admission/history bytes,
  saved schedule and all task-baseline automation states remain unchanged. Validation: 1273 full-suite/67 actual-HA
  cases pass. There are 32 archived/29 currently returned history records, including three tolerated older omissions.
  Existing original profiles now block new trial writes/clears;
  preservation failures remain durable. Earlier cleanup lost a saved inactive TxProfile, so selective clearing beside
  unrelated profiles, wider app features and unattended operation remain unproven. The original
  cloud connection, configuration, saved schedule and history are verified. Complete app compatibility, current
  changes during an open session remain separate work. Managed zero-only offline restart recovery is implemented:
  a current-introduction cloud failure persists Pause, fresh complete native/Boot/owned0 and reconciled lifecycle
  facts establish local readiness, queued observations remain untouched, and reconnect never implicitly releases
  the hold. Slow registration alone preserves ordinary online startup. Validation: 1288 full-suite and 67 actual-HA
  cases pass. The connected idle cloud-blocked replacement passed with a fresh process/Boot, higher-revision Pause
  and local readiness at zero. A queued observation was retained then genuinely acknowledged; reconnect kept Pause
  until separate HA Return restored app/schedule 32 A. All 43 native observations were zero/closed, with no new
  transaction/id and unchanged full configuration, saved plans and admission/history bindings. The lead stayed in;
  no additional charging test or Start was issued. The offline paused dashboard remains accurate during backfill.
  Commissioning of this installation is closed for routine use on 2026-11-10 at the owner's request. App/schedule
  authority remains the daily default at 32 A, proxy/health services are enabled, and the five deliberately disabled
  EV/reserve automations remain off. No more dedicated charging, cable or phone tests are planned for the delivered
  features. Follow-up uses normal charging logs or a concrete fault; optional extensions do not block daily use.

- [~] One charging plan shared with the Raedian app (`ocpp_hub.set_plan`, answers and takes over the app's planning, written to the charger as a recurring profile). Tested with a fake charger and vendor.
- [~] Schedule helper and weekly schedules: a Home Assistant schedule helper applied with a button (daily or weekly profile), and the vendor answered with the charger's real schedule. Tested with a fake charger; see the schedule item below for what is unproven.
- [~] Schedule: read-back of the charger's 24-hour schedule and the service `ocpp_hub.set_schedule` (daily, recurring, under the ceiling). Tested with a fake charger; the NEO's behaviour with a recurring profile is unproven.
- [~] Limit sync: the hub's and the vendor's limit are saved in Home Assistant, shown together with the limit in effect (read from the charger when it can say), and the vendor is answered with it. Tested with a fake charger and vendor; whether the NEO answers `GetCompositeSchedule` is not known yet.
- [~] Socket entities `plugged_in`, `car_charging`, `problem` and `error_code`, derived from the status and error code; and a debug log of the raw frames. Tested with a fake charger; not yet seen on the NEO.
- [~] Charge sessions: a current-session sensor, an unbounded stored history, `_last_session` with the newest charges, the service `ocpp_hub.get_sessions`, and a ready dashboard (`docs/dashboard.md`). Tested with a fake charger; not yet seen with a real charge.
- [~] Control: a charge limit `number` (0 to the ceiling, default 16 A, under 6 A pauses) and a start/stop `switch`, through one guarded write path in `core/control.py`. Tested with a fake charger; whether the NEO obeys a charging profile is unproven.
- [~] Passing the vendor's commands on to the charger (option, off by default): everything except Reset, firmware and diagnostics, under the current ceiling, with transaction ids translated. Tested against a fake vendor and charger; not yet tried with the real vendor.

## Open Raedian and older-integration work

- [ ] Prove the charging plan on the charger and in the app: the app's reading of the plan shows the hub's windows (the format of a filled answer was assumed equal to a write; the first answer of the charger seeds the plan), and the charger follows the recurring profile. What the Raedian planning on the charger itself does is unknown; the hub no longer writes to it.

- [ ] Check the vendor side of a real charge: that the status changes and the charge show up in the Raedian app, and what the vendor sends back.
- [ ] Read the raw frames of a charge (logger `custom_components.ocpp_hub.frames` at debug) to see every measurand and field the NEO sends, and decide from that which further entities are worth making.
- [ ] Measure a full restart of Home Assistant (a reload is measured: 14 seconds).
- [ ] Prove that the NEO follows a recurring daily schedule (set one, watch the first change of period). The service and the read-back exist.
- [ ] Persist the mirror queue and a charge that is running across a restart of Home Assistant (today both are lost; documented in the README).
- [ ] Optional username default for the vendor: use the charger's serial when only a password is given. Build only if the vendor answers HTTP 401.
- [ ] Complete the older integration's public/HACS release work. Use `pyproject.toml` and both manifests for the current version, and the changelog for unreleased work; this milestone does not describe the standalone service deployment.
- [ ] Remove the unused add-on copy on the Home Assistant server (needs the owner's yes).
- [ ] Option to switch off the connect notification.

## Recorded older-integration follow-ups (P2)

- A vendor can send many `TriggerMessage` requests; the answers are not rate limited.
- A vendor transaction id the mirror cannot map is logged on every frame.
- Two charger ids that slugify the same would share entity ids.
- The lock around the hub's own requests has no acquisition time-out.
- Reading configuration key by key is slow with a charger that stays silent.

## Decisions that differ from the first plan

- The original Raedian design used one HA integration without an add-on or MQTT. The later iCS deployment uses an independent service and thin HA client; it does not install that older HA listener.
- `ClearChargingProfile` from the vendor is answered `Unknown`, not `Rejected`.
- The mirror sensors are per charger, not one for the whole hub.
- Configuration is read one key per request, and the hub asks for a boot and a status with `TriggerMessage`, because the NEO answers only one key at a time and sends neither after a hub restart.
