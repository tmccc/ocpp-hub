# Changelog

## Unreleased — iCS prototype and commissioning package

- Retry ordinary charger introduction after transient native observation failure, with the same permanent binding
  and ownership checks. Failed read-only vendor RPCs no longer create uncertain electrical-command debt.
  Conservatively migrate recognized old read markers during startup into a verified Pause.
- Reconcile an unconfirmed owner release after a new explicit Pause and fresh current-Boot, closed all-phase zero,
  original configuration/history and settled-journal proof. Retain zero until a separate Return to app/schedule.
- Retire vendor mutations and provisional Start grants when a final guard proves the command was never sent.
  Preserve uncertainty and the Start grant after cancellation, malformed replies or peer errors following dispatch.
  Update profile transaction ownership
  only on an exact Accepted reply, including replacement acknowledgements arriving after the previous Stop.
- Pace restrictive timer retries by elapsed time so wall-clock corrections cannot extend a failed zero or Stop
  cooldown. Once cutoff has been observed, retain that occurrence's Stop duty across a clock rewind. A restarted
  gateway retries an authorized restrictive action without inheriting a stale wall deadline.
- Allow history-retention acknowledgement under the journal's commissioned native32 capability as well as native6,
  preserving the original configuration binding, matching-fault, actual-zero and settled-journal requirements.
- Add a supervised one-off native timer alongside the original daily schedule, with verified installation
  and automatic selective cleanup after charging or a missed start. Delayed starts end at 06:00 local
  time. The proxy supervises closure of the identified session, retires the timer on an observed
  unplug, preserves it across idle current changes and reports unconfirmed cleanup to Home Assistant.
  Enforce the cutoff during interrupted telemetry or a pending Pause, and retry the read-only
  charger introduction after a temporary observation failure on reconnect. Hold unidentified open sessions
  at zero until source verification, including across a proxy restart. Preserve independently acknowledged
  starts while retiring the timer and recover unplugged sessions that still await their final Stop.
  Preserve archived cutoff duties when a later unsent booking is cancelled. Keep independent-session
  continuation conditional on the current source, and recover either temporary Start report order after
  an interrupted current selection without stopping the session before its deadline.
  Keep reading the source while reconnect recovery is pending, and hold a newly observed unidentified
  session at zero immediately. Restore normal daily operation after an independent session closes while
  preserving an expired manual Start's outstanding report. Record a matching unnumbered native history
  report under verified zero without treating it as proof of the charging source.
  Recover a verified daily that shares an expired manual Start's tag without consuming that command's
  reporting debt. Keep an established cloud lifecycle channel open during temporary zero-profile
  verification so a genuine Start acknowledgement can finish.
  Revoke that cloud introduction if the charger reports changed Boot metadata, including on the same socket.

- Renew an expired, previously accepted schedule tag against the connected cloud when the iCS sends a direct
  StartTransaction. Concurrent retries share the decision. Archive the firmware's unnumbered, zero-draw start
  attempts and exclude them from session and transaction numbering. Add guarded offline reconciliation for
  installations already held at zero by this history fault.
- Close commissioning of the installed iCS setup for routine use. Verify app/schedule control at 32 A and enabled
  proxy/health supervision; retain the five deliberately disabled automations. No further dedicated charge, cable or
  phone test is required for the delivered features. Use normal operation logs for follow-up; optional features
  remain separate from daily-use acceptance.
- Add managed zero-only recovery after a proxy restart during cloud absence. Preserve queued observations without
  acknowledging them; retain strict lifecycle, mutation, Boot, native configuration, history and exact zero-receipt
  checks. A proven failed cloud attempt for the current introduction converts app intent to a durable Pause before
  recovery; slow registration and older failures do not. Reconnection retains Pause until explicit Return. No Start
  or positive ceiling is sent by recovery. Validation: 1288 full-suite and 67 actual-HA cases pass. Verify a real
  cloud-blocked process replacement at zero: fresh Boot, forced Pause and local readiness, retained observation with
  real later acknowledgement, and no automatic Return after reconnect. Explicit HA Return restores app/schedule 32 A.
  All 43 native samples remain at zero, with no new session/id, unchanged configuration/plans/admission/history and
  the lead connected. Correct the dashboard's verified-pause explanation while current selection awaits backfill.
- Add an explicit, zero-protected native32 capability and an idle-only 6–32 A HA current selector. Preserve original
  admission/history bindings, separate selected current from the paused zero hold, bind request amperage and revision,
  verify actual owned profile/composite state, and retain Pause priority and interrupted-update recovery.
  Permit guarded normal local Start during cloud absence using fresh local readiness and current static authorization;
  retain lifecycle/uncertainty checks, actual deferred cloud acknowledgements and the stricter Return/restart gates.
  Keep the two-minute diagnostic restricted to a selected 6 A. Deploy and verify all 27 integer settings without
  Start, plus paused future selections retaining zero. Verify two one-minute HA local charges at a 32 A offer, with
  cloud connected and disconnected: the car drew up to 14.4 A, delivering 61/64 Wh before real closed-zero Stops.
  Confirm genuine offline replay/acknowledgements; restore app/schedule control at 32 A with the connected car at 0 W,
  unchanged saved plans/automations and all original history bodies retained in the archive. Both tests finished
  before 14:38 GMT.
- Add normal6A **Start charging** in HA from idle or verified pause, without the timed test's two-minute cutoff.
  Bind the actual transaction atomically and immutably; confirm known sessions at0W without claiming charging.
  Preserve Pause races, guarded post-ACK intent updates and restart-to-zero/no-replay. Complete genuine short
  sessions without pausing later app/native activity. Reserve Pause capacity after Start and Return; report fresh
  local-session/charging state while retaining all existing timed-test controls and current safety gates.
  Deploy the regular dashboard control and verify HA Start from pause, charging beyond two minutes, and HA Pause
  to a closed session at 0 W: 65 Wh delivered, 1464 W peak, cloud acknowledged and no delivery debt. Explicit Return
  restores app/schedule control at 6 A with the lead connected; configuration, saved schedule and automations remain
  unchanged. Preserve all 26 admitted history bodies within 30 archived records, including three older omissions.
- Add an explicit fixed6A two-minute HA test charge with a one-shot guarded local Start, fresh native admission,
  independent deadline Stop, durable paused completion and no automatic Start replay after replacement. Preserve
  the unrestricted-Start prohibition and all current/history/resource/authorization safeguards. Bind cloud-test
  proof to the actual consumed grant and exact vendor mutation; cloud arming itself never initiates charging.
  Add fresh HA readiness/progress and tests for Pause races, capacity, storage failure, deadlines and both sources.
  Select a currently valid approved local tag while preserving expired/revoked cache entries; wait only for bounded
  zero-state reconciliation before the single Start attempt and retain private failure diagnostics.
  Verify actual HA/local and Android iCS/cloud two-minute charges:53Wh and50Wh, real Stop about2seconds after each
  target, both closed and cloud acknowledged. Restore app/schedule control at6A with the Leon connected at0W,
  unchanged settings/plans/automations and29 archived records; publish the timed dashboard control and progress.
- Allow general historical-record absence in explicitly acknowledged managed operation. Retain full originals and
  completed-record versions privately, report missing counts without blocking charging, and continue rejecting
  changed admitted records. Keep commissioning strict, allocation unchanged and all electrical/session guards.
  Add an exclusive offline acknowledgement with atomic archive/policy/receipt/fault retirement into inhibited
  recovery; fresh Boot and ordinary release remain mandatory. Apply the same history permission through guarded
  original-backend restoration. Test real TLS recovery, future omissions, alteration refusal and rollback.
  Deploy the policy with27 archived/25 currently returned records, recover actual app/schedule authority at6A,
  and verify the Leon connected at0W with no new transaction, no pending delivery and healthy HA status.
- Deploy the thin HA client and verify real Pause, persisted pause through service loss/restart, fault alert and
  recovery, and explicit Return at native6/owned6 with no new charge. Preserve saved plans and all27 native records.
  Replace obsolete dashboard cloud controls, slider and optional cached schedule/history with fresh local status,
  supported controls and native charger history. Expire the app-control label with its snapshot; verify actual HA
  template rendering, history table and unchanged functional automations, including five deliberately disabled ones.
- Add authenticated same-host management and a separate thin Home Assistant client, with measured status/history,
  explicit Pause/Return, timed stale-reading invalidation and persistent fault notifications after60seconds.
  Persist requested pauses atomically and across replacement; bind Return to its monotonic mode revision before
  claims and actual sends. Confirm physical zero/release, never retry an uncertain Return and keep local Start
  disabled/native6A unchanged. Require actual Stop delivery before reporting completed cloud acknowledgement.
- Add validated Docker component delivery to the existing deployment helper, retaining an external backup and
  digest verification without enabling the older OCPP listener. Add real native/OCPP/management socket tests and
  actual HA2026.11 config-entry/entity/lifecycle tests in an isolated matching image.

- Verify the first managed overnight native schedule: 00:00:00 start, 06:00:02 Stop, 5,614 Wh, acknowledged cloud
  lifecycle, empty outbox and 26 original records preserved alongside the new session. Load ceased around 03:52;
  the owner accepts scheduling and waives further active-load 06:00 cutoff testing. Retain the 6 A boundary.

- Treat routine cable removal as healthy in explicitly configured managed mode only when native evidence proves
  `A`/`Unplug`, a closed session, all-phase zero and output0 under unchanged native6/configuration/history/resources.
  Recover an already admitted service while unplugged without Start. Keep initial connected admission and32A
  restoration strict, expose actual vehicle connection and retain private validation failure details.
- Validate1088 offline cases and12 unplugged-condition mutations, deploy the correction, reconcile the reported
  unplugging fault under independently verified owned0/closed-zero, and recover shared6A app/schedule control.
  Confirm zero power, no Start/transaction, intact26-record history/plans and successful health monitoring.

- Add persistent bounded app/schedule supervision with durable records, process watchdog/status, disabled local
  Start and verified zero-to6 owner-control restoration without RemoteStart. Preserve native timetable identities,
  exact history queries during recovery and saved mode restrictions across entry points. Faults retain inhibition;
  fresh native/cloud/Boot/resource evidence is required for healthy status and shutdown retires pending releases.
- Install the bounded managed service with automatic startup/restart and independent minute health checks. Verify
  actual idle process-loss recovery without Start, USER6/owned6, all-phase zero, empty ledger/outbox and unchanged
  26-record native history/plans with the lead attached. Retain the initial unconfirmed release/manual zero
  reconciliation as a limitation, and preserve private diagnostics for future release failures. Overnight operation
  remains to be observed; no Home Assistant change.

- Validate actual proxy process loss during acknowledged 6 A charging: observe the same session still drawing 6.1 A
  while the old process is absent, then genuine replacement Boot, station0/exact-known Stop, real closure/cloud ACKs,
  explicit zero-retaining recovery acknowledgement and a separate real charging restart. Preserve one original
  monotonic charge budget across processes and restore original 32 A/cloud/plans with the lead attached. All23 starting
  history bodies remain plus three genuine sessions, including the safely stopped preparatory attempt.
- Validate a real connected 6 A load session through the proxy: 50 Wh in 138 seconds, stable 6.1 A/approximately 1.45 kW
  before and during an active cloud outage, local offline Stop with independently verified all-phase zero, and genuine
  queued-event upload/acknowledgement. Restore original 32 A/cloud/configuration/plans, preserve the fresh 22-record
  baseline plus the new charge, and remove all trial profiles with the lead attached. No permanent deployment.
- Add explicit native-bounded recovery acknowledgement that requires fresh native6/closed-zero evidence, current
  introduction, confirmed station0 and real history delivery before rearming controls. Retain zero, reconcile exact
  known active Stop and actual Stop replay after process loss, and keep an interrupted cleanup frozen.
- Expose the native strategy through dormant CLI validation, explicit connected baseline capture and supervised run,
  with exact listener/baseline binding and mutually exclusive vehicle-pause configuration.
- Validate connected local Start/Stop, real offline lifecycle retention/upload and actual proxy process replacement
  under native6. Confirm automatic zero recovery, rejection before explicit acknowledgement and a subsequent separate
  Start/Stop without unplugging. Restore original 32 A/cloud/plans, preserve all 20 earlier records and retain three real
  cloud-acknowledged test sessions. No power was drawn in this control/lifecycle trial.
- Add explicitly configured connected commissioning under the charger's persistent native6A limit, independent of
  SEAT pause telemetry. Recheck actual native limits at admission/Boot and inside positive/cleanup RPC gates; retain
  zero on introduction and reconnect, reconcile real lifecycle events, and restore the original route before32A.
  Keep uncertainty pending, exact owned cleanup station-last, original data preserved and final completion atomic.
- Validate native6 connected handover live with the lead attached through introduction, zero hold, profile cleanup
  and original-backend restoration. Independently verify original 32 A configuration, all 20 history records, saved plans
  and no remaining trial resources; preserve the live Home Assistant setup.
- Add a fresh reversible physical pause verifier with positive6A offers, complete native phase checks, sampled
  60-second zero windows and five-second current-result expiry. Handle observed publication lag and live meter
  telemetry without weakening completed-history, resource or durable handover-permission checks.
- Add automatic read-only SEAT pause assessment with a fixed TLS GET, current access borrowed without refresh,
  explicit cloud-source-age uncertainty and a preflight blocker before connected-handover mutation. Keep diagnostic
  reports separate from permission authority; validate the live idle response with optional power absent.
- Add a separate connected-handover controller with fresh vehicle/provider/nonce/Boot bindings, independent native
  USER6 verification, distinct closed-zero samples, explicit-release pause lifetime and staged original restoration.
  Keep production disabled without an explicitly integrated live provider; historical pilot files cannot enable it.
- Reconcile lost Clear and vehicle-restore replies without repeating verified deletions or uncertain Starts. Resume
  cleanup without reapplying a removed ceiling, preserve original resources/history, and observe genuine normal
  manufacturer-side charging only after original permission has been independently restored.
- Verify car-side pause through a normal-backend charger restart and fresh 6 A Start offer for 60 seconds; restore
  original settings and car permission with the lead connected, retaining all ten old records plus two real sessions.
- Automatically reserve fresh local transaction numbers above complete retained native history, pin both identities,
  preserve durable counters and receipts across restart, and reject history advancing during initial handoff.
- Add offline-verified connected restart under a confirmed zero-current hold, exact native-session matching, saved
  configuration/Boot/station binding, read-only silent-Boot probing and explicit owner recovery acknowledgement.
- Block app/owner positive writes until initial native admission completes. Refresh snapshots overtaken by legitimate
  control/session/receipt changes within a bounded budget; preserve frozen cleanup phases after late audit failures.
- Wait for pending Set/Clear replies before native ownership classification; retire the old digest on an actual accepted
  Set transition while retaining sticky faults and detection of subsequent unrelated native-byte changes.
- Keep ordinary unplugged cleanup unchanged and connected cleanup unavailable without fresh independent interlock
  evidence. No live charger, Home Assistant or permanent service change accompanies the offline implementation.
- Verify physical local Start, nominal 6 A charging and local Stop with the vendor link retired, then real cloud
  acknowledgement of all seven queued events after local authorization expiry, preserving immutable session data.
- Preserve all nine previous history bodies plus the new 38 Wh session; complete guarded cleanup and normal-backend
  restoration. Record the private allocation bootstrap used in that trial; its automatic CLI replacement is now
  implemented and validated offline above.
- Verify an unplugged local zero-current hold while the vendor connection is deliberately retired, followed by
  successful cloud reconnection, 6 A release and complete restoration with all nine history records unchanged.
- Verify the corresponding Start/Stop/backfill path with guarded synthetic TLS peers before the physical test.
- Record three real charging sessions through iCS2.0Lite with app Start/Stop/restart and local Stop/release.
  A 6 A station setting gave about 6.1 A reported current, peaking at 6.2 A; retain the measurement distinction.
- Verify all lifecycle acknowledgements, preserved original history plus three new records, unchanged app current
  setting/schedule/configuration, guarded profile cleanup and original-backend restoration after the physical trial.
- Record successful automatic confirmation on three introductions of the corrected source, while retaining explicit
  limits for wider app features, unrelated-profile deletion, connected-vehicle restart and unattended installation.
- Reproduce the exact 154 ms future schedule that caused intermittent startup rejection with unchanged epoch/Boot.
- Validate complete composite replies before classifying clock jitter; retain strict present-time coverage.
- Retry only read-only schedule queries after a single Accepted profile: at most three reads, small future offsets,
  250 ms waits and one monotonic second including retry RPCs. Preserve Stop, ticket, desired-state and device guards.
- Record verified unplugged Stop0/Release6, profile-free cleanup and exact fresh-baseline restoration in later trials.
- Request a 60-second composite schedule while retaining strict coverage-at-response, limit and connection checks.
  Reproduce the old one-second failure with whole-second timestamps and delayed real TLS peer replies.
- Block fresh trials, new profile writes and automatic clears when saved original profiles could be affected.
  Keep legacy journals readable and retain a recorded zero hold/known-session Stop without resurrecting removed profiles.
- Persist original-resource preservation faults, independently record owned absence, freeze rollback before auditing,
  and expose safe fault counts. Model broad clearing and inactive-profile pruning as separate unproven hypotheses.
- Revoke cached readiness and freeze writes after owned-profile drift or unexpected inventory, retaining safety Stop.
- Record the second unplugged trial: the 6 A Absolute profile and cloud observation worked; cleanup lost an original
  inactive profile. Original connection/settings, schedule and history are verified, but resource restoration is incomplete.
- Standardise new local Boot/Heartbeat clocks to UTC seconds without rewriting retained event timestamps.
- Preserve registration acceptance, per-leg failures, request stages, reply-match lengths and numeric close codes,
  including normal remote closes; exclude protocol bodies, identities and arbitrary remote reasons from diagnostics.
- Add strict single-key station capability discovery and explicit Relative/Absolute profile choices, including stack 0.
  Pin choice/capabilities to the device introduction, discard mixed-Boot discovery, and keep Stop authoritative.
- Carry Absolute anchors through ownership and native rollback checks; verify schedules at response completion.
  Extend the independent firmware model and real TLS tests for timing, rejection, recovery and unchanged replay.
- Keep the latest restart corrections offline. Earlier trials verified nominal physical current limiting and core app
  controls; the new restart path, wider app compatibility and unattended deployment still need live validation.
- Separate the original OCPP identity from the native hardware serial and validate both before native admission.
- Correct credential preparation: iCS authentication keys are write-only, so blank native backups cannot restore them.
- Add explicitly configured absent-header admission while retaining exact source/path/TLS; never infer a fallback.
- Interpret only the observed missing OCPP status file as disconnected so a restarted device can enter the listener.
- Record the authorised unplugged trial and verified original-state restoration. Local TLS/Boot and isolated cloud
  registration worked; the 6 A station profile was rejected and the continuing cloud session remains unresolved.
- Add a separate standalone commissioning package with dormant private-file preflight, exact endpoint admission,
  independently configured credentials, certificate name/date checks and a bounded 6 A run. Keep offline adapters intact.
- Add a complete read-only native observer with bounded asynchronous HTTP and strict JSON/framing validation.
- Record trial resource intent independently of retired transaction aliases. Preserve originals, freeze rollback,
  clear only verified owned profiles with the station cap last, and verify original-backend reconnection separately.
- Add a restricted unplugged recovery listener and retain safety Stop during recovery; preserve ambiguous history.
- Add strict expiry/parent/revocation handling, nested authorisation preflight and immutable lifecycle reply metadata.
- Supply configuration/service examples and pinned runtime requirements. Keep Home Assistant undeployed and unchanged.

- Add an isolated HA-free durable gateway and an offline TLS runtime. Keep the normal Home Assistant setup unchanged.
- Keep local controls independent of cloud reconnects, registration retries, negotiated OCPP heartbeats, Ping/Pong and
  quarantined history delivery. Retire a cloud session when its actual charger connection disappears or changes.
- Give local stop priority and persist/reapply a zero-current station profile in the offline model. Verify schedule coverage,
  preserve lower resume ceilings, finish owned operations after client cancellation and roll back failed releases.
- Add exact observed `IoCharger` metadata, Fast/Eco/EcoPlus, recurring/one-off plan and delayed-stop codecs, with explicit
  policy opt-in, active-transaction translation, stable plan namespaces and confirmed ownership before clearing.
- Handle changing app-generated tags with optional bounded single-start grants. Commit grants, lifecycle facts,
  receipts and queued events atomically; quarantine ambiguous mutations and storage failures rather than invent success.
- Bound ids/resources, retire transaction profiles without alias reuse, reject malformed JSON/time/type variants and
  preserve actual peer CALLERROR fields. Generated OCPP request ids fit the 36-character maximum.
- Retain complete privacy-transformed 517-, 551- and 721-frame captures with provenance. Replay all 365 CALLs of the
  controls capture through both authenticated loopback TLS legs, including independently modelled 6 A profile effects,
  nine explicitly simulated missing meter acknowledgements and one deliberately refused diagnostic upload.
- Capture a subsequent vehicle-reported finished charge: physical power stops and SuspendedEV is reported while
  the OCPP transaction remains open even after unplugging. Replay the complete 789-frame trace without inventing a StopTransaction.
- Document actual authorised Android-app trials and their restoration limits. Those trials used the original backend;
  no proxy was connected and no live Home Assistant setup changed. Hardware ceiling enforcement, authenticated cutover,
  full authorisation/cache semantics and real cloud history replay remain unvalidated. This is not a release.

## 0.2.1

First public release. The same as 0.2.0, whose release job did not run.

- Fix: the release gate uses the newest run of each required check.

## 0.2.0

First public release.

- Brand images (icon and logo) in `custom_components/ocpp_hub/brand/`, shown by Home Assistant 2026.3 and newer; a README for a public audience; issue templates and private security reporting.

- A calendar entity per charger (`calendar.ocpp_hub_<id>_charging_plan`) shows the charging plan as repeating events, so it can be seen in the calendar dashboard and used in automations. It is read-only; the plan is still changed in the app or with `ocpp_hub.set_plan`.

- The planning switch of the Raedian app (command `b3`) is followed: with the planning off the hub clears its profile so the charger charges freely, with it on the plan is written again. The plan itself is kept either way, and `_app_plan` shows `planning` on or off as an attribute. Which value is on and which off was inferred from four switches.

- Fix: a window the app has switched off is kept in the plan and answered back to the app (it was dropped, so a new line added while the app's planning was off came back empty); only switched-on windows reach the charger.

- `tools/raedian_frames.py` lists the Raedian protocol frames in a Home Assistant log and marks the commands the hub does not decode; `docs/raedian-protocol.md` documents the frame, the commands seen so far (clock sync, device information and others, mostly undecoded) and how to add one.

- Fix: a window the app switches off (instead of removing it) is left out of the plan, so taking a day out of the plan in the app works.

- Fix: the app's planning question is answered before the limit on waiting vendor commands (the app opens with a burst of commands and the planning question was turned away with the rest); the limit is 16 instead of 4.

- Fix: the weekday byte of the app's planning is a day mask (Monday 1, Tuesday 2, Wednesday 4 ...), not a day number. The plan the hub answers now shows on the right days in the app, and a window for several days is read as one window per day.

- One charging plan kept by the hub, shared with the Raedian app: the app's planning is answered from it and what the app writes is taken over by it, `ocpp_hub.set_plan` sets it from Home Assistant, and it is written to the charger as a recurring profile (limit inside and outside the windows from the options). Needs commands passed on. Experimental: what the charger's own firmware planning does is not known.

- The planning of the Raedian app is decoded from its DataTransfer messages and shown as `_app_plan` (the windows per weekday). The protocol layout and its check byte are documented in `core/raedian.py`.

- When commands are passed on, the vendor's own `DataTransfer` channel is passed on too (the Raedian app uses it for its planning screen); the request and the charger's answer are logged in full. Reset, firmware and diagnostics stay refused.

- Schedule helper: choose a Home Assistant schedule helper in the options; the button *Apply schedule* turns its blocks (inside/outside limits, optionally a limit per block) into one recurring daily or weekly profile, under your ceiling. Nothing is written until the button is pressed. The vendor is answered with the charger's real schedule when it asks for more than the present moment.

- Schedule: the charger's schedule for the next 24 hours is shown as an attribute of `_limit_in_effect`, and the service `ocpp_hub.set_schedule` installs a daily recurring schedule of limits (under your ceiling; under 6 A pauses).

- Limits: the limit Home Assistant set and the one the vendor's app set (when commands are passed on) are saved and shown again after a restart, `_limit_in_effect` shows the limit that applies (asked from the charger with `GetCompositeSchedule`), and the vendor is answered with it when it asks for the composite schedule.

- New entities from the status the charger already reports: `plugged_in`, `car_charging`, `problem` and the diagnostic `error_code`.
- Raw OCPP frames are written to the log when the logger `custom_components.ocpp_hub.frames` is set to debug (off by default).

- Session information lives with the session: one `_current_session` sensor (energy so far, with start, duration, tag, peak and average power, highest current per phase, offered current and the hub's limit as attributes) replaces the three loose session sensors, and every stored charge now carries the same numbers.
- Power and phase currents read 0 instead of unknown while the socket delivers nothing, and a reading that carries only the energy register no longer erases the rest.

- Option *Commands from the backend*: pass the vendor's commands on to the charger (off by default). Reset, firmware and diagnostics stay refused; a profile above your current ceiling and configuration keys that could cut the hub off are refused too. Logged with `[forward]`.
- A button *Clear charge limit* removes the hub's profile from the charger.

- Control: `number` charge limit (0 to a ceiling you set, default 16 A; under 6 A pauses) and `switch` to start and stop a charge. The ceiling is an option; every write is logged with `[write]`.

- Charge sessions: the running charge is shown live (`_session_energy`, `_session_started`, `_session_duration`), every finished charge is kept without limit in Home Assistant's storage, `_last_session` lists the newest ones, and the service `ocpp_hub.get_sessions` returns the whole history. A ready dashboard is in `docs/dashboard.md`.

## 0.1.0

- Scaffold release: repository tooling, CI (checks, hassfest, HACS validation), a release gate that waits for green checks on the tagged commit, and an integration shell with a config flow for the port.
- The engine in `core/` starts with the OCPP-J frame helpers; it imports nothing from Home Assistant.
- There is no OCPP server yet. Setting the integration up only stores the port.
