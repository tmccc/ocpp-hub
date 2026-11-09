## Standalone iCS prototype — real app and local control verified in a supervised trial

This branch adds a separate Linux/Python 3.14 trial package on top of the recorded offline prototype below.
The latest automatic allocation and restricted restart changes are validated offline only. They have not been run on
the household charger, installed as a service or loaded into Home Assistant. The seven physical trials below describe
the preceding supervised implementation.

Seven authorised trials connected the charger temporarily on 8 November 2026. The fifth included real charging through
the proxy and Android app. It verified app Start/Stop/restart, local Stop with a zero-current hold, release back to app
control, metadata reads and the existing Fast-mode write. The station setting was 6 A; the meter reported about
6.1 A while settled, with a peak of 6.2 A and 1,518 W. This is evidence of nominal current limiting, not an exact
mathematical maximum of 6.0 A.

Three actual sessions delivered 77, 20 and 32 Wh. Every session closed and received its cloud acknowledgement; all six
original full history records remain alongside the three new records. The app's original 32 A setting was untouched:
the separate station profile limited the actual charge. The same accepted cloud connection handled all three sessions
and the local/app transitions without a queued lifecycle event or unexpected cloud fault.

After lead removal, two more charger introductions automatically confirmed the station limit. The owned profile was
then removed through the normal guarded path. Native snapshots independently verified the original full configuration,
empty profile inventory and saved overnight schedule, preserved history, a closed unplugged zero-power state and the
original backend online. The journal reached complete; the trial listener, runtime, capture processes and watchdog
stopped, and temporary phone preferences were restored exactly. No permanent service was installed. Live Home Assistant
was not changed; its normal integration does not load this prototype.

The sixth trial deliberately retired the vendor connection while keeping the physical charger connected locally.
Ordinary local Stop installed and confirmed the zero-current hold with the cloud absent. The original cloud runner
then reconnected and registered successfully, and Release restored the confirmed 6 A setting without a fault or
queued event. That test was unplugged; physical local Start and actual session-history backfill were exercised in the
seventh trial below. A guarded TLS-loopback test also covers the full Start/Stop/backfill path, including real synthetic
acknowledgements and retention of the zero hold. The sixth trial again restored the complete fresh baseline: original
configuration, empty profile inventory, saved schedule and all nine existing full history records, with the original
backend online and no charging session open. Its journal completed and all experiment processes stopped.

The seventh trial started a real charge locally while the vendor runner, sender and session tasks were retired.
The car charged at a nominal 6 A, peaking at 6.1 A and 1,528 W. Local Stop then installed the verified zero-current
hold and closed the real session, delivering 38 Wh. Throughout charging and Stop, vendor counters remained unchanged
and the local transaction had no cloud mapping. Start, Meter and Stop facts remained queued locally.
After the local authorization expiry had passed, the normal vendor runner reconnected. Actual cloud responses to Boot
and all seven queued events created the cloud mapping and emptied the queue, while the zero hold stayed active.
The original Start/Stop payloads and accepted local authorization metadata remained byte-for-byte unchanged.
After unplugging, normal guarded cleanup removed the trial profile and restored the original connection, full
configuration and schedule. All nine previous history bodies remain alongside the one new completed session.

Before that seventh trial, a separately reviewed private bootstrap reserved the fresh journal's transaction counter
strictly above every ID in complete native history, avoiding reuse of earlier proxy session numbers. Guarded tests
verified persistence across restart, a real TLS Start/Stop/backfill sequence using that range, and rejection of invalid
or exhausted IDs. The new offline implementation now performs that reservation automatically during fresh admission
and rechecks history at first charger introduction. Its first physical validation remains pending.

The earlier loss of an inactive profile during the second trial remains unexplained. Later successes start from a
profile-free baseline; they neither recreate that old resource nor prove selective clearing beside unrelated profiles.
Eco/EcoPlus, saved-schedule editing, indefinite operation, live automatic recovery while a vehicle remains connected and
full-battery auto-stop through the proxy remain unproven. This is a verified supervised control path, not a claim of
complete app compatibility or unattended production readiness.

### Automatic allocation and restricted restart — offline validation

A fresh `run` reads complete retained native history with the observed `getTransaction` request and exact empty data
object. Both OCPP and physical identities are checked, and native configuration is reread around the snapshot.
Before listening, an empty prepared journal atomically reserves its next local number above every retained transaction ID,
with bounded signed-integer headroom and private provenance. First introduction rechecks that history has not advanced
beyond the reservation during handoff. Errors, missing/duplicate/invalid IDs, changed hardware and exhausted ranges
fail closed before charging-capable writes. Restart retains the existing durable counter rather than reseeding it
from rolling history. This cannot reconstruct records already absent from native history; keep the durable journal.

Connected recovery requires an existing verified journal, matching native session ID, present owned station profile,
and unchanged configuration, station selection and Boot binding. Replacement transports immediately claim local zero
ownership. Recovery attempts and verifies the zero profile first, then requests Stop for the exact known session. If
the hold is refused, that known-session safety Stop remains available while readiness stays false. Recovery waits for
the real Stop message and cloud acknowledgements. Unknown sessions, changed bindings, missing profiles and incomplete
first commissioning remain explicit refusals. Ambiguous cloud Starts remain quarantined; no duplicate Start or
synthetic Stop is created. A silent charger is asked for an actual BootNotification after ten seconds; a saved Boot
never introduces a new connection.

Legitimate Stop/lifecycle/receipt changes can overtake the native HTTP snapshot. Both native observers compare the
current transport, Boot revision, phase and control/session/resource authority around each read, then discard stale
snapshots before ownership classification. Refresh is bounded to three attempts within fifteen seconds. Late audits
cannot overwrite frozen cleanup or recreate a removed profile. Tests reproduce these races over real TLS peers.
An in-flight Set/Clear reply must resolve before ownership classification. A genuine accepted Set retires the previous
raw-profile digest; a fresh read pins the newly confirmed bytes, and subsequent unexpected changes still block controls.

The explicit `resume` command acknowledges completed recovery but retains the zero-current hold. It requires the
current Boot/audit, actual native and journal closure, reconciled history and writes, intact resources and valid
allocation provenance. A later explicit `start` or `release` is still needed to permit charging. Legacy journals remain
usable for immutable receipt replay, Stop/history and guarded cleanup, while missing new provenance blocks new starts.
Recovery tests cover plugged idle, actual synthetic current flow, lost Stop messages, rejected zero profiles, lost cloud
Start replies, early app/owner writes, pre-Boot safety Stop and successive fresh journals with preserved old history.

Connected restart is separate from connected cleanup. Removing the safety profile and restoring the original backend
with the lead attached needs an independently verified vehicle-side inhibit. A subsequent normal-backend physical
test confirmed a separate native USER6 limit, car-side Stop/Start, and car pause surviving a closed-session charger
restart and a fresh iCS Start: a 6 A offer continued for 60 seconds with every current/power phase at zero. Original
settings and vehicle permission were restored with the lead still attached. All ten original history bodies remained,
alongside two genuine completed sessions totalling 129 Wh. This is capability evidence, not present permission.

The standalone proxy now has a separate, offline-tested connected-handover controller. Ordinary unplugged admission
and rollback remain unchanged. **No live vehicle-pause provider is supplied or enabled by default.** The command path
refuses connected cleanup before any freeze or write when that provider is absent; a historical pilot JSON, a zero
power reading, a command acceptance or an operator `allow_connected` flag cannot substitute for fresh vehicle state.
No charger or live Home Assistant change accompanies this implementation.

The third trial verified ordinary Stop at 0 A and Release back to 6 A with accepted 60-second composites and the same
accepted cloud session. Exact-id removal of the owned profile succeeded from a profile-free baseline; fresh native
readings and the journal independently confirmed complete restoration. The refreshed Android app displayed Idle.

The fourth trial recorded the exact startup failure after two successful introductions. At host completion
`16:38:44.845726Z`, the otherwise valid 60-second schedule began at `16:38:45Z`: 154.274 ms in the future. Socket epoch
and Boot revision matched. Observational instrumentation retained the actual validator sample without changing the
control outcome. This identifies a coverage timing failure rather than a connection loss or the former one-second
expiry. The trial then removed its owned profile and independently restored its complete fresh baseline.

### Bounded read-only startup retry

`prototype/composite.py` now validates the complete reply, every period, ampere limit and optional phase count before
classifying a small future start. The controller still requires `0 <= age < returned duration` at actual response
completion. After one Accepted profile write, a future offset of at most one second permits at most two additional
read-only schedule queries, separated by 250 ms. A shared one-second monotonic budget covers both retry waits and RPCs;
the initial request retains its ordinary timeout. Expired, malformed, excessive-current or more distant future replies
are not retried. Persistent skew remains unready; the code never repeats the profile write to conceal a bad read.

Each reply and each short wait rechecks the socket, Boot revision, actual operation ticket and original desired state.
A newer Stop or same-value operation supersedes the old verification. Release from a zero hold still works because
the desired-state snapshot is not incorrectly equated with the positive limit being prepared. Synthetic tests reproduce
the observed 154 ms failure, prove recovery through a fresh present-time read, bound a stalled retry, and retain strict
failure and safety-Stop behaviour. The corrected source passed all three introductions in the fifth live trial without
manual arming. The transient retry branch is reproduced deterministically offline; those live introductions each
received present-time coverage on their first read.

The first trial established:

- The charger accepted an encrypted TLS 1.2 local connection and introduced itself with BootNotification. It sent no
  Authorization header, including after a Basic challenge. Its original identity and write-only keys were preserved.
- A temporary self-signed allowance was needed for the local certificate; hostname and expiry checks remained enabled.
  The original strict trust setting was restored afterwards. This does not prove acceptance of a production certificate.
- The proposed 6 A `ChargePointMaxProfile`, relative kind and stack 16, received an actual correlated `Rejected` reply.
  Native inventory confirmed profile 7001 was never installed. Effective physical current limiting remains unproven.
- Two isolated upstream connections accepted the exact original BootNotification without Basic authentication. The
  proxy's continuing cloud session nevertheless disconnected and retained two status observations. The cause remains
  unresolved; iCS2.0Lite control through the complete proxy was not established.
- Rollback independently confirmed the exact original configuration, original schedule/profile inventory, unchanged
  session history, zero current/power, original backend online, no proxy profiles and retirement of the proxy socket.
  The trial process and independent restoration fallback have stopped. The car remained unplugged throughout.

The second trial established:

- The charger reported maximum stack 10, 96 schedule periods and capacity for 33 profiles. The earlier stack 16 was
  outside that reported range. An Absolute, stack 1, 6 A station profile was Accepted and independently visible in
  native inventory. This proves profile installation, not physical current enforcement with a vehicle.
- One manufacturer-cloud session remained accepted for at least nine minutes, acknowledged both Available statuses
  and recorded no reconnect or cloud fault. The refreshed Android app displayed Idle. Charging controls in the app
  and a complete charging session through the proxy remain untested.
- The first composite reply reported 6 A but its one-second interval had already ended at response completion.
  An independent read-only 60-second composite also reported 6 A and covered completion. The manual check did not
  override automatic readiness; that path remained unconfirmed during the live trial.
- Cleanup removed the owned station profile, but subsequent inventory also lacked the original closed-transaction
  TxProfile. The original-resource check correctly refused completion. The retained wire logs do not include the
  Clear exchange; the reviewed exact-id request path and receipt do not distinguish broad clearing from pruning an
  inactive profile. Neither explanation is established. No further clear or stale-profile recreation was attempted.
- Original configuration, schedule, session and history match the baseline; the original backend accepted Boot and
  Available statuses. Native readings show unplugged, zero current/power and no trial profile. The standalone
  listener and restoration watcher stopped, and temporary phone preferences were restored. The journal remains
  incomplete because the original resource inventory differs.

### Compatibility corrections and offline checks

New local Boot and Heartbeat replies now use whole-second UTC `Z`, matching the observed manufacturer's clock replies.
An independent fake charger copies that clock representation, and a deliberately format-sensitive fake cloud accepts
its new status updates. Separate tests reproduce Accepted Boot followed by status rejection and disconnect, including
normal close codes 1000/1001. This demonstrates the failure mechanism; it does not prove the real iCS server's cause.
Already recorded timestamps and event bodies are never rewritten. Retained fractional/offset statuses survive cloud
reconnect and process restart and can be delivered unchanged when the synthetic backend accepts them.

The standalone configuration now explicitly selects `station_profile_kind`, `station_stack_level` and
`station_profile_start`. Relative mode requires a null start; Absolute mode requires a whole-second UTC anchor in
the past, within the supported native epoch range. The example selects Absolute, stack 1 and a fixed 2020 anchor.
That choice was accepted in the second unplugged trial. The same choice is used for the 6 A ceiling,
zero hold, reconnect, ownership receipts and rollback. Rejection never switches kind or falls back to a transaction limit.

Before any station profile write, the trial reads six capabilities, one key per RPC: `NumberOfConnectors`,
`SupportedFeatureProfiles`, `ChargingScheduleAllowedChargingRateUnit`, `ChargeProfileMaxStackLevel`,
`ChargingScheduleMaxPeriods` and `MaxChargingProfilesInstalled`. They must establish one connector, SmartCharging,
ampere support, room for the profile and a maximum covering the explicitly reserved stack. Documented `Current`/`A`
values permit amperes; power-only, missing, unknown, duplicate or malformed replies fail closed. Stack 0 is valid if
explicitly selected and reported. The selected stack also reserves the gateway's cloud-policy boundary.
Discovery is repeated for a new connection and pinned to the device's Boot introduction. A Boot change during the
reads discards the entire snapshot. A changed introduction also invalidates readiness and any in-flight verification.
A concurrent Stop wins over a positive ceiling prepared before its capability replies arrived. Discovery failure
retains local inhibit and still permits the known-session Stop; it cannot falsely confirm a physical zero hold.

Absolute ownership includes the exact requested start instant. The native decoder accepts only narrowly
matching records and checks current/future coverage conservatively; its supported Absolute representation matches
the second trial's independently observed record. A different or unsupported representation blocks automatic cleanup.
The independent firmware model
now enforces kind, stack, start time, duration and capacity, and does not apply a future profile early. Composite
verification now requests 60 seconds and still requires the returned schedule to cover response completion. A real TLS
fake-device regression delays a whole-second schedule reply beyond one second: the old request fails, and the longer
request succeeds. Expired, future, short returned intervals, high limits and changed device introductions still fail.

Until selective clearing is proven, any original profile blocks a fresh trial and new profile writes, even if its
transaction is closed. Older journals remain readable for diagnosis and recovery, but cannot issue automatic clears
while their baseline contains original profiles. A recorded, still-present station profile can be lowered to zero
and the known transaction can be stopped; verified absence never permits recreating the profile during recovery.
Saved plans remain separately protected and do not count as charging profiles.

An original resource loss or change records a durable preservation fault and freezes further mutations. Independently
verified absence of an owned profile is recorded separately, even when that fault prevents completion. Status reports
the fault and missing/changed original counts without exposing identifiers or bodies. Later matching inventory does
not silently clear the fault. Separate synthetic broad-clear and inactive-profile-pruning models reproduce the
observed discrepancy without choosing an explanation or automatically recreating an old transaction profile.
Observed changes to an owned profile or unexpected native resources produce a separate durable inventory fault,
revoke cached readiness and block further writes/clears. A narrowly scoped zero hold and known-session Stop remain
available; repairing a matching readback alone does not silently reconcile that fault.

Private status now separates charger and vendor faults, preserves accepted registration counts after disconnect,
and reports request stage, whitelisted action, reply-id matching/length and numeric close codes. Endpoints, identities,
payloads, close reasons and exception messages are excluded. Raw private evidence from both trials remains outside
the repository. These later corrections perform no new charger, vendor or Home Assistant connection or deployment.

The station discovery and explicit Absolute option follow the
[pinned reference integration](https://github.com/lbbrhzn/ocpp/blob/c6a2fa0f994f5b25160c6cf77eb1fcdbf3f60d5b/custom_components/ocpp/ocppv16.py),
with stricter refusal of unknown capabilities. Nominal physical current limiting and core app/local controls are now
verified in the supervised trial; the wider app and unattended-operation limitations above remain.

`tools/trial_proxy.py` is the standalone entry point. Its default action is `check`, which reads private configuration,
credentials, baseline and certificates without creating a journal, doing DNS, binding a listener or contacting a device.
The original `OfflineRuntime` and its loopback-only adapters remain enforced independently. Both runtime paths share
only transport-independent connection lifecycle code.

```bash
python3.14 tools/trial_proxy.py --config /private/trial.json
```

A dormant configuration example and an **uninstalled** service template are in `packaging/trial/`. Runtime dependencies
are pinned in `requirements-trial.txt`; test/development dependencies remain in `requirements-offline.txt`.
`/usr/bin/openssl` checks listener certificate validity and the configured listener IP/DNS name during preflight.

### Connection and credential preparation

- Configure one literal private listener address/port, one exact charger IP and full `/charger/<identity>` path,
  and one exact upstream WSS URL including the original identity. Wildcard/public listeners, URL credentials,
  redirects, query strings and ambient HTTP proxies are refused.
- Keep the original OCPP `Id` separate from native `bPSN`: `identity` pins the connection identity (up to 64 characters),
  and `hardware_identity` independently pins the physical serial. Native observations must match both. Neither is an idTag.
- Incoming proxy authentication and manufacturer-cloud authentication are independently explicit. Each Basic secret
  is a private JSON object with `username` and `password`; the configured identity must match. The LAN observer has its own separate
  credential. Passwords are never command-line arguments or status/log fields. Android app Bearer tokens are not used.
- Either leg is explicitly `basic` with its private secret file, or explicitly `none` with that secret set to `null`.
  There is no automatic fallback. Incoming `none` refuses an Authorization header and still pins the source IP,
  full path and TLS listener; it is appropriate only after actual bounded observation proves that device behaviour.
  iCS OCPP authorisation-key fields are write-only and always read back blank; that cannot establish what its websocket
  upgrade sends. The selected mode needs independent evidence.
- A Basic leg requires a known charger password. Never overwrite an unknown existing cloud password for a
  trial: the native backup cannot recover a write-only secret. Omit those key fields from connection updates and rollback;
  do not submit their blank readbacks. A separate bounded private handshake observation can establish the existing header.
- Both legs retain TLS. Upstream CA and hostname checks are mandatory; listener key pairing, SAN/IP name and certificate
  dates are checked offline. Production certificate trust remains separate from the temporary trial allowance.
- Files must be owned regular files, not symlinks. Secrets/configuration and state are private to the runtime user;
  public certificates/CA files may be readable but cannot be writable by other users. Keys are loaded from verified
  bytes through anonymous descriptors, avoiding a second pathname-open race. State directory permissions are checked
  before SQLite creates its lock/WAL files.

### Baseline and bounded operation

The explicit `capture-baseline` action performs only the existing read-only LAN getters. It saves a private native
profile/plan inventory and original configuration backup. It does not change the charger. The native reader uses the
charger's existing LAN HTTP API at its exact IP, supports only the observed read-only requests, rejects duplicate JSON
keys and bounds each response and the entire observation without leaving background worker threads.
Ordinary audits inspect every current and power channel and the exact native session ID. Complete history is read only
for fresh admission and first introduction; existing recovery does not depend on fetching or reseeding rolling history.
After restart this firmware can report the exact missing `/tmp/Ioc/OCPP.json` envelope instead of an offline flag.
Only that observed OCPP-specific reply becomes `online: false`; other failed getters remain errors, and absence can
never establish successful original-backend restoration.

```bash
python3.14 tools/trial_proxy.py capture-baseline --config /private/trial.json
```

A fresh first trial requires native CP A, unplugged, zero current/power and an actually closed transaction. It refuses
all pre-existing profiles, including an inactive TxProfile, until selective clearing is proven. A baseline can still
be captured and inspected without admitting a new trial. Original plans and profile 7001 collisions remain protected.
A new private journal is required; an old journal can resume only with its exact original trial baseline.

Only the explicit `run` action opens the trial connections. It is restricted to a 6 A single-phase ceiling and a
60–1800 second duration, defaulting to 900 seconds. Its duration includes startup. No trial service is installed or
started by this repository or by preflight.

```bash
python3.14 tools/trial_proxy.py run --config /private/trial.json --duration 900 --interactive
```

The optional terminal interface accepts one JSON object per line:

```json
{"command":"status"}
{"command":"start"}
{"command":"stop"}
{"command":"release"}
{"command":"rollback-plan"}
{"command":"rollback"}
{"command":"confirm-original-backend"}
{"command":"exit"}
```

Local Start requires exactly one explicitly approved owner tag. Start/stop/ceiling operations retain their original
priority and ownership checks. Status separates transport connection, actual Boot, vendor registration, effective
composite confirmation, local hold and blocked history delivery. The service also writes a private status file.
A timer expiry or shutdown attempts a zero-current hold and known-session Stop, retains uncertainty if either cannot
be confirmed, and leaves its journal intact. It does not automatically restore the manufacturer's connection.

The trial permits only constant ampere profiles, an explicit Relative/Absolute station choice, and the metadata/Fast/delayed-stop
commands. Plan editing and Eco/EcoPlus changes remain disabled in this narrower trial path. New profiles cannot replace
any original connector/purpose scope, including a different transaction at that scope: actual iCS captures showed that
setting a new TxProfile can replace an older one. The existing offline codecs and tests still cover those wider app
shapes; enabling them live requires independently proven restoration semantics.

### Authorisation behaviour

Strict trial mode validates actual status, expiry and optional parent metadata. Trusted cloud RemoteStart performs
its upstream Authorize before acquiring the charger's outgoing gate. A nested Authorize/Start can then receive the
validated grant without a lock cycle. Dynamic starts require an actual future cloud expiry; missing expiry is never
invented as cloud evidence. Case-insensitive lookups preserve original wire text, and a parent tag never grants
permission to arbitrary sibling tags.

Configured local owner tags remain an explicit local authority with bounded local replies. Actual cloud denial or
expiry blocks a new Start, including a local API Start; permission is checked again inside its cap/control sequence.
A local Stop wins if it arrives while cloud authorisation is awaited. Metadata is bounded, and configured/current
revocations cannot be evicted into a fallback permission. A later missing-expiry response cannot widen a dynamic grant.
Start/Stop reply metadata is committed with the original lifecycle fact and retained exactly on historical retries.
Actual acknowledgement metadata updates the book atomically. Stop without an idTag never guesses which tag its
optional cloud metadata describes. Late cloud rejection of an already local Start retains the event and installs zero.

### Rollback and recovery

Rollback records resource intent before every write and keeps those receipts after normal transaction/profile aliases
retire. Adopted or original native resources are never treated as trial-owned. After verified deletion, an id which
reappears cannot become owned again without a new recorded trial intent.

`rollback-plan` observes the complete native inventory. `rollback` requires fresh native unplugged/zero-power evidence,
an actual closed session, no pending Start and reconciled mutating/lifecycle outcomes. It freezes charging-capable work
before auditing. If the idle audit fails, safety Stop and cloud history delivery remain
available. After proving idle it retires the cloud runner while retaining the local receiver, removes only independently
matching owned profiles when the original baseline has no profiles, and
clears station profile 7001 last. Every deletion is checked against a fresh physical inventory, including after a lost
ACK/restart. An ACK without actual removal cannot complete cleanup; reconnect cannot automatically reinstall a cleared cap.
An original-resource discrepancy remains a durable blocker even after connection restoration or matching later reads.

Resource cleanup ends in `backend_restore_pending`. Restore the charger's original connection profile from its private
backup as a separate operation. `confirm-original-backend` succeeds only once the proxy charger socket has retired and
fresh native readings show the original configuration, original resources, no owned profiles and a connected backend.
Configuration restoration alone cannot prove that reconnection has happened. History and any unsent observations remain
in the journal; they are not erased to make rollback look successful.

The separate connected path is exposed through `connected-handover-plan`, `connected-handover`,
`confirm-connected-original-backend`, `confirm-connected-original-settings` and
`restore-connected-vehicle-permission`. A commissioning adapter must explicitly supply a `PauseProvider`, the vehicle
identity digest and the immutable original full configuration to `TrialSession`; the default CLI supplies none.
The read-only SEAT checker described below is separate from that provider; there is no file-based permission import.
Home Assistant's deliberately read-only SEAT integration is not used for vehicle writes.

#### Automatic SEAT pause check

`prototype/seat_pause.py` reads the inspected SEAT charging endpoint through one TLS-verified GET. It pins the
vehicle and host, refuses redirects and does not refresh authentication, wake the car, send charging commands or
reuse cached success. `prototype/seat_access.py` can borrow current access and device proof from the existing private
production SEAT store, read-only and as its owner. It never exports or redeems the refresh token. Expired access,
rejected proof or an unresolved authentication rotation makes the check unavailable.

Use a private configuration file containing `session` (the absolute existing store path) and `vehicle_digest`
(the store's 64-character vehicle SHA-256). `python tools/seat_pause.py check --config /private/seat-pause.json`
validates files/access offline. The explicit `verify` action makes the read and prints only classified status.
Exit 3 means current pause is unverified; exit 2 means private access is unavailable. No credential belongs on the
command line. Do not copy the store or run another authentication-refresh client to make this checker work.

The standalone proxy accepts `--car-pause-config /private/seat-pause.json`. Its `verify-car-pause` command runs the
check on demand, and connected-handover planning/execution checks it automatically before any freeze or mutation.
Default preflight remains offline. The checker does not constitute a `PauseProvider` or enable connected admission.

The real endpoint has no established vehicle observation timestamp. Exact mode `stopped` is reported as
`pause_reported_source_age_unknown`; manual/timer/off modes, a completed charge or zero power do not prove the car's
pause permission. Missing optional power stays unknown, rather than becoming zero. Malformed or contradictory
readings fail closed. HTTP fetch time, HTTP Date and the separate vehicle-status timestamp never establish freshness.
Every report therefore retains `current_pause_verified=false`, `durable_pause_verified=false` and
`handover_authority=false`. No permission revision, release-only guarantee or pause lifetime is fabricated.

A read-only live check confirmed manual mode with a connected car; the genuine response omitted optional charging
power. This verifies the reader and its conservative classification, not current pause or unattended handover.

#### Fresh physical pause verification

`prototype/physical_pause.py` supplies a separate current-behaviour verifier. `PhysicalPauseObserver` brackets each
complete direct native read with the process's monotonic clock. `PhysicalPauseVerifier` requires real nominal 6 A
charging, an explicitly recorded car Stop, at least 60 seconds of sampled zero draw under an actual positive 6 A offer,
a car Start that returns to real charging, then another Stop and second 60-second zero window. An accepted command,
a full car or an app status alone cannot complete this reversible challenge.

Saved and runtime USER must both be 6 A, HWCFG and original hardware rating 32 A, and both `offerCur` and effective
`outLimit` must be 6 A. Native transaction identity, full configuration, schedules, profiles and history remain pinned.
Only live `MeterValues` on the same explicitly open native history row (`stopTime` is integer0) may grow; every other
field and every completed history body remain pinned. This only normalises the private comparison and never changes
a native record. Each zero window requires at least 13 distinct observations, a maximum seven-second sampling gap,
CP B and all three current/four power channels exactly zero. The observed session label may remain `Charging`:
it describes the open session, not power demand. Both known `Plug`/`Charging` labels are accepted alongside the
independent open-session and positive-offer checks. The current result expires five seconds from the
last native read's start. Reads are bounded to two seconds; owned control transitions have a 90-second deadline and
the whole challenge a ten-minute deadline.

The charger's CP and meter files may publish at different instants during an owned transition. Residual current can
remain unverified settling data within that deadline; it cannot start/count a zero window. Nonzero readings or loss
of the required CP/offer/session after a zero window begins reject the challenge. Source failure, a new session,
changed limits/resources or a later
control invalidates current evidence. Call `supersede()` before restoring car permission and on observer failure.
Process replacement cannot initialise current evidence from historical JSON.

The classified result is `current_vehicle_refusal_verified`, alongside `reversible_challenge_passed`. It proves
sampled present behaviour while the positive offer exists. `current_pause_permission_verified`,
`durable_pause_verified` and `handover_authority` remain false. It supplies no permission revision, compare-and-set
restoration or guarantee that pause survives a closed session/backend switch; the stronger handover gates remain.

Operational control stays outside this read-only verifier. A connected normal-backend pilot must journal restoration
authority before lowering USER, reconcile startup history/session publication lag, and support cleanup even when no
charge starts. Observe a car Stop actually reaching zero before a later Start: overlapping an unapplied Stop with
Start can make an already-charging observation look like restoration. Restore accepting permission at 6 A, verify real
charging, then use ordinary iCS Stop and retain the actual completed history before closed-zero USER32 restoration.
Keep the saved overnight schedule and the lead attached throughout; permit cleanup even after the experiment expires.

A normal-backend live run on 2026-11-09 completed both fresh pause windows: 18 distinct samples over 63.5 seconds,
then real 6 A charging after Start, followed by 16 zero samples over 62 seconds. The current flag was superseded before
restoring car permission. The complete original USER32 configuration and unchanged 00:00–06:00 schedule were restored;
two independent native reads confirmed normal charging, with all 12 pre-existing history bodies preserved and the lead
still attached. An earlier vendor disconnection at midnight correctly invalidated an incomplete run rather than
turning an old zero reading into current authority. No live Home Assistant setup or permanent proxy service changed.

The isolated owner's control test used the normal authenticated SEAT service after the Android app became
unavailable. Current real command replies contained a string `requestId`, whereas the inspected APK model names
`actionId` and discards its value. These private submission receipts are separate from actual physical completion;
neither field is a permission revision, an idempotency guarantee or a durable pause lease. Home Assistant's own
read-only endpoint allowlist and authentication-refresh ownership were unchanged.

Each protected step requires a new provider response bound to the car, charger, hardware, baseline, operation nonce,
current Boot/connection and complete native snapshot. The response must prove current pause, unchanged owner revision,
matching restart/positive-offer capability, observation age at most five seconds and known validity. The pause must
require explicit release, survive process loss and remain reserved for at least fifteen minutes; expiration of a
readback must never resume the car. The original physical test alone does not establish that provider contract.

Native checks require saved USER6 **and** runtime USER60/HWCFG320, all three currents and four power readings at zero,
CP B/Plug, a closed native/local session, reconciled lifecycle/vendor writes and zero effective `outLimit`.
Two distinct native timestamps must be at least two seconds apart. `offerCur` can retain 60 after Finished and is
not an active-output test. Every other readable configuration field, original resource and retained history body stays
pinned. Unknown/changed resources or revoked vehicle permission block further restoration.

Cleanup freezes positive writes, retires the vendor runner, clears only exactly owned profiles and verifies actual
absence, with station profile 7001 last. Its durable handover state is separate from ordinary recovery. The explicit
`TrialSession.resume_connected_handover()` continuation cannot reinstall a cleared profile: verified absence needs no
listener; remaining owned resources use a fresh proof-gated cleanup-only transport without ceiling reapplication.
An unconfirmed Clear is reconciled through complete native inventory rather than sent again after verified absence.

Connection and USER restoration remain explicit operator steps. First restore only the original connection fields
while retaining USER6 and the car pause, then confirm the original backend. Only then restore USER32 and confirm the
complete original configuration. Vehicle permission is restored last through a provider compare-and-set operation.
A lost response uses a distinct outcome read and never repeats an uncertain vehicle Start. Normal manufacturer-side
charging after that restoration is observed as actual native activity, without inventing a proxy transaction or
claiming ownership of new manufacturer resources. Only ordinary transaction profiles tied to the genuine native
session or a newly completed native history record are accepted as new resources; unrelated plans/station profiles
block completion. Original history/resources must still be intact. The final handover stage and trial phase commit
atomically, while the independently verified vehicle outcome is retained before any remaining completion check.

A connected plan never disables the stronger unplugged fallback. If the lead is removed, ordinary `rollback` can
still clear its owned resources. `confirm-original-backend` verifies the normal unplugged original configuration but
retains any recorded vehicle-permission obligation instead of claiming completion. After the owner has restored that
permission, `confirm-unplugged-vehicle-permission` verifies it read-only through the provider; it never sends Start.

After process loss, a verified existing trial may now open restricted recovery while the vehicle remains connected,
subject to the exact native session and saved binding checks above. New socket admission and changed introductions
persist zero-current local ownership before charging-capable messages can proceed. Recovery receives actual lifecycle
retries, stops only the known session and delivers retained history. Unknown cloud Starts and pending-start uncertainty
still require independent reconciliation; they are never automatically retried or discarded. A failed native audit does
not disable a narrowly scoped safety Stop. Recovery does not implicitly release the hold.

The fifth trial verifies automatic readiness, nominal physical current limiting and core app/local controls from a
profile-free baseline. Selective clearing beside unrelated profiles and the wider unattended-operation cases remain blocked.
The synthetic commissioning tests remain restricted to literal loopback under `tools/offline_validate.py`.

#### Connected handover with an independent native current limit

`prototype/native_bounded.py` and `native_bounded_handover.py` provide a separate, explicitly configured supervised
strategy. `TrialSession(..., native_bounded=NativeBoundedSpec(...))` requires the unchanged original configuration,
the exact four trial connection fields, saved-plan digest and commissioned native-limit/actual-Boot evidence.
`capture_native_bounded_baseline()` captures the original 32 A configuration while connected, closed and at zero.
Admission reserves fresh transaction numbers with the original route under independently read-back saved USER6,
runtime USER60/HWCFG320, closed SESSION and all seven meter channels at zero. Actual Boot after routing repeats the
native audit and installs a local zero hold. Reconnection retains zero; positive sends recheck native6 inside the
outgoing RPC gate. Ordinary unplugged admission and the stronger vehicle-permission strategy are unchanged.

This strategy permits unexpected charging at nominal6A; it never labels zero consumption as a paused vehicle.
`native-bounded-handover` freezes positive controls, stops only actual known transactions, drains lifecycle delivery,
and checks independent closed-zero evidence before every exact owned Clear, with station7001 last. Uncertainty leaves
restoration pending. No unknown native session is adopted, guessed Stop sent or history event invented. Verified
profile absence cannot rearm a ceiling during restart. Full original history bodies, plans and unrelated config stay
pinned throughout.

The coordinator's adapter callbacks perform only the four original route fields and, last, the saved USER32 scalar.
`restore_route()` retires the proxy and requires a fresh native offline view while6 remains effective.
`observe_backend()` requires a later independent original-route online view. `restore_settings()` repeats online,
closed-session, profile-free and all-phase-zero checks immediately before the recorded USER32 write. Lost replies are
reconciled from actual configuration; completion and trial phase commit together. Genuine manufacturer charging after
the authorized final write is retained as external native activity. This remains a supervised commissioning API,
without Home Assistant deployment, automatic service installation or a car-pause lifetime guarantee.

A supervised connected trial on 2026-11-09 verified this path with the lead attached throughout. The native6A limit
survived actual charger restarts, the genuine Boot established the proxy's zero hold, and the manufacturer cloud
registered successfully. Twelve fresh observations covered 60.22 seconds at zero draw with USER6/HW32 and a closed
native session. Scoped cleanup established actual trial-profile absence before original-route restoration; a fresh
native offline-to-online transition preceded USER32 restoration. A later independent audit confirmed the exact full
original configuration, all 20 original history bodies, unchanged saved plans, no trial profiles and an empty proxy
lifecycle/outbox. This trial verified handover and restoration; it created no new charging session and did not deploy
Home Assistant or a permanent proxy service.

The standalone CLI also accepts `--native-bounded-spec /absolute/private/native-spec.json`. Its default `check`
validates the specification against the immutable baseline and exact listener without opening a journal or contacting
the charger. `capture-bounded-baseline` is a separate read-only action for the original32A connected, closed station;
`run` still requires independently verified native6 before admission and explicit external route/configuration steps.
The native strategy and `--car-pause-config` are mutually exclusive.

`tools/managed_proxy.py` provides a persistent, bounded `app_schedule` mode with explicit `check`, `serve` and
read-only `health` actions. It uses the same private configuration/native specification and durable journal as
the commissioning engine. Only `serve` opens connections. Deploy it as a dedicated user service with boot linger,
`Restart=on-failure`, `Type=notify` and `WatchdogSec=60`; retain the journal, baseline, specification and credentials
outside immutable versioned code releases. Use a dedicated Python 3.14 environment with `requirements-trial.txt`
and OpenSSL. A separate periodic `health` invocation detects a dead process or stale native observations without
opening the journal or restarting the charger for a cloud outage.

Local Start is disabled before it can change ownership, and that restriction is persisted in the journal across
all entry points. The owner app's Start and the charger's unchanged native timetable remain permitted at the
initial 6 A ceiling. Include the exact independently verified native timetable identity in the explicit local tag
allowlist. Startup/recovery verifies genuine Boot, current native6, actual owned station0, unchanged configuration,
plans and retained history, and real cloud registration/history acknowledgements. It then acknowledges recovery
at zero and releases the ceiling to6 once per introduction, without sending RemoteStart. If the firmware does
not resume after a recovery Stop, the owner app or timetable must initiate another charge.

The supervisor accepts a known, bounded owner charge as healthy. Unknown transactions, missing owned resources,
configuration/history drift or uncertain delivery latch a durable fault, retain local inhibition and lower only
an already owned station ceiling where authority permits. It never invents a Stop identity or recreates a verified
absent resource. Shutdown retires pending release authority before retaining the normal zero/known-Stop hold;
it does not clear profiles, restore32 or rewrite the backend. Service replacement preserves transaction numbers,
receipts and history. Live Home Assistant integration is a separate deployment.

Private atomic status and bounded `.events.jsonl` transitions sit beside the journal. Health requires recent status
and independent native observation, actual Boot/cloud connectivity, active shared ownership, verified6A limits,
preserved resources, disabled local Start and sufficient validity of the certificate actually loaded into TLS.
The event-loop watchdog reports process responsiveness independently of cloud/native health. Listener certificate
renewal requires a controlled service restart; file replacement alone cannot falsely renew its reported validity.

A bounded managed service was installed on 2026-11-09 in an isolated runtime with immutable code, private durable
state, user-service boot linger/restart/watchdog and a separate minute health timer. The initial release was not
confirmed and correctly retained zero; an independent closed-zero/owned0/empty-ledger reconciliation preceded
operator acknowledgement and recovery. Its original cause was not captured, so later release errors now retain
private diagnostics. A subsequent deliberate idle process kill triggered the supervisor's automatic replacement,
genuine Boot/cloud reconciliation and successful app/schedule release without operator action. Independent final
native/ledger checks confirmed USER6/owned6, closed session, all-phase zero, no proxy Start or transaction, empty
outbox and all26 retained history bodies/saved plans unchanged. The lead remained attached. No test charge or
Home Assistant deployment was performed. The midnight-to06 timetable remains enabled at6A; its next unattended
overnight run still needs observation.

Managed operation now explicitly permits a disconnected vehicle when the actual pilot/connection pair is
`A`/`Unplug`, the native session is closed and every phase current/power and output limit is zero. Full native6,
configuration, schedule, history and owned-profile checks still apply. Routine unplug/replug does not latch a fault
or generate Start; an already admitted managed service can recover while unplugged using fresh Boot/owned0 and
real delivery reconciliation before release6. Initial connected admission and original32 restoration remain strict.
This option requires disabled local Start and is enabled only by the managed runner. Status reports actual vehicle
connection separately from service connectivity. Detailed future native validation failures stay in the private
journal even when the next sample recovers.

The unplugging correction was deployed on 2026-11-09 after1088 offline tests and12 targeted condition mutations
passed. Unplug/replug and restart with the vehicle absent were exercised with independent native HTTP/TLS peers;
the physical lead was left connected during installation. The old inhibition was acknowledged only after fresh
closed-zero/owned0, exact history and empty-ledger verification. Controlled replacement recovered genuine Boot,
cloud registration and shared6A control. A final independent native/ledger audit confirmed zero power, no Start or
transaction, unchanged plans/all26 history bodies, and a successful independent health check.

After a proxy process replacement, `{"command":"native-bounded-resume"}` explicitly acknowledges restricted recovery.
Fresh native6/closed/all-phase-zero evidence, the current actual Boot/binding, confirmed owned station0 and an empty,
genuinely acknowledged lifecycle/outbox are required. It clears uncertainty while retaining local ownership and zero;
Start or release is a later explicit command. An exact known active session receives zero then a real RemoteStop.
If the charger finished while the process was absent, restricted recovery receives its actual Stop replay without
creating a replacement event. Unknown transactions and any started cleanup remain blocked.

A further connected trial on 2026-11-09 verified three real local Start/Stop sessions: with the cloud connected,
with its supervisor deliberately retired, and after actual proxy process loss/replacement. Offline Start and Stop
were retained without cloud mappings, then acknowledged through the normal vendor runner without changing their
stored bodies. The terminated process left the independent USER6 and owned zero profile intact. The charger
reconnected to a different process using the same journal, introduced itself through real Boot and re-established
zero without a charger reboot. Start was rejected during recovery; explicit acknowledgement retained zero before
the separate post-recovery Start/Stop. All three sessions received real cloud mappings and acknowledgements.

Final independent observation confirmed the exact original 32 A configuration/cloud link, unchanged saved plans,
all 20 original history bodies plus the three genuine test sessions, no trial profiles and an empty outbox. The lead
stayed attached and Home Assistant remained unchanged. These sessions drew zero power: the trial establishes
control/lifecycle, cloud-outage handling and restricted process recovery. That trial did not validate charging under
load or install an unattended service.

A subsequent short load trial on 2026-11-09 verified real charging through the same proxy. The charger recorded
50 Wh over 138 seconds. Sixteen independent fresh native observations showed 6.1 A and 1447–1449 W with saved/runtime
USER6, HW32 and an owned station6 profile. The acknowledged session continued unchanged through deliberate vendor
disconnection: positive samples covered 43.91 seconds with the cloud connected and 32.84 seconds while disconnected.
Local Stop while offline established native/local closure, zero output and all seven meter channels at zero. The
queued Stop and observations received genuine cloud acknowledgements after reconnect, without changing their stored
bodies. The short-charge harness had a 180-second Stop deadline, rejected an expired budget or unbudgeted release,
and retained exact session/owner/Boot/mutation checks around the active cloud cut.

Final independent native observation confirmed original 32 A/cloud/configuration and saved plans, no trial profiles,
an empty outbox, all 22 fresh-baseline history bodies and the one genuine50 Wh charge. One older completed record was
already absent from the native list before this trial; its archived body remains available and the cause is unknown.
The lead stayed attached, no Home Assistant settings changed and no permanent proxy service was installed. Longer
operation, unattended recovery policy and permanent deployment remain separate work.

An active process-loss trial on 2026-11-09 then verified restart while real charging was in progress. A preparatory
charge was stopped and restored normally after the operator consumed the reserved recovery window; the guard refused
that kill. The repeat used a fully prepared sequence and one original 180-second, same-host monotonic budget across
both proxy processes. Missing, expired, malformed or differently bound budget data permitted only Stop/recovery.

The old process was actually terminated during an acknowledged 6 A charge. An independent observation with that process
absent still found the same native transaction drawing 6.1 A, with USER6/HW32 and owned station6. The replacement
process launched 0.651 seconds after the crash witness. Its real outgoing/reply trace established station0 followed
by RemoteStop for the exact original local transaction, both Accepted. Zero was requested 21.618 seconds after the
crash; native/local closure, all-phase zero and real cloud history delivery were confirmed 30.854 seconds after the
last pre-crash native sample. This is bounded-current continuity through the outage, not instantaneous zero on loss.

Explicit native recovery acknowledgement retained zero, then a separate Start created a different actual transaction
and drew power. The two main sessions recorded 17 Wh and 8 Wh; the preparatory charge recorded 58 Wh. Final independent
verification found all 23 initial history bodies intact, those three genuine closed/cloud-mapped records added,
no trial profiles/outbox, unchanged plans and the exact original 32 A/cloud configuration. The lead stayed connected
and Home Assistant/permanent service setup remained unchanged. Longer unattended operation remains separate work.

---

## Offline iCS coexistence prototype — this branch

This branch builds on OCPP Hub 0.2.1 (`1229028b4b56cb474b04686e5fdf71f079a0888f`) with an isolated durable gateway
and an offline connection supervisor in `custom_components/ocpp_hub/core/prototype/`.
**The normal Home Assistant entry point is unchanged and does not load this prototype.** The app observations in this
section used the original backend; the later standalone unplugged trial is described above.

Local control and vendor-app traffic share a local transaction authority. Charger observations are committed before
local replies and delivered to the simulated cloud in order. Explicitly allowed vendor commands receive the actual
charger's correlated result or error. Unknown commands and undocumented iCS variants are refused.

### Implemented and checked offline

| Component | Responsibility |
| --- | --- |
| `prototype/runtime.py` | Separate local listener, reconnecting vendor session, registration retry, OCPP heartbeats and websocket Ping/Pong. |
| `prototype/store.py` | Private SQLite WAL journal, atomic lifecycle/permission commits, durable outbox, transaction mappings and uncertainty. |
| `prototype/local_control.py` | Local ownership, priority stop, persistent zero-current hold, station ceiling and effective-schedule verification. |
| `prototype/gateway.py` | Local transaction authority, ordered replay, connection generations, command admission and profile translation. |
| `prototype/authorisation.py` | Optional one-start grants for changing app-generated tags, with bounded expiry and active-session binding. |
| `prototype/ics.py`, `ics_writes.py` | Observed JSON-string metadata, mode, plan and delayed-stop codecs. |
| `prototype/plans.py` | Separate local/vendor plan ids, explicit adoption, confirmed ownership and bounded tombstones. |
| `prototype/rpc.py`, `policy.py` | One outstanding CALL per direction, stop priority, bounded receipts, limits and command allowlists. |
| `prototype/security.py` | Authenticated loopback TLS, certificate/hostname checks, exact identity/header forwarding and redirect refusal. |

- The local receiver and controls remain available during cloud outages. Cloud registration honours Accepted,
  Pending and Rejected replies and retry intervals across reconnects. Independent OCPP heartbeats continue through
  an ambiguous history delivery; websocket Ping/Pong detects dead transports. Losing or replacing the local charger
  retires its cloud connection. Old readers cannot detach a newer connection.
- A local stop installs a constant `ChargePointMaxProfile` at 0 A, then stops the known transaction. The hold survives
  restart and is reapplied on reconnect. An independent fake firmware can fire an already stored native timer with
  no network request and still cannot draw current through that hold. An explicit start/release preserves any
  earlier lower local ceiling. Cancelling an API waiter does not cancel its owned physical operation.
- A ceiling is confirmed only after a real Accepted profile result and a valid `GetCompositeSchedule` response
  covering the requested instant in amperes. Higher effective limits, rejected results, malformed periods and
  missing/stale/future coverage fail closed. Failed start/release sequences reassert zero and retain uncertainty.
- Starts, receipts, authorisation binding and outbox events share one SQLite commit. Stops retire profile aliases
  and bound tag grants in the same commit. Read-only storage and failure after an actual charger ACK return errors;
  they cannot publish successful ownership. Unknown outcomes block later writes until explicit reconciliation.
- Vendor profile ids are translated, allocated monotonically and retired after their transaction. Vendor station
  profiles cannot reach the reserved local stack. Plan clears resolve every named vendor resource before sending;
  unknown/local/unconfirmed resources cannot be cleared, and disable/re-enable keeps a stable mapping.
- Known `IoCharger` commands cannot be reclassified as reads through a mutable extension policy. Mode and delayed-stop
  writes require an open mapped transaction. Plans preserve their exact UTC activation timestamp, wire types and
  recurrence. An accepted write must match the observed response shape before ownership is committed.
- Default policy has a 16 A single-phase ceiling, static tags and no enabled proprietary extensions. Tests explicitly
  enable the captured subset with a 32 A ceiling. Dynamic vendor tags require an additional explicit trust setting;
  their grant is not a general offline allowlist. An unresolved accepted start remains blocked even after its tag
  expires, because the device might execute it late; this requires independent reconciliation.

### Reproduce without a live system

Use Python 3.14 on Linux and an absolute scratch directory. The runner loads no household settings or credentials.
Both TLS peers use synthetic identities, a fresh test certificate and ephemeral literal-loopback ports only.

```bash
export TMPDIR=/absolute/path/to/task-scratch
mkdir -p "$TMPDIR"
python3.14 -m venv "$TMPDIR/venv"
"$TMPDIR/venv/bin/python" -m pip install --require-virtualenv -r requirements-offline.txt
"$TMPDIR/venv/bin/python" tools/offline_validate.py tests
```

For the complete recorded request sequence through both real TLS legs:

```bash
"$TMPDIR/venv/bin/python" tools/offline_validate.py tests/test_prototype_recorded_runtime.py
```

The Python audit guard rejects non-loopback socket/DNS operations and named Unix service sockets before they occur.
It disables unrelated pytest plugin discovery and keeps temporary outputs under `TMPDIR`. It is a Python-level guard
for reviewed tests, not an operating-system network namespace. The runtime also rejects non-loopback destinations
before creating a socket. The offline runtime accepts no production endpoint or household credentials. The separate
trial command described above performs private-file preflight by default; only its explicit run action enables the
trial transport.

### Actual iCS evidence

On 8 November 2026, the owner authorised inspection of the installed iCS2.0Lite 1.7.3 Android app and reversible
charging-control trials through that app. Process-scoped HTTP debug logs and the charger's existing read-only LAN
getters supplied message evidence without TLS interception, certificate installation or app modification.
Those earlier app observations left the proxy disconnected and live Home Assistant unchanged.

These complete, sanitised fixtures overlap; their frame counts must not be added as unique traffic:

| Fixture | Complete frames | Evidence |
| --- | --- | --- |
| `ics-log7628.sanitized.json` | 517 | Initial status/metering, authorisation, historical lifecycle and metadata reads. |
| `ics-app-controls.sanitized.json` | 551 | Fast/Eco/EcoPlus, disabled/enabled/edited/deleted temporary plan and original daily plan. |
| `ics-vehicle-controls.sanitized.json` | 721 | Actual vehicle, current changes, remote starts/stops and delayed-stop set/cancel. |
| `ics-completion-observation.sanitized.json` | 789 | Vehicle-reported finish and subsequent unplugging, with the transaction still open. |

They are sanitised, reserialised derivatives, **not byte-identical wire data**. No parsed frame was dropped. Each
records its original archive SHA-256 and transformations. Consistent aliases replace ids/tags, one date shift applies
throughout, and energy offsets preserve Wh/kWh deltas. Private originals and reversible maps remain outside this repo.
Nine MeterValues requests have no captured reply; replay explicitly simulates those empty acknowledgements.

The full 721-frame runtime replay exercises all 365 recorded CALLs through both local TLS legs, with a synthetic Boot,
credentials and peer responses. One retained GetDiagnostics request is deliberately refused. A verified initial plan
mapping is seeded explicitly, rather than inferred from the first clear. Three distinct local/cloud transaction pairs
and three actual TxProfiles check active-session alignment and the independently modelled 6 A effect.

Observed proprietary traffic uses `DataTransfer.data` as a JSON **string**:

| Message | Supported observed shape |
| --- | --- |
| `getTransaction` | Read metadata by transaction id; exact Accepted reply adds the eco mode. Historical reads are allowed. |
| `setTransaction` | Open transaction id plus `eco` equal to `Fast`, `Eco` or `EcoPlus`. |
| `setPlan` | Recurring/Duration or one-off Relative/Duration and Relative/Full, explicit current, recurrence and UTC time. |
| `clearPlans` | Explicit nonempty list of confirmed vendor plan ids, including known cleared tombstones. |
| `delayStop` | Open transaction id plus canonical UTC `stopTime`; cancellation omits `stopTime` entirely. |

Metadata reads were captured only with `eco: Fast`; allowing Eco/EcoPlus read values is inferred from the observed
write-mode enum and is not evidence of actual metadata replies in those modes.

All observed proprietary write replies are exactly `{"status":"Accepted","data":""}`. Plans use wire amperes such as `32.0`;
retained native fields encode tenths of an ampere. Scaling is applied only where actual evidence establishes it.
The app's fresh RemoteStart uses a new tag, and its current slider installs a new transaction-specific profile.
A previous session's 6 A TxProfile did not constrain the next session; the app had to apply a new 6 A profile.
A paused car still produced Authorize/StartTransaction, so a transaction alone does not prove power is flowing.

The brief vehicle trial measured up to 6.1 A / 1,481 W after a nominal 6 A setting, using fresh native readings.
Captured OCPP current samples in that part of the trial were zero; they do not establish that nonzero measurement.
DelayStop's 15-minute minimum was set and immediately cancelled. The original 32 A setting, Fast mode and daily
schedule were restored, then Stop was confirmed at zero power with the cable still plugged in. That trial necessarily
added history and replaced an old unrelated profile; session/history/profile bytes were not restored to their baseline.
A subsequent uninterrupted charge used the original app/backend at its restored 32 A setting. The owner reported
that the SEAT app said charging had finished. Native readings reached 0 A / 0 W with CP B, and the charger sent
`SuspendedEV`. At the retained observation boundary, its fourth transaction was still open: no StopTransaction
was invented for the replay. Finishing power delivery and closing an OCPP session are separate events.
Removing the lead also left the native transaction open while the charger reported unplugged at 0 A / 0 W.
The complete 789-frame trace contains 399 CALLs and is covered by a second TLS replay, ending with an open transaction.
The retained final OCPP current sample was 0.4 A; zero current/power was established separately from native reads.
Manual cleanup through the original Android app is recorded separately from this fixture. This bounds the observation;
it does not prove that an automatic Stop could never arrive later. The final OCPP status remained SuspendedEV even
after native readings showed unplugged. A future Home Assistant adapter must retain the existing LAN plug indicator
rather than infer vehicle connection from OCPP status alone.

After that capture boundary, manual Stop in the Android app received Accepted, generated a matching
StopTransaction with reason Remote and received its acknowledgement. Native readback confirmed no active
transaction, unplugged and 0 A / 0 W. Configuration, schedule and SMART profile data exactly matched the baseline
for this uninterrupted trial. Its real session added one history record (5 to 6) and 769 Wh; history was not erased.

### Remaining boundaries after the first unplugged proxy trial

The commissioning package is separate from Home Assistant and does not establish full iCS2.0Lite compatibility.

- Local TLS admission and Boot were observed, without Basic credentials. Isolated upstream Boot registration worked,
  but the continuing proxy/cloud connection failed. Its actual failure sequence still needs private wire diagnostics.
  New local clocks now use whole-second `Z`, and a synthetic format-policy reproduction passes. Device Auto date
  formatting may follow its backend; whether the real iCS server rejected the earlier fractional/offset form remains
  an inference, not an established server defect. Historical payloads remain unchanged.
- The proposed `ChargePointMaxProfile` was rejected. Supported purpose/kind/stack combinations, composite schedules, 0 A enforcement and profile
  persistence across device reboot remain unproven. The fake firmware checks OCPP stacking, but cannot prove hardware
  behaviour. A current composite read does not rule out a pre-existing higher-stack profile becoming active later.
  A clean, independently verified profile/session boundary is required before any future cutover.
- A device reboot can lose a physical profile before the proxy can reapply its persisted hold. Firmware autonomous
  behaviour during that gap is unknown; the software must never be treated as a hardware isolation device.
- Full cloud history replay, billing/session reconciliation and app presentation after a real offline interval remain
  untested. An uncertain cloud Start cannot be safely repeated automatically. Existing pre-cutover sessions/plans need
  a clean boundary or independently verified adoption; the proxy never invents historical Stops.
- Strict expiry/parent-tag/cache handling is tested offline. Full real-app authorisation and historical reconciliation
  through the proxy remain unproven. Dynamic grants cover only one trusted start; unknown variants fail closed.
- Firmware updates, reset and diagnostic uploads stay refused. Unobserved plan stop models, fractional-current device
  behaviour, arbitrary vendor extension bodies and every app screen are outside the validated subset.
- Resource tables and per-connection reply caches are bounded. Exhaustion and unresolved outcomes need operator
  reconciliation; no silent id recycling or automatic live recovery UI has been added.
- The existing Home Assistant integration, dashboard and deliberately disabled EV/reserve automations are unchanged.
  The temporary charger backend change was fully restored. No release, HA deployment or public push has been made.

Original upstream documentation follows.

---

<p align="center">
  <img src="https://raw.githubusercontent.com/MarkBovee/ocpp-hub/main/docs/images/logo.svg" alt="OCPP Hub" width="420">
</p>

<p align="center">
  <a href="https://github.com/hacs/integration"><img src="https://img.shields.io/badge/HACS-Custom-41BDF5.svg" alt="HACS custom repository"></a>
  <a href="https://github.com/MarkBovee/ocpp-hub/releases"><img src="https://img.shields.io/github/v/release/MarkBovee/ocpp-hub?include_prereleases" alt="Latest release"></a>
  <a href="https://github.com/MarkBovee/ocpp-hub/actions/workflows/ci.yml"><img src="https://github.com/MarkBovee/ocpp-hub/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/MarkBovee/ocpp-hub" alt="License"></a>
</p>

# OCPP Hub

OCPP Hub turns Home Assistant into the local OCPP 1.6 backend of your EV charger. Home Assistant sees and controls the
charger directly, and the manufacturer's cloud can still get a copy of everything the charger says, so the vendor app
keeps working next to Home Assistant.

A charger talks to one OCPP backend at a time. Point it at Home Assistant and the manufacturer's app goes dark; leave
it on the manufacturer's cloud and Home Assistant can only watch. OCPP Hub sits in between: the charger connects to
Home Assistant, and the hub keeps the vendor's backend informed on the side.

> **Status: early (0.x).** Built and tested against a Raedian NEO 22 kW. Other OCPP 1.6J chargers should work for the
> standard parts (status, metering, limits, start and stop), but have not been tried. The [changelog](CHANGELOG.md)
> says what each release really does, and [docs/roadmap.md](docs/roadmap.md) what is proven and what is not yet.

## Features

- **Local OCPP 1.6J server** on a port you choose. No cloud service of ours is involved.
- **Live entities** for status, power, energy, current and voltage per phase, offered current, plugged in, charging,
  problem and connection.
- **Control** with a charge limit (A per phase) under a ceiling you set, and a switch to start or stop a charge.
- **Charge history** kept without limit in Home Assistant, with the running charge shown live.
- **Schedules and a charging plan**: windows per weekday in which the charger may charge, set from Home Assistant, from
  a schedule helper, or from the Raedian app, and shown as a Home Assistant **calendar**.
- **Vendor mirror**: a copy of the charger's traffic goes to the manufacturer's backend, so the vendor app shows the
  charger online with its sessions. Commands from the vendor are refused unless you allow them, and then always under
  your ceiling.

## Supported chargers

| Charger | Status |
|---------|--------|
| Raedian NEO (22 kW) | Tested: entities, limits, start and stop, vendor mirror, the Raedian app's planning |
| Other OCPP 1.6J chargers | Expected to work for the standard OCPP parts; not tested. Reports are welcome. |

OCPP 2.0.1 is not supported.

## Install

### HACS (recommended)

[![Open your Home Assistant instance and open this repository in HACS.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=MarkBovee&repository=ocpp-hub&category=integration)

1. In HACS, add `https://github.com/MarkBovee/ocpp-hub` as a custom repository of type *Integration* (or use the
   button above).
2. Download **OCPP Hub** and restart Home Assistant.
3. Add the integration under *Settings, Devices & services* and choose the port (default 9000).

[![Open your Home Assistant instance and start setting up OCPP Hub.](https://my.home-assistant.io/badges/config_flow_start.svg)](https://my.home-assistant.io/redirect/config_flow_start/?domain=ocpp_hub)

### Manual

Copy `custom_components/ocpp_hub` into the `custom_components` folder of your Home Assistant configuration and restart.

Home Assistant 2026.10 or newer is required.

## Connect a charger

In the charger's OCPP settings, set the backend address to `ws://<Home Assistant IP>:<port>`. Most chargers add their
own id to that address. The port has no authentication, so keep it on a trusted network and never forward it to the
internet.

Raedian NEO: the OCPP profile (address and `ChargePointSerialNumber`) is set in the RAEDIAN Install app, which is meant
for certified installers.

When the charger connects, Home Assistant creates a device with its entities and shows a notification with the
manufacturer, model, firmware and serial number.

## Entities

Entity ids are fixed by a key, not by the language of your instance. `<id>` is the charger's id in lower case.

| Entity | What it shows or does |
|--------|-----------------------|
| `sensor.ocpp_hub_<id>_status` | The connector status (available, preparing, charging, ...) |
| `sensor.ocpp_hub_<id>_power`, `_energy` | Power (W) and the energy meter (kWh, total increasing) |
| `sensor.ocpp_hub_<id>_current_l1` ... `_l3`, `_current_offered` | Current per phase and the current the charger offers |
| `sensor.ocpp_hub_<id>_voltage_l1` ... `_l3` | Voltage per phase (disabled by default) |
| `sensor.ocpp_hub_<id>_current_session`, `_last_session` | The running charge and the latest finished one, with details as attributes |
| `sensor.ocpp_hub_<id>_limit_in_effect` | The limit that applies now and the charger's schedule for the next day |
| `sensor.ocpp_hub_<id>_app_plan` | The charging plan: number of windows, the windows, and whether the planning is on |
| `calendar.ocpp_hub_<id>_charging_plan` | The charging plan as repeating calendar events |
| `binary_sensor.ocpp_hub_<id>_connected`, `_plugged_in`, `_car_charging`, `_problem` | Connection and socket state |
| `number.ocpp_hub_<id>_charge_limit` | Sets the current per phase (0 to your ceiling; under 6 A pauses) |
| `switch.ocpp_hub_<id>_charging` | Starts or stops a charge |
| `button.ocpp_hub_<id>_clear_charge_limit`, `_apply_schedule` | Removes the hub's limit; writes the schedule helper to the charger |
| `sensor.ocpp_hub_<id>_model`, `_firmware`, `_error_code`, `_mirror_status`, `_mirror_queue` | Diagnostics |

While the socket is not delivering, power and currents read 0 instead of unknown. While the charger is not connected,
its sensors are unavailable and `_connected` is off.

A ready dashboard is in [docs/dashboard.md](docs/dashboard.md).

## Control and safety

- **Ceiling.** The options hold a current ceiling (default 16 A). Nothing the hub or the vendor sends goes above it.
  Set it at or below what your installer configured; the hub never raises the charger's own hardware limit.
- **Charge limit.** The number sends a charging profile. The hub tries a station-wide `ChargePointMaxProfile` first,
  then `TxProfile` while a charge runs, then `TxDefaultProfile`, and remembers which one the charger accepted. A value
  under 6 A is sent as 0 A: a car does not charge below 6 A, so it pauses.
- **Start and stop.** The switch sends `RemoteStartTransaction` (tag `OCPPHUB`) or `RemoteStopTransaction`. A start
  needs a car connected.
- **Limit in effect.** After every change the hub asks the charger what it really applies (`GetCompositeSchedule`) and
  shows that. Limits survive a restart of Home Assistant and are marked `restored_unconfirmed` until the charger
  confirms them.

Every write to the charger is logged with the prefix `[write]`.

## Charge sessions

A charge runs from the charger's `StartTransaction` to its `StopTransaction`. `_current_session` shows the energy of
the running charge with its start, duration, tag, peak and average power and limits as attributes. `_last_session`
shows the latest finished charge and the 20 newest in `recent_sessions`.

Every finished charge is kept in Home Assistant's storage without limit (about 300 bytes each). The action
`ocpp_hub.get_sessions` returns the history, newest first, optionally for one `charger_id` and up to `limit`. A charge
that was running while Home Assistant restarted is not recorded.

## Schedules and the charging plan

There are three ways to tell the charger when it may charge. All of them write one recurring OCPP profile that the
charger keeps on its own, so the plan works even while Home Assistant is down.

### The charging plan

The plan is a list of windows per weekday. Inside a window the charger gets *Limit inside the blocks* (your ceiling
unless set), outside them *Limit outside the blocks* (a pause unless set). Both are in the options.

```yaml
action: ocpp_hub.set_plan
data:
  windows:
    - {day: monday, from: "12:00", to: "14:00"}
    - {day: friday, from: "22:00", to: "06:00"}   # runs overnight
```

An empty list removes the hub's profile. The plan shows in `_app_plan` and in the calendar
`calendar.ocpp_hub_<id>_charging_plan`, which you can open from the Calendar panel or use in automations. The calendar
is read-only.

### With the Raedian app

With *Commands from the backend* set to *Pass them on to the charger*, the Raedian app and Home Assistant share the same
plan:

- When the app shows its planning, the hub answers with the plan.
- When the app changes the planning, the hub takes it over, writes it to the charger and tells the app it worked.
- When you switch the planning off in the app, the hub removes its profile so the charger charges freely, and keeps the
  windows for later. Switching it on writes them again.

The app talks to the charger through its own binary protocol inside OCPP `DataTransfer`. The hub decodes the planning
part of it; the layout was worked out from captured traffic and is documented in
[docs/raedian-protocol.md](docs/raedian-protocol.md). A planning message that does not parse completely is refused.
This part is **experimental**: the charger also has a planning of its own that the hub does not write to, and whether
the firmware enforces it next to the hub's profile is not known yet.

### A daily schedule, or a schedule helper

- `ocpp_hub.set_schedule` sets a daily schedule as a list of a time and the limit from then on, for example
  `[{time: "00:00", limit: 6}, {time: "07:00", limit: 16}]`.
- A **schedule helper** (Settings, Devices & services, Helpers, Schedule) can be chosen in the options. Its blocks are
  the windows in which the charger may charge. Nothing is written when the helper changes: press
  `button.ocpp_hub_<id>_apply_schedule` to send it.

The plan, the schedule, the schedule helper and the charge limit share one profile on the charger, so the last one
written wins. When a limit or schedule replaces the plan, the plan is emptied instead of showing windows the charger no
longer follows. The charger counts recurring profiles from a fixed point in UTC, so the change between summer and
winter time moves a schedule by an hour until it is written again.

## Vendor mirror

Open the integration's options and fill in:

- **Backend address**: the vendor's OCPP address (`ws://` or `wss://`, host, port and path) without the charger id; the
  hub adds it, as the charger does. Leave it empty to switch the mirror off.
- **User name and password**: only if the backend asks for them (HTTP Basic authentication).

For Raedian chargers use `wss://cloud-protocol.raedian.com:9043/ocpp/` with no user name or password.

What the mirror does:

- It forwards every message of the charger except heartbeats, in order, and keeps up to 5000 frames while the vendor is
  unreachable. Transaction ids are translated between the hub and the vendor.
- By default every command from the vendor is refused with an answer the protocol allows, and nothing reaches the
  charger.
- With *Commands from the backend* set to *Pass them on to the charger*, the vendor's remote start and stop, charging
  profiles, a short list of harmless configuration keys, availability, unlock and `DataTransfer` reach the charger.
  Charging profiles stay under your ceiling. `Reset`, `UpdateFirmware` and `GetDiagnostics` are always refused. Every
  passed-on or refused command is logged with the prefix `[forward]`; `DataTransfer` is logged in full both ways
  because its content cannot be checked.

Two diagnostic sensors per charger, `_mirror_status` and `_mirror_queue`, show the state of the link.

## Troubleshooting

- **The charger does not connect.** Check the address in the charger (`ws://`, not `wss://`, and the port you chose)
  and that nothing else listens on that port.
- **Entities stay unknown after a restart.** A charger that reconnects often sends no boot or status; after fifteen
  seconds the hub asks for them with `TriggerMessage`.
- **See every OCPP message.** Set the logger `custom_components.ocpp_hub.frames` to `debug` (Developer tools, Actions,
  `logger.set_level`). The log then holds every frame both ways, including id tags and serial numbers, so turn it off
  again afterwards.
- **Read the Raedian app's messages.** `python tools/raedian_frames.py home-assistant.log` lists the protocol frames in
  a log and marks the commands the hub does not decode yet.

## Contributing

Issues and pull requests are welcome. See [docs/developer.md](docs/developer.md) for the layout of the code and how to
run the checks:

```bash
python3 -m venv .venv
.venv/bin/pip install pytest pytest-asyncio pyyaml voluptuous websockets ruff==0.16.4 mypy==1.15.0
.venv/bin/python tools/validate.py
```

The engine in `custom_components/ocpp_hub/core/` imports nothing from Home Assistant and is tested on its own against
a fake charger over a real websocket.

Do not post logs with id tags, serial numbers, addresses or credentials in issues. Security problems go through
[SECURITY.md](SECURITY.md).

## Disclaimer

OCPP Hub is an independent project. It is not made, endorsed or supported by Raedian or by any charger manufacturer,
and not by Home Assistant. Product names are used only to say what the integration works with. Writing to a charger
changes how it charges; use it within the limits of your installation, at your own risk.

## License

Apache-2.0. Parts of the design follow [ha-ocpp-relay](https://github.com/michael-adler/ha-ocpp-relay) (MIT) and the
behaviour of [lbbrhzn/ocpp](https://github.com/lbbrhzn/ocpp) (MIT); see [NOTICE](NOTICE).
