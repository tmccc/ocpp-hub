## Standalone commissioning package — unplugged trial completed, original setup restored

This branch adds a separate Linux/Python 3.14 trial package on top of the recorded offline prototype below.
An authorised unplugged trial connected the charger temporarily on 8 November 2026. Its original connection and
settings have been restored and independently verified. Live Home Assistant was not changed; its integration does not
load this package. The trial found unresolved limit and cloud-session compatibility gaps, so this is not ready for
unattended use or a charging test.

The actual trial established:

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

### Offline corrections after the unplugged trial

New local Boot and Heartbeat replies now use whole-second UTC `Z`, matching the observed manufacturer's clock replies.
An independent fake charger copies that clock representation, and a deliberately format-sensitive fake cloud accepts
its new status updates. Separate tests reproduce Accepted Boot followed by status rejection and disconnect, including
normal close codes 1000/1001. This demonstrates the failure mechanism; it does not prove the real iCS server's cause.
Already recorded timestamps and event bodies are never rewritten. Retained fractional/offset statuses survive cloud
reconnect and process restart and can be delivered unchanged when the synthetic backend accepts them.

The standalone configuration now explicitly selects `station_profile_kind`, `station_stack_level` and
`station_profile_start`. Relative mode requires a null start; Absolute mode requires a whole-second UTC anchor in
the past, within the supported native epoch range. The example selects Absolute, stack 1 and a fixed 2020 anchor.
That is a candidate for a later trial, not a finding that iCS accepts it. The same choice is used for the 6 A ceiling,
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

Absolute ownership includes the exact requested start instant. The candidate native decoder accepts only narrowly
matching records and checks current/future coverage conservatively; its Absolute representation still needs actual
device evidence. A different or unsupported representation blocks automatic cleanup. The independent firmware model
now enforces kind, stack, start time, duration and capacity, and does not apply a future profile early. Composite
verification requires a schedule covering response completion, rather than a nearby future or already-ended interval.

Private status now separates charger and vendor faults, preserves accepted registration counts after disconnect,
and reports request stage, whitelisted action, reply-id matching/length and numeric close codes. Endpoints, identities,
payloads, close reasons and exception messages are excluded. Raw private evidence from the previous trial remains
outside the repository. This work performs no new charger, vendor or Home Assistant connection or deployment.

The station discovery and explicit Absolute option follow the
[pinned reference integration](https://github.com/lbbrhzn/ocpp/blob/c6a2fa0f994f5b25160c6cf77eb1fcdbf3f60d5b/custom_components/ocpp/ocppv16.py),
with stricter refusal of unknown capabilities. They remain hardware-unverified on this charger.

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
charger's existing LAN HTTP API at its exact IP, supports only seven observed read requests, rejects duplicate JSON
keys and bounds each response and the entire observation without leaving background worker threads.
After restart this firmware can report the exact missing `/tmp/Ioc/OCPP.json` envelope instead of an offline flag.
Only that observed OCPP-specific reply becomes `online: false`; other failed getters remain errors, and absence can
never establish successful original-backend restoration.

```bash
python3.14 tools/trial_proxy.py capture-baseline --config /private/trial.json
```

A fresh first trial requires native CP A, unplugged, zero current/power and an actually closed transaction. It refuses
pre-existing station-ceiling profiles and profile 7001 collisions. Original plans and profiles remain protected.
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
an actual closed session, no pending Start and reconciled mutating/lifecycle outcomes. It freezes charging-capable work,
retires the cloud runner while retaining the local receiver, removes only independently matching owned profiles and
clears station profile 7001 last. Every deletion is checked against a fresh physical inventory, including after a lost
ACK/restart. An ACK without actual removal cannot complete cleanup; reconnect cannot automatically reinstall a cleared cap.

Resource cleanup ends in `backend_restore_pending`. Restore the charger's original connection profile from its private
backup as a separate operation. `confirm-original-backend` succeeds only once the proxy charger socket has retired and
fresh native readings show the original configuration, original resources, no owned profiles and a connected backend.
Configuration restoration alone cannot prove that reconnection has happened. History and any unsent observations remain
in the journal; they are not erased to make rollback look successful.

After process loss, an existing trial may open a restricted recovery listener once the vehicle is physically unplugged,
even if iCS still considers its session open. It persists zero-current local ownership before binding, blocks new starts,
and can receive lifecycle retries, stop the known session and deliver/reconcile retained history. Unknown cloud Starts
and pending-start uncertainty still require independent reconciliation; they are never automatically retried or discarded.
A failed native audit does not disable a narrowly scoped safety Stop. Recovery does not implicitly release the hold.

The offline corrections above are prepared for another authorised unplugged trial; actual profile acceptance and
continuing iCS cloud behaviour still need live confirmation before any charging test.
The synthetic commissioning tests remain restricted to literal loopback under `tools/offline_validate.py`.

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
