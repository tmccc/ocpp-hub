## Offline ICS coexistence prototype — this branch

This branch adds an isolated prototype in `custom_components/ocpp_hub/core/prototype/`, based on OCPP Hub
`1229028b4b56cb474b04686e5fdf71f079a0888f` (0.2.1). It reuses Hub's OCPP frame vocabulary and command builders,
with a separate durable gateway boundary. **The normal Home Assistant entry point is unchanged and does not load
the prototype. Nothing here has been connected to an actual charger or the iCS backend.**

The intended arrangement is a local controller with a vendor mirror. Local start, stop and current limits work
without the simulated cloud. Approved vendor commands and proprietary exchanges use real correlated peer replies,
not synthetic acknowledgements. Core charger observations are accepted locally and delivered to the cloud in order.

| Component | Responsibility |
| --- | --- |
| `prototype/store.py` | Private SQLite WAL journal, atomic acceptance before local replies, persistent ids, outbox and mutation uncertainty. |
| `prototype/gateway.py` | Local transaction authority, ordered replay, both-direction passthrough, reconnect generations and explicit local ownership. |
| `prototype/rpc.py` | One outgoing CALL per direction, stop priority, bounded retries/receipts, independent receive paths. |
| `prototype/ics.py` | Exact observed `IoCharger/getTransaction` JSON-string metadata codec with historical id translation. |
| `prototype/policy.py` | Explicit single-phase ceiling, profile ownership, local tag and vendor extension/configuration allowlists. |
| `prototype/security.py` | Authenticated TLS test adapters, exact identity/header preservation, certificate checks and redirect refusal. |
| `tools/offline_validate.py` | Test runner restricting Python sockets and DNS to literal loopback; named Unix service sockets are denied. |

### What the prototype tests

- Commit Start/Stop state, local replies and cloud events before replying to the charger. Disk failures cannot
  produce a successful acceptance. Duplicate lifecycle messages retain the same transaction id across restarts.
- Restore session correlation and queued records after restart. Translate cloud/local transaction ids in both
  directions, including a complete charge that started and finished while the cloud was absent.
- Keep pending or uncertain mutation intent as hashes, without saving proprietary bodies or credentials.
  An upstream Start with a possibly lost acknowledgement is quarantined instead of automatically duplicated.
- Forward explicitly allowed proprietary requests, configuration reads/writes and their actual results/errors.
  Receive loops remain available during nested exchanges; duplicate requests share one execution per connection.
- Give an explicit local stop priority over queued cloud work. Keep its inhibit across restart until explicitly
  released. Reject blanket profile clears, profiles above the ceiling and single-phase watt/phase bypasses.
- Reject queued requests from retired connections, including stale boot replies and replies from older local work.
- Verify both TLS legs with synthetic credentials and certificates, including wrong credentials, duplicate headers,
  wrong certificate trust/hostname, redirects and credential-safe logs/storage.

### Reproduce without a live system

Use Python 3.14 on Linux and an absolute task scratch directory. Test peers use ephemeral loopback ports and
synthetic identities only. Dependencies are installed in that private virtual environment, not Home Assistant.

```bash
export TMPDIR=/absolute/path/to/task-scratch
mkdir -p "$TMPDIR"
python3.14 -m venv "$TMPDIR/venv"
"$TMPDIR/venv/bin/python" -m pip install --require-virtualenv -r requirements-offline.txt
"$TMPDIR/venv/bin/python" tools/offline_validate.py tests
```

The runner disables unrelated pytest plugin discovery, stores pytest temporary files under `TMPDIR` and rejects
non-loopback Python socket/DNS operations before they occur. This is a Python audit guard with reviewed test
commands, **not an operating-system network namespace**. No production configuration or credentials are loaded.

### Deliberate boundaries

This is not a deployment package or a claim of full iCS2.0Lite compatibility. The original generic extension tests
are synthetic; the additional iCS checks below use a complete, privacy-transformed actual log capture. One exact
observed read codec is implemented. Unknown extensions, tags and unobserved codec variants are refused. There are no automatic starts or automatic charger configuration writes. Firmware/reset/diagnostic
operations are refused by default rather than acknowledged without execution.

An opaque exchange needs an explicit `(vendorId, messageId)` read/write rule. Opaque write contents cannot be
proven to respect a current ceiling without their real codec; they are blocked while local control owns the charger.
The synthetic prototype ceiling is 16 A on one phase. It grants no permission for an actual charger write.

Exactly-once effects across a failed remote connection cannot be guaranteed by OCPP alone. Uncertain cloud Starts
and pending/uncertain mutating RPCs require independent reconciliation; no automatic retry invents success.
Same-connection RPC duplicates are cached, but full opaque replies are not persisted, so generic RPC retry identity
across a fresh connection is not an exactly-once guarantee. Historical transaction records are retained as tombstones;
restored sessions are marked unconfirmed until current evidence arrives. Replayed old meter samples are not live data.

### iCS2.0Lite compatibility evidence

Validation on 8 November 2026 used an existing charger log export, retained LAN records and static inspection of the
installed Android app package. No charger settings, charging actions or live Home Assistant configuration were changed.
A strict certificate/hostname-verified TLS-only connection to the existing public backend succeeded without a client
certificate. No HTTP upgrade, Authorization header, charger identity, app login or OCPP exchange was sent to that backend.
This verifies TLS reachability and trust only, not websocket admission or the actual device credentials.

`tests/fixtures/ics-log7628.sanitized.json` contains **all 517 complete parsed OCPP frames** from the existing LOG7628
export: 263 requests and 254 replies. This is a sanitized, reserialized derivative, **not byte-identical wire data**.
The fixture records the original archive SHA-256 and transformations. Consistent aliases replace request/transaction
ids and tags; all dates share one shift; a common energy-counter offset preserves Wh/kWh deltas. Private originals and
the reversible redaction map remain outside the repository. Nine MeterValues requests have no recorded reply; none were
dropped or presented as a confirmed cloud acknowledgement.

| Area | Evidence and offline result | Remaining limit |
| --- | --- | --- |
| Status and metering | 47 StatusNotification and 194 MeterValues requests parse and are accepted by the real engine; decimal-string kWh, V, A and Celsius samples are preserved. | A transaction id or fresh meter report alone does not mean a vehicle is connected or charging. |
| Transactions and history | Two observed Starts and one Stop replay with distinct local/cloud ids. The completed session retains its 43.683 kWh delta. | A synthetic Boot and simulated peers are necessary; replay does not prove the vendor app stores mirrored/offline history correctly. |
| Vendor requests | Actual RemoteStartTransaction and both GetConfiguration exchanges pass with an explicit offline policy. | The actual authorisation tag is private and differs from the test default. This does not authorise a real start. |
| iCS metadata | Both observed `IoCharger/getTransaction` requests contain a JSON **string** with `transactionId`; Accepted replies add `eco: Fast`. The new codec translates ids in both directions, including completed sessions. | Only this exact cloud-origin read shape is supported and explicitly allowlisted. It is metadata, not a complete history response. Other modes, fields and directions fail closed. |
| Diagnostics | An actual GetDiagnostics request is retained in the fixture and explicitly refused in replay. | Vendor diagnostic upload/support is intentionally unavailable under the default policy. |
| Authorisation | Observed Authorize status matches an explicitly approved local tag. | Local replies do not reproduce the cloud's optional expiry/parent-tag metadata, including optional StopTransaction `idTagInfo`. Full cloud authorisation/cache semantics remain unverified. |
| Current ceiling | Retained settings and GetConfiguration show a 32 A configured limit; explicitly configured synthetic profiles at 32 A pass and 33 A fail. The default remains 16 A. | No actual SetChargingProfile or current-change exchange was captured. Device acceptance, profile precedence and minimum-current semantics are unverified. |
| Schedules and app controls | Static Android code establishes separate authenticated HTTP APIs and schedule models. App remote start uses point/connector ids, stop uses an API transaction id, and limit setting is a mutating GET with a string current value. The subsequent authorised app trial below captures existing-plan toggling. | HTTP API ids must not be assumed interchangeable with OCPP ids. Plan creation/editing and fractional-current behavior remain unverified. |
| Admission and idle operation | Both loopback WSS legs verify synthetic credentials and certificates; the exact iCS codec is tested over real local TLS websockets. | Actual Boot, Basic credentials and websocket admission remain untested. The engine answers charger Heartbeats locally but has no independent upstream heartbeat scheduler; negotiated intervals, reconnect/outbox scheduling and long-idle Ping/Pong need a runtime runner. |
| Existing sessions | Retained LAN history contains several unresolved/open records; unknown pre-cutover ids and overlapping Starts are refused. | A future cutover requires a clean session boundary or independently verified migration/reconciliation. No historical Stop may be invented. |

All 517 frames are parsed, and all 263 recorded requests are exercised against simulated peers: the diagnostic request
is an expected refusal, Authorize/Heartbeat are local responses, and supported lifecycle records use the durable outbox.
Nine absent MeterValues replies are explicitly simulated as empty acknowledgements. Request ids generated by the
prototype now fit the [OCPP-J 1.6 §4.1.4](https://raw.githubusercontent.com/mobilityhouse/ocpp/master/docs/v16/ocpp-j-1.6-specification.pdf)
36-character maximum; previously persisted oversized ids require explicit
reconciliation rather than silently changed. Newly accepted historical Starts remain unconfirmed until a current meter
observation arrives. The read-only historical mapping never permits a stop/profile command for a completed session.

**Assessment: the observed message subset is compatible with the explicit offline policy after these fixes; full app
coexistence and a live switchover are not yet validated.** The idle-runtime gap, actual authentication/boot, session
reconciliation and uncaptured schedule/current commands must be addressed or validated before a controlled trial.
The prototype is deliberately not wired into the existing integration setup or its UI.

### Follow-up: authorised Android app observations

A later, explicitly authorised trial used the existing iCS2.0Lite 1.7.3 app on the owner's USB-connected phone.
Its installed APK matched the previously inspected package. Readable process-scoped HTTP logs were available without
TLS interception, an app modification, certificates or proxy changes. Matching native station ids/serials verified
that app and charger referred to the same equipment. The original charger/cloud connection remained in place.

Six actual correlated charger/cloud control exchanges were captured: three `IoCharger/setTransaction` requests
(Fast on opening the page, then Eco, then Fast), one `IoCharger/clearPlans` and two `IoCharger/setPlan` requests.
All are OCPP DataTransfer messages with **JSON-string** data and actual replies `{"status":"Accepted","data":""}`.
Private complete exports and ring snapshots are retained outside the repository; the six redacted pairs are explicitly
selected message-type evidence, not a complete or byte-identical session fixture.

- `setTransaction` carries `transactionId` and `eco`. Merely opening the transaction page sent `eco: Fast` automatically.
- `clearPlans` carries an array of plan ids; the observed id matches the existing plan.
- `setPlan` carries the same plan id, type/stop model, `limitCurrent`, `stopValue`, `validTime`, start time, connector,
  recurrence, authorisation tag and eco mode. Observed wire current `32.0` becomes native plan current `320`; this proves
  scaling for this plan field only and must not be assumed for unrelated SMART profile fields.

Fast → Eco → Fast and the existing future plan's enabled → disabled → enabled controls were tested. Re-enabling the
plan through the app also changed its `validTime`. Replaying the observed status API with the original instant in the
app's millisecond ISO format restored the exact native plan, including its original activation time and record id.
The API rejected the same instant without milliseconds; no timestamp was rounded. Final readback verified exact
configuration, schedule, profile and history equality with baseline, plus the same enabled cloud schedule record.
The current slider was left at 32 A; no current-limit or start/stop test disturbed the separate historical SMART profile.
Opening the app populated one previously `NA` historical session mode field with `Fast`; actual operating mode was
already Fast. This metadata difference is retained explicitly rather than described as an exact session-state rollback.

The app API reported a stale `Plugged`/active session while native readings remained unplugged with zero current and
power. The detailed app page displayed 0.0 A/Suspended even while the station list said Charging. These are not evidence
of actual charging. No Home Assistant configuration or charger backend was changed, and the temporary phone logging
property was restored.

**Compatibility consequence:** the prototype correctly refuses all six newly observed proprietary writes under its
current policy. They need explicit codecs, identifier translation and ownership/current-limit rules before support
can be enabled. This trial supplies actual mode/timer evidence; it does not establish a working live proxy, actual
current-change behavior, schedule creation/editing or successful history replay. Original documentation follows.

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
