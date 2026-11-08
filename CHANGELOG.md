# Changelog

## Unreleased — iCS prototype and commissioning package

- Add a separate standalone commissioning package with dormant private-file preflight, exact endpoint admission,
  independently configured credentials, certificate name/date checks and a bounded 6 A run. Keep offline adapters intact.
- Add a complete read-only native observer with bounded asynchronous HTTP and strict JSON/framing validation.
- Record trial resource intent independently of retired transaction aliases. Preserve originals, freeze rollback,
  clear only verified owned profiles with the station cap last, and verify original-backend reconnection separately.
- Add a restricted unplugged recovery listener and retain safety Stop during recovery; preserve ambiguous history.
- Add strict expiry/parent/revocation handling, nested authorisation preflight and immutable lifecycle reply metadata.
- Supply configuration/service examples and pinned runtime requirements. Nothing is deployed or connected to a live charger.

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
