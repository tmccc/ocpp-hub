# Roadmap and status

The one place that says how far the project is. Update it in the same change that moves an item. `[x]` is done and verified, `[~]` is built but not proven live, `[ ]` is open. Dates are when it was last confirmed.

## Goal

Home Assistant is the real local OCPP 1.6 backend of a Raedian NEO 22 kW, shows it as entities, and keeps the vendor backend informed through a read-only mirror, replacing the `lbbrhzn/ocpp` integration.

## Done

- [x] Engine in `core/`: frames, charger session, central system, commands, transaction-id map. No Home Assistant imports, `mypy --strict`, tested over a real websocket with a fake charger (PR 2).
- [x] Vendor mirror in `core/`: store-and-forward queue, replay of boot and status, transaction-id pairing, vendor commands refused, Basic auth only when configured (PR 2).
- [x] Home Assistant layer: setup and unload, config flow, options flow, sensors and binary sensor, diagnostics with redaction, translations (English, Dutch) (PR 3).
- [x] Notification when a charger connects, with vendor, model, firmware, serial, OCPP version and mirror state (2026-10-20).
- [x] Deployed with `tools/deploy_ha.py`; `lbbrhzn/ocpp` removed; OCPP Hub listens on port 9000 and the NEO connects (2026-10-20).
- [x] Reconnect after a reload of the integration: the NEO is back in 14 seconds, because the hub drops connections instead of closing them (2026-10-20).
- [x] Vendor mirror against the real Raedian backend: `wss://cloud-protocol.raedian.com:9043/ocpp/`, no credentials; the mirror shows connected and the Raedian app shows the charger online (2026-10-21).
- [x] Independent review and audit of the engine and of the Home Assistant layer, P0 and P1 findings fixed with a test each.

- [x] First real charge with a car (2026-10-21): the NEO obeyed a 6 A station profile (5.9 A per phase, about 4.05 kW), reported preparing, charging, suspended by the car, finishing and available, and the charge was stored with its numbers (5.0 kWh in 84 minutes, stopped by the car being unplugged).

## In progress

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
  Existing original profiles now block new trial writes/clears;
  preservation failures remain durable. Earlier cleanup lost a saved inactive TxProfile, so selective clearing beside
  unrelated profiles, wider app features and unattended operation remain unproven. The original
  cloud connection, configuration, saved schedule and history are verified. Live Home Assistant remains unchanged.

- [~] One charging plan shared with the Raedian app (`ocpp_hub.set_plan`, answers and takes over the app's planning, written to the charger as a recurring profile). Tested with a fake charger and vendor.
- [~] Schedule helper and weekly schedules: a Home Assistant schedule helper applied with a button (daily or weekly profile), and the vendor answered with the charger's real schedule. Tested with a fake charger; see the schedule item below for what is unproven.
- [~] Schedule: read-back of the charger's 24-hour schedule and the service `ocpp_hub.set_schedule` (daily, recurring, under the ceiling). Tested with a fake charger; the NEO's behaviour with a recurring profile is unproven.
- [~] Limit sync: the hub's and the vendor's limit are saved in Home Assistant, shown together with the limit in effect (read from the charger when it can say), and the vendor is answered with it. Tested with a fake charger and vendor; whether the NEO answers `GetCompositeSchedule` is not known yet.
- [~] Socket entities `plugged_in`, `car_charging`, `problem` and `error_code`, derived from the status and error code; and a debug log of the raw frames. Tested with a fake charger; not yet seen on the NEO.
- [~] Charge sessions: a current-session sensor, an unbounded stored history, `_last_session` with the newest charges, the service `ocpp_hub.get_sessions`, and a ready dashboard (`docs/dashboard.md`). Tested with a fake charger; not yet seen with a real charge.
- [~] Control: a charge limit `number` (0 to the ceiling, default 16 A, under 6 A pauses) and a start/stop `switch`, through one guarded write path in `core/control.py`. Tested with a fake charger; whether the NEO obeys a charging profile is unproven.
- [~] Passing the vendor's commands on to the charger (option, off by default): everything except Reset, firmware and diagnostics, under the current ceiling, with transaction ids translated. Tested against a fake vendor and charger; not yet tried with the real vendor.

## Open

- [ ] Prove the charging plan on the charger and in the app: the app's reading of the plan shows the hub's windows (the format of a filled answer was assumed equal to a write; the first answer of the charger seeds the plan), and the charger follows the recurring profile. What the Raedian planning on the charger itself does is unknown; the hub no longer writes to it.

- [ ] Check the vendor side of a real charge: that the status changes and the charge show up in the Raedian app, and what the vendor sends back.
- [ ] Read the raw frames of a charge (logger `custom_components.ocpp_hub.frames` at debug) to see every measurand and field the NEO sends, and decide from that which further entities are worth making.
- [ ] Measure a full restart of Home Assistant (a reload is measured: 14 seconds).
- [ ] Prove that the NEO follows a recurring daily schedule (set one, watch the first change of period). The service and the read-back exist.
- [ ] Persist the mirror queue and a charge that is running across a restart of Home Assistant (today both are lost; documented in the README).
- [ ] Optional username default for the vendor: use the charger's serial when only a password is given. Build only if the vendor answers HTTP 401.
- [ ] Make the repository public, switch on HACS validation, tag a release. Version and changelog are still `0.1.0` / unreleased.
- [ ] Remove the unused add-on copy on the Home Assistant server (needs the owner's yes).
- [ ] Option to switch off the connect notification.

## Known small follow-ups (P2)

- A vendor can send many `TriggerMessage` requests; the answers are not rate limited.
- A vendor transaction id the mirror cannot map is logged on every frame.
- Two charger ids that slugify the same would share entity ids.
- The lock around the hub's own requests has no acquisition time-out.
- Reading configuration key by key is slow with a charger that stays silent.

## Decisions that differ from the first plan

- One integration, no add-on and no MQTT; the hub listens on port 9000 directly, without the trial run on 9001.
- `ClearChargingProfile` from the vendor is answered `Unknown`, not `Rejected`.
- The mirror sensors are per charger, not one for the whole hub.
- Configuration is read one key per request, and the hub asks for a boot and a status with `TriggerMessage`, because the NEO answers only one key at a time and sends neither after a hub restart.
