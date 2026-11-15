---
name: home-assistant
description: "Use when testing, deploying or debugging either Home Assistant integration: inspection, explicit client/domain deployment, restart, logs, config entries and the HA test environment."
---

# Home Assistant workflow for this repository

## Choose the integration

Read [the navigation guide](../../../docs/code-navigation.md) and [release process](../../../docs/release-process.md).
The current iCS installation uses an independent OCPP service and the thin `ics_proxy` HA client. The older
`ocpp_hub` HA integration owns a different listener. Follow the intended domain; HA client reload/unload does not
change the standalone service's durable intent or drop its charger connection.

## Inspect first, with HA-MCP

- Start with `ha_get_overview`, `ha_get_integration`, matching-domain logs and `ha_get_system_health(include="config_check")`. Select `ics_proxy` for the thin client or `ocpp_hub` for the older listener.
- Prefer read-only tools. Use a write or delete tool only when the owner asked for that specific change.
- Do not read the Supervisor token or `.storage` directly when HA-MCP can answer.

## Deploy

- Always deploy HA code with `python tools/deploy_ha.py`, selecting the intended domain. The current client requires `--domain ics_proxy --container NAME`; the helper does not install the standalone service.
- The default domain is the older `ocpp_hub`. Its SSH transport reads `HA_HOST`, `HA_SSH_USER` and `HA_SSH_KEY` from the git-ignored `.env`. Both transports run checks, back up outside `custom_components`, install and verify by sha256.
- Supply the existing exact-source qualification bundle with `OCPP_QUALIFICATION_REPORT` and the caller-selected `--collection-executors` map when needed. Read the release guide for evidence provenance and exceptional explicit overrides.
- `--dry-run` first; it changes nothing. A restart is a separate step: `ha_restart` through HA-MCP, or `--restart --yes` when the owner approved it.
- Deploy, restart, push, tag and GitHub posts are separate outward actions; one approval does not cover the next.

## Config entries and reloads

- Changed HA Python modules need an authorized restart to load reliably; config-entry reload is not a substitute for replacing already imported code.
- Restarting the older HA listener drops its charger connection. The independent iCS service keeps running through HA client reload/restart; verify the correct process boundary.
- Select the intended integration's logger when debugging and restore its previous level afterwards.

## Test environment

- Follow `docs/developer.md` and committed requirements for pinned environments. `validate.py --quick` runs static checks; full validation needs a qualification report and does not launch the four worker suites.
- `tests_ha_proxy/` tests the active thin client against both supported actual-core HA runtimes. `tests_ha/` tests the older integration using the pinned HA test plugin. Do not substitute one suite for the other.
- Read the corresponding `conftest.py`; request `recorder_mock` before `hass` when recorder fixtures require it. Keep entity keys stable across languages and upgrades.
- Documentation-only work needs link/path/symbol checks and `git diff --check`. Review does not authorize tests, twenty-worker qualification or live charger actions.

## Avoid

- Hard-coded hosts, ports of a private network or credentials in code, docs, tests or skills (`tests/test_skills_hygiene.py` enforces it for skills).
- Calling a device service from the integration without the owner's approval for that specific write.
