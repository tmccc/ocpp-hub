# Release process

## Choose the change and deployment domain

The [code navigation guide](code-navigation.md) identifies the active runtime and its tests.

| Change | Validation and deployment boundary |
| --- | --- |
| Documentation only | Verify links, referenced files/symbols and `git diff --check`. No qualification or deployment is required. |
| Standalone iCS service | `tools/managed_proxy.py` and its imported core/tools. Keep private state separate from immutable code releases; its supervisor is independent of HA. |
| iCS Home Assistant client | `custom_components/ics_proxy/`, deployed with explicit `--domain ics_proxy --container NAME`. |
| Older Raedian HA integration | `custom_components/ocpp_hub/`, deployed as `ocpp_hub`; HACS release packaging below applies to this integration. |

Shared-core changes can affect both integration domains. Trace imports and tests before choosing verification scope.
An instruction to review code or update documentation does not authorize qualification, deployment, restart or a
charging test. Follow the owner's current testing and operational constraints.

## Versioning

Keep the version aligned in `pyproject.toml`, both integration manifests and the newest numeric release heading in
`CHANGELOG.md`. `python tools/version.py bump X.Y.Z` updates the package and both manifests. Add the changelog
heading and release notes separately. The helper's `check` action compares all four versions. The `Unreleased`
section records work after the latest release. Documentation changes alone do not require a version bump.
Version comparison preserves dotted components and normalizes the documented prerelease spelling only. The
release job uses `python -m tools.release_notes "$GITHUB_REF_NAME" --output /tmp/release_notes.md` to select that
exact numeric heading, skipping `Unreleased`. Missing or empty notes fail the release instead of selecting another
section. Nested headings inside the selected notes are preserved.

Pre-releases use `X.Y.Z-rcN` (HACS shows it as a pre-release).

## Qualification for implementation changes

The normal full gate requires all four complete twenty-worker suites on the exact clean commit. Follow
[the developer guide](developer.md#setup-and-checks) for pinned runtimes, raw evidence and explicit collection
executors. Targeted checks do not establish full qualification. The validator audits evidence; it does not launch
the four suites:

```bash
python tools/validate.py --qualification-report "$QUALIFICATION_BUNDLE" \
  --collection-executors "$TRUSTED_COLLECTION_EXECUTORS"
```

Bare `python tools/validate.py` needs `OCPP_QUALIFICATION_REPORT` in its environment; without a report it fails.
Executor selection is explicit when evidence needs a remote runtime. Retain failed attempts and keep every result
bound to its actual source. If the owner restricts testing or authorizes retained evidence, document the precise
source relationship, checks and limits separately; do not call mixed-source evidence a fresh same-commit pass.

## Current iCS deployment

The repository provides no standalone-service installer. Its entry point supports `check`, `serve` and `health`;
only `serve` starts network activity. For an already configured installation, offline preflight has this form:

```bash
python tools/managed_proxy.py check --config "$PROXY_CONFIG" \
  --native-bounded-spec "$NATIVE_SPEC" --management-config "$MANAGEMENT_CONFIG"
```

Service replacement must preserve its journal, allocation, history, original baseline, native specification,
credentials and requested control mode under the existing supervisor. A restart can perform protective charger
work; it is not a read-only status check. Do not improvise an installer or substitute a fresh journal.
`health` reads saved status rather than contacting the charger; global health can be false during a cloud outage
while local control remains healthy.

For the thin HA client, first inspect the selected target without changing it, then deploy only when authorized:

```bash
python tools/deploy_ha.py --domain ics_proxy --container "$HA_CONTAINER" --dry-run
OCPP_QUALIFICATION_REPORT="$QUALIFICATION_BUNDLE" python tools/deploy_ha.py \
  --domain ics_proxy --container "$HA_CONTAINER" --batch \
  --collection-executors "$TRUSTED_COLLECTION_EXECUTORS"
```

The helper runs the validation gate, creates an external backup, installs and verifies files. Without the explicit
domain it selects the older `ocpp_hub` integration. It does not deploy the standalone service. When separately
authorized, `--restart --yes --ha-api-config "$HA_API_CONFIG"` performs the configuration check and HA restart.
`--skip-checks` is an exceptional explicit override after a reviewed replacement gate, not the normal workflow.

Verify installed software identity, fresh local/native and HA state, preserved records and the requested mode.
Record cloud connectivity separately from local readiness. Respect commissioning's documented physical limits;
routine updates do not require another dedicated charging trial when the owner has excluded one.

## Older integration and HACS releases

On public working forks, HACS publishing validation runs on release tags and manual workflow runs. Hassfest
continues to validate both integrations on branch pushes.

1. Work on a branch (`feature/<name>`, `fix/<name>`), commit with Conventional Commits, open a pull request. CI runs the checks, hassfest and the HACS validation.
2. Bump the version and write the changelog entry on the release branch.
3. Pass the full evidence gate above, then use `tools/deploy_ha.py --domain ocpp_hub` for an authorized candidate deployment.
4. Agree the hardware verification needed for the changed behavior. Any charger write or charging trial needs the owner's explicit instruction and a stated current ceiling. Preserve the distinction between new observations and historical commissioning evidence.
5. Merge to `main` once all pull request checks are green.
6. Push the `vX.Y.Z` tag on the merged commit. The release job waits for hassfest and HACS on that exact commit (`tools/release_gate.py`) and then builds `ocpp_hub.zip` and the release notes from the changelog.

## Rules that always apply

- Push the tag only after every check on the commit is green; the release job enforces it.
- Deploying, restarting Home Assistant, pushing, tagging and posting on GitHub are separate outward actions. An approval for one does not cover the next.
- Do not claim physical behavior from offline tests alone. Additional hardware checks depend on the change and the owner's instructions; they are not an automatic step for documentation or routine commissioned updates.
