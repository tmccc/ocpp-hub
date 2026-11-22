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
Before building the archive, CI also checks that the tag matches the version in the packaged manifests.

Pre-releases use `X.Y.Z-rcN` (HACS shows it as a pre-release).

## Qualification for implementation changes

The normal full gate requires all four complete twenty-worker suites on the exact clean commit. Follow
[the developer guide](developer.md#setup-and-checks) for pinned runtimes, raw evidence and collection executors.
The validator audits the four suites and runs the additional iCS checks on the frozen release candidate.
Workers finish their assigned cases after a test fails so the run can report the other failures together.
Every case must pass for the suite to qualify.

For a standalone proxy release, use the full commit from the target's verified installation record. Supply the
Python interpreter from either pinned Home Assistant runtime for the week rehearsal.

```bash
python tools/validate.py --qualification-report "$QUALIFICATION_BUNDLE" \
  --collection-executors "$TRUSTED_COLLECTION_EXECUTORS" \
  --installed-proxy-commit "$INSTALLED_PROXY_COMMIT" --ics-ha-python "$HA_TEST_PYTHON"
```

The corresponding environment settings are `OCPP_QUALIFICATION_REPORT`, `OCPP_INSTALLED_PROXY_COMMIT` and
`OCPP_ICS_HA_PYTHON`. The gate runs the recovery search for each iCS release and the installation week when
the standalone proxy source or its dependency pins differ from the installed build. This includes the service
pins in `requirements-trial.txt`. The week commissions a synthetic journal with the installed build, uses the
candidate, then reopens the same journal on the installed build. Both builds use the supplied rehearsal interpreter,
so the rehearsal checks state compatibility across the source versions in one environment. The standalone release
gate also checks the candidate's service pins against the actual engine qualification runtime.
Passing results beside the qualification bundle are reused for matching source, builds and test environments.
A failed check stops validation before it produces a deployment snapshot.

The normal Home Assistant deployment helper selects the target automatically. For `ics_proxy` it adds the
recovery search to the pre-deployment checks. The two standard Home Assistant suites include the owner restart
tests. The older `ocpp_hub` deployment keeps its existing gate. Use `--deployment-target qualification-only`
when you only need to audit the four suites' evidence.

Executor selection is explicit when evidence needs a remote runtime. Retain failed attempts and keep every result
bound to its actual source. If the owner restricts testing or authorizes retained evidence, document the precise
source relationship, checks and limits separately; do not call mixed-source evidence a fresh same-commit pass.

## Current iCS deployment

The repository provides no standalone-service installer. Its entry point supports `check`, `serve`, `health` and
`incident`. The `serve` action starts network activity. Run offline preflight with the existing configuration.
Run the full release validator above as part of preparing the standalone replacement, before this configuration check.

```bash
python tools/managed_proxy.py check --config "$PROXY_CONFIG" \
  --native-bounded-spec "$NATIVE_SPEC" --management-config "$MANAGEMENT_CONFIG"
```

Service replacement must preserve its journal, allocation, history, original baseline, native specification,
credentials and requested control mode under the existing supervisor. A restart can perform protective charger
work; it is not a read-only status check. Do not improvise an installer or substitute a fresh journal.
The current supervisor retries service failures after five seconds and disables the startup burst limit.
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
domain it selects the older `ocpp_hub` integration. It does not deploy the standalone service. Backup retention keeps the archive created by the current Docker
install and uses modification times for earlier archives, including ones created by SSH. When separately
authorized, `--restart --yes --ha-api-config "$HA_API_CONFIG"` performs the configuration check and HA restart.
Docker verifies the selected Core process through its local API and waits for a new process to report running.
The Docker installer holds the component lock from the Core preflight through restart verification.
A failed configuration check restores the previous files before the installer releases the lock. If a restart may
have reached Core, the installer retains the candidate and reports that activation could not be verified.
The legacy SSH installer verifies each staging directory before publishing. Both transports use the same
component lock through verification and any requested restart. A failed SSH directory move or refused configuration
check restores the previous component. A hard interruption during a directory move can leave the previous component
staged and the lock in place.
For the legacy SSH restart, the API hostname must match the SSH target. Use `ssh_host` in the private API
configuration for an explicit SSH alias. Verification uses the configured Uptime timestamp entity, which
defaults to `sensor.uptime` and can be set via `uptime_entity`.
`--skip-checks` is an exceptional explicit override after a reviewed replacement gate, not the normal workflow.
An unchecked archive stamps the build `+dirty` when local changes or untracked integration files are included.

Verify installed software identity, fresh local/native and HA state, preserved records and the requested mode.
Record cloud connectivity separately from local readiness. Respect commissioning's documented physical limits;
routine updates do not require another dedicated charging trial when the owner has excluded one.
When comparing a journal migration with the running service, rehearse journal and gateway initialization on
a copy. Gateway initialization also creates the table for saved authorization refusals.

### Rehearsed standalone software rollback

The read-only rollback checker accepts only the build pairs and journal formats listed here. The installed
`11b1515` build and its predecessor `f40f7ed` passed the separate installation-week rehearsal. The historical
checker reports `build_pair_unrehearsed` for this pair. For supported pairs, it checks both the installation
and rehearsal requirement files alongside the service source.

The rollback to `7a08f2a` was rehearsed from `fc2dc0a` on a fresh journal and from `051500f`, `3fdace8` and
`73b1856`, and `10597b0` on saved copies of the migrated journal. Each rehearsal used one advancing SQLite journal.
The migrated rehearsals preserved the saved 32 A selection, the archive and original receipts. The older
build kept the newer Pause and completed the next simulated scheduled charge after Return.

For one of the listed historical pairs, the read-only check reports whether the actual stopped service build and
the retained journal match the rehearsed conditions. The installation-week rehearsal exercises the software change
with simulated peers. The checker requires a separately rehearsed build pair and journal format.

```bash
python -B tools/check_proxy_rollback.py --config "$PROXY_CONFIG" \
  --current-root "$CURRENT_BUILD" --previous-root "$PREVIOUS_BUILD"
```

Establish a fresh idle Pause and zero before stopping the service. Keep the service stopped from the check until
the planned switch. The check reads a single snapshot under the existing journal lock and reports a refusal for
an unproved build or layout, unresolved request, undelivered lifecycle, or uncertain control state. The lock covers
the check itself. A passing result establishes eligibility for that stopped journal. Verify a fresh Pause and zero
after starting the previous build on the same journal, before considering Return.

Finish pending Start mapping recovery on the build that recorded it. The saved-copy rehearsals from `3fdace8`,
`73b1856`, and `10597b0` also covered a completed Start mapping and operator delivery recovery. The checker verifies the
rehearsed recovery records against closed transactions, vendor replies and the completed Pause. Pending
recovery and proof formats outside those covered by the check still block rollback.

To reproduce the offline rehearsal, extract the two exact commits to new directories under task scratch.

```bash
mkdir "$TMPDIR/current" "$TMPDIR/previous"
git archive fc2dc0a95585ca19d75384965aacf1d4d51345ab | tar -x -C "$TMPDIR/current"
git archive 7a08f2ade3b9e597099aaece2aef2f8542852a41 | tar -x -C "$TMPDIR/previous"
.venv/bin/python -B tools/offline_validate.py tests_rollback \
  --rollback-current="$TMPDIR/current" --rollback-previous="$TMPDIR/previous"
```

The first archived build creates the journal. The rehearsal uses the same simulated charger, vendor and journal
across both releases. It verifies a genuine
managed charge and its delivery, the original newer Pause after the switch, and two later charges after Return.
Separate cases prove refusal before the previous process starts. The cross-build rehearsal takes explicit source
snapshots alongside the usual release qualification.

The [one-week installation rehearsal](developer.md#one-week-installation-rehearsal) uses synthetic records in the
same migrated layout. Its week of ordinary delivery is separate from the saved journal check, which reads the
archived records and the verified Pause that resolved a historical timed Start.

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
