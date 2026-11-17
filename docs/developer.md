# Developer guide

Start with the [code navigation guide](code-navigation.md) for the active service, its shared imports, control flows,
test ownership and review checkpoints. The two HA integration domains have different runtime and deployment roles.

## Layout

| Path | What it is |
| --- | --- |
| `tools/managed_proxy.py`, `tools/managed_reporting.py`, `tools/trial_proxy.py` | Persistent iCS service, status output and HA-free core loader/preflight. |
| `custom_components/ocpp_hub/core/prototype/` | Current iCS protocol, native observation, managed controls, persistence and recovery. The directory name is historical. |
| `custom_components/ocpp_hub/core/` | Shared engine modules, loaded as `ocpp_hub_core` by the standalone service. No Home Assistant imports. |
| `custom_components/ics_proxy/` | Active thin HA client: management API, polling, entities, notifications and diagnostics. |
| `custom_components/ocpp_hub/` outside `core/` | Older Raedian HA integration, with its own listener, entities and services. Separate from the iCS client. |
| `tests/` | Engine tests (plain pytest, no Home Assistant). `conftest.py` loads the core by path as `ocpp_hub_core`; `fake_charger.py` and `fake_vendor.py` are real websocket peers. |
| `tests_model/` | Finite input registry, parameter generators, recovery obligations and model tests. Some registered families also live in `tests/`. |
| `tests_ha_proxy/` | Actual-core tests of the active iCS client on both supported HA runtimes. |
| `tests_ha/` | Tests of the older `ocpp_hub` integration, using the pinned HA test plugin. |
| `tools/` | Service/admin entry points, validation, qualification/evidence auditing, HA deployment and release helpers. |
| `.agents/skills/` | Skills for coding agents: `ocpp-expert` and `home-assistant`. |

## Setup and checks

```bash
python3.14 -m venv .venv
.venv/bin/pip install -r requirements-offline.txt
.venv/bin/python tools/validate.py --quick
.venv/bin/python tools/validate.py --qualification-report /absolute/task/qualification.json
```

`--quick` runs static checks only, including strict typing for the core and HA-free client API, compilation of both
integrations, version/translation/YAML checks and `git diff --check`. The line limit is120 characters, including comments.
Full validation re-audits raw execution evidence against the current clean commit; it never treats a report's own
success flag as proof or silently launches the combined suite in a single process. An explicit `--tests-only -k`
selection is useful for development and is labelled targeted, with the remaining suites outside that result.
Documentation-only changes need link/path/symbol checks and `git diff --check`, without executing qualification.
Code review begins with static reading; a review prompt is not permission to run tests or deployment commands.

The Home Assistant layer tests need their own virtualenv. Pin the plugin to the Home Assistant version of the server you deploy to (the plugin release pins it):

```bash
python3.14 -m venv .venv-ha
.venv-ha/bin/pip install -r requirements-ha-legacy.txt
TMPDIR=/absolute/task .venv-ha/bin/python tools/offline_ha_validate.py --legacy-ha tests_ha
```

The separate `ics_proxy` client is tested on actual HA2026.10.4 and2026.11.0. The latter uses
`requirements-ha-proxy.txt` and keeps the older HA test plugin disabled. Its fixtures instantiate actual HA registries,
config entries and entities, with synthetic loopback management peers and DNS construction isolated from multicast.
The packaged legacy Hub retains its HA10 minimum; it is a different deployment domain.

`tools/parallel_qualify.py` freezes ordinary tracked Git blobs and runs20 pinned Docker workers with separate network
namespaces, read-only source/runtime, task-owned scratch, four spare CPUs and a bounded timeout. All runtime paths,
immutable image IDs and optional SSH worker settings are explicit arguments. Output belongs outside the repository.
The engine/model phase uses `--suite engine-model`; the other required phases are `ha-proxy`, `ha-legacy` and
`ha-proxy-legacy`. Use each phase's matching provisioned virtualenv/image and the exact package pins in its requirements
file. Run the three HA suites first so client failures surface before the expensive engine/model phase.
Host-runtime mode explicitly mounts the worker's `/usr` and virtualenv; image-runtime mode uses the image's
interpreter/libraries and mounts its matching virtualenv at `/venv`. Package/interpreter identity and the mounted
virtualenv digest are recorded and checked; source and executed harness bytes are checked before and after the run.

For an existing provisioned Linux worker, the invocation has this form; all capitalized paths/IDs are operator inputs:

```bash
python3.14 tools/parallel_qualify.py --source "$REPOSITORY" --output "$TASK_OUTPUT" \
  --worker-host "$SSH_ALIAS" --worker-root "$REMOTE_TASK_ROOT" --worker-python "$WORKER_PYTHON" \
  --venv "$WORKER_VENV" --image "$FULL_IMAGE_ID" --workers 20 --suite engine-model
```

Repeat with the matching HA runtimes and suite names. `--runtime-mode image --venv-target /venv` selects an image's
matching virtualenv; `--readonly-mount HOST=CONTAINER` supplies any explicitly provisioned OpenSSL runtime needed by
that image. Configured auxiliary mounts have a deliberately narrow contract: image-mode `/usr/bin/openssl` requires
an actual regular file with no symlink, and `/task/openssl-runtime` requires a directory. Other auxiliary destinations
are unsupported. No auxiliary target may equal, contain or sit inside the concrete batch (source and harness), evidence
scratch, venv or interpreter, or protected system code roots; the exact image-mode OpenSSL file is the sole system-file
exception. A configurable primary venv target must also remain separate from source/evidence. Paths with parent
traversal and duplicate targets fail. The launcher checks final destinations before dispatch; the worker and fresh
collector independently repeat the checks and verify source types on their actual executor. New runtime assets require
an explicit policy update, not an overlay of pinned code. Every attempt has its own directory; interrupted runs are
retained and cannot be combined into one pass.
Raw setup/call/teardown, once-only assignments, process exits, namespaces, source digests and model event/suffix evidence
must all pass the auditor. The source-pinned registry declares46,634 raw/16,402 runnable engine inputs and
584 raw/360 runnable HA inputs per HA runtime. The auditor regenerates all inputs and exclusions from frozen
source, including HA families, then verifies exact input/event/recovery evidence. See the
[recovery coverage matrix](recovery-coverage.md) for scopes, focused-only risks and limits.
CI derives the exact model collection from that registry across20 independent runner jobs and includes all HA suites.
The model shard collector retains the complete `tests_model` root and adds exact registered engine test functions
from other roots. Its structural regression checks the real collection against the canonical manifest, including
process-death families, while excluding ordinary named tests from the model-only CI partition.

Large targeted socket-test batches also need separate offline network environments. The Python loopback guard alone
shares the host's connection tables; sustained parallel HTTP traffic has exhausted that table and caused unrelated
peer startup timeouts. Use the same read-only source/runtime mounts and separate `--network none` containers as the
full worker, with only each task's scratch writable. Retain failed attempts and investigate their evidence before rerun.

The final qualification JSON contains `commit`, `source_digest` and a `phases` object mapping those four suite names
to their absolute batch directories. `validate.py --qualification-report` requires all four, all20 workers, and the
same source identity, and re-runs the raw auditor for each. Missing, skipped, changed-source or failed phases fail
validation. `OCPP_QUALIFICATION_REPORT` can supply the same path to deployment checks.

Full auditing also performs a fresh **collection-only** pass over every test file in the frozen source, using that
suite's exact pinned Docker runtime. Pytest expands real parameter generators; the auditor compares every resulting
ordinary and model ID with the original run. No test bodies run during this pass. Source definition/file inventories,
raw generated items, effective arguments and deselections are retained. Filters, ignore settings, project omission
hooks and inherited `PYTEST_ADDOPTS`/`PYTEST_PLUGINS` cannot silently shorten the universe. Host filter variables are
rejected and Docker workers explicitly override image-level pytest selection variables.

A fresh audit therefore needs access to the provisioned runtime and Docker executor. The default is local collection;
a missing local runtime fails rather than trusting `independent-collection.json` or choosing SSH from raw evidence.
For remote runtimes, explicitly pass a trusted executor map:

```bash
python tools/validate.py --qualification-report "$QUALIFICATION_BUNDLE" \
  --collection-executors "$TRUSTED_COLLECTION_EXECUTORS"
python tools/recovery_coverage.py --output "$MATRIX_OUTPUT" --qualification-report "$QUALIFICATION_BUNDLE" \
  --collection-executors "$TRUSTED_COLLECTION_EXECUTORS"
```

The caller-supplied JSON pins `commit` and `source_digest` and maps every suite under `executors` to either
`{"kind":"local","batch":"/absolute/local/batch"}` or an explicit
`{"kind":"ssh","host":"WORKER_ALIAS","supervisor_python":"/absolute/worker/python","batch":"/absolute/remote/batch"}`.
Use the actual batch for each of `engine-model`, `ha-proxy`, `ha-proxy-legacy` and `ha-legacy`. The descriptor is never
automatically loaded from a run's evidence; source pins, chosen suite/batch and the remotely executed helper hash are
checked before collection. SSH and collection have bounded timeouts and no cached-denominator fallback.
`parallel_qualify.py` forwards its explicitly chosen worker settings to its immediate audit. Later validator,
coverage-matrix and deployment-gate invocations must pass their intended trusted executor map themselves.
The supported `deploy_ha.py --collection-executors` option forwards the same caller-selected map to its validation gate.
CI performs the same controlled discovery before its ordinary and HA jobs; this does not replace the separate
four-suite twenty-worker qualification.

## Rules for code

- Every function in code has a short plain comment directly above it saying what it is for (add why when it is not obvious).
- Every test has an `# Intent:` and a `# Why:` comment directly above it.
- Put protocol logic in `core/`, and keep `core/` free of Home Assistant imports. If a function needs `hass`, it belongs in the layer above.
- Prefer small pure modules: they are cheap to test and to mutation-check (`tools/mutation_check.py <module> <test file>`; a surviving mutant is a missing test).
- Name data shapes (dataclasses) instead of passing loose dictionaries around.
- Managed restoration uses `RecoveryCoordinator`; check `RestoreProgress.shared` before settling domain records.
  Forward named `Pending` results through typed guards, retaining strict, storage and cancellation failures.
  Follow the [recovery ownership map](code-navigation.md#module-map) for compatibility adapters and actual effect
  boundaries.
- Never write to a charger from tests against a real device. The engine is tested with a fake charger over a real websocket.

## Control transitions and Start settlement

`Journal.starts` (`core/prototype/start_lifecycle.py`) owns Start receipt, pending-bit and shared managed-charge
metadata writes, local Start outcomes and the sole process-local lease slot. `NormalStart` loads the current
request, wire receipt, metadata projections and actual ledger. An older receipt remains historical until the new
request prepares. Receipt tag/local identity is usable only with matching projections and current-request or
explicit published legacy provenance; contradictory stored copies never become corrected permission.

Wire Accepted does not mean managed publication succeeded. `start_rpc_accepted` retains its existing publication
meaning, and `control.start_pending` remains stored evidence. In particular, a Rejected reply after a newer ticket
preserves the existing sticky pending state. No journal key, field, schema or replay mechanism is added. Startup
request normalization remains in `ManagedRequests`; a lease is never restored from disk.

The owner joins the existing preparation and authority-publication transactions. Live scope, native/resource
checks, current intent, restrictive execution, timer and task retirement stay with their existing callers.
Closed-zero, denial, operator and ordinary local completion share identity and write ownership while retaining
their different proof rules, exact reason strings and original terminal request bytes. Local closure leaves
cloud Start/Stop acknowledgement duties intact. A memory-releasing entry point requires its own transaction;
callers that supply an outer transaction release memory only after its successful COMMIT. No await belongs inside
a journal transaction.

`start_settlement.settle_start` remains the pure production failure selector. Initial zero counts as effects even
before RemoteStart dispatch; prepared receipt retirement precedes a diagnostic-only exit. The async caller
rebuilds facts on storage retries and executes the selected settlement without an intervening await.

The pilot `control_slice.py`, `control_slice_state.py` and `start_reference.py` live under `tests_model/`. Their
original assertions are preserved as reference checks. The temporary runtime observer/callback plumbing is gone;
production imports none of these modules. Run `tests/test_start_lifecycle.py` for fast stored-state, stale-preimage
and COMMIT-failure checks, and `tests/test_start_settlement.py` for the pure selector and preserved reference.
The real late-Accepted-after-new-Pause witness remains in `tests/test_control_slice_peer.py`.

Use a coherent milestone selection covering the moved writers: native/peer handoffs, same-tag attribution,
expiry and later sessions, cancellation/SQL failures, operator/denial proofs, and actual process death around
preparation, dispatch and publication. Keep fast inner checks separate from those longer boundary tests. These
focused checks are development evidence; release still requires exact-clean-source four-suite qualification.

## Reference implementation

When you build or change how the hub handles a charger, read how [lbbrhzn/ocpp](https://github.com/lbbrhzn/ocpp) (MIT) does it first and reuse what it proved on real chargers; `.agents/skills/ocpp-expert/SKILL.md` lists the files and functions to read. Compare the result with what the Raedian NEO really does, and credit adapted code in `NOTICE`.

## Fixtures

Recorded charger sessions go in `tests/fixtures/` with their stored bytes and complete frame sequences preserved, with a note of where and when they were captured. Sanitised, reserialised derivatives must declare that provenance; they are not original wire bytes. Raw local captures (`capture_*.jsonl`) can hold card ids and serial numbers, so they stay out of git. Do not trim a capture to the frames that look relevant; the frames you leave out are where bugs hide.

## Deploying to a Home Assistant server

Follow [the release process](release-process.md) after the owner requests deployment. For the current thin client,
select both its domain and the existing HA container explicitly:

```bash
python tools/deploy_ha.py --domain ics_proxy --container "$HA_CONTAINER" --dry-run
OCPP_QUALIFICATION_REPORT="$QUALIFICATION_BUNDLE" python tools/deploy_ha.py \
  --domain ics_proxy --container "$HA_CONTAINER" --batch \
  --collection-executors "$TRUSTED_COLLECTION_EXECUTORS"
```

Use the caller-selected executor map appropriate to the evidence. A restart requires its own authorization and the
documented restart options. This installs only the HA client, leaving the standalone service under its own supervisor.

For the older Raedian integration, use `--domain ocpp_hub`. Without `--container`, the helper uses the SSH target from
`HA_HOST`, `HA_SSH_USER` and `HA_SSH_KEY` in the git-ignored `.env`. The default domain is `ocpp_hub`; omitting the
domain does not select the iCS client. Neither transport installs or replaces the standalone proxy service.
