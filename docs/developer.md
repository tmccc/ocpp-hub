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

### Fast recovery replays

Run the standalone proxy's short recovery suite before a wider engine run:

```bash
TMPDIR=/absolute/task .venv/bin/python -B tools/offline_validate.py tests -m recovery_replay
```

The cases use the real SQLite journal and independent simulated charger. They cut the service after a Pause
commit and again before the replacement can finish that same Pause. They also deliver a real Start reply after a
newer Pause. The tests check the original request, charge outcome and actual commands before issuing Return, then
verify a subsequent ordinary charge and its history.

When a recovery defect is confirmed, mark its smallest reproducible case with `pytest.mark.recovery_replay`.
Verify that it fails on the faulty code at the relevant assertion and passes on the correction. Check the original
request and independent charger observations before any new Pause or other command can repair the outcome. Keep
this development run under three minutes on the controller and retain the complete engine run for release.

### Search overlapping recovery histories

The offline discovery run varies a held Start reply, two newer Pauses, an intervening Return, an actual SQLite
write refusal, and process replacement. It checks the saved intent, exact receipts and actual charger messages
after each step. The successful suffix settles the original reported lifecycle, then verifies a later charge after
Return.

```bash
timeout --kill-after=5s 295s .venv/bin/python -B tools/offline_validate.py tests_discovery \
  --discovery-output="$TMPDIR/discovery-19" --discovery-seed=19
```

Use an absolute task scratch directory for `TMPDIR` and a new output directory for each run. The default search
selects 20 histories. Use `--discovery-cases=4` for a quick sample or a different seed for more orders.
`summary.json` records the orders and the boundaries reached. A rule failure saves `failure.json` and attempts to
shorten it by removing one action at a time. Each kept deletion reproduces the same rule and event with fresh peers.
`reduced.json` keeps the last confirmed sequence if shortening runs out of time or a candidate cannot run.

```bash
timeout --kill-after=5s 295s .venv/bin/python -B tools/offline_validate.py tests_discovery \
  --discovery-output="$TMPDIR/discovery-replay" \
  --discovery-replay="$TMPDIR/discovery-19/reduced.json"
```

Use `failure.json` to replay the original sequence. A replay that finds a rule failure exits with a failing test.
The replay summary records whether the rule and event matched the saved ones. Confirm any real defect against
the independent evidence and add its shortest useful sequence to the fast recovery replays. The discovery run
is an explicit test target alongside release qualification.

### Recreating a reported interruption

Use the [local Start incident report](../README.md#read-an-interrupted-start-incident) to choose the offline event
order. Start with the report's selected current and the intent at the send. Match the original request and ticket
before using any reply or Pause from the observation list.

| Evidence in the report | Offline sequence to recreate |
| --- | --- |
| Saved reply `accepted`, authority publication false | Let the simulated charger reply Accepted. Kill the proxy after the reply commit and before the control decision commits. |
| Send observed, journal `issued`, observed and saved replies unknown | Hold the simulated charger's matching reply after it receives Start. Kill the proxy while the reply is held. |
| Observed reply `accepted`, journal `issued`, saved reply unknown | Let the matching reply reach the proxy. Kill it before the journal's reply commit finishes. |
| `start_sent`, newer `pause_committed`, then `start_reply` | Hold the old Start reply while accepting the newer Pause. Send the reply and check that the newer intent and older saved reply both survive replacement. |

The sequences reproduce the recorded evidence. A report may leave the original cause or exact crash boundary
unknown. `tests/test_managed_incident.py` uses the report to select a sequence and rerun it with fresh simulated
peers and a real journal. It also checks a genuine reply that arrives after a newer Pause.

### Home Assistant checks

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

Run the owner workflow on each supported client runtime with that runtime's Python:

```bash
TMPDIR=/absolute/task .venv-ha-proxy/bin/python -B tools/offline_ha_validate.py tests_ha_proxy/test_owner_workflow.py
```

Home Assistant and the standalone proxy run as separate processes with a simulated charger. The test stops the
proxy just after it saves Pause in one case and drops its reply while it keeps running in the other. A new Home
Assistant process loads the same saved entry. Both restart orders preserve the original Pause, verify zero at
the charger and let a later Return from Home Assistant restore scheduled charging.

#### One-week installation rehearsal

Use the two source snapshots in the [rollback instructions](release-process.md#rehearsed-standalone-software-rollback)
and run the week on either supported Home Assistant runtime.

```bash
.venv-ha-proxy/bin/python -B tools/offline_ha_validate.py tests_installation \
  --week-current="$TMPDIR/current" --week-previous="$TMPDIR/previous"
```

The test advances seven dates on one simulated charger timetable. Program time runs normally while the charger
reports the scheduled session times. Home Assistant changes the selected current from 6 A to 16 A, the charger
unplugs and reconnects, and one active charge continues after a brief interruption. On the newer build, a Pause
from Home Assistant ends that day's scheduled session early and keeps the vendor app from starting another one.
The older build reopens the same migrated journal, keeps the exact Pause and charges, and runs the last three
scheduled sessions after Return.

`tools/parallel_qualify.py` freezes ordinary tracked Git blobs and runs20 pinned Docker workers with separate network
namespaces, read-only source/runtime, task-owned scratch, four spare CPUs and a bounded timeout. All runtime paths,
immutable image IDs and optional SSH worker settings are explicit arguments. Output belongs outside the repository.
The engine/model phase uses `--suite engine-model`; the other required phases are `ha-proxy`, `ha-legacy` and
`ha-proxy-legacy`. Use each phase's matching provisioned virtualenv/image and the exact package pins in its requirements
file. Run the three HA suites first so client failures surface before the expensive engine/model phase.
Host-runtime mode explicitly mounts the worker's `/usr` and virtualenv; image-runtime mode uses the image's
interpreter/libraries and mounts its matching virtualenv at `/venv`. Package/interpreter identity and the mounted
virtualenv digest are recorded and checked; source and executed harness bytes are checked before and after the run.

The supervisor keeps one queue of the collected cases. A worker finishing a case pulls more work from that queue,
with one case of lookahead so pytest can keep its session fixtures. Each container keeps its own CPU, namespace,
port range and scratch directory for the entire session. The dispatch log and worker evidence must agree on every
grant, start and completion. An interrupted grant stays recorded and is never retried within the attempt.

Each engine execution worker mounts a private 128 MiB tmpfs for the Start recovery matrix. That matrix creates
each case's configuration and SQLite journal there, including the journal reopened after simulated replacement.
The default Python and pytest scratch paths, disk-specific tests, shared fixtures and qualification logs are
disk-backed. The auditor checks each case against the canonical Start recovery IDs and verifies the worker
mounts and the actual Start journals. Failed cases copy their RAM files to the worker's disk when pytest can
report the failure. Abrupt container removal can discard in-flight RAM files while retaining disk-backed logs.

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
must all pass the auditor. The source-pinned registry declares33,194 raw/9,202 runnable engine inputs and
584 raw/360 runnable HA inputs per HA runtime. The auditor regenerates all inputs and exclusions from frozen
source, including HA families, then verifies exact input/event/recovery evidence. See the
[recovery coverage matrix](recovery-coverage.md) for scopes, focused-only risks and limits.
Hosted CI collects the complete engine suite once and distributes that shared plan across 20 independent runners.
The plan includes ordinary engine tests, the full `tests_model` root and registered model cases under `tests/`.
Each job verifies its exact assignment and all reported phases against the plan. Model cases also retain their
input, event and recovery checks against the registry. The aggregate check requires all engine, static and HA
jobs to pass.

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
CI uses the same controlled discovery for its engine plan and HA jobs. The four-suite twenty-worker
qualification supplies the separate release evidence.

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

Physical authority has one writer, `Journal.control` in `core/prototype/control_state.py`. Intent/revision stays
with `ManagedRequests`, and actual Start outcomes stay with `Journal.starts`. Admission replaces the control
record; claim, hold, current selection and pending-Start projection preserve their existing captured preimages.
An operation completes only its current ticket. The owner preserves unknown fields, adds no schema or cache,
performs no I/O beyond the existing journal write and joins its caller's transaction without nesting one.

`ManagedController._tick()` is the orchestration: pending work, fault maintenance, supervised observation,
assessment and view publication. The poll and the API use `ControlProof` in `supervision.py` to keep observed
authority scoped to the current control record. A delivery wait can block local and cloud readiness while a
verified zero remains visible. A fault or shutdown withdraws authority labels while retaining physical evidence. Shared
and local-hold predicates distinguish legacy falsey flags from explicit Boolean confirmations. Optional cloud
authorization is read only after local readiness, preserving fault precedence. A snapshot never grants permission
for a later send; exact native/Boot/ticket checks remain at the actual effect and COMMIT boundaries.

`ManagedCharging` and `ChargeLease` in `managed_charging.py` own normal charging and bounded local/cloud tests.
The historical `managed_test_charge` journal key and public test command/status names remain unchanged.
Normal Start may ask the registered cloud to renew a configured tag whose stored Accepted reply has aged out.
`_renew_start_tag()` pins the lease, controller authority and exact stored authorization through the matched reply;
the gateway also pins its vendor connection. It runs before zero preparation or any charger effect. Explicit
denials remain denials, offline expiry stays enforced, and timed test admission does not gain this exception.
`start_authorization` is a diagnostic summary, never permission to send.

In the separate older integration, `PlanStore` retains a failed secondary planning-profile synchronization code.
The app-plan sensor exposes `profile_sync_pending` and `profile_sync_error` separately from the accepted native
planning flag. A successful explicit reapply clears the error; no background retry task is introduced. Session
history loading validates both the outer object and the sessions list before processing individual records.

`Journal.phase` in `installation_lifecycle.py` is the sole installation phase/introduction-flag writer. Its small
`InstallationState` names shared read predicates; every `read()` samples only the phase, and the separate
`starting` property preserves raw legacy values. Transitions retain the existing keys and values without adding
validation, transactions, device calls or a cache. Supplied control preimages and phase/control/flag write order
remain explicit, including native-history recovery's phase-before-hold order and phase-only introduction writes.
`test_installation_lifecycle.py` covers unknown JSON, the distinct fault policies, phase-only authority, ordered
publication, actual statement and deferred-FK COMMIT rollback, and the absence of an implicit transaction.

`ManagedRequests` owns durable control-intent serialization and Pause/Return request transitions. Domain recovery
uses `write_intent(journal, current, mode)` with its original captured intent and transaction; constructing a
request owner during native admission would prematurely normalize requests. `ControlIntent.next` advances the
revision. Legacy read recovery explicitly coalesces an already-paused mode. The same saved Pause revision decides
precedence across restart, including when the Pause receipt has completed. It supersedes interrupted work from an
older revision. A Start admitted during an existing Pause can share its revision. Exact physical hold checks use
the corresponding saved Pause or Stop ticket for the current revision.

In the reply transaction, Start and Return check the current intent, charger Boot scope, control revision and
ticket through `ManagedController.publication_is_current`. Start and explicit Return app publication then share
`_publish_app`. Confirmed charger outcomes and Start lifecycle records retain their own settlement. Start keeps
its Pause supersession and metadata publication. Return preserves the order of intent, previous-current settlement
and request completion. Its post-COMMIT memory notification stays with the existing handoff.

Return's running claim follows the recovery owner's busy/stopping exits and precedes completion launch. Queue
refusal retains `return_not_confirmed`; once launched, the owned completion alone requeues or records uncertainty.
The management request driver selects this claim; direct restore retains its existing accepted-before-publication
contract. Both entry points keep the same exact request guard and owned completion.
Stale revision, domain admission, physical Pause/native proof and each send/publication recheck remain distinct
moments. `return_in_progress` can restrict its existing accepted/running lookup to a particular intent revision.
The legacy healthy/app status path still completes only its selected accepted Return without changing intent.
`test_control_lifecycle.py` checks state filters, opaque historical results, startup and real SQL/COMMIT rollback.
`test_control_lifecycle_peer.py` drives the real engine through healthy status, busy/stopping and queue-full entry.
The unused controller exception wrappers are removed; callers use typed decisions and `RecoveryCoordinator.restore`.

`Journal.starts` (`core/prototype/start_lifecycle.py`) owns Start receipt and shared managed-charge metadata writes,
local Start outcomes and the sole process-local lease slot; pending-bit changes go through `Journal.control`.
`ChargeStart` loads the current
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

`start_settlement` owns the pure caller-named retirement rules and outcomes as well as failure selection.
The caller selects preflight refusal, unissued at zero, closed at zero, generic settlement or cancelled unprepared;
there is no rule-priority picker. These rules deliberately differ in legacy acceptance, request-state filters,
compact versus spaced result encoding, metadata local projection and proof strength. Missing attempted/published
markers remain distinct from explicit False. Metadata-only expiry stays separate from request completion.

`RetirementCandidate` centralizes the original snapshot contracts. Generic settlement pins only its selected
ledger row and exact lease object; cancellation pins metadata, full request and raw receipt, including None versus
an empty dictionary; closed-zero also pins pending and the canonical local session. Unrelated historical rows
are not frozen. A candidate grants no physical permission. Native Pause proof, cancellation/owned-I/O checks,
initial-zero restoration and current/Boot/resource guards stay with their callers. Unissued cleanup owns its
transaction; generic, closed and cancellation settlement join their caller transaction; memory follows COMMIT.

Failure selection consumes the same charge evidence with live worker facts. Initial zero counts as effects even
before RemoteStart dispatch. It still selects before reading fallible wire evidence, then selects prepared
retirement from the actual receipt; unrelated work exits before that receipt read. Stored results remain opaque
except for preflight-refusal classification. The shared SQL writer preserves each caller's exact action,
revision and state filters, result encoding, terminal bytes and cancellation row-count requirement.

The pilot `control_slice.py`, `control_slice_state.py` and `start_reference.py` live under `tests_model/`. Their
original assertions are preserved as reference checks. The temporary runtime observer/callback plumbing is gone;
production imports none of these modules. Run `tests/test_start_lifecycle.py` for fast stored-state, stale-preimage
and COMMIT-failure checks. `tests/test_start_retirement.py` covers the pure rules and scoped snapshots;
`tests/test_start_retirement_sqlite.py` checks actual SQL filters/bytes and raw-shape boundaries.
`tests/test_start_settlement.py` retains failure selection and the preserved reference.
The real late-Accepted-after-new-Pause witness remains in `tests/test_control_slice_peer.py`.

Use a coherent milestone selection covering the moved writers: native/peer handoffs, same-tag attribution,
expiry and later sessions, cancellation/SQL failures, operator/denial proofs, and actual process death around
preparation, dispatch and publication. Keep fast inner checks separate from those longer boundary tests. These
focused checks are development evidence; release still requires exact-clean-source four-suite qualification.

## Owned restore sequencing

`RecoveryCoordinator._run_restore` directly sequences each owned attempt. The initial read uses the original default closure mode, followed by closure reconciliation, session
agreement, the open-session check and zero proof. Optional native recovery is selected from current durable state
after those checks. The ticket is captured only at entry to the following verified read and zero proof. The release
guard then rechecks the current source domain and owner authority before the owned release. Later observations never
repeat the initial closure/session pipeline, recapture the ticket or loop back to recovery.

A Pending ends the attempt with its original reason/cause and no owned release result. Existing request policy
decides later work. The direct restore API retains its accepted-before-publication contract; the public request
driver alone selects the running claim before task launch. Busy/stopping exits, queue refusal, shielded completion
and post-completion supersession stay with the existing completion owner. No phase interpreter or replay exists.

The executor retains the exact native/Boot/resource/send guards, rollback permit, SQL publisher and post-COMMIT
memory callback. `release_disposition` still classifies each real completed result once; `restore_progress` maps
that verdict to progress in the same policy module. Claimed WAIT still acknowledges zero in its checked transaction; FAULT still records,
latches and raises the original failure. Pure constructors cannot perform either effect. The shared wire rollback
coroutine executes the same zero sequence inside the original LocalControl task, peer sequence and permit.

`test_recovery_contracts.py` covers terminal mapping. Existing real native, daily-schedule, storage, cancellation and
process-death tests retain their sequencing and effect witnesses. Preflight observers inspect the registered
`_restore_owned` frame with their original native/UUID/task conditions; no runtime observation callback is added.

`ResourceBook.confirmed_station_zero` owns the seven identical profile/receipt comparisons. Native observation,
resource publication and receipt SQL stay at each caller's original position. The pure comparison stays after
any lifecycle Pending return and inside its original short-circuit condition; malformed JSON retains its error.
Callers still select their original profile-kind scope. Weaker physical-presence checks remain weaker.
`test_station_zero.py` checks canonical equality and lazy decoding without a controller or network installation.

The experimental vehicle-pause handover and SEAT/physical-pause tools are retired. The session constructor rejects
unfinished or malformed old handover records before resource setup or network work, closes its journal on refusal,
and preserves completed historical records. `test_retired_handover.py` checks both remaining modes and every retired
operator command. Native-bounded commissioning retains its entry points.

`TrialSession` now requires `native_bounded=NativeBoundedSpec(...)`; remaining options after `settings` are keyword-only.
The plain commissioning Recovery/Rollback/StableObserver path and its old CLI commands are retired. The native
controller still owns admission, introduction, recovery acknowledgement and cleanup, with all phase/Boot/resource
checks and cleanup-only startup. An admitted plain journal is refused before resource setup; a prepared journal
receives full fresh admission. No existing journal is migrated or marked reconciled. `test_native_only.py` verifies
unchanged metadata/schema, missing specification, positional misuse, prepared admission and dormant CLI refusal.
Native commissioning Start and Release, shared lifecycle helpers and the daily mutation-in-flight fence remain.

The shared Gateway/resource tests retain their independent firmware evidence. Cleanup witnesses run the actual
native controller with real HTTP/TLS; native station kind/stack and incoming-auth dimensions remain. At
`backend_restore_pending`, repeated cleanup rechecks owned inventory so reappearance cannot receive success.

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
