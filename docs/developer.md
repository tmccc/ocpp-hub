# Developer guide

## Layout

| Path | What it is |
| --- | --- |
| `custom_components/ocpp_hub/core/` | The OCPP engine. Pure Python, no Home Assistant imports, `mypy --strict` clean. |
| `custom_components/ocpp_hub/` | The Home Assistant layer: config flow, setup, entities, services. Thin; it holds no protocol logic. |
| `tests/` | Engine tests (plain pytest, no Home Assistant). `conftest.py` loads the core by path as `ocpp_hub_core`; `fake_charger.py` and `fake_vendor.py` are real websocket peers. |
| `tests_ha/` | Tests of the Home Assistant layer: config flow, setup, entities, diagnostics, the mirror options. They need the Home Assistant test plugin, so they run in `.venv-ha`. |
| `tools/` | `validate.py` (CI parity in one command), `version.py`, `check_translations.py`, `deploy_ha.py`, `release_gate.py`, `mutation_check.py`. |
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
file. Host-runtime mode explicitly mounts the worker's `/usr` and virtualenv; image-runtime mode uses the image's
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
- Never write to a charger from tests against a real device. The engine is tested with a fake charger over a real websocket.

## Reference implementation

When you build or change how the hub handles a charger, read how [lbbrhzn/ocpp](https://github.com/lbbrhzn/ocpp) (MIT) does it first and reuse what it proved on real chargers; `.agents/skills/ocpp-expert/SKILL.md` lists the files and functions to read. Compare the result with what the Raedian NEO really does, and credit adapted code in `NOTICE`.

## Fixtures

Recorded charger sessions go in `tests/fixtures/` with their stored bytes and complete frame sequences preserved, with a note of where and when they were captured. Sanitised, reserialised derivatives must declare that provenance; they are not original wire bytes. Raw local captures (`capture_*.jsonl`) can hold card ids and serial numbers, so they stay out of git. Do not trim a capture to the frames that look relevant; the frames you leave out are where bugs hide.

## Deploying to a Home Assistant server

```bash
python tools/deploy_ha.py --dry-run     # shows the target and the files, changes nothing
python tools/deploy_ha.py --batch       # checks, back up, deploy, verify by sha256
```

It reads `HA_HOST`, `HA_SSH_USER` and `HA_SSH_KEY` (a path) from the git-ignored `.env`, never a password. Restarting Home Assistant is a separate step.
