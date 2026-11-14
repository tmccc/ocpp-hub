# Recovery coverage and qualification

The risk matrix lives in `tests_model/coverage.py`; finite families live in `tests_model/catalog.py`.
Safety and eventual recovery are separate obligations. Reaching zero alone does not prove that the next ordinary
session or fault notification works. Counts describe bounded input grammars, not every possible product history.

Generate the source-linked matrix, including safety, recovery, focused tests and remaining limits:

```sh
.venv/bin/python -B tools/recovery_coverage.py --output /absolute/task/coverage.json
```

The default output explicitly says **declared only**. Add
`--qualification-report /absolute/task/qualification-bundle.json` to freshly re-audit the original raw evidence
through the full validator before attaching execution claims. Generated reports belong outside the repository.

## Finite families

| Family | Raw | Admitted per suite | Inputs and mandatory recovery |
|---|---:|---:|---|
| `bound_owner_delivery` | 24 | 24 engine | Both genuine unpublished SIGKILL boundaries, unknown/exhausted cloud delivery,6/16/32A and both later demand orders; exact local closure settles only its owner request, retaining unmapped cloud debt for independent attestation, actual Start/Stop acknowledgments and two later sessions/outages. |
| `observation_retry` | 120 | 120 engine | Six observation classes, four actual reply/cloud cuts, same/replacement/process/new-Boot boundaries over real TLS; exact publication counts, preserved new untimed reports and two later mapped lifecycles. |
| `cloud_start_recovery` | 432 | 432 engine | One/two cloud CALLERRORs and four denied authorization statuses; native/local,6/16/32A, both demands, every close/Pause/replacement order; genuine mapped Stop, fresh Pause/Return and two later sessions/outages. |
| `issued_start_proof` | 36 | 36 engine | Three genuine SIGKILL issued/accepted-before-publication boundaries,6/16/32A, app/paused and both later demand orders; exact independent cancellation proof, terminal receipt before further controls and two later sessions/outages. |
| `current_transport` | 1,458 | 1,458 engine | All729 directed literal6..32A pairs under app/paused intent; authenticated management socket, independently verified old-value reset, exact actual profile/receipt, UUID replay without wire or durable changes, fresh closed-zero native view, and a separate completed Return plus exact target wire after paused selection. |
| `current_domain` | 2,916 | 524 engine |131 declared recovery pairs: every same value, every transition to/from6/32, and7→31/31→7; both intents/demands, exact UUID replay, immutable receipt after two later real sessions/outages. |
| `process_start` | 36 | 36 engine | Six actual SQL/preparation/COMMIT SIGKILL boundaries,6/16/32A, both demands; parent peers survive; independent committed-WAL read, new process/Pause/Return and two later sessions/outages. |
| `process_overlap` | 8 | 8 engine | Both6/32A directions, held native before/after applied target, both demands; real SIGKILL with mapped original session, automatic prior-cap restoration and atomic receipt retirement, original outage, new selection before Pause/Return, exact old UUID replay and two later sessions/outages. |
| `process_current` | 36 | 36 engine | Six directed6/16/32A changes, app/paused, request commit and both sides of success COMMIT; real SIGKILL, previous/committed selection, two later sessions/outages. |
| `storage_fifo` | 192 | 192 engine |6/32A, proof/ACK write or deferred-COMMIT failure twice,1/3 queued real pairs, changed malformed/CALLERROR reply, all repair/Pause/replacement orders; exact new proofs/ACKs, new Pause/Return and two later sessions/outages. |
| `two_receipt` | 11,520 | 6,240 engine | Existing timer/source/cutoff/cancel grammar; original plans and next daily occurrence. |
| `source_proof` | 1,920 | 960 engine | Source loss/restoration, observation, cutoff, replacement; fresh continuation proof. |
| `start_recovery` | 25,920 | 4,320 engine | Normal/fixed test,6/16/32A, nine actual refusal/native/protocol failures, demand, both closure orders, restore/Pause/successful-restart/observe/replay orders; later daily, outage and open-session reconnect. |
| `boot_reply` | 192 | 192 engine |6/16/32A, app/paused, real held zero/profile or composite reply, demand, reply/Boot/Pause orders and no-Boot controls; two later sessions/outages. |
| `boot_retry` | 72 | 72 engine | Real observed503,6/16/32A, app/paused, demand, all heal/Boot/Pause orders; zero proof and two later sessions/outages. |
| `current_resolution` | 1,368 | 1,368 engine | Six directed6/16/32 changes, nineteen real mode/boundary/outcome variants, demand, all resolve/Pause/Boot orders; confirmed selection and two later sessions/outages. |
| `current_replacement` | 72 | 72 engine | Six directed changes, native before/after profile or paused verification, Pause before/after replacement, demand; earlier selection and two later sessions/outages. |
| `stop_delivery` | 288 | 288 engine | Daily/local/closed-before-poll origins, malformed info/CALLERROR, demand, all real ACK/Pause/replacement/observe orders after exact offline proof; fresh Pause/Return and later session/outage. |
| `vendor_debt` | 24 | 24 engine | Accepted unbound/uncertain cloud Start, demand, all Pause/replacement/observe orders; distinct proof, new Pause/Return and later session/outage. |
| `ha_receipt` | 200 | 120 each HA runtime | Five controls including16A selection, initial/first/second/third response, five identity shapes; both terminals for matching IDs, completed false-success sentinel for invalid IDs; exact UUID, one mutation, readable status. |
| `ha_outage_lifecycle` | 384 | 240 each HA runtime | True first setup/failed reload, reload/unload/remove/disable-enable, stale healthy/fault callback, admitted retry/deadline/stale/lifecycle orders; recovery and second outage/deadline/recovery. |

Engine totals: **46,634 raw / 16,402 admitted**. Each HA client runtime has **584 raw / 360 admitted**.
Legacy HA has no registered finite family; its complete ordinary suite remains required. Ordinary functional tests
are additional executions and are counted separately by collection. The generated registry must agree with these totals.

Current transport coverage keeps every literal pair; it is not a scalar round-trip substitute for real wire checks.
The `current_domain` representation exclusions remove2,392 repeated recovery histories, while its raw2,916-input
universe remains visible. The131-pair cover preserves784 independently declared obligations: each old and target
literal, each same-value literal, every exact endpoint pair, and every old/new boundary-class/direction combination,
under each intent/demand. The independent checker also requires every retained case to own a unique obligation and
rejects missing or reversed representatives. This explicitly narrows repeated recovery histories; it does not prove
all729 pair-specific recovery histories equivalent. Existing failure, process-death and event-order families are retained.
Transport cases verify the actual effective station offer and fresh native closed zero; every target literal's positive
charging and zero-demand outage recovery remain in the heavier cover. Neither family claims autonomous serve polling.
Both families require the successful selection's exact completed metadata, selected/resume target, current intent,
confirmed offer and absence of unresolved current requests before recovery. This checks the shared successful boundary
independently of the old literal; pair-specific failure histories continue to use their existing separate families.

Storage/FIFO ACK histories save the actual cloud response before the ACK transaction fails. Each of two service
starts must witness a new exact-head DELETE transaction ending in rollback, while the saved response, queued tails,
proof and fault remain unchanged. Replacement retries that saved response without another cloud request. After SQL
repair the head may settle before the remaining Pause/replacement events; the first tail Stop response stays held
until that order finishes. `head-settlement-verified` records the later verification milestone, not a claim that the
physical ACK occurred after every prefix event. Proof-write cases retain their separate held-head reply boundary.

The HA receipt family excludes80 invalid-ID/failed-terminal inputs because identity validation rejects those replies
before reading their terminal state. The completed variant retains the false-success check for every action, position
and identity shape. Matching IDs retain both outcomes. Actual failed transport before a current change takes effect
is tested separately from applied-but-unanswered writes. All existing HA outage/lifecycle orderings remain enrolled.

Focused autonomous checks complement the finite families. The actual child service combines a saved accepted cloud
Start with failed acknowledgement COMMIT, repeated native503 responses, a genuine rejected zero profile and a new
Boot. Both storage/native repair orders at6/32A retain exact receipts and two later sessions/outages in the same service
process. HA's actual scheduler observes and recovers two successive transport/schema/cloud outages, including a
production-five-second polling control. Pending Pause tests retain positive independent peer output until an external
zero event. Shared manual recovery helpers require the exact completed Return receipt and at least three fresh actual
failed getConfig reads per outage; manual ticks or an older healthy paused status cannot satisfy those obligations.

Excluded inputs remain in the full manifest with exact reasons. Existing timer/source causal exclusions remain.
Start replay follows an accepted Pause; fixed timed tests require6A; `restart-ready` means successful listener startup
and real Boot response, so native restoration precedes it. Real unavailable startup/restoration has a separate test
with no premature controller. HA cannot retry setup after terminal unload/removal/disable without reactivation. Excluded
inputs are absent from runnable collection; they are never pytest skips.

Focused scheduler-loss controls cancel the real owned Start task before its first execution and inject a real SQLite
preparation-write failure. The attempt marker and prepared receipt commit together; neither missing preparation nor
an explicit rejected/unsent command may strand its exact management request after a fresh verified Pause. Actual subprocess tests additionally send SIGKILL at named boundaries while independently owned peers survive.
These controls model task/runtime/process death, not literal hardware power failure. Issued/accepted unknown debt keeps its stricter proof.

## Risk matrix

| Boundary | Safety | Recovery | Bound |
|---|---|---|---|
| Observation retry | Durable CALL identity is distinct from equal message bodies; pending receipts cannot be evicted | BEGIN/statement/COMMIT refusal rolls back receipt, queue and local status; repaired retry and later sessions succeed | A bounded recent retry window; new untimed reports after a fresh replacement Boot remain distinct. |
| Cloud Start reply | Explicit error retry, denied authorization and unknown delivery have different receipts | Retry budget survives restart; exact new proof after recovery itself fails; denied Start keeps mapping for real Stop | Mapping commits before the separate physical zero confirmation; held incoming zero commands prove both boundaries. Three transmissions with60/120-second waits; no automatic retry of an unknown outcome. |
| Start dispatch | Prepared/unsent/rejected differs from issued/accepted unknown; exact request/tag binding | Exact closed local lifecycle and atomic receipt/metadata settlement at verified zero; unmapped cloud debt remains separately blocked | Unknown/unbound debt still needs supported exact evidence. |
| Current/native proof | Real503/EOF/bad JSON, profile/composite rejection, applied CALLERROR, excessive composite | Prior selection unless target fully confirmed before revocation; new Pause/Return | All integer pairs for successful selection/idempotency; failure-order grammar uses representative currents. |
| Boot | Incomplete old proof cannot cross Boot | Fresh pair after incomplete proof; completed pair may survive identical metadata | Changed identity/configuration retains blocking focused tests. |
| Stop delivery | Original token/body/history retained; proof and pre-ACK Pause cannot fabricate ACK | Exact evidence, genuine wire ACK, new Pause, separate Return | Head permutations plus192 real repeated-storage/FIFO histories with1/3 queued pairs. |
| Cloud debt | Accepted unbound/uncertain commands never replay automatically | Distinct independent attestations then later ordinary lifecycle | Single cloud command grammar. |
| Closure disagreement | Exact known zero-output mismatch retains station zero | Fresh native/ledger agreement permits later sessions | Unknown IDs, nonzero phases and unrelated faults cannot use this path. |
| SQLite writes | Real abort/deferred-FK COMMIT at proof/ACK; actual bounded FULL and writer contention controls | Atomic rollback and later supported retry preserve evidence | Named boundaries and rollback/retry; no arbitrary WAL corruption or physical power-loss claim. |
| App receipts | Actual HA exchange rejects wrong UUID; never retries mutation | Honest recorded/unknown confirmation and readable status | Direct API coverage; charging recovery belongs to engine tests. |
| App lifetime | Public setup/retry/reload/unload/remove/disable reject obsolete callbacks | Second fault can notify at60s and dismiss on its own success | Twenty-two focused held-read/public-replacement/actual HA-stop histories per runtime; no cross-process HA restart claim. |
| Qualification | Canonical input regeneration, complete roots, raw once-only phases and source/runtime pins | All events and full suffix in one complete attempt | Synthetic negative controls are never real execution receipts. |

The machine matrix links exact tests for legacy state, source ownership, cutoff precedence and focused-only risks.
No risk is marked universally covered. Real hardware/network behavior and unbounded repeated failures remain outside
offline qualification. This work requires neither live charging nor deployment.

## Additional focused failure boundaries

The ordinary suites also retain explicit recovery checks outside the finite model families:

- `test_denial_operator_composition.py` combines an acknowledged earlier denial with another closed Start awaiting
  independent nonacceptance evidence. Real tool inspection/reconciliation, actual subsequent Start/Stop replies and
  one fresh Pause complete both proofs. Repeated lost replies, exhausted refusals, Stop refusal and a new denied
  reply retain the association; later sessions and native outages remain usable. The boundary tests bind immutable
  prior state, preserve valid proofs without a prior fault, and roll back joint proof/denial completion on SQL failure.
- `test_storage_runtime_boundaries.py` fails real vendor receipt retirement and preparation reads, then requires
  autonomous reconnection and later lifecycles. It distinguishes local storage failures from genuine cloud failure
  evidence. Actual outer commits, nested releases and cleanup refusal retain their correct transaction boundaries.
- `test_history_service_shutdown_storage.py` drives the real service at32A through an actual history-commit refusal,
  protective zero/Stop, durable remaining uncertainty, process replacement and later sessions/outages.
- The native-observation and introduction tests repeat malformed application responses, lose or refuse restrictive
  RPC replies, and supersede the initial zero with a newer Pause. Readiness still needs fresh validated content,
  current Boot/binding and confirmed closed zero. Validated content changes and HTTP framing remain restrictive.
- `test_managed_cloud_zero_controls.py` sends Fast and delayed-stop set/cancel through real managed TLS with cloud
  numbers0/1, exact positive local IDs, closed/unknown mappings and sequential reuse.
- `test_qualification_collection.py` expands real dynamic parameters without running test bodies. Full-mode controls
  consistently omit an ordinary parameter or file while retaining every model case; the independent denominator
  rejects them. Filtering, executor selection and configured mount boundaries have separate negative controls.

- `test_worker_storage_serve.py` keeps failed-completion and optional diagnostic SQL refusal active while the
  actual service reaches zero, observes real native HTTP loss, heals and completes two later sessions. The bounded
  current/Start polling and management read tests separately retain honest unknown status and real read failures.
- The measured-current, mixed-cause and native-repair tests separate fully validated measurement excess from
  configuration/history/resource/wire contradictions and ordinary unavailable/stale/closure waits. Real SQL refusal,
  replacement, newer Pause/Boot and an actual stop-fault operator bridge retain exact fault lineage. An interrupted
  unknown audit has no ordinary Pause waiver; attention/zero is not a claim of automatic recovery.
- `test_managed_start_cancelled_outer.py` cancels the actual outer task before its first instruction, then admits a
  genuine mapped original-plan daily at6/16/32A with both demands. Task disappearance cannot create Pause or lose
  the next session's outage supervision. Issued cancellation, real initial preflight refusal, atomic no-dispatch
  settlement and an earlier genuine owner receipt retain separate tests and their distinct outcomes.
- Process recovery checks compare the actual cloud Start allocation and final Stop with the independently read
  durable mapping. These are additional assertions in existing histories, not another count of the same cases.

These checks cover the specified histories and interfaces; they do not establish all possible combinations.

Additional named regression tests cover actual Accepted-Start death before/after publication with a late genuine
Start/Stop lifecycle, Return request/success commit death, and issued-unknown quarantine controls. Uncommitted current receipts become failed only with verified restoration to their prior selection through explicit Return or exact automatic original-session recovery; a new Pause revokes
only an older uncertain Return. Unknown issued debt
is deliberately retained without independent evidence; the separate issued-proof family proves its supported
operator cancellation workflow, fresh Pause/Return and later functional recovery. Ordinary lost RemoteStart
replies and CALLERRORs have directed real-TLS recovery tests too.
The finite process families cover known no-dispatch recovery, issued-proof recovery, current selection and automatic original-session recovery; they do not enumerate every
socket write or arbitrary interpreter instruction.

HTTP property checks cover every byte split of six valid messages and every incomplete prefix of two self-delimiting
messages, plus invalid versions/chunk numerals through a real loopback reader. UTC normalization boundaries traverse
real TLS for Start/Stop/Status/MeterValues, reject before durable effects, and permit a later genuine session.
On each HA runtime,11 raw malformed response shapes cross status/mutation/receipt positions, enormous integers
exercise age/current/progress fields, true first/reload failures recover, and every27 integer current service choices
is exercised. These are named property/regression cases; inner loop iterations are not extra finite-model executions.

Automatic current retirement additionally has labeled stored-state correlation controls, terminal-result byte preservation,
real statement/deferred-COMMIT rollback/retry, and unrelated-debt preservation. These direct boundary tests do not
claim that every corrupt state is reachable over the wire. Existing original-session source, Pause and failed-restoration
controls also check immutable terminal receipts after successful recovery.

Observation receipts commit with the queued event and local status effects. Pending publications are pinned;
terminal retention is bounded by the journal capacity (default10,000), with room for currently pending observations.
A replacement socket or process retains the logical generation until its first accepted Boot. A new Boot permits
fresh untimed events under reused counters; validated timed observations can still match their exact earlier CALL
and body. A new CALL ID always remains a new report. An identical untimed counter/body before replacement Boot
is inherently ambiguous: publication follows the retained retry window, while local cable-departure handling stays
conservative. No finite cache promises recognition after unbounded intervening traffic.

Mapping migration preserves original lifecycle, authorization metadata and clock-discontinuity references in one
SQLite transaction. A cloud number can recur only after the previous lifecycle and all its queued records drain.
Active controls use the sole open local session; historical queries refuse multiple matches. Late historical
meter reports wait while a newer mapping is unknown and are refused if its number was reused, preventing old
samples from being published into the new session. Migration rollback, wrong identity and repeated zero IDs have
directed checks. A code rollback must retain this journal and use a version compatible with any reused IDs.

Additional focused regressions cover healthy managed delivery while actual acknowledgement BEGIN, statement and
deferred-COMMIT writes fail, with two failure cycles, saved-reply replacement and later sessions/native outages.
They distinguish failure to save a received reply from failure of its acknowledgement commit. Unknown Start,
denied authorization, exhausted errors and Stop refusal retain separate recovery obligations.

The mixed-proof cases use the actual mapping tool and bind every proof across an accepted Pause and held native
reads. Multiple denied sessions need their own actual Stop receipts. Coupled vendor-command/mapping cases use
independent association and settlement evidence, atomic rollback and genuine later Stop delivery.

Worker regressions include both poll orderings, repeated cleanup refusal, a newer Pause/Boot and the actual service
and management API without helper ticks. Disconnect regressions fail the actual generation transaction and require
volatile Boot/control retirement, autonomous cloud disconnection and fresh reconnection. These are focused tests
linked by the risk matrix; their case counts do not change the finite-family totals above or claim a new qualification.

## Evidence contract

Follow `developer.md`: four complete20-worker phases (`engine-model`, `ha-proxy`, `ha-proxy-legacy`, `ha-legacy`)
on one clean commit, each with independent loopback peers, network namespaces and immutable runtime/source mounts.
Development samples never qualify. Every family has stable IDs and source-pinned inputs. The auditor independently
regenerates all rows, exclusions and summary from frozen source, including HA families. It rejects omitted families,
changed excluded inputs, duplicates, altered exclusions, wrong input digests, incomplete event lists, missing recovery
suffixes, skipped/failed phases, partial selection or source/runtime drift. CI shards derive their exact denominator
from the registry. Scope and limitation review remains necessary even when every declared case passes.
