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
| `two_receipt` | 11,520 | 6,240 engine | Existing timer/source/cutoff/cancel grammar; original plans and next daily occurrence. |
| `source_proof` | 1,920 | 960 engine | Source loss/restoration, observation, cutoff, replacement; fresh continuation proof. |
| `start_recovery` | 25,920 | 4,320 engine | Normal/fixed test,6/16/32A, nine actual refusal/native/protocol failures, demand, both closure orders, restore/Pause/successful-restart/observe/replay orders; later daily, outage and open-session reconnect. |
| `boot_reply` | 192 | 192 engine |6/16/32A, app/paused, real held zero/profile or composite reply, demand, reply/Boot/Pause orders and no-Boot controls; two later sessions/outages. |
| `boot_retry` | 72 | 72 engine | Real observed503,6/16/32A, app/paused, demand, all heal/Boot/Pause orders; zero proof and two later sessions/outages. |
| `current_resolution` | 1,368 | 1,368 engine | Six directed6/16/32 changes, nineteen real mode/boundary/outcome variants, demand, all resolve/Pause/Boot orders; confirmed selection and two later sessions/outages. |
| `current_replacement` | 72 | 72 engine | Six directed changes, native before/after profile or paused verification, Pause before/after replacement, demand; earlier selection and two later sessions/outages. |
| `stop_delivery` | 288 | 288 engine | Daily/local/closed-before-poll origins, malformed info/CALLERROR, demand, all real ACK/Pause/replacement/observe orders after exact offline proof; fresh Pause/Return and later session/outage. |
| `vendor_debt` | 24 | 24 engine | Accepted unbound/uncertain cloud Start, demand, all Pause/replacement/observe orders; distinct proof, new Pause/Return and later session/outage. |
| `ha_receipt` | 160 | 160 each HA runtime | Four controls, initial/first/second/third response, five identity shapes, two terminal results; exact UUID, one mutation, readable status. |
| `ha_outage_lifecycle` | 288 | 192 each HA runtime | True first setup/failed reload, reload/unload/remove, stale healthy/fault callback, admitted retry/deadline/stale/lifecycle orders; recovery and second outage/deadline/recovery. |

Engine totals: **41,376 raw / 13,536 admitted**. Each HA client runtime has **448 raw / 352 admitted**.
Legacy HA has no registered finite family; its complete ordinary suite remains required. Ordinary functional tests
are additional executions and are counted separately by collection. The generated registry must agree with these totals.

Excluded inputs remain in the full manifest with exact reasons. Existing timer/source causal exclusions remain.
Start replay follows an accepted Pause; fixed timed tests require6A; `restart-ready` means successful listener startup
and real Boot response, so native restoration precedes it. Real unavailable startup/restoration has a separate test
with no premature controller. HA cannot retry setup after terminal unload/removal without reactivation. Excluded
inputs are absent from runnable collection; they are never pytest skips.

## Risk matrix

| Boundary | Safety | Recovery | Bound |
|---|---|---|---|
| Start dispatch | Prepared/unsent/rejected differs from issued/accepted unknown; exact request/tag binding | Exact closed mapped lifecycle and atomic receipt/metadata settlement at verified zero | Unknown/unbound debt still needs supported exact evidence. |
| Current/native proof | Real503/EOF/bad JSON, profile/composite rejection, applied CALLERROR, excessive composite | Prior selection unless target fully confirmed before revocation; new Pause/Return | Representative currents; same-value/idempotency and old formats have focused controls. |
| Boot | Incomplete old proof cannot cross Boot | Fresh pair after incomplete proof; completed pair may survive identical metadata | Changed identity/configuration retains blocking focused tests. |
| Stop delivery | Original token/body/history retained; proof and pre-ACK Pause cannot fabricate ACK | Exact evidence, genuine wire ACK, new Pause, separate Return | Head permutations; FIFO tails/repeated rejection have focused tests. |
| Cloud debt | Accepted unbound/uncertain commands never replay automatically | Distinct independent attestations then later ordinary lifecycle | Single cloud command grammar. |
| Closure disagreement | Exact known zero-output mismatch retains station zero | Fresh native/ledger agreement permits later sessions | Unknown IDs, nonzero phases and unrelated faults cannot use this path. |
| SQLite writes | Real abort triggers at proof, ACK/dequeue and receipt/metadata completion | Atomic rollback and later supported retry preserve evidence | Named boundaries; no arbitrary WAL/disk/power-loss claim. |
| App receipts | Actual HA exchange rejects wrong UUID; never retries mutation | Honest recorded/unknown confirmation and readable status | Direct API coverage; charging recovery belongs to engine tests. |
| App lifetime | Public setup/retry/reload/unload/remove reject obsolete callbacks | Second fault can notify at60s and dismiss on its own success | Disable/HA-stop and arbitrary core scheduler histories are not exhaustive. |
| Qualification | Canonical input regeneration, complete roots, raw once-only phases and source/runtime pins | All events and full suffix in one complete attempt | Synthetic negative controls are never real execution receipts. |

The machine matrix links exact tests for legacy state, source ownership, cutoff precedence and focused-only risks.
No risk is marked universally covered. Real hardware/network behavior and unbounded repeated failures remain outside
offline qualification. This work requires neither live charging nor deployment.

## Evidence contract

Follow `developer.md`: four complete20-worker phases (`engine-model`, `ha-proxy`, `ha-proxy-legacy`, `ha-legacy`)
on one clean commit, each with independent loopback peers, network namespaces and immutable runtime/source mounts.
Development samples never qualify. Every family has stable IDs and source-pinned inputs. The auditor independently
regenerates all rows, exclusions and summary from frozen source, including HA families. It rejects omitted families,
changed excluded inputs, duplicates, altered exclusions, wrong input digests, incomplete event lists, missing recovery
suffixes, skipped/failed phases, partial selection or source/runtime drift. CI shards derive their exact denominator
from the registry. Scope and limitation review remains necessary even when every declared case passes.
