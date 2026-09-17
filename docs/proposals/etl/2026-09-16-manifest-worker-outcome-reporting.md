# #3111 — the manifest worker's outcome reporting: what is wrong, what the repo already has, and how it was sliced

Status: **slice 1 shipped (`5579eaf4`); slice 2 shipped (`548d6bdf`); slice 3 WITHDRAWN on
evidence (§5b); slice 4 is this branch.** Revised after Codex
checkpoint 1 (24 findings), which falsified three claims in the first draft and surfaced a
parser-contract blocker that decides the ticket's shape; §5a revised again after a second
checkpoint-1 pass (38 findings) before slice 2 was written. Touches
`app/jobs/sec_manifest_worker.py`, `app/workers/scheduler.py`,
`app/services/job_telemetry.py`, `app/services/processes/scheduled_adapter.py`.

## 1. The premise holds — re-run on the full population

#3111 is an inherited root-cause handoff, so its discriminator was re-run before any code
(working-order 3c).

**Structural, read at `b23d2a13`:**

- `sec_manifest_worker_tick` (`scheduler.py:8025-8043`) sets `tracker.row_count` and logs the
  breakdown. It never sets `tracker.progress`.
- `_finish_tracked` (`:2858`) computes `degradation_reason(tracker.progress)`; with `progress`
  `None` that returns `None` — documented: *"a job that does not report progress is judged
  exactly as it was before"* — so the terminal row is `status='success'`.
- A tick in which every row failed therefore writes `success`, `error_msg=NULL`,
  `progress_json=NULL`. The ticket's synthetic probe is the right proof.

**Population, dev 2026-09-16 (read-only):**

| measurement | value |
| --- | --- |
| `sec_filing_manifest` by `ingest_status` | tombstoned 1,385,991 · parsed 926,260 · deferred 377,961 · **pending 3** |
| pending/retryable/failed by source | `sec_8k` 3 — nothing else |
| `job_runs` for `sec_manifest_worker`, ALL TIME (2026-06-03 →) | 24,717 runs: success 23,260 · skipped 1,374 · failure 83 |
| of those, with `progress_json` | **0 of 24,717** |

⚠ **Correction to the first draft (Codex 24).** It said the 83 failures were "all whole-tick
exceptions, so the outer path works". Overstated: 27 of them are
`orphaned: reaped at boot (owning worker thread died without a terminal status)`, which is the
BOOT REAPER writing a terminal row for a run that never reached the handler — not evidence that
the exception path recorded anything. The remaining 56 (13 interpreter-shutdown, 3 connection
exhaustion, 1 deadlock, 1 missing manifest row, 38 `illegal transition 'parsed' -> 'parsed'`)
are caught exceptions. Full `error_msg` read, never truncated.

The drained backlog (3 pending) is load-bearing for the design: most ticks now process zero
rows, so `candidates_seen=0` cannot trip the silent-stall condition, which requires `seen > 0`.
"Legitimate zero-work stays healthy" holds by construction rather than by a special case.

## 2. The second defect, located precisely

`_dispatch_rows`' outer `except` (`sec_manifest_worker.py:800-811`) logs, rolls back and
`continue`s — **incrementing no counter**. The row is in `rows_processed` (`len(rows)`) and in
`processed_by_source`, but in none of `parsed` / `tombstoned` / `failed` / `skipped_no_parser`.
The per-tick counters are short and the shortfall is silent.

⚠ Two corrections to how the first draft described it:

- **It is not exclusively transition/commit failure (Codex 10).** Malformed parser outcomes and
  attribute access on them reach the same handler.
- **It does not prove nothing was committed (Codex 9).** A parser may have committed its own
  typed-table work, and a commit acknowledgement can fail after durability. The honest claim is
  "the manifest row keeps its prior status and re-drains", not "the row's effects were undone".

## 3. What the repo ALREADY has — and the manifest worker uses neither

This is the finding that reshapes the ticket. There are **two** complementary mechanisms, both
already built and both already surfaced:

| mechanism | what it records | where it surfaces |
| --- | --- | --- |
| `JobProgress` (#2218) → `tracker.progress` | `candidates_seen` / `outcomes` / `errors`, and the degradation verdict | `status='degraded'` + the reason in `error_msg`, which the admin row and `/system/jobs` already render; `context`/`outcomes` paths via `watermarks.py:288-291`; **and, since slice 5, the `errors` buckets via `ProcessRunSummary.progress_errors` → History's Errored column** |
| `JobTelemetryAggregator` (`app/services/job_telemetry.py`) | `record_processed` / `record_error(error_class, message, subject)` / `record_warning` / `record_skip(reason)` → `rows_errored`, `error_classes` JSONB, `rows_skipped_by_reason` | `scheduled_adapter.py:788-797` → `/processes` `ProcessRunSummary` |

⚠ **Codex 6 was right and it matters: `progress_json` has NO operator-facing reader.**
`app/api/jobs.py`'s queries select `row_count, error_msg, linked_request_id` and never
`progress_json`; `scheduled_adapter` does not read it either. So persisting progress alone
cannot satisfy the acceptance's *"jobs-page drilldown exposes failed scope"*.

> ⛔ **SUPERSEDED — and it was already too strong when written.** Slice 5's checkpoint 1
> established that `processes/watermarks.py:288-291` selected four `progress_json` paths and
> `ProcessDetailPage.tsx` rendered them, so a reader existed all along; what was missing was a
> reader of the **`errors` axis** specifically. Slice 5 (§9) adds one —
> `scheduled_adapter._progress_error_buckets` → `ProcessRunSummary.progress_errors` →
> the History tab's Errored column. The paragraph above is kept rather than rewritten because
> slices 2 and 4 were designed against it and §5c's deferral cites it by name; read it as a
> claim about the `errors` axis at `b23d2a13`, now discharged. The slice-2/4 conclusions do not
> change — `JobTelemetryAggregator` was still the right mechanism for the failed SCOPE.

⚠ **Codex 17 was right too.** `scheduled_adapter:794` maps `row_count → rows_processed` while
reading `rows_errored` from its own column, which this job never writes. So the first draft's
plan to change `row_count` to `parsed + tombstoned` would have made an all-failed tick display
`rows_processed=0, rows_errored=0` — strictly worse.

**Both objections dissolve against the SECOND mechanism.** `JobTelemetryAggregator` writes
`rows_errored` and a per-error-class JSONB carrying `sample_message` and `last_subject` — which
is the "failed scope" the acceptance asks for, on a surface that already renders it. Wiring it
needs no API or frontend change. `row_count` then does not need to move at all.

## 4. ⛔ The blocker that decides the ticket's shape

Classifying a per-row outcome as an ERROR versus a SKIP cannot be done today.

- `docs/etl/sources/README.md` §"Retry posture" defines `tombstone` as *"a deterministic defect
  (empty body, missing required field, score-floor miss, unparseable payload)"* — so the first
  draft's "a policy tombstone is a real disposal, therefore progress" is **wrong as a blanket
  rule** (Codex 18). Some tombstones are disposals; some are defects.
- `docs/etl/sources/sec_n_csr.md` §3 documents `PENDING_CIK_REFRESH` → **24h backoff, recorded
  as `failed`** — an expected dependency wait, by design, giving the daily `cik_refresh` time to
  write the ext_id. Putting `failed` wholesale into `errors` degrades the job for a documented
  benign wait (Codex 20).
- The right axis exists — `JobTelemetryAggregator.record_skip(reason)` vs `record_error(...)` —
  but **`ParseOutcome` (`sec_manifest_worker.py:125-136`) carries only a free-text `error`
  string and no reason code.** Classifying from that text would be a classifier over source
  text, which this repo's engineering rules treat as a data-treatment decision requiring a
  source rule found first, not a regex written first.

So an honest error/skip split needs `ParseOutcome` to carry a machine-readable reason, across
the 8+ registered parsers. That is a contract change and it is the real work in #3111.

## 5. Slice plan

Each slice is independently complete and independently useful.

**Slice 1 — count what is currently uncounted.** Add `dispatch_errors` to `WorkerStats`,
incremented in the outer `except`. Establishes the tick identity

```
rows_processed == parsed + tombstoned + failed + skipped_no_parser + dispatch_errors
```

asserted in a unit test over a synthetic batch covering every exit. ⚠ `raw_payload_violations`
is **not** a term — `_dispatch_rows` writes `failed += 1` on that path too (`:704`), so it is a
SUBSET of `failed` and adding it double-counts. ⚠ The identity holds only for a NORMALLY
RETURNED `WorkerStats` (Codex 8): a selection or prefetch failure, or an interruption, exits
before the return and loses the counts for rows already committed. State that; do not claim the
identity covers exceptional exits.

**Slice 2 — wire `JobTelemetryAggregator`.** Thread one aggregator from
`sec_manifest_worker_tick` through `run_manifest_worker` → `_dispatch_rows`, record per-row
errors + skips, and flush with `flush_to_job_run(conn, run_id=tracker.run_id, agg=…)`. This is
the slice that makes failures operator-visible, on an existing surface, with no API or UI
change. ⚠ Its original one-line rule — *"until slice 3, every non-success is an ERROR, which is
the conservative direction"* — was **falsified before implementation**; §5a is the replacement
and the evidence.

**Slice 3 — ~~give `ParseOutcome` a reason code~~ WITHDRAWN.** The plan was to split error vs
skip so documented dependency waits (`PENDING_CIK_REFRESH`) became `record_skip`. Specced, put
through Codex checkpoint 1, and killed by it: the reclassification does not make the tick
healthy (rule 2 of `degradation_reason` fires instead of rule 1), and the class it moves is not
benign — its own source rule bounds the wait at 5 retries and live code has no budget. **§5b is
the falsification and the evidence.** Slice 4 does not depend on it.

**Slice 4 — `JobProgress` on the tracker.** `candidates_seen=rows_processed`,
`outcomes={parsed, tombstoned}`, `errors={failed, dispatch_errors}`, everything else in
`context`. §5c is the shipped shape. ⚠ `skipped_no_parser` goes in **context, not errors** —
see §6 — and so does `raw_payload_violations`, which is a subset of `failed`.

## 5a. Slice 2's classification, and the measurements that decided it

Revised after a second Codex checkpoint-1 pass over this section (38 findings). Six of them
changed the design; the ones that changed a NUMBER are recorded here with the corrected number,
not quietly fixed.

### The inherited rule is wrong — but so was the first census that showed it

"Every non-success is an ERROR" was written as a conservative placeholder. It is not
conservative: on this corpus it reports deliberate policy as failure.

⚠ **First census was on the wrong population** (Codex 3). It counted all 1,386,090
`ingest_status='tombstoned'` rows, of which **734,752 were written in bulk SQL by
`app/services/manifest_pre_retention_sweep.py`** — a path that transitions `pending ->
tombstoned` with *no parser dispatch at all*, so those rows never passed through
`_dispatch_rows` and say nothing about what a tick produces. The sweep tags itself
(`_TOMBSTONE_ERROR = "retention floor (bulk pre-fetch sweep)"`, `:116`) precisely so this
distinction stays measurable, and the parser path writes plain `"retention floor"` — so the
exclusion is exact, not a guess.

Correct population = tombstones this worker actually wrote, **651,338 rows**:

```sql
select left(coalesce(error,'<null>'),60), count(*) from sec_filing_manifest
 where ingest_status='tombstoned'
   and error is distinct from 'retention floor (bulk pre-fetch sweep)'
 group by 1 order by 2 desc;
```

| reason | rows | what it is |
| --- | --- | --- |
| `retention floor` | 234,463 | retention policy (parser pre-fetch gate) |
| `424B2 volume cap: high-volume structured-note filer` | 150,467 | volume cap |
| `latest-N primary cap` | 67,603 | per-filer cap |
| `archive index missing files (primary=None, infotable=None)` | 70,204 | defect |
| `parser returned None (…)` | 41,267 | defect |
| `missing instrument_id` | 39,190 | defect |
| `no beneficial-ownership table identified (best_score=…)` | ~37,000 | defect |
| `missing primary_document_url` | 6,935 | defect |
| `no Item 1 marker (plain 10-K)` | 666 | defect |

**452,533 of 651,338 = 69.5% of worker-written tombstones are deliberate policy filters**
(85.7% was the wrong-population figure; the conclusion survives the correction, the magnitude
does not). ⚠ The residual 30.5% is **not** "therefore all defects" (Codex 6): the table is a
truncated-prefix grouping and other intentional tombstones exist in it, e.g. N-CSR's
`INSTRUMENT_NOT_IN_UNIVERSE` (`docs/etl/sources/sec_n_csr.md` §3). It is a mixed remainder that
cannot be split without slice 3's reason code — which is the whole argument for slice 3.

⚠ Also struck: the first draft's "six-figure error count on the operator's Processes row" (Codex
5). An aggregator is per-run and a scheduled batch is at most 200 rows, so corpus totals can
never accumulate into one run. The argument is **proportionality, not magnitude**.

⚠ The live corpus is fully drained — `pending 0`, `failed 0`, `tombstoned 1,386,090`, `parsed
926,280`, `deferred 377,961` — so a steady tick records nothing at all. Slice 2 is
instrumentation for the next drain (`sec_rebuild`, a parser-version bump, atom-discovered
filings), not a reading of today.

### The axis: did the worker COMPLETE its handling of the row

This is the rule that resolves the failed-vs-tombstoned asymmetry Codex 9 called unjustified.
It is not "is the outcome good" — that question needs slice 3. It is:

- **tombstoned** — the parser made a terminal decision and the row moved. Handling completed.
  69.5% policy. Not an error.
- **parsed** — handling completed. (⚠ `parsed` does not mean "successfully extracted" — §6.2.)
- **`failed` / raised / dispatch-failed** — handling did NOT complete; the row is coming back.

⚠ **Stated cost, not hidden:** the ~30.5% remainder stays invisible under this rule, and that
remainder is MIXED (it contains genuine defects AND further intentional tombstones such as
N-CSR's `INSTRUMENT_NOT_IN_UNIVERSE` — the paragraph above says so and this one must not
quietly relabel it). Some unknown share of real defects is therefore unreported. That is the
signal slice 3 buys, and it is why slice 3 is not optional.

### What slice 2 records

`record_error` only where handling did not complete:

| branch | `error_class` | recorded |
| --- | --- | --- |
| parser raised | `ParserRaised:<ExcType>` | at the `failed += 1` site |
| #938 raw-payload violation | `RawPayloadMissing` | at the `failed += 1` site |
| parser returned `failed` | `ParserReportedFailure:<source>` | at the `failed += 1` site |
| transition/commit raised (slice 1) | `DispatchFailed:<ExcType>` | at the `dispatch_errors += 1` site |

⚠ **Recorded AT the counter increment, never before it** (Codex 14). A parser raise whose
`transition_status` then also raises increments `dispatch_errors` and NOT `failed`; recording
the parser error early would emit two errors for one row and break the correspondence
`agg.rows_errored == failed + dispatch_errors`. Same-site recording makes each row contribute
at most one error by construction.

⚠ The two `<ExcType>` classes are prefixed rather than bare: a `RuntimeError` from a parser and
a `RuntimeError` from `transition_status` mean opposite things (one stamped a retry, one left
the row untouched) and must not merge into one operator-facing class. Exception names are a
weak taxonomy (Codex 33) — accepted for slice 2, superseded by slice 3's reason code.

⚠ `ParserReportedFailure` is keyed **by source**, which is a structured field, not a classifier
over text. That gives the split an operator needs without pre-empting slice 3, and bounds
cardinality at the registered-source count (~16). It remains the one known-mixed class: it
contains `sec_n_csr`'s documented 24-hour dependency WAIT (`PENDING_CIK_REFRESH`), which is by
design. The parser's own reason text rides along as `sample_message`. ⚠ `record_error` keeps ONE
sample per class, so a later wait overwrites an earlier genuine fault's message (Codex 10) — the
count is exact, the sample is the latest, and that is all slice 2 claims.

`record_skip("no_parser_registered")` for `skipped_no_parser` — a skip by definition, and
structurally unreachable on the scheduled path (§6.1).

**Not recorded:** `parsed` and `tombstoned` (handling completed), and `record_processed` is not
called at all — `report_progress` already owns that surface for this job and a second writer
with its own throttle would race it.

### ⛔ The reader gate — without which this slice writes to nobody

`scheduled_adapter.py:896` populates `last_n_errors` **only when `process_status == "failed"`**
(Codex 1). A tick that completes `success` having recorded 200 errors would render none of them
— the identical defect to `progress_json`'s (§3), shipped a second time. Verified: the Errors
tab (`frontend/src/pages/ProcessDetailPage.tsx:755`, `ErrorsTab`) renders `row.last_n_errors`
with **no** status gate and its own empty state, so the adapter condition is the only blocker.

Fix: suppress on `running` / `pending_retry` only — the two states the existing comment's
auto-hide-on-retry rule actually names ("retry in flight" / "next fire covers failed scope") —
and otherwise surface whatever the terminal run recorded. This is a strict widening with zero
effect on today's rows: with no producer wired, `error_classes` is NULL everywhere and
`_build_error_summaries` already returns `()`.

⚠ NOT touched: the inline row chip (`frontend/src/components/admin/ProcessRow.tsx`) keeps its
own `status === "failed"` gate. That one is row-level noise control, and the drill-in is the
surface #3111's acceptance names. ⚠ Also NOT in scope (Codex 2): the run-history table does not
render `rows_errored`. That is a real gap and a UI change; the Errors tab satisfies the
drilldown clause without it.

### The clobber this wiring would otherwise have caused

`flush_to_job_run` writes all eight columns unconditionally, including `processed_count`,
`target_count` and `last_progress_at`. Those three are owned by `JobRunHeartbeat`
(`app/services/job_heartbeat.py`), which `_tracked_job` installs for every tracked job that has
a run id and a background pool (Codex 23 — conditional, not universal), and the manifest
worker's loop does call `report_progress` (`sec_manifest_worker.py:715`, `:859`). An error-only
producer therefore flushes `processed_count=0, target_count=NULL, last_progress_at=NULL` over a
live liveness stamp that #2274 shipped and that stale-detection reads.

There is no production producer of `JobTelemetryAggregator` today, so the trap has never fired —
and it would have fired on the **first** wiring and every later one. Fixed at the shared module:
each progress column is written only when the aggregator carries THAT piece of state, via
`COALESCE(%s, <column>)` with NULL for "unset". Per-column, not one `has_progress_state` flag
(Codex 17/18/19): a target-only aggregator must not zero `processed_count`, and a
processed-only one must not NULL the denominator. `set_target(0)` is meaningful and survives,
because the guard is "was it set", not truthiness.

⚠ This does not make the aggregator a safe partial writer in general (Codex 21): an error-only
flush still replaces `error_classes`/`rows_skipped_by_reason` wholesale, which is the documented
last-writer-wins contract and correct for a single producer per run.

### Flush placement, and what it does not guarantee

Flushed inside the tracked body on the worker's connection, in a `finally`, then committed —
so a raise that escapes `run_manifest_worker` still persists what the loop already recorded
(Codex 26). The flush is wrapped so it can never raise into the job: a telemetry failure must
not turn completed business work into a job failure or trigger a retry (Codex 28). `conn` is
rolled back first, because on the raising path it may be in a failed transaction (Codex 27).

Stated, not engineered around:

- `flush_to_job_run` has no `status='running'` predicate, so a run already terminalised by the
  orphan reaper can still receive telemetry (Codex 24). Left alone — it is the shared module's
  existing contract and the columns are descriptive, not terminal state.
- The flush and `record_job_finish` are separate transactions on separate connections, so there
  is no atomic "telemetry + terminal status" (Codex 25). The worker's connection closes before
  the finalizer runs, so they cannot deadlock on the row.
- `tracker.run_id` is `0` when start-recording failed; the flush is skipped rather than issuing
  an UPDATE that matches nothing (Codex 31).
- Process death mid-tick loses the aggregate entirely. Mid-tick `maybe_flush` would bound that;
  deliberately not done in slice 2 (it adds a second in-loop DB writer on a hot path for a
  failure mode the orphan reaper already surfaces).

## 5b. Slice 3 is FALSIFIED, and slice 4 does not need it

Slice 3 ("give `ParseOutcome` a reason code, then split error vs skip") was specced, put
through Codex checkpoint 1, and **withdrawn on the evidence**. Recorded rather than quietly
dropped, because the reasoning is the deliverable.

### What slice 3 was for, and why it cannot do it

Its stated purpose was to stop slice 4 painting a healthy tick red: `sec_n_csr`'s documented 24 h
`PENDING_CIK_REFRESH` wait is recorded as `failed`, so gating the verdict on the error set would
degrade the job for a by-design wait. Moving that class to `record_skip` was supposed to fix it.

**It does not.** `degradation_reason` (`app/services/job_progress.py:97-104`) has a SECOND
condition: candidates seen, no terminal outcome. A tick whose only dispatched row is a
dependency wait gives `candidates_seen=1`, `outcomes={parsed:0, tombstoned:0}` and — after the
reclassification — `errors={}`. Rule 1 stops firing and rule 2 starts:
`saw 1 candidates and produced no terminal outcome`. The run is degraded either way. Codex
reproduced both branches.

### And the class was not benign to begin with

Citing the source rule is what killed it. `docs/specs/fund-data/n-csr-metadata.md:429` §7.4
defines the wait as **bounded**:

> `failed_outcome` with 24h backoff (reason: `class_id_pending_cik_refresh`). **Up to 5 retries
> (5 days)** — gives daily cik_refresh time to populate. **Beyond that → tombstone permanent
> with `class_id_unknown_persistent`**

Live code has **no budget**: `sec_n_csr.py:396-403` stamps a fresh 24 h retry every time, with
no attempt counter — and `sec_filing_manifest` has no column to hold one (`information_schema`
read: `last_attempted_at` / `next_retry_at` and nothing else). Nor does the stale sweep bound
it: `tombstone_stale_failed_upserts` (`sec_manifest.py:829`) is anchored to the `error` prefixes
`upsert error: ` / `upsert+tombstone error: ` / `upsert+log error: `, and the wait's text is
`resolver pending cik_refresh — no in-universe classes yet`, so it never matches.

So the wait is unbounded in violation of its own source rule. **Classifying it as a benign skip
would have hidden that**, and a degraded verdict on it is the honest reading, not a false alarm.
⚠ It fires on the tick the row re-drains — once per 24 h — not on every tick; the first draft's
"degraded every tick, indefinitely" was wrong (Codex 10).

### Two N-CSR defects found while citing the rule — recorded here, fixed in neither slice

1. **`EXT_ID_NOT_YET_WRITTEN` can be a permanent wait.** `classify_resolver_miss`
   (`_fund_class_resolver.py:98-110`) tests `EXISTS (SELECT 1 FROM instruments WHERE i.symbol =
   mf.symbol)` with no tradability filter, while the writer that would satisfy the wait requires
   `symbol = %s AND is_tradable = TRUE` (`mf_directory.py:124`, deliberately — #1233 §6.2 does
   not seed class_ids for inactive instruments). A non-tradable match therefore classifies as a
   wait for a bridge row the writer will never create. The classifier should mirror the writer's
   gate, which routes it to `INSTRUMENT_NOT_IN_UNIVERSE` → tombstone, per §7.4 row 4.
2. **The §7.4 retry budget is absent**, per above. Landing it needs a durable attempt counter,
   i.e. a migration — its own slice, at the corpus rung with clauses 8-12.

Both are real and evidenced; neither is this ticket's subject, and neither is expanded into one
here. ⚠ `docs/etl/sources/sec_n_csr.md:15` is also stale — it says `EXT_ID_NOT_YET_WRITTEN` →
transient (1h) where the code puts it on the 24 h branch. §7.4 row 3 says "Same as above", so
the CODE is right and the per-source doc line is the error. Corrected in this PR.

### Corrections to my own withdrawn draft, so they are not re-made

- The 79 tombstone constructors span **12** modules, not 16; 16 is the count of modules holding
  any `ParseOutcome` call at all.
- Calling the non-policy tombstone remainder a "~30.5% defect remainder" repeats a claim §5a had
  already corrected: that remainder is **mixed**, containing further intentional tombstones
  (N-CSR `INSTRUMENT_NOT_IN_UNIVERSE`) alongside real defects. Its defect share is unmeasured.
- The draft's tombstone census ran over ALL tombstones, not §5a's worker-written population
  (which excludes the 734,752 rows `manifest_pre_retention_sweep.py` writes in bulk SQL without
  ever dispatching a parser), so it cannot support a worker-dispatch proportion.
- "The dependency-wait class has never been observed" is not supportable from current state:
  zero `failed` rows today is consistent with earlier failures that later retried successfully,
  tombstoned, or were rebuilt. The honest claim is that **no such row is present now**.
- One claim survived: an AST census (`ast.walk` filtering `Call.func.id == "ParseOutcome"`) over
  `app/` returns `{'tombstoned': 79, 'parsed': 18, 'failed': 12}` across 109 calls, and all 12
  `failed` constructors are per-module `_failed_outcome` bodies. The 25 hits `rg -n
  'status="failed"'` returns include 13 `_record_ingest_attempt(..., status="failed")` calls — a
  different function sharing a kwarg name.

## 5c. Slice 4 — the verdict

The tick's counters already exist and already add up (slices 1 and 2). Slice 4 spends them:

```python
tracker.progress = JobProgress(
    candidates_seen=stats.rows_processed,
    outcomes={"parsed": stats.parsed, "tombstoned": stats.tombstoned},
    errors={"failed": stats.failed, "dispatch_errors": stats.dispatch_errors},
    context={...},
)
```

Each of the ticket's four acceptance clauses falls directly out of `degradation_reason`:

| acceptance clause | mechanism | verdict |
| --- | --- | --- |
| all-failed batch is visibly degraded | rule 1, `errors.failed > 0` | `degraded`, reason in `error_msg` |
| legitimate zero-work stays healthy | `candidates_seen=0`, so rule 2's `seen > 0` cannot hold | `success` |
| policy tombstones stay healthy | `tombstoned` is an OUTCOME, so rule 2 is satisfied | `success` |
| partial failure exposes counts | slice 2's `rows_errored` + `error_classes`, already rendered | `/processes` Errors tab |

⚠ **`raw_payload_violations` goes in `context`, never `errors`.** It is a SUBSET of `failed`
(`_dispatch_rows` writes both on the #938 path), and `JobProgress`'s docstring forbids a bucket
appearing in two axes. Nothing about the verdict changes — `errors` is tested for "any non-zero",
not summed — but the contract is explicit and the overlap would mislead a later reader who does
sum it.

⚠ **`skipped_no_parser` goes in `context` too** (§6 correction 1): it is dead in production, and
`errors` is for work the job could not do.

⚠ **Deliberate: a dependency-wait tick degrades.** Per §5b that is the honest reading while the
§7.4 budget is missing. The objective trigger to revisit is that budget landing — at which point
the wait becomes provably bounded and a reason-code split can be re-argued on evidence.

⚠ **`dispatch_errors` is in `errors` but stays out of `row_count`**, which the tick's existing
comment already pins: `row_count` means rows whose status TRANSITIONED, and a dispatch error is
exactly the case where none did.

### ⛔ "Next eligible retry" is DEFERRED, with the reason

The acceptance also asks the drilldown to expose it, and slice 4 was built with it —
`WorkerStats.earliest_next_retry_at`, the minimum stamp the tick committed — then **removed at
Codex checkpoint 2**, which pointed out it had no reader. That is the defect this ticket exists
to remove, so shipping it would have been self-contradictory. Both candidate homes fail:

- **`progress_json`** — §3 of this very document establishes it has no operator-facing reader.
  ⛔ **This half of the deferral is DISCHARGED by slice 5** (§9e): the `errors` axis now has a
  reader, and §3's claim was already too strong. The deferral STANDS on the second reason
  alone — a per-manifest-row retry stamp is a different field with different semantics from an
  error census, so slice 5 does not smuggle it in.
- **`job_runs.next_retry_at`**, which `scheduled_adapter._read_latest_terminal_run` DOES select
  (`:302`, `:1084`) — but that column is machinery, not a display field. `jobs_retry_sweeper`
  scans it and **re-enqueues the job** (`job_retry.py:108` — *"only ever set on a
  `status='failure'` row"* — `:219`, `scheduler.py:7976`). Writing a per-manifest-row stamp
  there would re-fire `sec_manifest_worker` off a row-level backoff. Not a display change at
  all; a scheduling one.

So exposing it needs a new field carried through the adapter to the Processes drill-in, i.e. an
API + FE change, which is its own slice. **The acceptance clause is partially unmet and that is
stated rather than papered over.** What IS delivered is the other half of the same sentence —
the failed SCOPE, via slice 2's `rows_errored` + `error_classes`, already rendered.

⚠ The verdict itself does NOT depend on `progress_json` having a reader: it lands in
`status='degraded'` plus the reason in `error_msg`, which the admin row and `/system/jobs`
already render (§3). `progress_json` is the audit payload beside it.

**What slice 4 does not change:** no retry behaviour, no backoff, no manifest state transition,
no second health model, and no change to #2274's liveness axis.

## 6. Corrections to the first draft, recorded so they are not re-made

1. **`skipped_no_parser` is unreachable in production, and the reason given was wrong (Codex
   13/14).** The fairness path picks sources from `registered_parser_sources()`
   (`sec_manifest_worker.py:331`), so a scheduled tick can never pick an unregistered source —
   that part held. But the first draft then said the per-source rebuild path makes it reachable
   "on the operator `sec_rebuild` route". **False**: `grep -rn "run_manifest_worker("` returns
   exactly one caller, `scheduler.py:8030`, with `source=None`. `sec_rebuild` resets rows for
   the *scheduled* drain; it does not invoke the per-source worker. So the counter is dead in
   production, belongs in `context`, and the real missing-parser signal remains
   `GET /coverage/manifest-parsers`.
2. **`parsed` does not mean "successfully extracted" (Codex 19)**, and `parsed+tombstoned`
   measures terminal HANDLING, not new extraction. Any wording promising extraction is wrong.
3. **The `row_count` spike claim was wrong (Codex 15).** "At most one transition" assumed the
   baseline moves; `check_row_count_spike` compares against the previous **success**, so
   consecutive degraded runs all compare against the same one and can repeat the warning
   indefinitely. Moot now that slice 2 leaves `row_count` alone.

## 7. Deferred with the reason, not silently

**The two pollers stay out.** The ticket invites "wire per-CIK `poll_errors` too or explicitly
track that follow-up" — this is the explicit track, and the reason is evidence, not scope:

- `subjects_polled` **includes** errored subjects (per-CIK increments before probing;
  expected-filings returns `len(due)`), so using it as an `outcomes` bucket reports failed
  attempts as completed work (Codex 1).
- Recheck errors already fold into the shared `poll_errors` (`sec_per_cik_poll.py:384`), so a
  per-lane success count needs per-lane error counters first (Codex 2).
- Chronic degradation is plausible for both: an errored subject's `next_recheck_at` is left NULL
  = immediately due, so a persistently failing cohort can occupy the recheck budget and degrade
  every tick (Codex 3); expected-filings re-polls bad CIKs every interval until window expiry
  (Codex 4).
- `_probe_subject` returns `errored=False` for a rejected manifest write caught as `ValueError`
  (`:234`), and `expected_filings_poller` ignores `run_force_refresh`'s failures (`:419`) — so
  both would report silent failures as completed polls (Codex 21, 22).
- ⚠ None of that is measured: there is **no full-population error census for either poller**
  (Codex 5). Wiring them without one would set an alarm threshold on an unknown distribution.

⚠ Also recorded, not fixed: `PerCikPollStats` has no `recheck_poll_errors`, so the recheck
lane's errors are indistinguishable from the primary lane's.

## 8. What no slice does

- No change to retry behaviour, backoff or any manifest state transition. `degraded` is a
  reporting state; `record_job_finish` documents that it deliberately takes the non-failure
  retry path.
- No change to #2274's liveness (`last_progress_at` / heartbeat). Liveness and outcome quality
  are different axes and the ticket says so.
- No second health model. Both mechanisms used here already exist and are already surfaced.

Refs #3111. Refs #2218. Refs #2274. Refs #2437.


## 9. Slice 5 — `JobProgress.errors` reaches the Errored column

Status: **spec, rewritten after Codex checkpoint 1 (21 findings)**, which falsified three claims
in the first draft, replaced the display rule, and pointed at two documented source rules the
draft had reasoned around instead of citing. Filed on #3111 by the slice-4 close-out as *"the
obvious next slice — an API shape change with data semantics, so it wants its own diff"*.

Touches `app/services/processes/__init__.py`, `app/services/processes/scheduled_adapter.py`,
`app/services/processes/ingest_sweep_adapter.py`, `app/services/processes/bootstrap_adapter.py`,
`app/api/processes.py`, `frontend/src/api/types.ts`,
`frontend/src/components/admin/__fixtures__/processes.ts`,
`frontend/src/pages/ProcessDetailPage.tsx`, plus the two Python test modules and
`ProcessDetailPage.test.tsx`.

### 9a. The defect, and the full-population partition behind it

Slice 4 shipped the **Errored** column reading `job_runs.rows_errored`. Dev-verify then found
the one `degraded` run in the corpus rendering `—`:

```
133470 | daily_candle_refresh | degraded | rows_errored=0 | progress_json.errors={'failed': 1}
```

Census re-run here with the predicate the VERDICT uses (`any bucket > 0`), not a sum — a sum is
the wrong discriminator because `{a: 2, b: -2}` degrades the run and totals zero (Codex 12). Dev
DB, 2026-09-17, exhaustive partition, reproduce with:

```sql
with p as (select rows_errored, progress_json as pj, progress_json->'errors' as errs from job_runs)
select count(*) total,
       count(*) filter (where pj is null)                                     pj_null,
       count(*) filter (where pj is not null)                                 pj_present,
       count(*) filter (where pj is not null and jsonb_typeof(errs) is distinct from 'object') pj_no_errors_obj,
       count(*) filter (where errs = '{}'::jsonb)                             errs_empty,
       count(*) filter (where exists (select 1 from jsonb_each(errs) e
                                       where jsonb_typeof(e.value)='number' and (e.value)::numeric > 0))  any_positive,
       count(*) filter (where exists (select 1 from jsonb_each(errs) e where jsonb_typeof(e.value) <> 'number')) non_numeric,
       count(*) filter (where exists (select 1 from jsonb_each(errs) e
                                       where jsonb_typeof(e.value)='number'
                                         and (e.value)::numeric <> trunc((e.value)::numeric)))            non_integral,
       count(*) filter (where rows_errored > 0) scalar_pos,
       count(*) filter (where rows_errored < 0) scalar_neg
  from p;
```

| axis | count |
| --- | --- |
| total `job_runs` | 133,952 |
| `progress_json` **NULL** — the job does not report progress at all | 133,706 |
| `progress_json` present | 246 |
| …present but carrying no `errors` object | **0** |
| …`errors` present and empty `{}` | 7 |
| …**any bucket > 0** | **1** (run 133470) |
| …any non-numeric bucket value | **0** |
| …any non-integral numeric value | **0** |
| `rows_errored > 0` | **0** |
| `rows_errored < 0` | **0** |

The 245 present-but-not-positive rows are 239 `success` + 6 `failure` — no `degraded` run hides
in there, which is what the verdict predicts. Most carry error KEYS with ZERO values
(`{"failed": 0}`; `scheduler.py:3464` seeds that shape before the sweep runs): **presence of a
key is not an error**, and filtering on the key rather than the value would flag 239 clean
successes.

⚠ **Correction to the draft (Codex 5).** It claimed a run can only reach `degraded` *through*
`JobProgress.errors`. False — `degradation_reason` (`job_progress.py:105`) has a **second** rule:
saw candidates, produced no terminal outcome. Such a run is `degraded` with an empty error map
and **correctly** renders `—`; that case is in the acceptance below rather than papered over.

### 9b. Source rule — the predicate is already written down, twice

**Rule 1 — which buckets count.** `degradation_reason` (`app/services/job_progress.py:96-103`)
filters `{name: n for name, n in progress.errors.items() if n > 0}` and states why in its own
comment: *"`n > 0` rather than truthiness (Codex ckpt-3): a negative count is nonsense either
way, but truthiness makes `{"api_errors": -1}` degrade while `{"done": -1}` reads as progress"*.
The draft derived a `> 0` filter from the census. **It is not ours to derive** — the extraction
mirrors that predicate exactly, because the Errored cell and the Status cell are computed from
the same map and must never disagree about which buckets fired.

**Rule 2 — NULL is not zero.** `sql/254`'s `COMMENT ON COLUMN job_runs.progress_json` is
explicit: *"NULL means the job does not report progress, which is NOT the same as reporting
zero."* The draft collapsed NULL, unsupported adapters and measured-zero into one `{}` (Codex 7)
— the exact "a default value is not a measurement" shape it claimed to be avoiding. Corrected:
the field is **`dict[str, int] | None`**, `None` for "does not report", `{}` for "reports, none
positive".

### 9c. Display rule — buckets first, never summed

`rows_errored` and `JobProgress.errors` are disjointly written and **not summable**:
`sec_manifest_worker.py:760` pins `agg.rows_errored == failed + dispatch_errors` while `:367`
puts the same two counters into its `JobProgress`, so a sum double-counts. This slice does not
fold them.

The close-out left open *"no rule says which is authoritative per job"*. Answer it by enumerating
producers, not by picking an authority:

- **`rows_errored` has exactly ONE producer in the tree.** `JobTelemetryAggregator` is the sole
  writer (`job_telemetry.py:407`) and `rg` finds one instantiation — `scheduler.py:8093`, the
  `sec_manifest_worker` tick.
- That job is also a `JobProgress.errors` producer, and there the scalar is the buckets' **sum by
  construction** (`:367` vs `:760`).
- **Every other `JobProgress.errors` producer writes no `rows_errored` at all**
  (`scheduler.py:3464`, `:3508`, `:9296`, `:9909`; `:7073`/`:7107` declare no `errors` axis).

> **Render the positive `JobProgress.errors` buckets when there are any. Otherwise render
> `rows_errored` when it is `> 0`. Otherwise `—`. Never add them, never show both.**

⚠ **Buckets first, not scalar first** (Codex 16 changed this). The draft preferred the scalar and
justified it as "the buckets are only its decomposition". That is wrong on the operational axis:
`failed` means the row is coming back on a retry stamp, `dispatch_errors` means the state
transition itself failed. Equal totals do not make the split uninformative — so for the one job
that writes both, `failed 3 · dispatch_errors 2` is strictly more than `5` and still never
double-counts.

⚠ **This is a narrowing gate, so what it REJECTS is enumerated**: the scalar on a run that also
has positive buckets. Population affected today: **0 runs** (both `any_positive` and `scalar_pos`
partitions above are disjoint and the latter is empty). Right for `sec_manifest_worker`, the only
job able to produce that case, because there the scalar is recoverable as the buckets' sum.
**Trigger to revisit (widened per Codex 17):** any producer emitting a positive `errors` bucket
that is NOT part of its `rows_errored` total — which the manifest worker could do without a
second scalar producer ever landing. Guarded executably by a test pinning
`WorkerStats.as_progress().errors` against `rows_errored`, so the assumption fails a gate rather
than a reader's memory.

`rows_errored > 0`, not "non-zero" (Codex 11): `sql/137` puts no non-negative constraint on the
column, and `> 0` is both the existing UI predicate and rule 1's.

### 9d. Shape

`ProcessRunSummary` (`processes/__init__.py`) and `ProcessRunSummaryResponse`
(`api/processes.py`) each gain:

```python
progress_errors: dict[str, int] | None   # positive buckets only; None = job does not report progress
```

A **map**, not a scalar: `JobProgress`'s docstring warns `outcomes` buckets may overlap and says
nothing that makes `errors` buckets disjoint, so summing them here is the same unlicensed fold in
miniature. It also mirrors `rows_skipped_by_reason`, already `dict[str, int]` on this model.

**No dataclass default.** All three adapters set it explicitly — a default would let a new adapter
silently report a counter it was never asked about. ⚠ Corrected from the draft (Codex 4): they
pass **`None`**, not `{}`, and the reason is *not* "they report errors through `rows_errored`" —
that was false. `ingest_sweep_adapter:487` hard-codes `rows_errored=0` even for failed log rows,
and `bootstrap_adapter:299` counts failed **stages**, not rows. Neither is `job_runs`-backed and
neither has a `JobProgress`, so `None` — "does not report" — is the only truthful value.

**Both SQL projections must change** (Codex 1): `_read_latest_terminal_run` (`:300`) and
`list_runs` (`:1158`) each add `progress_json` to the SELECT, or the builder's `.get` returns
`None` forever and the change is inert. `_read_latest_terminal_sync_run` synthesises a terminal
row for orchestrator-driven jobs and has no such column; it gains an explicit
`"progress_json": None` beside its existing `"rows_errored": None`.

**`_convert_run` forwards it explicitly** (`api/processes.py:384`, Codex 2) — it hand-constructs
the response, so a model field alone would drop the measurement.

Extraction is defensive in the style of `_build_error_summaries`: a non-dict payload, a non-dict
`errors`, a `bool` value (Python's `True > 0` is `True`, and a boolean is not a count — Codex 9),
a non-`int` value and any value `<= 0` are all dropped, so one malformed row cannot break the
History tab. ⚠ **What that int filter could cost, stated rather than assumed** (Codex 10): a
positive non-integral value would degrade the run and render `—`. The census above measures
**0 non-numeric and 0 non-integral values across all 133,952 rows**, and every producer in the
tree assigns an `int` counter (`self.failed`, `summary.candles_failed`, `report.api_errors`,
`report.parse_failures`, …), so the filter drops nothing any producer can emit. If one ever does,
the behaviour is today's — `—` in the column with the Status cell still telling the truth — not a
regression.

### 9e. ⛔ This slice invalidates a premise used twice as a deferral reason — and the premise was
already narrower than written

Two comments say, in code, that `progress_json` has no operator-facing reader:

- `scheduled_adapter.py:909` — the slice-2 suppression comment.
- `sec_manifest_worker.py:361` — the §5c deferral of "next eligible retry", whose first rejected
  home is *"`progress_json`, which this ticket's own §3 established has no operator-facing
  reader"*.

⚠ **That claim was already false when written (Codex 6)**, and this slice is not what breaks it:
`processes/watermarks.py:288-291` selects four `progress_json` paths and `ProcessDetailPage.tsx`
renders the result. What did not exist is a reader of the **errors axis**, which is what slice 5
adds. §3's table, §3's prose, §5c's deferral paragraph and both code comments are corrected in the
same diff (Codex 21) — a stale claim is worst in the place a reader trusts most.

⚠ **The §5c deferral does NOT reopen.** Its second home stays barred for an unrelated and
unchanged reason (`job_runs.next_retry_at` is the retry sweeper's re-enqueue trigger), and a
per-manifest-row retry stamp is a different field with different semantics from an error census.
The premise moved; the slice stays deferred, recorded on #3111 rather than quietly acted on.

### 9f. Two operator-visible contradictions this creates, and their copy fixes

1. **The Errored tooltip becomes false** (Codex 18). It currently reads *"…A run can also degrade
   on JobProgress errors, which this column does not count"*. Replaced with per-branch wording
   that names the source actually rendered in that cell.
2. **The Errors tab would deny what History shows** (Codex 19). `ErrorsTab` reads only
   `error_classes` and says *"No errors on the latest terminal run"* — a universal denial it
   cannot support, and it would sit beside a History row reading `failed 1`. Narrowed to state
   what it reads. Copy only; no new read path.

### 9g. Acceptance

Backend, per Codex 20 — both SQL paths through serialisation, not one:

- `list_runs` and `get_row().last_run` both surface `progress_errors` for run 133470 (`{"failed": 1}`).
- `progress_json` NULL → `None`, not `{}` (sql/254's distinction survives to the API).
- Buckets present but all zero → `{}`.
- Multiple positive buckets are preserved separately, unsummed.
- A mixed map (`{"a": 2, "b": 0, "c": -1, "d": true, "e": "x"}`) yields `{"a": 2}` exactly.
- Orchestrator-driven job (synthesised `sync_runs` terminal) → `None`.
- `ingest_sweep` / `bootstrap` adapters → `None`.
- `WorkerStats.as_progress().errors` sums to the aggregator's `rows_errored` (the §9c revisit trigger, as a gate).

Frontend:

- `rows_errored=0, progress_errors={"failed":1}` → `failed 1`; `rows_errored=5, progress_errors=null` → `5`;
  both positive → buckets only; neither → `—`; `progress_errors={}` → `—`.
- A `degraded` run with no positive buckets (the no-terminal-outcome rule) → `—`, and that is correct.

Dev-verify on the real stack: `/admin/processes/daily_candle_refresh` → History renders `failed 1`
on run 133470 and `—` on its neighbours.

Revert-probes: dropping the value filter, dropping the bucket branch, dropping either SQL
projection, and reverting `_convert_run` must each fail a test.

### 9h. What slice 5 does not change

No producer, no counter, no degradation verdict, no retry behaviour, no job, no schema. It is a
read path and a render. `rows_errored` keeps its meaning and its column position.
