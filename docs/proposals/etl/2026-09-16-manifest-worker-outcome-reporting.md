# #3111 — the manifest worker's outcome reporting: what is wrong, what the repo already has, and why it is four slices

Status: **slice 1 shipped (`5579eaf4`); slice 2 is this branch; slices 3 and 4 outstanding.** Revised after Codex
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
| `JobProgress` (#2218) → `tracker.progress` | `candidates_seen` / `outcomes` / `errors`, and the degradation verdict | `status='degraded'` + the reason in `error_msg`, which the admin row and `/system/jobs` already render |
| `JobTelemetryAggregator` (`app/services/job_telemetry.py`) | `record_processed` / `record_error(error_class, message, subject)` / `record_warning` / `record_skip(reason)` → `rows_errored`, `error_classes` JSONB, `rows_skipped_by_reason` | `scheduled_adapter.py:788-797` → `/processes` `ProcessRunSummary` |

⚠ **Codex 6 was right and it matters: `progress_json` has NO operator-facing reader.**
`app/api/jobs.py`'s queries select `row_count, error_msg, linked_request_id` and never
`progress_json`; `scheduled_adapter` does not read it either. So persisting progress alone
cannot satisfy the acceptance's *"jobs-page drilldown exposes failed scope"*.

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

**Slice 3 — give `ParseOutcome` a reason code**, then split error vs skip so documented
dependency waits (`PENDING_CIK_REFRESH`) become `record_skip` and deterministic defects stay
errors. The contract change is the cost; it is also what makes the signal trustworthy enough to
alarm on. Source rules to cite: `docs/etl/sources/README.md` §"Retry posture" plus each
per-source §3.

**Slice 4 — `JobProgress` on the tracker**, once slice 3 makes the error set honest. Mapping,
with slice 3's classification feeding `errors`:
`candidates_seen=rows_processed`, `outcomes={parsed, tombstoned}`, `errors={…}`,
`context={raw_payload_violations, processed_by_source, skipped_no_parser_by_source}`.
⚠ `skipped_no_parser` goes in **context, not errors** — see §6.

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
