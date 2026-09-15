# #2274 — a tick site for `sec_filing_documents_ingest`, the one job the residual recommends

Scope: a per-item `report_progress` call plus one final `force=True` call in the existing
candidate loop of `app/services/filing_documents.py::ingest_filing_documents`, and the job's
name in `scripts/verify_2274_heartbeat_exposure.py::COVERED_JOBS`. No threshold change, no
new constant, no other job body, no schema change, no migration.

This is the work the heartbeat-writer spec
(`docs/proposals/ops/2026-09-15-2274-job-heartbeat-writer.md` §3) explicitly deferred:

> The other five have no tick site and are untouched. Adding tick sites to them is separate
> work and is NOT in scope — **a tick site is a claim about a loop's item granularity, and
> inventing one per job without reading the loop is how a fake heartbeat gets shipped.**

So the bar it sets is: read the loop, and claim only the granularity the loop actually has.

## 1. Source rule, and which of the two producers this uses

`app/services/processes/stale_thresholds.py:41-45` states the producer contract for this job
as the justification for its 1,800s override:

> SEC bulk-download / archive-driven scheduled jobs. **Each emits one tick per accession or
> per archive completion**; quarterly / monthly ingest jobs are slow-tick by design. 30 min
> sits well above the observed worst-case inter-tick gap.

No such producer exists. `ingest_filing_documents` never calls `report_progress`, and
`job_runs.last_progress_at` is NULL on **all 1,662** stored runs of this job. The registry's
comment is the rule and the code contradicts it — the same shape as #3086, where
`sql/181_quotes_last_positive.sql`'s own COMMENT was the mark rule the read path violated.

⚠ **Two mechanisms write these three columns and the registry comment describes the other
one.** `JobTelemetryAggregator.record_processed` (`app/services/job_telemetry.py:197-204`)
bumps `processed_count` + `last_progress_at` and is what "one tick per archive completion"
refers to; `report_progress` → `job_heartbeat.JobRunHeartbeat` writes the same columns
through the listener `_tracked_job` already installs. This uses `report_progress` because
the aggregator has to be threaded through the service signature by hand and the listener
does not — which is the stated reason `app/services/job_heartbeat.py` exists. The column
semantics are identical either way, so the registry's cadence claim is satisfied by either
producer.

⚠ The two mechanisms' documented conventions differ on FAILED items:
`job_telemetry.py`'s producer example calls `record_processed()` only after `ingest()`
returns, whereas `report_progress`'s shipped call sites tick on every branch
(`market_data.py:714` ticks a skipped instrument, `fundamentals/__init__.py:617,622` tick a
failed fetch and an empty-facts result). §4.2 takes the `report_progress` convention and
says why; the divergence is real and is reconciled there rather than left implicit.

## 2. Premise, measured on dev (2026-09-15 19:5xZ). Queries are in the stake comment on #2274

| | |
| --- | --- |
| runs, all stored | 1,662 |
| runs with a heartbeat, all stored | **0** |
| orphan reaps, 90 days | **48** (`error_msg ILIKE '%orphan%'`) |
| orphan reaps, last 14 days | 4 — **3 of them today** (16:35Z / 17:35Z / 18:35Z, killed 491s / 1,052s / 415s in) |
| durations, `status='success'` | p50 **123s**, p90 362s, p95 558s, p99 985s, max **9,662s** |
| candidate SELECT, the unprotected prelude | **5.85s**, one timed execution against dev |
| candidate backlog, no `LIMIT` | **1,937,602** |

⚠ "all stored" is this job's whole `job_runs` history, not a window. The duration
percentiles are `status='success'` only and therefore **cannot bound a run's remaining
time** — a censored run is excluded by construction. The 9,662s maximum is a floor on the
worst case, not a ceiling.

The three reaps today were each caused by one of this loop's own deploys re-detaching
`~/Dev/eBull` at `origin/main`. That is the incident class `dev_reload`'s docstring records
and the protection `6ffa4a04` shipped. It does not reach this job because
`_LIVE_JOB_SQL` requires `last_progress_at IS NOT NULL` — though that is not the *only*
condition the protection needs (§5.1).

### 2.1 What the reap actually costs — smaller than "lost work", and not nothing

The loop commits per item, so completed work is durable. What a reap costs is the in-flight
fetch, a partial batch, and a `failure` row that is a pure deploy artefact.

⚠ The delay is negligible against the backlog and the spec should say so rather than imply
urgency: at 500 candidates per hourly fire, 1,937,602 candidates is a ~161-day drain, so 48
reaps of a partial batch each are well inside the noise of that queue. The selector is
`ORDER BY fe.filing_date DESC`, so a reaped run's unprocessed candidates are still the
newest un-ingested rows and are re-selected next fire — but no exact-scope retry is
guaranteed, and a burst of >500 newer filings would defer them.

**So the value claimed here is the liveness contract and the 48 spurious `failure` rows on
an operator-visible SEC-health surface, NOT rescued work.** Every loop session so far has
had to explain these away by hand in its own run note.

## 3. The loop, and what one iteration actually is

`ingest_filing_documents` (`app/services/filing_documents.py:265`):

1. one bounded `SELECT` → `candidates: list[...]`, `LIMIT 500` (the `limit` param's default);
2. `for filing_event_id, accession, cik, primary_url in candidates:` — per iteration
   `fetcher.fetch_filing_index(...)` (an SEC round trip), `parse_filing_index(...)`,
   `upsert_filing_documents(...)` + `conn.commit()`.

⚠ **The unit is a filing-event attempt, not a distinct accession.**
`sql/144_filings_fanout_per_instrument.sql:11` documents that the same accession may appear
for several instruments, and the live batch confirms it: **500 candidate rows carry 490
distinct `provider_filing_id`s**. The registry comment says "per accession"; what the
counter counts is per filing event, and §5.3 states that where the column is read.

⚠ "One fetch and one committed write per iteration" would also be false. A fetch raise, a
`None` body and an empty parse all write nothing; `resilient_client` may issue up to
`1 + max_retries` requests for one item. The accurate claim is **one attempted SEC round
trip per iteration**, which is what makes the granularity real rather than synthetic — the
same sense in which `market_data.refresh_market_data`'s per-instrument loop and
`fundamentals.refresh_financial_facts`'s per-triple loop are item-granular.

### 3.1 Cadence — what is measurable before the producer exists, and what is not

500 candidates inside a 966s run is **~2s/item on average**, so `report_progress`'s existing
10s clause is expected to fire roughly every 5 items, giving ~180× margin against the
1,800s threshold.

⚠ That is an average over one run and it bounds neither the worst item nor the worst
inter-tick gap. **Within-run gap distribution cannot be measured before the producer
exists** — no run has ever emitted a second tick — exactly as the writer spec records for
the other four. It is a dev-verify follow-up, not a precondition.

⚠⚠ **There is one concrete unbounded-gap path.** On HTTP 429 `resilient_client._retry_delay`
returns the `Retry-After` header verbatim with no ceiling (`app/providers/resilient_client.py:361-362`
— 5xx caps it via `min`, 429 does not), slept inside the fetch, up to `max_retries` times.
A hint above 1,800s therefore stalls the loop past the threshold. The consequence is
**graceful**: the heartbeat goes stale, `live_job` stops returning the row, and the job is
preemptible exactly as it is today. No new failure mode, and no reason to invent a cap here.

⚠ It cannot be shown not to have happened from stored state: this job has 0 runs carrying a
`RateLimited` warning class, but it has no `warning_classes` producer either, so that zero
is vacuous. Recorded as vacuous rather than quoted as evidence.

## 4. Design

### 4.1 One tick site, in a `finally`, not one before each `continue`

```python
total = len(candidates)
for idx, (filing_event_id, accession, cik, primary_url) in enumerate(candidates, start=1):
    try:
        ...  # unchanged body, including its four `continue` branches
    finally:
        report_progress(idx, total)
if candidates:
    report_progress(total, total, force=True)
```

The two shipped producers repeat the call before each `continue` (`market_data.py:714`,
`fundamentals/__init__.py:617,622`, with a common tail tick at `:646`). This loop has
**four** early-exit branches — fetch raise, `raw is None`, `not docs`, upsert raise — and a
tick missed on any one is a heartbeat that stops precisely when the job is having trouble. A
future fifth branch would silently lose its tick. `continue` runs `finally`, so one call
covers every path.

⚠ `report_progress` swallows callback exceptions (`progress.py:144-147`) so a heartbeat
fault cannot convert into a lost item. It catches `Exception`, **not** `BaseException`: a
`KeyboardInterrupt` or `SystemExit` raised inside the callback during unwinding would
replace the body's exception. Accepted rather than engineered around — it requires a signal
landing inside a 3s-bounded UPDATE, and the alternative (a bare-except in a `finally`) is
worse.

⚠ An exception escaping the loop skips the post-loop forced call, so the last item's count
may remain at its throttled value. Immaterial: both consumers read the TIMESTAMP, which the
`finally` already stamped, and the writer spec records the same lag as accepted.

### 4.2 `idx` counts items ATTEMPTED

`enumerate(..., start=1)` advances after the item is attempted, matching `processed_count`'s
meaning in `sql/140_per_run_progress_telemetry.sql:29-33` ("producer's `record_processed`
bumps it") and the `report_progress` call sites that tick skips and failures.

Deliberately **not** `filings_parsed`. A fetch error is work done and rate budget spent, and
a counter that moved only on success would stall the heartbeat on a bad batch —
reintroducing the reap for exactly the runs most likely to be long. §5.3 states the cost of
that choice, which is real and is not hidden here.

### 4.3 The final `force=True` tick, and why it is guarded on a non-empty batch

Both shipped producers end with `report_progress(total, total, force=True)` so the last
count lands when the final delta is under the throttle. Guarded on `if candidates:` because
an empty batch has attempted nothing: a `(0, 0)` tick would stamp `last_progress_at` on a
run that did no work, which is the fabricated-progress claim `set_active_progress`'s
`initial_tick=False` exists to forbid.

⚠ `force=True` bypasses `report_progress`'s throttle only. `JobRunHeartbeat.__call__` keeps
its own 5s write floor (`job_heartbeat.py:140-141`), so the final call may be **emitted and
not persisted** — a 7-item run can leave `processed_count` at 5. That is the lag the writer
spec already documents as immaterial ("Both consumers read the TIMESTAMP"). §6 keeps the
three layers — call site, callback emission, DB write — separate for exactly this reason.

### 4.4 The 5.85s prelude stays unprotected, deliberately

No tick can precede the `SELECT` that produces `total` without fabricating one. 5.85s is one
timed execution and excludes connect + provider setup, so treat it as an order of magnitude
rather than a bound — against a 1,800s threshold and a 180s drain budget it is not the gap
that decides anything.

## 5. Consequences, both directions

### 5.1 `dev_reload` — two consumers, and the wait is NOT bounded by one run

`live_job()` is read in two places, not one:

* `_supervise()` (`dev_reload.py:582`) — withholds an automatic reload;
* `_stop_child(extend_while_live=True)` (`dev_reload.py:440-452`) — extends an
  already-started drain in `_DRAIN_EXTENSION_S` (60s) slices instead of escalating to
  SIGKILL. This change therefore also affects post-SIGTERM escalation.

Occupancy, via `scripts/verify_2274_heartbeat_exposure.py`'s true union (`range_agg`, not a
sum):

| window | covered jobs today | + `sec_filing_documents_ingest` | this job alone |
| --- | ---: | ---: | ---: |
| 7 days | 5.0% | **8.1%** | 3.8% |
| 90 days | 32.2% | 34.6% | 4.6% |

⚠ Occupancy is **not** a deploy-deferral probability and is not converted into one — deploys
are not uniformly distributed, which is the error that script's docstring records. It is
also not *protected* occupancy: the query truncates runs crossing the window's left edge,
counts an orphan's span until it is reaped, and ignores first-tick delay, heartbeat expiry
and the runtime ceiling. Its baseline covers `COVERED_JOBS` only, so the already-protected
`strategy_backtest_run` is excluded.

⚠⚠ **`_LIVE_JOB_SQL` bounds each RUN's age by `RUNTIME_CEILING_S`; it does not bound the
DEFERRAL.** The supervisor re-probes every 15s and the scheduler keeps starting runs, so
successive live runs can hold a reload past 86,400s in principle. What limits it for this
job is that it is hourly and 3.8%-occupant, not continuous — the reason the residual's
recommendation named this job and not `sec_manifest_worker`. Worst observed single-run wait
in the last 7 days is **1,989s (33 min)**, and the real wait also includes probe cadence,
drain and respawn.

⚠⚠ Operationally: a `touch app/__init__.py` deploy may not respawn the child for tens of
minutes and **that is designed behaviour, not a broken deploy**. `dev_reload` names the
blocker every 60s (`_DEFER_PROGRESS_S`); read the supervisor log before concluding a touch
failed.

### 5.2 Rule 4 — better on healthy long runs, WEAKER on an all-failing batch

`stale_detection` rule 4 keys on `COALESCE(last_progress_at, started_at)` for the ACTIVE
run. Today this job always takes `started_at`, so a 1,900s run reads `mid_flight_stuck`
however healthy it is; the prior pass put this job's false-red exposure at **0.0026**
expected rows (non-failure runs, 90 days) — and `2026-09-15-2274-mid-flight-stuck-halt-mask.md:165`
puts it at **0.0023** over `status='success'` only. Different populations, same order; both
are ~0.002 of one row, which is why **no threshold moves** here either.

⚠⚠ The honest cost: a batch whose every item fails still ticks, so rule 4 now reads that run
as healthy where it would previously have gone red at 1,800s. This is the right call —
`mid_flight_stuck` means *wedged*, and a job doing one HTTP round trip per item is not
wedged — but it is a real loss of an accidental signal, and it is not "strictly better".

⚠ Incidental, NOT fixed here and not turned into an audit: `sec_filing_documents_ingest`
records `tracker.row_count = result.documents_inserted` and propagates neither
`fetch_errors` nor `parse_misses` into error/degradation telemetry, so an all-fail batch
already finishes `success` with `row_count=0`. One line on the PR; pre-existing.

### 5.3 API fields change; no UI does

`scheduled_adapter._build_active_run` (`:768-780`) maps the columns into three
`ActiveRunSummary` fields. For this job today `processed_count` is `0` — the column is
`INTEGER NOT NULL DEFAULT 0` (`sql/140:55`), **never NULL** — so `rows_processed_so_far` is
`None` (the `> 0` guard) and `progress_units_done` is `None` (no `target_count`). After this
change all three carry values while a run is live.

⚠ `target_count` NULL means unbounded per `sql/140:21-27`; setting it is the bounded case
the same block documents, and `len(candidates)` is the run's true scope (≤ `limit`), not the
1.9M backlog.

⚠ **No front-end renders them.** `progress_units_*` and `rows_processed_so_far` appear in
`frontend/src/api/types.ts:2196-2198` and in tests only; `ProcessRow.tsx` reads
`last_progress_at` (`:106`, `:321`) for its elapsed-no-progress text and nothing else. So
the operator-visible delta is the elapsed-since-heartbeat text and the verdict
(`health_verdict.py:207`), not a progress counter. An earlier draft of this spec claimed an
`N/500` counter; withdrawn.

⚠ Units differ between the live and terminal surfaces and this change does not reconcile
them: live `processed_count` is filing-event attempts, terminal `row_count` is documents
inserted. Pre-existing across every producer; named so it is not mistaken for new.

### 5.4 No nesting

`ingest_filing_documents` has one non-test caller (`scheduler.py:9929`), and
`sec_filing_documents_ingest` is never called from another job body — the manual path
(`app/jobs/runtime.py:448`) is `_adapt_zero_arg`, which calls the same function and enters
the same single `_tracked_job`. So `_resolve_inner`'s child-to-parent hazard (the
`fundamentals_sync` → `daily_financial_facts` case) does not arise. ⚠ That is an argument
about tracked-job nesting only; it does not prove the active callback has no inner callback,
and it does not need to — `JobRunHeartbeat` chains and guards each side independently.

## 6. Tests — three layers, kept separate

The #3080 lesson is that a probe at the helper's layer cannot see a caller that discards the
helper's result, so these assert at the **call site**: the module-level `report_progress`
symbol in `app.services.filing_documents` is monkeypatched to a recorder. Emission counts
and DB writes are properties of `report_progress` and `JobRunHeartbeat`, which have their
own tests; asserting "N ticks reach the database" here would be asserting two other modules'
throttles.

1. N successful candidates → N call-site invocations, `items_done` = 1..N, `items_total` = N,
   plus a final `(N, N, force=True)`.
2. every `fetch_filing_index` raising → still N invocations (the `finally` path).
3. upsert raising → still N invocations.
4. zero candidates → **no** invocation at all, final tick included.
5. N chosen off the 5-item throttle boundary (e.g. 7) so a passing test cannot be an
   artefact of the throttle aligning with the batch size.

The existing suite (`tests/test_filing_documents_ingest.py`) must stay green unchanged —
this alters no stored output.

## 7. Revert-probes before pushing

- tick in the success path instead of `finally` → tests 2 and 3 red;
- drop the `if candidates:` guard → test 4 red;
- drop the final forced call → test 1's last assertion red (N=7 is off the throttle
  boundary, so the last iteration does not emit it incidentally);
- remove the job from `COVERED_JOBS` → the script stops reporting a job it now covers
  (visual, not a test).

## 8. Out of scope, named rather than silently omitted

- #2274's **acting** half. Nothing terminates a wedge; rule 5 only surfaces one.
- The other four uncovered bodies. `sec_manifest_worker` has 19 orphans/90d against this
  job's 48 and runs near-continuously, so its deferral profile is materially different;
  `daily_research_refresh` is not in `SCHEDULED_JOBS` and has no ProcessRow to feed. Both
  need their own loop read, per §3's bar.
- `dev_reload`'s reap behaviour on the explicit-restart path (this ticket's own open item —
  a ticket phrase, not an existing code symbol).
- The 1.9M-row backlog's ~161-day drain at 500/hour. Real, measured above, and a different
  ticket's question.
