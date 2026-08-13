# First-install bootstrap orchestrator + admin panel

Author: claude (autonomous)
Date: 2026-05-07
Status: Draft (post-Codex round 4)

## Problem

Fresh eBull installs leave the database empty. Setup creates an
operator + session, but no automation populates the universe, SEC
filer directories, CUSIP cross-walks, filing events, or filing
manifest. The scheduler immediately starts firing jobs that no-op
against an empty universe — `sec_insider_transactions_backfill: instruments=0`
is the visible symptom in the operator's logs.

Two manual steps unblock today:

- `POST /jobs/nightly_universe_sync/run` — populates `instruments` +
  Tier 3 coverage.
- `POST /jobs/sec_first_install_drain/run` — seeds
  `sec_filing_manifest` + `data_freshness_index`.

Plus 15+ other ingests must run before the system is fully populated
end-to-end (see "Stages" below). None of this is surfaced in the
admin UI as a single first-install backfill narrative; the operator
has to know to invoke the right endpoints in the right order.

## Goal

After the operator clicks one button, the system reaches a
fully-backfilled state suitable for normal scheduled-job operation,
**without** requiring them to know the underlying job topology.

Specifically:

1. Universe + market data + SEC filer directories + CUSIP universe +
   `filing_events` + filing manifest + freshness index + typed-form
   parsers (DEF14A, 10-K business summary, Form 3/4, 8-K, dividend
   calendar, 13F, NPORT) + ownership observations + fundamentals are
   all populated end-to-end.
2. Per-stage progress, ETA where computable, and per-stage error
   visibility are surfaced in the admin UI.
3. Scheduled jobs that depend on a populated DB stay quiet (skip with
   a clear reason) until bootstrap is `complete`.
4. Errors do not abort the run — the orchestrator iterates past
   failures, logs them per stage, and offers a "retry failed" path.
   Nightly schedules continue making further attempts on their own
   cadence once `_bootstrap_complete` flips true.

Non-goals:

- Automatic kickoff at setup completion. Operator triggers explicitly
  (Option B from the design discussion). Dashboard banner nudges; it
  does not auto-fire — gated to comply with prevention-log entry
  "Fire-and-forget job triggers missing first-time guard" (#145).
- AI-driven jobs (thesis, ranking, recommendations). Out of scope —
  these are not yet penciled in for the v1 demo and have no
  first-install backfill semantics.
- Cooperative cancel mid-run. Out of scope for v1 (see "Cancel" below).

## Settled-decisions check

- **#719 (process topology):** API process serves HTTP only. The
  bootstrap orchestrator runs in the jobs process, dispatched via
  the existing manual-job queue (`pending_job_requests` +
  `pg_notify('ebull_job_request', ...)`). The HTTP endpoint writes a
  `request_kind='manual_job'` row pointing at a new
  `bootstrap_orchestrator` job we register in `_INVOKERS`. The
  existing listener path (`app/jobs/listener.py:88`) dispatches it
  unchanged. ✓
- **Provider strategy (eToro = source of truth for universe; SEC =
  US filings):** Stages reuse existing job invokers — no new provider
  paths. ✓
- **Free regulated-source-only fundamentals (#532):** Stage S16
  (`fundamentals_sync`) uses the existing SEC XBRL Company Facts
  path. ✓

## Prevention-log applicable entries

- **"Fire-and-forget job triggers missing first-time guard" (#145):**
  Bootstrap is operator-explicit, never auto-fired. The status
  endpoint must distinguish `pending` (never run) from `complete`
  (done) so a returning operator does not re-fire by default — the UI
  shows "Run bootstrap" only when status is `pending` or
  `partial_error`.
- **"Hint / warning state with no clear-on-next-transition" (#321):**
  Per-stage `last_error` must clear when that stage transitions to
  `running` on retry — leaving stale errors visible while the stage
  is actively re-attempting is misleading.
- **"Multiple ResilientClient instances sharing a rate limit must
  share throttle state" (#168):** Stages in the SEC lane already go
  through the shared process-wide token bucket
  (`_PROCESS_RATE_LIMIT_CLOCK`). Lane design enforces strict serial
  execution inside the SEC lane to avoid splitting the budget.
- **"Naive datetime in TIMESTAMPTZ query params" (#278):** All
  bootstrap timestamps stored as `timestamptz`; service code uses
  timezone-aware UTC `datetime.now(timezone.utc)`.
- **"Multi-query read handlers must use a single snapshot" (#1024):**
  `GET /system/bootstrap/status` reads run + stages within one
  `conn.transaction()` so a stage transitioning mid-fetch cannot
  produce an internally-inconsistent payload.

## Stages and lanes

Bootstrap runs in three phases:

- **Phase A — `init`:** sequential, single thread. Populates
  `instruments` + tier 3 `coverage`. All later phases depend on this.
- **Phase B — `lanes`:** two threads in parallel
  (`etoro_lane`, `sec_lane`). Stages within each lane run strictly
  serially.
- **Phase C — `finalize`:** single-threaded state transition only;
  no work units. Computes final run status from per-stage outcomes.

### Phase A — init (1 stage)

| # | Key | Job invoker | Purpose | ETA |
|---|---|---|---|---|
| A1 | `universe_sync` | `nightly_universe_sync` | Populates `instruments` + Tier 3 `coverage` rows | ~30s, ~1.5k rows |

### Phase B — eToro lane (1 stage; runs parallel with SEC lane)

| # | Key | Job invoker | Purpose | ETA |
|---|---|---|---|---|
| E1 | `candle_refresh` | `daily_candle_refresh` | Full-universe candles (eToro) | minutes; rolling rate |

### Phase B — SEC lane (16 stages, sequential, shared 11 req/s bucket)

| # | Key | Job invoker | Reads | Purpose | ETA |
|---|---|---|---|---|---|
| S1 | `cusip_universe_backfill` | `cusip_universe_backfill` | `instruments` | SEC official 13(f) list → `external_identifiers` | minutes |
| S2 | `sec_13f_filer_directory_sync` | `sec_13f_filer_directory_sync` | — | quarterly form.idx → `institutional_filers` | minutes |
| S3 | `sec_nport_filer_directory_sync` | `sec_nport_filer_directory_sync` | — | NPORT filer trust CIK harvest | minutes |
| S4 | `cik_refresh` | `daily_cik_refresh` | `instruments` | SEC `company_tickers.json` → `external_identifiers` SEC CIK rows; **prereq for every SEC stage that resolves CIK by ticker** | seconds |
| S5 | `filings_history_seed` | `bootstrap_filings_history_seed` (new) | CIK-mapped `instruments` | broad `refresh_filings` sweep — all form types, ~2y window — populates `filing_events` historically (the table every typed parser reads) | universe × ~3 req ÷ 11 req/s; ~minutes |
| S6 | `sec_first_install_drain` | `sec_first_install_drain` | `instrument_sec_profile` + filer tables | submissions.json per CIK → `sec_filing_manifest` + `data_freshness_index` | filers × ~4 req ÷ 11 req/s ≈ ~60min |
| S7 | `sec_def14a_bootstrap` | `sec_def14a_bootstrap` | `filing_events` (DEF 14A) | first DEF 14A parse pass | filings ÷ 11 req/s |
| S8 | `sec_business_summary_bootstrap` | `sec_business_summary_bootstrap` | `filing_events` (10-K/A) | first business-section ingest | filings ÷ 11 req/s |
| S9 | `sec_insider_transactions_backfill` | `sec_insider_transactions_backfill` | `filing_events` (Form 4) | first Form 4 backfill | filings ÷ 11 req/s |
| S10 | `sec_form3_ingest` | `sec_form3_ingest` | `filing_events` (Form 3) | initial Form 3 parse | filings ÷ 11 req/s |
| S11 | `sec_8k_events_ingest` | `sec_8k_events_ingest` | `filing_events` (8-K) | initial 8-K event extraction | filings ÷ 11 req/s |
| ~~S12~~ | ~~`sec_dividend_calendar_ingest`~~ | ~~`sec_dividend_calendar_ingest`~~ | — | **dropped from v1 bootstrap** — reads `filing_events.items` (8-K parsed-items array) which neither `refresh_filings` nor `_upsert_filing_from_master_index` populates today; `sec_8k_events_ingest` (S11) writes a separate `eight_k_items` table. Bootstrap stage would always report `rows_processed=0`. Wire into bootstrap once `filing_events.items` is populated by an upstream parser (separate ticket). | — |
| S13 | `sec_13f_quarterly_sweep` | `sec_13f_quarterly_sweep` | `institutional_filers` | first 13F holdings sweep | filings ÷ 11 req/s |
| S14 | `sec_n_port_ingest` | `sec_n_port_ingest` | NPORT filers | first NPORT-P parse pass | filings ÷ 11 req/s |
| S15 | `ownership_observations_backfill` | `ownership_observations_backfill` | `ownership_*_legacy` (filled by S7–S14) | walk legacy → `ownership_*_observations` (DB-only) | seconds-to-low-minutes |
| S16 | `fundamentals_sync` | `fundamentals_sync` | CIK-mapped `instruments` | first XBRL Company Facts pass | universe ÷ 11 req/s |

**Total: 17 stages (1 init + 1 eToro lane + 15 SEC lane).** S12 was
dropped (see strikethrough above). Renumbering of S13–S16 in
implementation: the spec's `stage_order` values are integers but
non-contiguous gaps are fine; PR2's `_BOOTSTRAP_STAGE_SPECS` may
either skip the gap or renumber.

#### v1 historical-depth caveat

The "fully backfilled end-to-end" goal is true for the *plumbing* —
universe, CIK mapping, filing manifest, freshness index, ownership
rollup tables — but **filing history depth is bounded by SEC
submissions.json's inline `recent` block** (typically the last
~12 months of filings). PR2's `bootstrap_filings_history_seed`
requests a 2-year window, but `SecFilingsProvider.list_filings`
processes only the inline `recent` block today
(`app/providers/implementations/sec_edgar.py:766`). Walking
secondary submissions pages for a deeper history is a separate
follow-up. For v1 demo purposes, ~12 months of history is
sufficient: the typed parsers (S7–S14) get real data, and nightly
cron picks up new filings going forward.

#### Stage invokers that need wrapping/registration in PR2

Three stages call invokers that are not in the current `_INVOKERS`
map (`app/jobs/runtime.py:162`); PR2 (#994) registers them:

- `daily_cik_refresh` — already a free function in scheduler.py;
  just needs `_INVOKERS[JOB_DAILY_CIK_REFRESH] = daily_cik_refresh`.
- `daily_financial_facts` — same situation; not in the gated list
  but reachable via the master-index path.
- `sec_first_install_drain` — currently exposed as
  `run_first_install_drain(*args)` taking parameters. PR2 adds a
  zero-arg wrapper `sec_first_install_drain_job()` that picks
  sensible defaults (full universe scope, no CIK filter) and
  registers that wrapper in `_INVOKERS`.
- `bootstrap_orchestrator` (the run-the-whole-thing entrypoint).
- `bootstrap_filings_history_seed` (the new S5 invoker).

#### S6 input note: `instrument_sec_profile`

`sec_first_install_drain` reads issuer cohorts from
`instrument_sec_profile`. That table is populated by
`fundamentals_sync`'s SEC-profile bootstrap path. On a fresh DB
where S6 runs before S16 (`fundamentals_sync`), the issuer cohort
is empty and S6 would run only against the institutional + blockholder
filer tables (S2 + S3 outputs). v1 acceptance: this is fine — the
filer-driven cohort still seeds the manifest with quarterly 13F /
NPORT filers. The instrument cohort gets manifest entries when the
nightly per-CIK poll (`sec_per_cik_poll`) runs after `fundamentals_sync`
populates `instrument_sec_profile`. Operator may also re-run
bootstrap after the first nightly to widen coverage.

#### S5 — `bootstrap_filings_history_seed` (new invoker)

Existing scheduled jobs do not provide a clean fit for "walk every
CIK-mapped instrument's submissions.json and seed `filing_events`
for all form types over a multi-year window":

- `daily_research_refresh` is hardcoded to `["10-K","10-Q","8-K"]`
  with a 30-day window
  (`app/workers/scheduler.py:1574-1577`). Insufficient — DEF 14A,
  Form 3, Form 4 histories are out of scope for that job.
- `daily_financial_facts` walks the SEC daily master-index, which
  is "today's index" only; for historical depth it would need to
  iterate hundreds of past days, hammering SEC for marginal value.

`bootstrap_filings_history_seed` is therefore a thin new invoker
that calls the existing `refresh_filings` service helper with a
broader argument set:

```python
refresh_filings(
    provider=sec,
    provider_name="sec",
    identifier_type="cik",
    conn=conn,
    instrument_ids=cik_mapped_instrument_ids,
    start_date=date.today() - timedelta(days=730),
    end_date=date.today(),
    filing_types=None,  # all form types
)
```

It is registered in `_INVOKERS` as `bootstrap_filings_history_seed`
so it can also be triggered manually via the existing admin Run-now
button if an operator ever needs to widen historical depth on
demand. It is **not** added to `SCHEDULED_JOBS` — there is no
nightly cadence for "broad historical sweep"; the daily incremental
paths take over after bootstrap.

#### Why this order in the SEC lane

- S1 reads `instruments`, so it cannot precede A1. It runs first in
  the SEC lane because it is independent of all later SEC writes.
- S2, S3 populate filer-directory tables that S6 (drain) depends on.
- **S4 (`daily_cik_refresh`) and S5
  (`bootstrap_filings_history_seed`) are the critical bridge for
  typed parsers.** S4 populates `external_identifiers` SEC CIK rows
  for every CIK-mappable instrument
  (`app/workers/scheduler.py:1387`); S5 then walks each CIK's
  submissions.json via `refresh_filings` and populates
  `filing_events` historically. The typed parsers (S7–S14) read
  `filing_events`, verified by direct reads:
  `app/services/def14a_ingest.py:194`,
  `app/services/business_summary.py:1290`,
  `app/services/insider_transactions.py:1614`, etc.
- S6 (`sec_first_install_drain`) seeds `sec_filing_manifest` +
  `data_freshness_index`. The future-state plan (post-#873) is for
  the manifest worker to be the sole `filing_events` writer; until
  then we keep S5 in the chain to populate `filing_events`
  directly.
- S7–S14 run after both `filing_events` (from S5) and the manifest
  (from S6) are populated, so each parser has its required input.
- S15 (DB-only ownership rollup) reads legacy ownership tables that
  the S7–S14 parsers fill via write-through.
- S16 (`fundamentals_sync`) reads CIK-mapped `instruments` (S4
  output) and could in principle run earlier, but the SEC bucket is
  fully utilised by S5–S14; running it last keeps the lane simple
  and the rate budget predictable.

### Phase C — finalize

After both lane threads have joined, the orchestrator inspects all
18 stage rows:

- All `success` (or harmlessly `skipped`) → `bootstrap_state.status='complete'`.
- One or more `error` → `bootstrap_state.status='partial_error'`.

If A1 fails, lanes never start and the orchestrator sets
`partial_error` immediately.

### Lane parallelism rationale

eToro stages hit only eToro APIs; SEC stages hit only SEC EDGAR. The
two outbound rate limits are independent. Inside the SEC lane stages
must run serially: shared process-wide token bucket means parallel
SEC stages would multiplex into the same 11 req/s budget — total
wall-clock is unchanged but each stage's ETA doubles, log noise
doubles, and 503 retry pressure rises.

S14 is DB-only and does not contend for the SEC bucket, but it is
still scheduled inside the SEC lane after S13 because its input rows
(`ownership_*_legacy`) are populated by S6–S13. Putting S14 in its
own thread would just race the writes its read depends on.

## Per-stage execution contract

Each stage runs through this exact sequence inside its lane thread:

1. **Pre-check:** read the stage row's status. If `success`, skip
   (retry-failed paths set successful stages back to `success` rather
   than `pending` — see retry-failed below).
2. **Mark running:** UPDATE the stage row to `status='running'`,
   set `started_at = now()`, increment `attempt_count`, clear
   `last_error`.
3. **Acquire `JobLock(database_url, job_name)`:** the same advisory
   lock the scheduled + manual paths use
   (`app/jobs/runtime.py:789`). If the lock is held (e.g. operator
   manually triggered the same job concurrently), the stage records
   `status='error'`, `last_error="another instance holds the job
   advisory lock; retry from the bootstrap panel after it
   completes"`, and the lane proceeds to the next stage. The
   advisory lock guarantees we never run the same invoker twice
   simultaneously across bootstrap, manual triggers, or scheduled
   fires.
4. **Invoke the underlying job function** via `_INVOKERS[job_name]()`.
   The function's own `_tracked_job` writes a normal `job_runs` row,
   which gives us the full per-job forensic trail in addition to the
   `bootstrap_stages` summary.
5. **Catch exceptions:** any exception is caught and recorded as
   `status='error'`, `last_error=str(exc)[:1000]`, `completed_at=now()`.
   The lane continues to the next stage.
6. **On success:** UPDATE `status='success'`, `completed_at=now()`,
   `rows_processed=<from job_runs>`.

The `JobLock` wrapper means bootstrap dispatch is **not** equivalent
to "calling `_INVOKERS[name]()` directly bypassing all wrappers" —
prerequisites *are* bypassed (intentional: bootstrap is the operator
forcing first-install work), but the advisory lock is acquired so
overlapping manual / scheduled triggers cannot run twice
simultaneously.

## Database schema

New migration (next sequence number) creates three tables.

### `bootstrap_runs`

```sql
CREATE TABLE bootstrap_runs (
    id              BIGSERIAL PRIMARY KEY,
    triggered_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    triggered_by_operator_id BIGINT REFERENCES operators(id),
    status          TEXT NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running','complete','partial_error')),
    completed_at    TIMESTAMPTZ,
    notes           TEXT
);
```

History — every "Run bootstrap" or "Re-run bootstrap" click is one
row. Latest row drives the UI. Retry-failed reuses the latest row.

### `bootstrap_stages`

```sql
CREATE TABLE bootstrap_stages (
    id              BIGSERIAL PRIMARY KEY,
    bootstrap_run_id BIGINT NOT NULL REFERENCES bootstrap_runs(id) ON DELETE CASCADE,
    stage_key       TEXT NOT NULL,
    stage_order     SMALLINT NOT NULL,
    lane            TEXT NOT NULL CHECK (lane IN ('init','etoro','sec')),
    job_name        TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','running','success','error','skipped')),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    rows_processed  INTEGER,
    expected_units  INTEGER,
    units_done      INTEGER,
    last_error      TEXT,
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    UNIQUE (bootstrap_run_id, stage_key)
);

CREATE INDEX bootstrap_stages_run_status_idx
    ON bootstrap_stages (bootstrap_run_id, status);

-- Defense-in-depth: at most one bootstrap_runs row may have
-- status='running'. The /run handler also takes a row-level lock on
-- bootstrap_state to serialise concurrent POSTs; this index exists
-- to make a second concurrent insert fail loudly rather than
-- silently create a duplicate run if the row lock is ever bypassed.
CREATE UNIQUE INDEX bootstrap_runs_one_running_idx
    ON bootstrap_runs (status)
    WHERE status = 'running';
```

`expected_units` + `units_done` drive ETA. Set lazily by the
orchestrator when a stage starts (e.g. universe stage knows its
target row count up front; ones that don't know just leave both
`NULL` and the UI shows a spinner instead of a fake percentage).

### `bootstrap_state`

```sql
CREATE TABLE bootstrap_state (
    id                INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    status            TEXT NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending','running','complete','partial_error')),
    last_run_id       BIGINT REFERENCES bootstrap_runs(id),
    last_completed_at TIMESTAMPTZ
);
INSERT INTO bootstrap_state (id) VALUES (1) ON CONFLICT DO NOTHING;
```

Singleton row. The `_bootstrap_complete` prerequisite reads
`SELECT status FROM bootstrap_state WHERE id = 1`.

### `bootstrap_stage_errors` (deferred — out of scope for v1)

Reserved for a follow-up if we want per-item error history below the
stage level. Not in v1: `last_error` on the stage row is sufficient,
and the underlying jobs already log per-item failures via their own
`job_runs` rows.

## State machine

`bootstrap_state.status` transitions:

```
pending ─run─▶ running ──run finalises, no errors──▶ complete
                  │
                  └─run finalises, ≥1 stage error──▶ partial_error
                                                       │
                                                       └─retry-failed─▶ running ──no errors──▶ complete
```

Key invariant: `partial_error` is a **terminal run-completion
state**, never set mid-run. While the run is in flight, status is
`running` and individual stages may already have `status='error'`
recorded. `bootstrap_state.status` does not flip to `partial_error`
until both lane threads have joined. This keeps "continue past
errors" (Goals §4) consistent with the state machine: errors during
a run are visible per-stage but do not interrupt the run, and the UI
keeps polling at the 5s `running` cadence until the final transition.

`_bootstrap_complete` returns `(True, "")` only when status is
`complete`. Both `pending` and `partial_error` return `(False, ...)`.
`partial_error` does **not** unblock the prerequisite — scheduled
jobs running against a half-populated DB produce tens of thousands
of `instruments=0` no-op log lines plus partial data shapes that
confuse downstream operators. The operator must either retry failed
stages, fix underlying causes and re-run, or use `mark-complete`
(audit-logged escape hatch).

### Cancel — out of scope for v1

The orchestrator does not expose `cancel`. A long bootstrap run is
~60-90 minutes wall-clock and cannot be safely interrupted mid-stage
without leaving partial state in `sec_filing_manifest` etc. that
later writes would conflict with. If the operator needs to abort,
they restart the jobs process; boot recovery (below) finalises
`bootstrap_state.status='partial_error'`, then they choose
retry-failed or mark-complete. v2 may add a cooperative cancel
signal.

### Boot recovery for orphaned runs

The existing `JobRuntime` reaper handles orphaned `sync_runs`
(`app/jobs/runtime.py:517`) — it does **not** know about bootstrap
state. We add a dedicated bootstrap recovery step in the jobs
process startup sequence (alongside the existing reaper):

```python
def reap_orphaned_bootstrap(conn) -> None:
    # If bootstrap_state.status='running' on cold start, no live
    # thread is executing this run. Sweep the latest run's stages:
    #   - status='running'  → 'error', last_error='jobs process restarted mid-run'
    #   - status='pending'  → 'error', last_error='orchestrator did not dispatch before restart'
    # Then transition bootstrap_state to 'partial_error' so the
    # operator gets an accurate picture and retry-failed has work.
    # The partial-unique index on bootstrap_runs.status='running'
    # also has to be released — UPDATE bootstrap_runs.status to
    # 'partial_error' for the latest row.
    ...
```

This runs once at jobs-process startup, in the same connection that
runs the existing sync reaper. Idempotent: a `pending` /
`complete` / `partial_error` state is left alone. Only a stuck
`running` state triggers the sweep.

Edge case (`status='running'` but zero stages were ever started):
both the `running` and `pending` sweep clauses above ensure
retry-failed has stages to act on. Without the `pending → error`
sweep, retry-failed would 404 with "no failed stages" and leave the
operator stuck — they'd have to use mark-complete or wait for
dev-environment intervention.

## Concurrency model

The bootstrap orchestrator runs in the jobs process as a
`manual_job` invoker (`bootstrap_orchestrator`). It is *not*
re-entrant: a second `POST /system/bootstrap/run` while a run is
already `running` returns 409 with the existing `run_id`.

Lane parallelism uses two `threading.Thread` workers (one per Phase
B lane) launched from the orchestrator function. Both threads share
the same `psycopg.Connection` pool. Stages within a lane are awaited
sequentially.

Why threads not asyncio: the underlying job invokers are sync
functions that wrap `psycopg` connections + `httpx.Client` calls
through `ResilientClient`. Lifting them to async would be invasive
and has no benefit when each lane is single-flight by construction.

The shared SEC token bucket is `threading.Lock`-guarded
(`_PROCESS_RATE_LIMIT_LOCK` at
`app/providers/implementations/sec_edgar.py:80`), so parallel
non-SEC work in the eToro lane is safe with the SEC lane.

## API endpoints

All under the existing `/system/...` namespace
(`app/api/system_status.py` + new `app/api/bootstrap.py`).

### `GET /system/bootstrap/status`

Single multi-table snapshot.

```json
{
  "status": "running",
  "current_run_id": 7,
  "last_completed_at": null,
  "stages": [
    {
      "stage_key": "universe_sync",
      "lane": "init",
      "job_name": "nightly_universe_sync",
      "stage_order": 1,
      "status": "success",
      "started_at": "2026-05-07T...",
      "completed_at": "2026-05-07T...",
      "rows_processed": 1547,
      "expected_units": null,
      "units_done": null,
      "last_error": null,
      "eta_seconds": null,
      "attempt_count": 1
    },
    ...
  ]
}
```

Read with a single `conn.transaction()` snapshot.

### `POST /system/bootstrap/run`

Single-flight (one bootstrap run at a time).

Concurrency contract: the handler opens a transaction, takes
`SELECT ... FOR UPDATE` on the singleton `bootstrap_state` row, then
inspects status. This serialises concurrent POSTs at the DB level
even when two API workers race. If status is `running` after lock
acquisition, return 409 `{ "current_run_id": N }`. Otherwise, INSERT
into `bootstrap_runs` with `status='running'` — the partial unique
index (see schema below) provides defense-in-depth: if the
`FOR UPDATE` were ever bypassed by a pathological retry, a second
insert would fail with a unique-violation rather than create two
runs.

- If status `running`: returns 409 `{ "current_run_id": N }`.
- Otherwise (status `pending`, `complete`, or `partial_error`):
  creates a fresh `bootstrap_runs` row, seeds **18 pending**
  `bootstrap_stages` rows (1 init + 1 eToro lane + 16 SEC lane),
  flips `bootstrap_state.status` to `running` with `last_run_id`
  pointing at the new row, then writes a `pending_job_requests` row
  with `request_kind='manual_job'`,
  `job_name='bootstrap_orchestrator'` +
  `pg_notify('ebull_job_request', ...)`. Returns 202 + `run_id`.

This works under the existing #719 contract because we register
`bootstrap_orchestrator` in `_INVOKERS` (`app/jobs/runtime.py:162`)
as a normal manual job. The dispatcher's existing `manual_job`
listener path (`app/jobs/listener.py:88`) picks it up unchanged. The
orchestrator function reads the latest `bootstrap_runs` row + its
pending stages and runs them.

Re-running after `complete` or `partial_error` is allowed by design:
operators may want to re-bootstrap after a long absence (universe
churn, new SEC filer cohort). Each run is its own row in
`bootstrap_runs`; previous runs' history is preserved.

### `POST /system/bootstrap/retry-failed`

For a `partial_error` state. Reuses the latest `bootstrap_runs.id`
(does not create a new run).

**Dependency-aware reset:** resets stages with `status='error'` to
`pending` AND additionally resets every later-numbered stage in the
**same lane** whose `status` is `success` or `skipped`. Reasoning:
when an upstream stage failed, downstream stages may have run
against partial / stale data and recorded a misleading success.
Re-running the upstream stage and then the downstream chain in the
same lane gives the correct final state. The eToro lane is one stage
deep so this is moot; the SEC lane is where the dependency walk
matters.

The reset clears `last_error`, `started_at`, `completed_at` on the
affected rows but preserves `attempt_count` so the lane runner
increments it on the next run. Flips `bootstrap_state.status` back
to `running` with the same `last_run_id`. Writes a fresh
`manual_job` queue row + NOTIFY.

UI shows a clear preview of which stages will be re-run (failed +
downstream-in-same-lane) before the operator confirms.

409 if state is `running`. 404 if no prior run exists or latest run
has no failed stages.

The orchestrator function checks each stage's `status` before
dispatching it to the lane runner: stages still marked `success` are
skipped this time, so retry-failed touches only the affected
stages.

### `POST /system/bootstrap/mark-complete`

Operator escape hatch. Forces `bootstrap_state.status='complete'`.
Used when the operator has manually fixed the cause of a stage
failure and wants to release the scheduler gate without re-running
heavy stages. Audit-logged via the existing operator-action audit
trail; requires the existing operator session auth.

**Running-state guard: returns 409 if `bootstrap_state.status='running'`.**
Releasing the scheduler gate while the orchestrator threads are still
mutating data would let nightly jobs run against half-populated
tables — exactly the case the gate exists to prevent.

## Scheduler prerequisite

Add to `app/workers/scheduler.py`:

```python
def _bootstrap_complete(conn: psycopg.Connection[Any]) -> PrerequisiteResult:
    if _exists(
        conn,
        psycopg.sql.SQL(
            "SELECT EXISTS(SELECT 1 FROM bootstrap_state WHERE id = 1 AND status = 'complete')"
        ),
    ):
        return (True, "")
    return (False, "first-install bootstrap not complete; visit /admin to run")
```

Wire as `prerequisite=_bootstrap_complete` on:

- `JOB_ORCHESTRATOR_FULL_SYNC`
- `JOB_FUNDAMENTALS_SYNC` (S16 in bootstrap)
- `JOB_DAILY_FINANCIAL_FACTS`
- `JOB_DAILY_RESEARCH_REFRESH`
- `JOB_DAILY_CIK_REFRESH` (S4 in bootstrap; nightly is gated)
- `JOB_SEC_INSIDER_TRANSACTIONS_INGEST`
- `JOB_SEC_INSIDER_TRANSACTIONS_BACKFILL` (S9 in bootstrap)
- `JOB_SEC_FORM3_INGEST` (S10 in bootstrap)
- `JOB_SEC_DEF14A_INGEST`
- `JOB_SEC_DEF14A_BOOTSTRAP` (S7 in bootstrap)
- `JOB_SEC_8K_EVENTS_INGEST` (S11 in bootstrap)
- `JOB_SEC_FILING_DOCUMENTS_INGEST`
- `JOB_SEC_BUSINESS_SUMMARY_INGEST`
- `JOB_SEC_BUSINESS_SUMMARY_BOOTSTRAP` (S8 in bootstrap)
- `JOB_SEC_DIVIDEND_CALENDAR_INGEST` (S12 in bootstrap)
- `JOB_OWNERSHIP_OBSERVATIONS_SYNC`
- `JOB_SEC_13F_QUARTERLY_SWEEP` (S13 in bootstrap)
- `JOB_SEC_N_PORT_INGEST` (S14 in bootstrap)
- `JOB_DAILY_CANDLE_REFRESH` (E1 in bootstrap; nightly is gated)

Self-reference note: stages S4–S16 + E1 are also in the gated list,
but gating their *scheduled* path is safe because:

1. The `prerequisite=` gate only fires on the scheduled path
   (`app/jobs/runtime.py:766` — `_wrap_scheduled_invoker` checks
   prerequisite before lock acquisition).
2. Manual invocations (`POST /jobs/{name}/run`) explicitly bypass
   prereqs (`app/jobs/runtime.py:765` comment: "bypass prerequisites
   so the operator can force a run").
3. The bootstrap orchestrator dispatches stage jobs by calling the
   invoker inside a `JobLock` context (no scheduler-side gate).

So these jobs run during bootstrap (orchestrator-direct invocation)
but stay silent in their nightly crons until
`bootstrap_state.status='complete'`.

Do **not** wire the gate on:

- `JOB_NIGHTLY_UNIVERSE_SYNC` (A1 in bootstrap) — itself the gate
  prerequisite read state. Gating would deadlock.
- `JOB_DAILY_PORTFOLIO_SYNC` — eToro portfolio works pre-universe
  (proven by the operator's logs showing `mirror_positions_up=361`
  against empty `instruments`).
- `JOB_ORCHESTRATOR_HIGH_FREQUENCY_SYNC` — only fires
  portfolio_sync + fx_rates; safe pre-bootstrap.
- `JOB_FX_RATES_REFRESH`, `JOB_ETORO_LOOKUPS_REFRESH`,
  `JOB_EXCHANGES_METADATA_REFRESH` — independent eToro metadata.
- `JOB_RETRY_DEFERRED`, `JOB_MONITOR_POSITIONS`,
  `JOB_EXECUTE_APPROVED_ORDERS` — already gated by their own
  state-vacuous prerequisites.
- `JOB_CUSIP_UNIVERSE_BACKFILL` (S1), `JOB_SEC_13F_FILER_DIRECTORY_SYNC`
  (S2), `JOB_SEC_NPORT_FILER_DIRECTORY_SYNC` (S3),
  `JOB_SEC_FIRST_INSTALL_DRAIN` (S6),
  `JOB_OWNERSHIP_OBSERVATIONS_BACKFILL` (S15),
  `bootstrap_filings_history_seed` (S5; new invoker, has no
  scheduled cadence) — these read no state that depends on
  bootstrap completion; safe pre-bootstrap and may be re-run
  independently.

## Frontend

### `BootstrapPanel.tsx` (new)

Replaces `SeedProgressPanel` placement at the top of the admin page
for fresh installs. Renders:

- Header row: status pill ("Pending" / "Running" / "Complete" /
  "Partial — N errors") + action button. Button copy by status:
  - `pending`: "Run bootstrap"
  - `running`: disabled "Running…" (no cancel — see Cancel scope)
  - `complete`: "Re-run bootstrap" (secondary tone)
  - `partial_error`: primary "Retry failed (N)" + secondary "Re-run all" + secondary "Mark complete"
- Per-stage list: **18 rows**, grouped by phase/lane (1 init, 1
  eToro, 16 SEC). Each row:
  - stage_key + job_name (caption)
  - phase/lane badge (`init` / `eToro` / `SEC`)
  - status (pending / running / success / error / skipped)
  - progress (`units_done / expected_units` if known else spinner)
  - elapsed + ETA where ETA is computable
  - error message (truncated 80 chars; click expands)

Polls `GET /system/bootstrap/status` every 5s while status is
`running`, every 60s otherwise. Uses existing `useAsync` hook.

### Dashboard banner

When `bootstrap_state.status !== 'complete'` and a session exists,
render a top-of-page banner on every authenticated route linking to
`/admin#bootstrap`. Component: `BootstrapNudgeBanner.tsx`. Banner
suppresses itself if dismissed for the current session
(`sessionStorage.bootstrapBannerDismissed`); the next reload
re-shows it as long as bootstrap remains incomplete — we do not want
"dismiss forever" because the actual fix path is via the admin
panel.

### `BootstrapProgress.tsx` (existing, setup wizard)

Unchanged — that lives on the setup wizard for the live universe
sync after the operator saves credentials. The new
`BootstrapPanel.tsx` lives on the admin page for explicit operator
control.

## Test plan

### Unit (Python)

- `tests/test_bootstrap_orchestrator.py`:
  - Happy path: all 18 stages succeed → `bootstrap_state.status='complete'`.
  - Single-stage failure mid-SEC-lane: subsequent SEC stages still
    run; eToro lane runs to completion; final state is
    `partial_error`; `last_error` recorded for the failed stage.
  - A1 failure: SEC + eToro lanes never start; final state is
    `partial_error` with all Phase B stages still `pending`.
  - Retry-failed: reuses same `bootstrap_runs.id`, only re-runs
    failed stages (skipped success stages stay `success`), marks
    final state `complete` if all retried succeed.
  - Idempotent run: second concurrent run rejected with current
    `run_id` (409).
  - Mark-complete: writes audit row, flips state without re-running
    stages.
  - JobLock contention: a stage whose `JobLock` is held by another
    invoker records `error` with the contention reason.
- `tests/test_bootstrap_state_prerequisite.py`:
  - `_bootstrap_complete` returns `(False, ...)` while pending /
    running / partial_error.
  - `_bootstrap_complete` returns `(True, "")` after
    `mark-complete`.
  - Scheduler `prerequisite` gate logs `skipped` rows for each
    gated job pre-bootstrap.
- `tests/test_bootstrap_endpoints.py`:
  - `GET /system/bootstrap/status` returns full payload.
  - `POST /system/bootstrap/run` writes pending_job_requests +
    NOTIFY.
  - 409 conflict on second run.
- `tests/test_bootstrap_boot_recovery.py`:
  - `bootstrap_state.status='running'` on cold start →
    `partial_error`; in-flight stages → `error`.
- Smoke: `tests/smoke/test_app_boots.py` already runs lifespan;
  add assertion that the `bootstrap_state` row exists post-migration.

### Integration

- `tests/integration/test_bootstrap_flow.py`:
  - Stub `_INVOKERS[stage]` to deterministic test fakes that
    insert sentinel rows.
  - Run orchestrator end-to-end against `ebull_test`.
  - Assert all sentinel rows present + final state.

### Frontend

- `BootstrapPanel.test.tsx` — render with each status shape, button
  copy + disabled state, retry-failed action, error expand.
- `BootstrapNudgeBanner.test.tsx` — visibility transitions on
  status change + dismiss/reload.

## Implementation order (PR plan)

| PR | Scope | Branch | Depends |
|---|---|---|---|
| 1 | DB migration + `bootstrap_state` repo functions + smoke assertion | `feature/<umbrella>-1-schema` | — |
| 2 | `BootstrapOrchestrator` service + lane runners + per-stage `JobLock` wrapper + register `bootstrap_orchestrator` in `_INVOKERS` + boot recovery sweep + tests | `feature/<umbrella>-2-orchestrator` | 1 |
| 3 | API endpoints (`/system/bootstrap/{status,run,retry-failed,mark-complete}`) + jobs-process trigger via NOTIFY | `feature/<umbrella>-3-api` | 2 |
| 4 | Scheduler prerequisite wiring on 19 gated jobs + tests | `feature/<umbrella>-4-prereq` | 1 |
| 5 | `BootstrapPanel.tsx` + `useBootstrapStatus` hook + dashboard banner + frontend tests | `feature/<umbrella>-5-frontend` | 3 |
| 6 | Smoke + integration test + admin-ops runbook update | `feature/<umbrella>-6-smoke` | 5 |

PR4 can land in parallel with PR3 (only depends on PR1 schema). The
others are linear.

## Open scope calls (already decided per user clarification 2026-05-07)

- **Trigger model:** Explicit operator click (Option B). Banner
  nudges; setup wizard does not auto-fire.
- **Candle refresh scope:** Full universe, not capped. User said
  "even if bulky, it's needed".
- **Failure mode:** Continue past errors; per-stage error log;
  retry-failed-only button; nightly cadence picks up further
  attempts after `mark-complete` or successful retry.
- **Visibility surface:** Admin page is canonical. Replace
  `SeedProgressPanel` placement — surface bootstrap-incomplete state
  prominently when `status !== 'complete'`.

## Out of scope

- Per-CIK error drilldown (deferred — `last_error` on the stage row
  is sufficient for v1; underlying jobs already log to their own
  `job_runs` rows for forensics).
- AI / ranking / thesis bootstrap — not yet penciled.
- Multi-region universe (UK / EU). v1 covers eToro tradable +
  US-SEC only, matching settled-decisions.
- Phase 2 universe expansion (#841 — EdgarTools 13F adoption + 10x
  filer count). Bootstrap design uses current `institutional_filers`
  table; #841 expands that table without changing bootstrap shape.
- Cooperative cancel mid-run. Operator restart + boot recovery is
  the v1 abort path.
