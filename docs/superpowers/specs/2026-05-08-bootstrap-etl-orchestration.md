# Bootstrap ETL orchestration — pre-write validation + parallelism

Author: claude (autonomous, post-#1035)
Date: 2026-05-08
Status: Draft (pre-Codex)

## Problem

The 24-stage chain shipped in #1035 runs sequentially. First live
attempt revealed two structural defects:

1. **Silent partial-failure.** `sec_bulk_download` returned
   `mode="bulk"` even when 2 of 14 archives errored mid-transfer.
   Stage marked `success`. C1.a + C2 ingesters then no-op'd
   ("archive not present, skipping") but ALSO marked `success`.
   Operator panel showed clean green run with the largest two
   archives' data missing entirely. Downstream legacy chain
   filled the gap per-CIK at 7 req/s — slow scalar mode the bulk
   path was meant to replace.

2. **Flat sequential dependency chain.** Stages B1–B4 (CUSIP
   universe, 13F filer dir, NPORT filer dir, CIK refresh) each
   fire one HTTP call against SEC; combined ~30 s. They're
   sequential when they could be 3 of 4 in parallel with A3 (bulk
   download — bandwidth-bound, separate budget). Same for the
   Phase C DB-bound stages, which serialize for no benefit when
   they hit different tables with separate connections.

## Goal

After this redesign:

1. Every stage either lands its data OR surfaces `error` — no
   silent success on partial-or-empty writes.
2. Pre-write validation gates each ingester so a missing archive
   or empty lookup table is caught BEFORE any DB write, not after.
3. Independent stages run in parallel; the real dependency graph
   replaces the flat list.
4. Bulk path is the canonical write path; legacy per-CIK fallback
   only fires when bulk is unavailable (slow connection or archive
   download error).

## Dependency graph (ground truth)

```
A1 universe_sync                    (writes instruments, coverage)
  │
  ├── A2 candle_refresh             (eToro lane, separate rate budget)
  │
  ├── A3 sec_bulk_download          (network bandwidth-bound; HEADs on shared SEC clock)
  │     │
  │     └── 18 archive ZIPs cached on disk (2 nightly + 4 13F + 8 insider + 4 NPORT)
  │
  └── SEC reference lane (sequential, shares 7 req/s budget)
        ├── B4 cik_refresh          (writes external_identifiers.cik)
        ├── B1 cusip_universe_backfill  (writes external_identifiers.cusip)
        ├── B2 sec_13f_filer_directory_sync
        └── B3 sec_nport_filer_directory_sync

[A2 + A3 + (B1..B4 sequential) all parallel after A1]
join

Phase C (DB-bound, per-archive validated, no rate cost):
  C1.a  sec_submissions_ingest    needs B4 + submissions.zip
  C2    sec_companyfacts_ingest   needs B4 + companyfacts.zip
  C3    sec_13f_ingest_from_dataset    needs B1 + form13f_*.zip
  C4    sec_insider_ingest_from_dataset needs B4 + insider_*.zip
  C5    sec_nport_ingest_from_dataset  needs B1 + nport_*.zip
  → all 5 may run concurrently (5 separate psycopg conns;
    no shared lock contention; each writes its own table family).
join

Phase C' (rate-bound deep-history) — must complete BEFORE Phase D:
  C1.b  sec_submissions_files_walk  needs C1.a complete
  → SEC rate budget. D-stages depend on C1.b so deep-history filings
    are in filing_events before body parsers run.
join

Phase D (per-filing pipelined fetches, shared rate budget):
  D1 sec_def14a_bootstrap          needs C1.a + C1.b
  D2 sec_business_summary_bootstrap needs C1.a + C1.b
  D3 sec_8k_events_ingest          needs C1.a + C1.b
  → share PipelinedSecFetcher's clock; can run sequentially or
    concurrently with one shared fetcher.
join

Phase E (DB-only finalize):
  E1 ownership_current_refresh    needs C3+C4+C5 each with rows_written>0 in current run
  E2 fundamentals_derivation       needs C2 with rows_written>0 in current run
  → parallel.

Phase F (legacy fallback — runs when bulk write path did NOT
  succeed for that data family, regardless of root cause):
  filings_history_seed, sec_first_install_drain, etc.
  → status `skipped` when the corresponding C-stage = `success`;
    runs (and writes) when the C-stage = `error` OR `blocked`.
    Trigger is the C-stage outcome, not the root cause: a B-stage
    failure that propagates to a `blocked` C-stage still fires
    fallback, because the data is still missing downstream.
```

## Provenance tracking — per-current-run validation

The naive precondition "`<bulk>/X.zip` exists" passes against a
stale archive from a previous failed run, and "`cik_to_instrument`
non-empty" passes against a stale row from a previous run. Both
defeat the purpose of pre-write validation.

Resolve via per-run provenance tags:

- **Bulk archive manifest.** A3 writes
  `<bulk>/.run_manifest.json`: `{run_id, archives:[{name,
  sha256, content_length, downloaded_at}]}` after each successful
  archive. Phase C invokers load the manifest, look up the
  archive's `run_id`, and require it to equal the current
  `bootstrap_run_id`.
- **Reference-table provenance.** B1/B4 are scheduler jobs.
  Checking `job_runs.status='success'` alone is insufficient: a
  conditional-fetch 304-Not-Modified can mark the job success
  with zero upserts. Need TWO invariants — invocation provenance
  AND coverage adequacy:

  1. **Invocation provenance.** Each B-stage's invoker wrapper
     writes a `bootstrap_archive_results` row keyed on
     `(bootstrap_run_id, stage_key, archive_name='__job__')`
     BEFORE the stage transitions to `success`. The row's
     existence — NOT its `rows_written` value — proves the B-job
     ran in the current bootstrap run. `rows_written` records
     the upsert count for telemetry only; an idempotent re-run
     against a populated mapping legitimately reports 0 upserts.
     Phase C precondition checks ROW EXISTENCE only.

  2. **Coverage adequacy.** The mapping table must cover the
     *current A1 universe* with a meaningful ratio — `>= 1`
     passes a populated-but-stale partial map (one CIK row from a
     previous universe, hundreds of newly-added unmapped
     instruments). Compute via JOIN:
     Numerator AND denominator both apply the same cohort filter
     (verified at `app/workers/scheduler.py:1501` —
     `daily_cik_refresh` joins to
     `exchanges.asset_class='us_equity' AND is_tradable=TRUE`):
     ```sql
     -- mapped count IN COHORT
     SELECT COUNT(*) FROM instruments i
       JOIN exchanges e ON e.exchange_id = i.exchange
       JOIN external_identifiers ei
         ON ei.instrument_id = i.instrument_id
        AND ei.provider = 'sec'
        AND ei.identifier_type = 'cik'
      WHERE i.is_tradable = TRUE
        AND e.asset_class = 'us_equity'
     ```
     vs:
     ```sql
     -- cohort denominator (matches B4's producer cohort)
     SELECT COUNT(*) FROM instruments i
       JOIN exchanges e ON e.exchange_id = i.exchange
      WHERE i.is_tradable = TRUE
        AND e.asset_class = 'us_equity'
     ```
     Require `mapped_cik / us_equity_cohort >= BOOTSTRAP_MIN_CIK_COVERAGE_RATIO`
     (default `0.50`).

     For CUSIP (C3/C5): `cusip_universe_backfill` (verified at
     `app/workers/scheduler.py:3749` calling
     `backfill_cusip_coverage(conn)` at
     `app/services/sec_13f_securities_list.py:236-239`) only
     evaluates instruments where
     `is_tradable=TRUE AND company_name IS NOT NULL AND company_name <> ''`.
     Numerator + denominator pinned to that exact cohort:
     ```sql
     -- mapped CUSIP count IN COHORT
     SELECT COUNT(*) FROM instruments i
       JOIN external_identifiers ei
         ON ei.instrument_id = i.instrument_id
        AND ei.provider = 'sec'
        AND ei.identifier_type = 'cusip'
      WHERE i.is_tradable = TRUE
        AND i.company_name IS NOT NULL
        AND i.company_name <> ''
     ```
     vs
     ```sql
     SELECT COUNT(*) FROM instruments
      WHERE is_tradable = TRUE
        AND company_name IS NOT NULL
        AND company_name <> ''
     ```
     Require `mapped_cusip / cusip_cohort >= BOOTSTRAP_MIN_CUSIP_COVERAGE_RATIO`
     (default `0.50`).

     The denominator MUST match the producer cohort. If we used
     `country='US'` while the producer used `asset_class='us_equity'`
     the validation could pass or fail spuriously. The 0.50 default
     is a heuristic that catches "tiny stale map" failures while
     tolerating cases where a sub-fraction of cohort instruments
     are genuinely not on SEC's filer list (closed-end funds,
     certain ADRs).

     Operator overrides via env vars
     `BOOTSTRAP_MIN_CIK_COVERAGE_RATIO` /
     `BOOTSTRAP_MIN_CUSIP_COVERAGE_RATIO`.
     `rows_skipped_unresolved_*` telemetry on each Phase C stage
     surfaces specific instrument-level gaps for operator review.

  C1.a / C2 / C4 validate B4 (`cik_refresh`) row exists in
  `bootstrap_archive_results` for current run AND
  `mapped_cik / us_equity_cohort >= BOOTSTRAP_MIN_CIK_COVERAGE_RATIO`.
  C3 / C5 validate B1 (`cusip_universe_backfill`) row exists AND
  `mapped_cusip / cusip_cohort >= BOOTSTRAP_MIN_CUSIP_COVERAGE_RATIO`.
  Each C-stage must check its own dependency.

The current `bootstrap_run_id` is threaded into every invoker via
the existing orchestrator → `bootstrap_stages.bootstrap_run_id`.
The invoker reads the latest run id at the top.

## Pre-write validation contract (revised)

Every Phase C / D / E invoker enforces preconditions BEFORE the
ingester runs. Failure raises `BootstrapPreconditionError` so the
orchestrator marks the stage `error` with the precondition message.

| Stage | Precondition | Failure |
|---|---|---|
| A3 sec_bulk_download | All 18 archives in inventory landed (per-archive `error == None`); `.run_manifest.json` written | raise `BootstrapPartialDownloadError` listing failed archives |
| C1.a | manifest `submissions.zip` `run_id` == current run AND B4's `bootstrap_archive_results('__job__')` row exists for current run AND `mapped_cik / us_equity_universe >= BOOTSTRAP_MIN_CIK_COVERAGE_RATIO` (default 0.50) | raise |
| C2 | manifest `companyfacts.zip` `run_id` == current run AND B4 invocation row + same CIK ratio check | raise |
| C3 | manifest covers all 4 `form13f_*.zip` for current run AND B1 invocation row + `mapped_cusip / cusip_cohort >= BOOTSTRAP_MIN_CUSIP_COVERAGE_RATIO` (default 0.50) | raise |
| C4 | manifest covers all 8 `insider_*.zip` for current run AND B4 invocation row + same CIK ratio check | raise |
| C5 | manifest covers all 4 `nport_*.zip` for current run AND B1 invocation row + same CUSIP ratio check | raise |
| C1.b | C1.a status = `success` AND `bootstrap_archive_results.rows_written > 0` for `(current_run_id, 'sec_submissions_ingest')` | raise |
| D1 | C1.a status = `success` for current run AND C1.b status = `success` for current run | raise |
| D2 | Same | raise |
| D3 | Same | raise |
| E1 | `bootstrap_archive_results.rows_written > 0` for each of C3, C4, C5 in the current run | raise |
| E2 | `bootstrap_archive_results.rows_written > 0` for C2 in the current run | raise |

Note: D-stages enforce only "C1.a + C1.b succeeded in current run"
as the freshness proof. They do NOT require `filing_events` rows
matching a form-type filter to be from the current run, because:
(a) `filing_events` has no `ingest_run_id` column;
(b) bodies the D-stages fetch are idempotent re-writes — fetching
the same DEF 14A body twice ON CONFLICT-updates rather than
duplicating;
(c) the operator-visible telemetry on each D-stage (`rows_written`,
`fetch_count`) surfaces "D-stage ran but found nothing" without
turning that case into a precondition failure.

**Per-run write counters** (new): each Phase C invoker writes a
single `bootstrap_archive_results` row keyed on
`(bootstrap_run_id, stage_key, archive_name)` with
`rows_written + rows_skipped_*` columns. Phase E preconditions
query this table.

D-stages depend on C1.b (not just C1.a) so secondary-page deep-
history filings are in `filing_events` before body parsers run.

## Required archives — exhaustive

Inventory is **18 archives**, not 14:

- 2 nightly: `submissions.zip`, `companyfacts.zip`
- 4 13F (rolling 3-month windows)
- 8 insider (quarterly `<YYYY>q<N>_form345.zip`)
- 4 N-PORT (quarterly `<YYYY>q<N>_nport.zip`)

A3 must land **all 18** by default (configurable via
`BOOTSTRAP_REQUIRED_ARCHIVES_GLOB` env var for ops that
deliberately exclude families). Anything less = stage `error`.

## Multi-archive job exception handling

Current C3/C4/C5 wrappers catch per-archive exception, log, and
`continue`. That can mark the stage `success` with
`total_rows_written=0`. New rule: per-archive failures are
collected; at end of loop, if ANY archive failed OR aggregate
`rows_written == 0`, raise. Stage = `error`.

## Rate-budget invariant (corrected)

Codex review WARNING: A3 hits `www.sec.gov` (HEAD + GET + Range
probes) and so DOES count against SEC's per-IP budget. Treating it
as a separate budget is wrong by default.

Real picture: A3 issues ~19 small requests (1 probe + 18 HEADs)
plus 18 long-running streamed GETs. The streamed GETs are open
TCP connections, not request slots — they don't count against
the per-second rate ceiling. The 19 small requests do.

Fix: A3's `httpx.AsyncClient` shares the process-wide
`_PROCESS_RATE_LIMIT_CLOCK` + `_PROCESS_RATE_LIMIT_LOCK`. Every
HTTP request — HEAD, range-GET probe, AND streamed `client.stream()`
body GET — calls `acquire_rate_token()` BEFORE the request opens
on the wire. The streamed GET acquires once at stream-open (not
per-chunk); the per-chunk reads do not count as fresh requests.

Concretely: A3 wraps `client.head()`, `client.get(...range...)`,
and `client.stream("GET", ...)` open in the same
`await rate_limiter.acquire()` call as the synchronous
`ResilientClient`. Without this, A3 can fire 18 streamed GETs
back-to-back at stream-open within milliseconds, bursting past
the per-IP budget regardless of how slowly bytes flow afterwards.

## Parallelism implementation

Current orchestrator uses two threads: `init_thread` for A1, then
`etoro_thread` + `sec_thread` for everything else. Stages within
a thread run strictly sequentially.

Refactor:
- Replace the two-thread model with a per-phase fan-out.
- Each stage declares `requires` (list of stage_keys it depends on)
  + `lane` (rate budget it shares).
- Orchestrator topologically sorts the stages by `requires`, groups
  into "ready batches", and fires each batch concurrently.
- Each lane carries an explicit `max_concurrency` value:
  - `init` (A1): 1.
  - `etoro` (A2): 1 (separate rate budget).
  - `sec_rate` (B1, B2, B3, B4, C1.b, D1, D2, D3): **1** —
    serialise against shared SEC clock.
  - `sec_bulk_download` (A3): 1 — only one bulk downloader at a time.
  - `db` (C1.a, C2, C3, C4, C5, E1, E2): **N** (default 5) —
    DB-bound stages with separate `psycopg.connect()` calls; no
    shared rate budget; write different table families so no
    row-level lock contention. `statement_timeout='10min'` per
    connection bounds runaway. Concurrent execution within this
    lane is the design intent.

Across lanes: parallel. Within a lane: `max_concurrency` bounds
the ready-batch fan-out.

Phase C5 (5 ingesters): all in `db` lane with `max_concurrency=5`
→ run concurrently. E1 + E2 same.

## Stage status (status enum semantics)

Three terminal-failure-shaped statuses, distinguished by who
detected the failure:

- `error` — the invoker was called and raised. The raise can come
  from runtime failure OR from precondition validation inside the
  invoker. Either way the operator's panel shows the error message.
- `blocked` — the orchestrator NEVER called the invoker because a
  `requires` stage finished `error` or `blocked`. No invoker
  run-attempt was made.
- `skipped` — operator-policy reason (e.g. legacy fallback skipped
  because bulk path succeeded). Distinct from `blocked` because no
  failure occurred upstream.

Pre-write validation always raises from inside the invoker → status
`error`. `blocked` is a propagation status, not a precondition
status.

## Failure-mode invariants

- A3 partial download → A3 = `error`. C1.a/C2/C3/C4/C5 = `blocked`.
  C1.b = `blocked`. D1/D2/D3 = `blocked`. Legacy fallback chain
  fires (filings_history_seed, sec_first_install_drain, etc.) so
  the operator still gets data via per-CIK scalar.
- Phase C precondition fails (archive provenance mismatch, ref
  table not refreshed in this run) → that C-stage `error`. Other
  C-stages independent of the failure can still run. Downstream
  D-stages depending on the failed C-stage = `blocked`. Legacy
  fallback specific to that data family runs.
- Phase D empty form-filter result → D-stage `success` with
  `rows_written=0` telemetry (NOT `error`). Idempotent body-fetch
  pipeline can run zero iterations correctly; the operator panel
  surfaces zero-row outcomes via the per-stage telemetry, not via
  a stage-error. Reconciled with the D-stage precondition contract
  in the table above.

## Migration notes

Three new migrations:

- `sql/130_bootstrap_stages_lane_extension.sql`: ALTER `lane` CHECK
  to allow `('init', 'etoro', 'sec', 'sec_rate', 'sec_bulk_download', 'db')`.
  Existing rows with `lane='sec'` stay valid; new specs use the
  finer-grained lane names.
- `sql/131_bootstrap_stages_status_blocked.sql`: ALTER `status`
  CHECK to allow `'blocked'` AND preserve every existing terminal
  state. Final allowed set:
  `('pending', 'running', 'success', 'error', 'skipped', 'blocked')`.
  The existing schema permits `skipped`; the migration must NOT
  drop it.
- `sql/132_bootstrap_archive_results.sql`: NEW table
  `bootstrap_archive_results (bootstrap_run_id BIGINT, stage_key TEXT,
   archive_name TEXT, rows_written BIGINT, rows_skipped JSONB,
   completed_at TIMESTAMPTZ, PRIMARY KEY (bootstrap_run_id, stage_key, archive_name))`.
  Each Phase C invoker writes one row per processed archive.

Frontend: Bootstrap panel renders `blocked` with red-error styling
but a distinct sublabel ("Skipped — upstream failure").

## Implementation PRs

PR1: ETL precondition contract + sec_bulk_download error-on-partial.
PR2: Phase C concurrent dispatch + new stage status.
PR3: Frontend `blocked` status rendering.

PR1 + PR2 unblock the morning re-run; PR3 is operator-UX polish.
