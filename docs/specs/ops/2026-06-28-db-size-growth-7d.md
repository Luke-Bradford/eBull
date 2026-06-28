# db_size_growth_7d trend signal for postgres-health (#1564)

## Problem
`/system/postgres-health` reports the absolute `db_size_bytes` + a warn breach,
but no **trend**. The operator can see "47 GB / warn 60 GB" yet not whether the
DB grew 0.1 GB or 4 GB this week. #1556 added the absolute size; this is the
stretch trend signal.

## Source rule
N/A — this is an ops/infra sampler over `pg_database_size()`, not an
ownership/filings/metric data-treatment decision. No SEC reg or EDGAR doc
governs it. Treatment fixed by Postgres semantics: `pg_database_size(current_database())`
is the authoritative on-disk size (same function the existing `_q_db_size`
probe and the pre-push bloat-warn hook already use — single source of truth).

## Premise verification (full population, dev DB)
- `to_regclass('public.pg_size_sample')` → NULL (table absent; new).
- `pg_database_size('ebull')` → 47 GB; `bootstrap_runs` latest = `complete`.
- `/system/postgres-health` has **zero frontend consumers** (`grep -r postgres frontend/src` empty)
  → the JSON response fields ARE the operator-visible surface. No FE work.

## Design
Daily sample table + sampler job + read-path delta + 2 endpoint fields.

### 1. Migration `sql/206_pg_size_sample.sql`
Standalone sample table (no FK, no partition parent → prevention-log L969
TRUNCATE-on-_PLANNER_TABLES N/A):
```sql
CREATE TABLE IF NOT EXISTS pg_size_sample (
    sampled_on    DATE PRIMARY KEY,
    db_size_bytes BIGINT NOT NULL,
    sampled_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### 2. Sampler job (`app/workers/scheduler.py`)
- `JOB_PG_SIZE_SAMPLE = "pg_size_sample"` constant.
- `def pg_size_sample()` body inside `_tracked_job(JOB_PG_SIZE_SAMPLE)`:
  `INSERT INTO pg_size_sample (sampled_on, db_size_bytes)
   VALUES (CURRENT_DATE, pg_database_size(current_database()))
   ON CONFLICT (sampled_on) DO UPDATE
     SET db_size_bytes = EXCLUDED.db_size_bytes, sampled_at = NOW();`
  (Idempotent — re-run same day updates, never duplicates.)
- `ScheduledJob(name=JOB_PG_SIZE_SAMPLE, source="db_size_sample",
   cadence=Cadence.daily(hour=2, minute=15), catch_up_on_boot=True,
   role="steady_state", display_name="DB size sample")`.
  - **Dedicated single-job lane `db_size_sample`** (Codex ckpt-1 MED2): a
    daily-precision job on the catch-all `db` lane loses the JobLock race to
    long `db`-lane jobs (`raw_data_retention_sweep` 02:00 filesystem rehash) and
    skips a full day — the exact #1526/#1527 de-starvation pattern already
    extracted for `db_liveness`/`db_retry`/`db_positions`. Add `"db_size_sample"`
    to `app/jobs/sources.py::Lane` + `tests/test_job_registry.py::_ALLOWED_SOURCES`
    + pin in `tests/test_db_lane_steady_starvation.py`. `JOB_NAME_TO_SOURCE` is
    auto-built from `ScheduledJob.source`, no manual map edit.
  - NOT `exempt_from_universal_bootstrap_gate` — pre-bootstrap DB is tiny, so
    not sampling then is harmless, and the exemption carries a strict
    allow-list test + spec/Codex gate we don't need.
  - No `prerequisite` — empty-DB safe (the INSERT runs regardless).
- Register in `app/jobs/runtime.py` `_INVOKERS`:
  `JOB_PG_SIZE_SAMPLE: _adapt_zero_arg(pg_size_sample)` + the two imports.
- `app/services/processes/scheduled_adapter.py` `_LANE_BY_JOB`:
  `"pg_size_sample": "ops"` (explicit; defaults to "ops" anyway).

### 3. Read-path delta (`app/services/postgres_health.py`)
- `_q_db_size_growth_7d_baseline(conn) -> tuple[int, date]`: the most-recent
  sample **on or before** `CURRENT_DATE - 7 days`:
  ```sql
  SELECT db_size_bytes, sampled_on FROM pg_size_sample
   WHERE sampled_on <= CURRENT_DATE - INTERVAL '7 days'
   ORDER BY sampled_on DESC LIMIT 1;
  ```
  Returns None when no such row (cold start, <7d history) — `_safe`-wrapped.
- Pure function `compute_db_size_growth_7d(current_bytes, baseline, *, today) ->
  tuple[int | None, date | None]` (Codex ckpt-1 MED1 — staleness floor): returns
  `(current - baseline_bytes, baseline_date)` only when both present AND
  `baseline_date >= today - DB_SIZE_GROWTH_BASELINE_MAX_AGE_DAYS` (=10d: 7d
  target + 3d slack for missed samples); else `(None, None)`. Puts the
  "baseline too stale to call it 7d-growth" decision in a testable pure
  function, not SQL. `baseline_date` field reports the exact baseline age so the
  signal stays honest when the sampler skipped days. Table-tested.
- Snapshot fields `db_size_growth_7d_bytes: int | None`,
  `db_size_growth_7d_baseline_date: date | None`. **Informational only — no
  breach flag** (issue: no alarm semantics in v1).
- Delta uses the live `db_size_bytes` already probed by `_q_db_size` (None when
  that probe failed → growth None).

### 4. Endpoint (`app/api/system.py`)
`PostgresHealthResponse`: add `db_size_growth_7d_bytes: int | None`,
`db_size_growth_7d_baseline_date: date | None`; pass through from snapshot.

### 5. Tests
Pure-logic (`-m "not db"`):
- `compute_db_size_growth_7d`: cold-start (baseline None → (None, None)),
  normal delta (current 50 GB, baseline 47 GB → +3 GB), current-None
  (probe failed → (None, None)), negative delta (shrink after retention sweep),
  **stale baseline** (baseline_date 11d old → (None, None) despite both present),
  **boundary** (baseline exactly 10d old → still computes).
- Registry wiring asserted by existing `test_job_registry` /
  `test_db_lane_steady_starvation` once the lane is added.

DB-tier (`-m db`, one test): sampler `ON CONFLICT` idempotence — run
`pg_size_sample()` twice same day, assert one row, second value wins.

Endpoint shape: extend `tests/test_api_postgres_health.py` to assert the two
new fields are present + pass through.

## Post-merge
Daemon restart (new scheduler job registered). NO `sec_rebuild` (no SEC output
changed). First sample lands on next daily fire / boot catch-up; growth field
stays None until ≥7d of samples accumulate.

## Files (~6)
`sql/206_pg_size_sample.sql`, `app/workers/scheduler.py`, `app/jobs/runtime.py`,
`app/services/postgres_health.py`, `app/api/system.py`,
`app/services/processes/scheduled_adapter.py`, + tests.
