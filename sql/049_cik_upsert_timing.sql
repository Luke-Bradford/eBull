-- 049_cik_upsert_timing.sql
--
-- Per-CIK transaction timing instrumentation for SEC XBRL ingest
-- (#418, under the #414 fundamentals ingest redesign).
--
-- ADR 0004 (merged in #417) settled on `executemany(page_size=1000)`
-- for the per-CIK upsert shape. The ADR notes the DB-path bench alone
-- cannot settle whether residual operator-UI latency during a seed
-- comes from GIL contention, lock contention, or both. Answering that
-- in-prod requires per-CIK timing — today `data_ingestion_runs` is per
-- provider batch (one row per seed/refresh round), so a "CIK X took
-- 4.7s" signal has to be parsed out of log lines.
--
-- This table stores one row per `_run_cik_upsert` invocation.
-- Populated inside the existing `finally` block in
-- ``app/services/fundamentals.py`` alongside the
-- `fundamentals.cik_timing` log emit so the log and the row are always
-- in sync. Parent ingestion_run_id is NULL for skip-path exits (the
-- upsert never entered the run body, so the run has no claim on this
-- row).

CREATE TABLE IF NOT EXISTS cik_upsert_timing (
    timing_id            BIGSERIAL PRIMARY KEY,
    ingestion_run_id     BIGINT REFERENCES data_ingestion_runs(ingestion_run_id) ON DELETE SET NULL,
    cik                  TEXT NOT NULL,
    mode                 TEXT NOT NULL CHECK (mode IN ('seed', 'refresh')),
    outcome              TEXT NOT NULL,
    facts_upserted       INTEGER NOT NULL DEFAULT 0,
    seconds              NUMERIC(10, 3) NOT NULL,
    started_at           TIMESTAMPTZ NOT NULL,
    finished_at          TIMESTAMPTZ NOT NULL
);

-- Latest-run lookup: GET /sync/ingest/cik_timing/latest returns p50/p95
-- for every row that shares the newest ingestion_run_id. Index on
-- (ingestion_run_id DESC, seconds) keeps that query a single index scan.
CREATE INDEX IF NOT EXISTS cik_upsert_timing_run_desc_idx
    ON cik_upsert_timing (ingestion_run_id DESC NULLS LAST, seconds);

-- Regression alerting: cross-run comparison ("this CIK is 10x slower
-- than last seed") joins on cik + ingestion_run_id. Supports both.
CREATE INDEX IF NOT EXISTS cik_upsert_timing_cik_finished_idx
    ON cik_upsert_timing (cik, finished_at DESC);
