-- 105_ingest_backfill_queue.sql
--
-- Ingest backfill queue (#793 + #790 P4 — Batch 4 of #788).
--
-- New ingest pipelines (Form 3 baseline, N-PORT, FINRA short
-- interest, etc.) ship without a historical pass — they only fetch
-- filings that arrive after the pipeline goes live. The operator
-- needs an explicit "backfill on activation" pass for already-
-- seeded instruments OR for the full coverage cohort, surfaced as
-- a queue so progress is visible and re-runs are idempotent.
--
-- This table is the queue. Rows are emitted:
--   * On a new ingest pipeline migration (one row per
--     (instrument_id, pipeline_name) for already-seeded
--     instruments).
--   * Manually via the operator's ingest-health page when they
--     click "re-run section" or "full backfill" on a provider
--     group.
--   * Programmatically when a downstream consumer (e.g. ownership-
--     rollup) detects coverage gaps and wants a targeted re-fetch.
--
-- The drainer worker lives in the jobs process (#719 settled
-- topology — never the API process). It picks the highest-priority
-- pending row, runs the named pipeline against the named
-- instrument, and updates the row's status to ``running`` →
-- ``complete`` / ``failed``. Idempotent: re-queueing the same
-- (instrument, pipeline) row promotes the existing row via
-- ON CONFLICT instead of inserting a duplicate.
--
-- Out of scope for this migration:
--   * The drainer itself (lands alongside the jobs process wiring).
--   * Per-pipeline cost / ETA estimates — surfaced separately by
--     the ingest-health page from observed last-run durations.

CREATE TABLE IF NOT EXISTS ingest_backfill_queue (
    instrument_id   BIGINT NOT NULL
        REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    -- Free-form pipeline tag — kept TEXT (not enum) because new
    -- pipelines ship under their own name and we don't want to
    -- block them on a CHECK migration. Convention: snake_case
    -- matching the service module that runs the pipeline (e.g.
    -- ``insider_form3_ingest``, ``institutional_holdings``,
    -- ``blockholders``).
    pipeline_name   TEXT NOT NULL,
    -- Lower priority = runs first. Default ``100`` sits in the
    -- middle of the default scale; an operator-triggered
    -- "backfill now" sets ``10`` so it pre-empts the routine
    -- backfill traffic.
    priority        INTEGER NOT NULL DEFAULT 100,
    status          TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'complete', 'failed')),
    queued_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    -- The trigger source — informational, helps the operator
    -- understand why a backfill row exists.
    triggered_by    TEXT NOT NULL DEFAULT 'system'
        CHECK (triggered_by IN ('system', 'operator', 'migration', 'consumer')),
    PRIMARY KEY (instrument_id, pipeline_name)
);

-- Hot path for the drainer worker: pick the next pending row by
-- priority + queued_at. Partial index keeps it cheap as the queue
-- table grows over time (completed rows persist for audit).
CREATE INDEX IF NOT EXISTS idx_ingest_backfill_pending
    ON ingest_backfill_queue (priority, queued_at)
    WHERE status = 'pending';

-- Lookup index for the operator page's "running now" surface.
CREATE INDEX IF NOT EXISTS idx_ingest_backfill_running
    ON ingest_backfill_queue (started_at DESC)
    WHERE status = 'running';

-- Lookup index for the recent-failures surface — read by the
-- operator's ingest-health page.
CREATE INDEX IF NOT EXISTS idx_ingest_backfill_failed
    ON ingest_backfill_queue (completed_at DESC)
    WHERE status = 'failed';

COMMENT ON TABLE ingest_backfill_queue IS
    'Per-(instrument, pipeline) backfill queue. Rows are emitted on '
    'pipeline activation (migration), operator request, or coverage-'
    'gap detection. Drained by a worker in the jobs process (#719); '
    'never by the API. Status transitions: pending → running → '
    'complete | failed. Re-queue is idempotent via the PK ON CONFLICT.';
