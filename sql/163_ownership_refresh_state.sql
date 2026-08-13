-- 163_ownership_refresh_state.sql
--
-- Issue #1233 — PR12: separate drift watermark from `_current` row data so
-- diff-aware MERGE writers do not feed the repair-sweep forever-loop.
-- Spec: docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md §3.3
--
-- Side-table holds one row per (instrument_id, category) carrying the
-- last `MAX(observations.ingested_at)` value the matching
-- `refresh_X_current` helper drained AND the wall-clock of the most
-- recent refresh attempt. Repair-sweep predicate JOINs this table to a
-- per-observations-table `MAX(ingested_at) GROUP BY instrument_id` CTE
-- aggregate; `IS DISTINCT FROM` is NULL-safe.
--
-- Backfill from `MAX(_current.refreshed_at) GROUP BY instrument_id`:
-- false-positive (first sweep refreshes more instruments than strictly
-- necessary due to tx-time vs clock_timestamp skew, each call is a
-- MERGE no-op) is benign; false-negative (state claiming reconciliation
-- that never happened) would mask real drift — unacceptable for the
-- safety-net job. See Codex 1d HIGH-1 in the spec for the trade-off
-- discussion.
--
-- Indexes scoped to funds + esop only — sql/119 already provisioned
-- `(instrument_id, ingested_at DESC)` for insiders / institutions /
-- blockholders / treasury / def14a. See Codex 1d MED-1.

BEGIN;

CREATE TABLE IF NOT EXISTS ownership_refresh_state (
    instrument_id                             BIGINT      NOT NULL,
    category                                  TEXT        NOT NULL CHECK (category IN (
        'insiders', 'institutions', 'blockholders', 'treasury', 'def14a', 'funds', 'esop'
    )),
    last_drained_observations_max_ingested_at TIMESTAMPTZ,
    last_refresh_attempted_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_id, category)
);

COMMENT ON TABLE ownership_refresh_state IS
    'Per-(instrument, category) drift watermark for the ownership repair sweep (#1233 PR12). Decoupled from _current.refreshed_at so diff-aware MERGE writers do not freeze the watermark on no-op refreshes.';

CREATE INDEX IF NOT EXISTS idx_ownership_refresh_state_category
    ON ownership_refresh_state (category, last_drained_observations_max_ingested_at);

CREATE INDEX IF NOT EXISTS idx_funds_obs_instrument_ingested
    ON ownership_funds_observations (instrument_id, ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_esop_obs_instrument_ingested
    ON ownership_esop_observations (instrument_id, ingested_at DESC);

INSERT INTO ownership_refresh_state (
    instrument_id, category,
    last_drained_observations_max_ingested_at, last_refresh_attempted_at
)
SELECT c.instrument_id, 'insiders', MAX(c.refreshed_at), MAX(c.refreshed_at)
FROM ownership_insiders_current c
GROUP BY c.instrument_id
ON CONFLICT (instrument_id, category) DO NOTHING;

INSERT INTO ownership_refresh_state (
    instrument_id, category,
    last_drained_observations_max_ingested_at, last_refresh_attempted_at
)
SELECT c.instrument_id, 'institutions', MAX(c.refreshed_at), MAX(c.refreshed_at)
FROM ownership_institutions_current c
GROUP BY c.instrument_id
ON CONFLICT (instrument_id, category) DO NOTHING;

INSERT INTO ownership_refresh_state (
    instrument_id, category,
    last_drained_observations_max_ingested_at, last_refresh_attempted_at
)
SELECT c.instrument_id, 'blockholders', MAX(c.refreshed_at), MAX(c.refreshed_at)
FROM ownership_blockholders_current c
GROUP BY c.instrument_id
ON CONFLICT (instrument_id, category) DO NOTHING;

INSERT INTO ownership_refresh_state (
    instrument_id, category,
    last_drained_observations_max_ingested_at, last_refresh_attempted_at
)
SELECT c.instrument_id, 'treasury', MAX(c.refreshed_at), MAX(c.refreshed_at)
FROM ownership_treasury_current c
GROUP BY c.instrument_id
ON CONFLICT (instrument_id, category) DO NOTHING;

INSERT INTO ownership_refresh_state (
    instrument_id, category,
    last_drained_observations_max_ingested_at, last_refresh_attempted_at
)
SELECT c.instrument_id, 'def14a', MAX(c.refreshed_at), MAX(c.refreshed_at)
FROM ownership_def14a_current c
GROUP BY c.instrument_id
ON CONFLICT (instrument_id, category) DO NOTHING;

INSERT INTO ownership_refresh_state (
    instrument_id, category,
    last_drained_observations_max_ingested_at, last_refresh_attempted_at
)
SELECT c.instrument_id, 'funds', MAX(c.refreshed_at), MAX(c.refreshed_at)
FROM ownership_funds_current c
GROUP BY c.instrument_id
ON CONFLICT (instrument_id, category) DO NOTHING;

INSERT INTO ownership_refresh_state (
    instrument_id, category,
    last_drained_observations_max_ingested_at, last_refresh_attempted_at
)
SELECT c.instrument_id, 'esop', MAX(c.refreshed_at), MAX(c.refreshed_at)
FROM ownership_esop_current c
GROUP BY c.instrument_id
ON CONFLICT (instrument_id, category) DO NOTHING;

COMMIT;
