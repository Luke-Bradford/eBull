-- Widen ownership_*_observations.instrument_id (and matching _current
-- tables) from INTEGER → BIGINT to match the parent ``instruments``
-- PK type (sql/001_init.sql defines ``instruments.instrument_id`` as
-- BIGINT). Codex sweep BLOCKING for #1020.
--
-- Without this, a future widening of the instruments PK above 2.1B
-- would silently truncate FK-side joins on ownership tables. The
-- mismatch also defeats merge-join planner choices because Postgres
-- has to insert an implicit cast on every join column.
--
-- For PostgreSQL partitioned tables, ALTER on the parent cascades to
-- every partition automatically.

ALTER TABLE ownership_insiders_observations
    ALTER COLUMN instrument_id TYPE BIGINT;
ALTER TABLE ownership_insiders_current
    ALTER COLUMN instrument_id TYPE BIGINT;

ALTER TABLE ownership_institutions_observations
    ALTER COLUMN instrument_id TYPE BIGINT;
ALTER TABLE ownership_institutions_current
    ALTER COLUMN instrument_id TYPE BIGINT;

ALTER TABLE ownership_blockholders_observations
    ALTER COLUMN instrument_id TYPE BIGINT;
ALTER TABLE ownership_blockholders_current
    ALTER COLUMN instrument_id TYPE BIGINT;

ALTER TABLE ownership_treasury_observations
    ALTER COLUMN instrument_id TYPE BIGINT;
ALTER TABLE ownership_treasury_current
    ALTER COLUMN instrument_id TYPE BIGINT;

ALTER TABLE ownership_def14a_observations
    ALTER COLUMN instrument_id TYPE BIGINT;
ALTER TABLE ownership_def14a_current
    ALTER COLUMN instrument_id TYPE BIGINT;

ALTER TABLE ownership_funds_observations
    ALTER COLUMN instrument_id TYPE BIGINT;
ALTER TABLE ownership_funds_current
    ALTER COLUMN instrument_id TYPE BIGINT;

ALTER TABLE ownership_esop_observations
    ALTER COLUMN instrument_id TYPE BIGINT;
ALTER TABLE ownership_esop_current
    ALTER COLUMN instrument_id TYPE BIGINT;
