-- 119_ownership_observations_ingested_at.sql
--
-- Issue #864 (per spec v3 finding #1) — add monotonic system-time
-- column to every ownership_*_observations table so the repair sweep
-- in #873 can key on max-of-``ingested_at`` per instrument.
--
-- Why a new column instead of reusing ``known_from``?
-- ``known_from`` is VALID-time (the SEC accepted timestamp / parser
-- assignment) — it does NOT advance on a re-ingest of the same
-- accession or on a parser-version rewash. The repair sweep needs
-- SYSTEM-time so a re-ingest of an unchanged row still bumps the
-- watermark and signals ``_current`` to refresh. ``known_from`` cannot
-- carry that semantic without breaking history queries.
--
-- Why NOT reuse ``created_at``? Observation tables predate this column
-- and the existing rows would carry the original insert time. The
-- repair sweep must observe a bump on every UPSERT (including
-- DO-UPDATE on conflict), which ``created_at`` doesn't give us.
--
-- ``record_*_observation`` is updated in the same PR to bump
-- ``ingested_at`` to ``clock_timestamp()`` on DO UPDATE so every
-- UPSERT advances the watermark.
--
-- Codex pre-push finding #3: column DEFAULT is ``clock_timestamp()``
-- (not ``NOW()`` / ``transaction_timestamp()``) so an INSERT inside
-- a long batch transaction stamps each row at the moment of INSERT
-- rather than at transaction start. Without this, a batch rewash that
-- INSERTs 1000 rows in one tx would assign all 1000 the same
-- timestamp, breaking the repair sweep's ability to identify the
-- newest contribution within the batch.
--
-- Partitioned-table note: ALTER TABLE on the partitioned parent
-- propagates to every existing partition (Postgres 14+). All five
-- ownership_*_observations tables are partitioned by ``period_end``
-- RANGE — the ALTERs below cascade.

BEGIN;

ALTER TABLE ownership_insiders_observations
    ADD COLUMN ingested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp();

ALTER TABLE ownership_institutions_observations
    ADD COLUMN ingested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp();

ALTER TABLE ownership_blockholders_observations
    ADD COLUMN ingested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp();

ALTER TABLE ownership_treasury_observations
    ADD COLUMN ingested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp();

ALTER TABLE ownership_def14a_observations
    ADD COLUMN ingested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp();

-- Repair-sweep predicate index: per-instrument max(ingested_at) lookup.
-- The sweep query is
--   SELECT MAX(ingested_at) FROM ownership_*_observations
--   WHERE instrument_id = $1
-- so an index on (instrument_id, ingested_at DESC) keeps it cheap.
-- One per category — partitioned-table indexes auto-propagate.
CREATE INDEX idx_insiders_obs_instrument_ingested
    ON ownership_insiders_observations (instrument_id, ingested_at DESC);

CREATE INDEX idx_institutions_obs_instrument_ingested
    ON ownership_institutions_observations (instrument_id, ingested_at DESC);

CREATE INDEX idx_blockholders_obs_instrument_ingested
    ON ownership_blockholders_observations (instrument_id, ingested_at DESC);

CREATE INDEX idx_treasury_obs_instrument_ingested
    ON ownership_treasury_observations (instrument_id, ingested_at DESC);

CREATE INDEX idx_def14a_obs_instrument_ingested
    ON ownership_def14a_observations (instrument_id, ingested_at DESC);

COMMIT;
