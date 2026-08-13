-- 113_ownership_insiders_observations.sql
--
-- Issue #840 P1 (Phase 1 schema unification, sub-PR A) —
-- foundational tables for the immutable observations + materialised
-- _current pattern. Spec at
-- docs/superpowers/specs/2026-05-04-ownership-full-decomposition-design.md
-- (Phase 1, §Data model design).
--
-- This migration introduces the shape; per-category siblings land in
-- 114-118 (institutions, blockholders, treasury, def14a). The shape
-- repeats inline across categories rather than via a Postgres composite
-- type — composite types are clunky to upsert / index, and a dedicated
-- schema-shape test (TestProvenanceBlockUniformity in
-- tests/test_ownership_observations.py) enforces drift-free uniformity
-- at CI time per Codex plan-review finding #4.
--
-- Two-axis dedup model:
--   1. ``source`` priority chain (form4 > form3 > 13d > 13g > def14a > 13f > nport > ncsr).
--   2. ``ownership_nature`` enum: direct | indirect | beneficial | voting | economic.
-- Dedup ONLY within compatible natures — Cohen's GME 13D/A
-- (beneficial 75M) and Form 4 (direct 38M) BOTH render under the new
-- model.
--
-- Identity (Codex review for #840.A): legacy Form 4 rows allow
-- ``filer_cik IS NULL`` (natural persons / pre-CIK filings). The
-- holder identity falls back to ``LOWER(TRIM(holder_name))`` when CIK
-- is null. ``holder_identity_key`` is a generated stored column that
-- materialises the resolved key and feeds every PK / unique index so
-- NULL-CIK rows are not silently dropped during backfill.
--
-- Partitioning: per Codex plan-review finding #1, the partition floor
-- must cover legacy data (period_end going back to early 2010s).
-- Hard-coded ranges 2010-2030 cover every observed legacy
-- ``period_end``; a default partition catches anything outside the
-- range. The schema-shape test
-- (TestProvenanceBlockUniformity::test_default_partition_is_empty_post_backfill)
-- asserts the default partition stays empty post-backfill — fails CI
-- if any row landed there.

BEGIN;

-- ---------------------------------------------------------------------
-- ownership_insiders_observations — append-only fact log
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ownership_insiders_observations (
    instrument_id           BIGINT NOT NULL,
    holder_cik              TEXT,                      -- nullable: legacy NULL-CIK Form 4 rows
    holder_name             TEXT NOT NULL,
    holder_identity_key     TEXT NOT NULL GENERATED ALWAYS AS (
        CASE WHEN holder_cik IS NOT NULL AND length(trim(holder_cik)) > 0
             THEN 'CIK:' || trim(holder_cik)
             ELSE 'NAME:' || lower(trim(holder_name)) END
    ) STORED,
    ownership_nature        TEXT NOT NULL
        CHECK (ownership_nature IN ('direct', 'indirect', 'beneficial', 'voting', 'economic')),

    -- Provenance block (uniform across every ownership_*_observations table).
    source                  TEXT NOT NULL
        CHECK (source IN ('form4', 'form3', '13d', '13g', 'def14a', '13f', 'nport', 'ncsr', 'xbrl_dei', '10k_note', 'finra_si', 'derived')),
    source_document_id      TEXT NOT NULL,
    source_accession        TEXT,
    source_field            TEXT,
    source_url              TEXT,
    filed_at                TIMESTAMPTZ NOT NULL,
    period_start            DATE,
    period_end              DATE NOT NULL,
    known_from              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    known_to                TIMESTAMPTZ,
    ingest_run_id           UUID NOT NULL,

    -- Fact payload.
    shares                  NUMERIC(24, 4),

    -- Natural key uses ``holder_identity_key`` (generated) so NULL
    -- CIKs don't silently break the PK. Same accession + same nature
    -- with different CIKs / names = distinct rows.
    PRIMARY KEY (instrument_id, holder_identity_key, ownership_nature, source, source_document_id, period_end)
) PARTITION BY RANGE (period_end);

COMMENT ON TABLE ownership_insiders_observations IS
    'Immutable per-filing-fact log for insiders (Form 4 / Form 3 / DEF 14A bene). Append-only; rebuild source for ownership_insiders_current. Spec: docs/superpowers/specs/2026-05-04-ownership-full-decomposition-design.md (Phase 1).';

-- Quarterly partitions 2010-2030 cover every observed legacy
-- period_end. Default partition catches outliers; a CI test asserts
-- it stays empty post-backfill (Codex plan-review finding #1).
DO $$
DECLARE
    yr INT;
    qtr INT;
    qstart DATE;
    qend DATE;
    pname TEXT;
BEGIN
    FOR yr IN 2010..2030 LOOP
        FOR qtr IN 1..4 LOOP
            qstart := MAKE_DATE(yr, (qtr - 1) * 3 + 1, 1);
            qend := qstart + INTERVAL '3 months';
            pname := FORMAT('ownership_insiders_observations_%sq%s', yr, qtr);
            EXECUTE FORMAT(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF ownership_insiders_observations FOR VALUES FROM (%L) TO (%L)',
                pname, qstart, qend
            );
        END LOOP;
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS ownership_insiders_observations_default
    PARTITION OF ownership_insiders_observations DEFAULT;

CREATE INDEX IF NOT EXISTS idx_insiders_obs_instrument_period
    ON ownership_insiders_observations (instrument_id, period_end DESC);

CREATE INDEX IF NOT EXISTS idx_insiders_obs_holder_period
    ON ownership_insiders_observations (holder_identity_key, period_end DESC);

-- ---------------------------------------------------------------------
-- ownership_insiders_current — materialised dedup snapshot
-- ---------------------------------------------------------------------
-- Natural key (instrument_id, holder_identity_key, ownership_nature).
-- Rebuilt by refresh_insiders_current(instrument_id) under a
-- per-instrument pg_advisory_xact_lock so concurrent refreshes
-- serialise (Codex plan-review finding #3). The PK is the second-line
-- guard if the lock is ever bypassed.

CREATE TABLE IF NOT EXISTS ownership_insiders_current (
    instrument_id           BIGINT NOT NULL,
    holder_cik              TEXT,
    holder_name             TEXT NOT NULL,
    holder_identity_key     TEXT NOT NULL,
    ownership_nature        TEXT NOT NULL
        CHECK (ownership_nature IN ('direct', 'indirect', 'beneficial', 'voting', 'economic')),

    -- Winning observation's full provenance.
    source                  TEXT NOT NULL
        CHECK (source IN ('form4', 'form3', '13d', '13g', 'def14a', '13f', 'nport', 'ncsr', 'xbrl_dei', '10k_note', 'finra_si', 'derived')),
    source_document_id      TEXT NOT NULL,
    source_accession        TEXT,
    source_url              TEXT,
    filed_at                TIMESTAMPTZ NOT NULL,
    period_start            DATE,
    period_end              DATE NOT NULL,
    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    shares                  NUMERIC(24, 4),

    PRIMARY KEY (instrument_id, holder_identity_key, ownership_nature)
);

COMMENT ON TABLE ownership_insiders_current IS
    'Materialised latest-per-(instrument, holder, nature) snapshot from ownership_insiders_observations. Rebuilt deterministically by refresh_insiders_current(). Read by the rollup endpoint after #840.E.';

CREATE INDEX IF NOT EXISTS idx_insiders_current_holder
    ON ownership_insiders_current (holder_identity_key);

COMMIT;
