-- 114_ownership_institutions_observations.sql
--
-- Issue #840 P1.B — institutions observations + _current per the
-- Phase 1 schema unification. Mirrors the shape established in
-- migration 113 (insiders).
--
-- Natural key per spec §"Per-category natural keys":
--   - observations: (instrument_id, filer_cik, period_end, source_document_id)
--   - _current:     (instrument_id, filer_cik)
--
-- Note: institutions don't use the cross-source priority chain the
-- way insiders do (13F is the only source today; nport/ncsr land in
-- Phase 3). Dedup is "latest period_end wins per (instrument, filer)".
-- ``ownership_nature`` for 13F is ``voting`` or ``economic`` per the
-- spec — current 13F-HR ingest tracks the holding-level voting figure
-- in ``voting_authority`` already; this PR maps to ``economic`` as
-- the default (full reported position) and the operator UI will gain
-- a separate voting overlay later. Mapping pinned in
-- ``record_institution_observation`` docstring.
--
-- Provenance block matches insiders byte-for-byte (Codex finding #4
-- enforced by the schema-shape uniformity test).
--
-- ``filer_cik`` is the canonical identity here. The legacy
-- ``institutional_holdings`` table joins to ``institutional_filers``
-- via ``filer_id`` (BIGSERIAL FK); the backfill (#840.E-prep) has to
-- resolve filer_id → cik before recording observations. Codex
-- plan-review finding #2 documented the gap; the test suite below
-- pins the contract.

BEGIN;

CREATE TABLE IF NOT EXISTS ownership_institutions_observations (
    instrument_id           INTEGER NOT NULL,
    filer_cik               TEXT NOT NULL,
    filer_name              TEXT NOT NULL,
    filer_type              TEXT
        CHECK (filer_type IS NULL OR filer_type IN ('ETF', 'INV', 'INS', 'BD', 'OTHER')),
    ownership_nature        TEXT NOT NULL
        CHECK (ownership_nature IN ('direct', 'indirect', 'beneficial', 'voting', 'economic')),

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

    shares                  NUMERIC(24, 4),
    market_value_usd        NUMERIC(20, 2),
    voting_authority        TEXT
        CHECK (voting_authority IS NULL OR voting_authority IN ('SOLE', 'SHARED', 'NONE')),
    -- 13F-HR can carry up to three rows per (accession, instrument):
    -- the equity position, PUT exposure, CALL exposure. Legacy
    -- ``institutional_holdings`` keeps them distinct via
    -- ``COALESCE(is_put_call, 'EQUITY')`` in a partial UNIQUE index.
    -- The new model promotes that exposure kind to a first-class
    -- column so all three coexist instead of collapsing on
    -- ON CONFLICT (Codex review for #840.B caught the prior shape).
    exposure_kind           TEXT NOT NULL DEFAULT 'EQUITY'
        CHECK (exposure_kind IN ('EQUITY', 'PUT', 'CALL')),

    PRIMARY KEY (instrument_id, filer_cik, ownership_nature, period_end, source_document_id, exposure_kind)
) PARTITION BY RANGE (period_end);

COMMENT ON TABLE ownership_institutions_observations IS
    'Immutable per-quarter-per-filer 13F-HR fact log. Append-only; rebuild source for ownership_institutions_current. Spec: docs/superpowers/specs/2026-05-04-ownership-full-decomposition-design.md (Phase 1).';

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
            pname := FORMAT('ownership_institutions_observations_%sq%s', yr, qtr);
            EXECUTE FORMAT(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF ownership_institutions_observations FOR VALUES FROM (%L) TO (%L)',
                pname, qstart, qend
            );
        END LOOP;
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS ownership_institutions_observations_default
    PARTITION OF ownership_institutions_observations DEFAULT;

CREATE INDEX IF NOT EXISTS idx_inst_obs_instrument_period
    ON ownership_institutions_observations (instrument_id, period_end DESC);

CREATE INDEX IF NOT EXISTS idx_inst_obs_filer_period
    ON ownership_institutions_observations (filer_cik, period_end DESC);


CREATE TABLE IF NOT EXISTS ownership_institutions_current (
    instrument_id           INTEGER NOT NULL,
    filer_cik               TEXT NOT NULL,
    filer_name              TEXT NOT NULL,
    filer_type              TEXT
        CHECK (filer_type IS NULL OR filer_type IN ('ETF', 'INV', 'INS', 'BD', 'OTHER')),
    ownership_nature        TEXT NOT NULL
        CHECK (ownership_nature IN ('direct', 'indirect', 'beneficial', 'voting', 'economic')),

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
    market_value_usd        NUMERIC(20, 2),
    voting_authority        TEXT
        CHECK (voting_authority IS NULL OR voting_authority IN ('SOLE', 'SHARED', 'NONE')),
    -- Mirror the observations table: distinct equity / PUT / CALL
    -- exposures coexist in _current (Codex review for #840.B).
    exposure_kind           TEXT NOT NULL DEFAULT 'EQUITY'
        CHECK (exposure_kind IN ('EQUITY', 'PUT', 'CALL')),

    PRIMARY KEY (instrument_id, filer_cik, ownership_nature, exposure_kind)
);

COMMENT ON TABLE ownership_institutions_current IS
    'Materialised latest-per-(instrument, filer, nature) 13F-HR snapshot. Rebuilt deterministically by refresh_institutions_current().';

CREATE INDEX IF NOT EXISTS idx_inst_current_filer
    ON ownership_institutions_current (filer_cik);

COMMIT;
