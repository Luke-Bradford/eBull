-- 123_ownership_funds.sql
--
-- Issue #917 — Phase 3 PR1: N-PORT mutual-fund holdings ingest.
-- Spec: docs/superpowers/specs/2026-05-04-ownership-full-decomposition-design.md
-- (Phase 3, §"Per-category natural keys" + §Data model design).
--
-- Mirrors the shape established by migration 114
-- (ownership_institutions_observations) — same provenance block,
-- same partition strategy (RANGE on ``period_end`` quarterly
-- buckets 2010-2030 + DEFAULT), same `_current` materialised
-- snapshot pattern.
--
-- Differences from institutions:
--
--   * Identity is ``fund_series_id`` (SEC series identifier, e.g.
--     ``S000004310``), NOT ``fund_filer_cik``. One RIC (filer CIK)
--     typically has 5-50 series; the rollup endpoint aggregates by
--     series, not by filer. ``fund_filer_cik`` is recorded for
--     audit and per-filer rollup queries but is NOT in the natural
--     key.
--   * CHECK constraints lock the per-source enum values into the
--     schema:
--       - ``ownership_nature = 'economic'`` (N-PORT is a full
--         reported position; no voting/beneficial split).
--       - ``source = 'nport'`` (this table is N-PORT exclusive;
--         N-CSR semi-annual will reuse this table in #918 — at
--         which point the CHECK widens to allow ``ncsr`` too).
--       - ``payoff_profile = 'Long'`` and ``asset_category = 'EC'``
--         (equity-common). N-PORT carries debt / derivative /
--         preferred / short positions in the same per-fund holdings
--         array; the ingester filters those at the write boundary,
--         and the schema CHECK is the second-line guard against a
--         bug in the ingester.
--   * ``shares NOT NULL`` — every retained holding must carry a
--     positive share balance. The ingester's write-side guard
--     enforces ``shares > 0``; the NOT NULL is the schema-level
--     defence.
--
-- Codex pre-impl review findings (2026-05-05) addressed in this
-- migration:
--
--   * #2 (synthetic-{cik} collision risk): the CHECK constraint on
--     ``fund_series_id`` rejects any value that isn't a literal
--     ``S0000xxxxx`` SEC series identifier. Filings missing a
--     series_id are rejected at parse time, not synthesised.
--   * #3 + #4 (debt vs equity confusion + payoff_profile guard):
--     CHECK on ``asset_category = 'EC'`` and ``payoff_profile = 'Long'``
--     enforced in the schema.
--   * #5 (amendments in _current): handled by the refresh function's
--     ORDER BY clause in app/services/ownership_observations.py
--     (`filed_at DESC, period_end DESC, source_document_id ASC`).
--
-- ``_PLANNER_TABLES`` in tests/fixtures/ebull_test_db.py is updated
-- in the same PR per the prevention-log entry "Test-teardown list
-- missing new FK-child tables".

BEGIN;

-- ---------------------------------------------------------------------
-- ownership_funds_observations — append-only fact log
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ownership_funds_observations (
    instrument_id           INTEGER NOT NULL,
    fund_series_id          TEXT NOT NULL CHECK (fund_series_id ~ '^S[0-9]{9}$'),
    fund_series_name        TEXT NOT NULL,
    fund_filer_cik          TEXT NOT NULL,
    ownership_nature        TEXT NOT NULL CHECK (ownership_nature = 'economic'),

    -- Provenance block (uniform across every ownership_*_observations).
    source                  TEXT NOT NULL CHECK (source = 'nport'),
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
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Fact payload.
    shares                  NUMERIC(24, 4) NOT NULL CHECK (shares > 0),
    market_value_usd        NUMERIC(20, 2),
    payoff_profile          TEXT NOT NULL CHECK (payoff_profile = 'Long'),
    asset_category          TEXT NOT NULL CHECK (asset_category = 'EC'),

    PRIMARY KEY (instrument_id, fund_series_id, period_end, source_document_id)
) PARTITION BY RANGE (period_end);

COMMENT ON TABLE ownership_funds_observations IS
    'Immutable per-N-PORT-filing fact log for fund holdings (Phase 3 PR1, #917). Append-only; rebuild source for ownership_funds_current. Spec: docs/superpowers/specs/2026-05-04-ownership-full-decomposition-design.md (Phase 3).';

-- Quarterly partitions 2010-2030. N-PORT was introduced in 2018; the
-- 2010-2017 buckets are present for shape-uniformity with sibling
-- ownership_*_observations tables and stay empty until a parser
-- regression test seeds them.
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
            pname := FORMAT('ownership_funds_observations_%sq%s', yr, qtr);
            EXECUTE FORMAT(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF ownership_funds_observations FOR VALUES FROM (%L) TO (%L)',
                pname, qstart, qend
            );
        END LOOP;
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS ownership_funds_observations_default
    PARTITION OF ownership_funds_observations DEFAULT;

CREATE INDEX IF NOT EXISTS idx_funds_obs_instrument_period
    ON ownership_funds_observations (instrument_id, period_end DESC);

CREATE INDEX IF NOT EXISTS idx_funds_obs_series_period
    ON ownership_funds_observations (fund_series_id, period_end DESC);

CREATE INDEX IF NOT EXISTS idx_funds_obs_filer_period
    ON ownership_funds_observations (fund_filer_cik, period_end DESC);


-- ---------------------------------------------------------------------
-- ownership_funds_current — materialised dedup snapshot
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ownership_funds_current (
    instrument_id           INTEGER NOT NULL,
    fund_series_id          TEXT NOT NULL CHECK (fund_series_id ~ '^S[0-9]{9}$'),
    fund_series_name        TEXT NOT NULL,
    fund_filer_cik          TEXT NOT NULL,
    ownership_nature        TEXT NOT NULL CHECK (ownership_nature = 'economic'),

    source                  TEXT NOT NULL CHECK (source = 'nport'),
    source_document_id      TEXT NOT NULL,
    source_accession        TEXT,
    source_url              TEXT,
    filed_at                TIMESTAMPTZ NOT NULL,
    period_start            DATE,
    period_end              DATE NOT NULL,
    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    shares                  NUMERIC(24, 4) NOT NULL CHECK (shares > 0),
    market_value_usd        NUMERIC(20, 2),
    payoff_profile          TEXT NOT NULL CHECK (payoff_profile = 'Long'),
    asset_category          TEXT NOT NULL CHECK (asset_category = 'EC'),

    PRIMARY KEY (instrument_id, fund_series_id)
);

COMMENT ON TABLE ownership_funds_current IS
    'Materialised latest-per-(instrument, fund_series) N-PORT snapshot. Rebuilt deterministically by refresh_funds_current() ordering by filed_at DESC so amendments win over originals.';

CREATE INDEX IF NOT EXISTS idx_funds_current_series
    ON ownership_funds_current (fund_series_id);

CREATE INDEX IF NOT EXISTS idx_funds_current_filer
    ON ownership_funds_current (fund_filer_cik);

COMMIT;
