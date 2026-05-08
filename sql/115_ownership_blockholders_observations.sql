-- 115_ownership_blockholders_observations.sql
--
-- Issue #840 P1.C — blockholders (13D/G) observations + _current.
-- Mirrors the shape established in migrations 113 (insiders) + 114
-- (institutions).
--
-- Natural key per spec §"Per-category natural keys":
--   - observations: (instrument_id, reporter_cik, ownership_nature, source, source_document_id)
--   - _current:     (instrument_id, reporter_cik, ownership_nature)
--
-- Identity follows #837's lesson: pin to the PRIMARY filer
-- (``blockholder_filers.cik``), not the per-row ``reporter_cik``.
-- Joint-reporter rows on the same accession share the SAME primary
-- filer; the observations table records ONE row per accession per
-- (filer, nature), collapsing joint reporters per the SEC rule that
-- joint filers claim the same beneficial figure.
--
-- ``ownership_nature`` for 13D/G is ``beneficial`` (Rule 13d-3 —
-- voting + investment power). Cohen's GME case: his 13D/A's
-- beneficial 75M and his Form 4's direct 38M live in DIFFERENT
-- categories under the two-axis model and BOTH render.
--
-- Provenance block matches insiders/institutions byte-for-byte.

BEGIN;

CREATE TABLE IF NOT EXISTS ownership_blockholders_observations (
    instrument_id           BIGINT NOT NULL,
    reporter_cik            TEXT NOT NULL,         -- primary filer cik per #837 lesson
    reporter_name           TEXT NOT NULL,
    ownership_nature        TEXT NOT NULL
        CHECK (ownership_nature IN ('direct', 'indirect', 'beneficial', 'voting', 'economic')),

    submission_type         TEXT NOT NULL,         -- 'SCHEDULE 13D' | 'SCHEDULE 13G' | '/A' variants
    status_flag             TEXT
        CHECK (status_flag IS NULL OR status_flag IN ('active', 'passive')),
    -- Cross-column invariant from legacy ``blockholder_filings`` (sql/095):
    -- 13D / 13D/A are active; 13G / 13G/A are passive. Two independent
    -- enum CHECKs would let bad combinations through; explicit
    -- compound CHECK enforces the SEC convention. Codex pre-push
    -- review for #840.C caught the prior version dropping this guard.
    CONSTRAINT obs_submission_type_status_consistent
        CHECK (
            status_flag IS NULL
            OR (submission_type IN ('SCHEDULE 13D', 'SCHEDULE 13D/A') AND status_flag = 'active')
            OR (submission_type IN ('SCHEDULE 13G', 'SCHEDULE 13G/A') AND status_flag = 'passive')
        ),

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

    aggregate_amount_owned  NUMERIC(24, 4),
    percent_of_class        NUMERIC(8, 4),

    PRIMARY KEY (instrument_id, reporter_cik, ownership_nature, source, source_document_id, period_end)
) PARTITION BY RANGE (period_end);

COMMENT ON TABLE ownership_blockholders_observations IS
    'Immutable per-13D/G-amendment fact log keyed on the primary filer. Append-only; rebuild source for ownership_blockholders_current. Identity per #837 lesson — primary filer cik, not per-row reporter_cik.';

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
            pname := FORMAT('ownership_blockholders_observations_%sq%s', yr, qtr);
            EXECUTE FORMAT(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF ownership_blockholders_observations FOR VALUES FROM (%L) TO (%L)',
                pname, qstart, qend
            );
        END LOOP;
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS ownership_blockholders_observations_default
    PARTITION OF ownership_blockholders_observations DEFAULT;

CREATE INDEX IF NOT EXISTS idx_block_obs_instrument_period
    ON ownership_blockholders_observations (instrument_id, period_end DESC);

CREATE INDEX IF NOT EXISTS idx_block_obs_reporter_period
    ON ownership_blockholders_observations (reporter_cik, period_end DESC);


CREATE TABLE IF NOT EXISTS ownership_blockholders_current (
    instrument_id           BIGINT NOT NULL,
    reporter_cik            TEXT NOT NULL,
    reporter_name           TEXT NOT NULL,
    ownership_nature        TEXT NOT NULL
        CHECK (ownership_nature IN ('direct', 'indirect', 'beneficial', 'voting', 'economic')),
    submission_type         TEXT NOT NULL,
    status_flag             TEXT
        CHECK (status_flag IS NULL OR status_flag IN ('active', 'passive')),
    CONSTRAINT current_submission_type_status_consistent
        CHECK (
            status_flag IS NULL
            OR (submission_type IN ('SCHEDULE 13D', 'SCHEDULE 13D/A') AND status_flag = 'active')
            OR (submission_type IN ('SCHEDULE 13G', 'SCHEDULE 13G/A') AND status_flag = 'passive')
        ),

    source                  TEXT NOT NULL
        CHECK (source IN ('form4', 'form3', '13d', '13g', 'def14a', '13f', 'nport', 'ncsr', 'xbrl_dei', '10k_note', 'finra_si', 'derived')),
    source_document_id      TEXT NOT NULL,
    source_accession        TEXT,
    source_url              TEXT,
    filed_at                TIMESTAMPTZ NOT NULL,
    period_start            DATE,
    period_end              DATE NOT NULL,
    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    aggregate_amount_owned  NUMERIC(24, 4),
    percent_of_class        NUMERIC(8, 4),

    PRIMARY KEY (instrument_id, reporter_cik, ownership_nature)
);

COMMENT ON TABLE ownership_blockholders_current IS
    'Materialised latest-amendment-per-(instrument, reporter, nature) 13D/G snapshot. Rebuilt deterministically by refresh_blockholders_current().';

CREATE INDEX IF NOT EXISTS idx_block_current_reporter
    ON ownership_blockholders_current (reporter_cik);

COMMIT;
