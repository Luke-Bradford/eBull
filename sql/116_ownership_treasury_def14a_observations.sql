-- 116_ownership_treasury_def14a_observations.sql
--
-- Issue #840 P1.D — treasury (XBRL DEI/us-gaap) + DEF 14A bene-table
-- observations + _current. Final per-category sub-PR for Phase 1.
--
-- Treasury natural keys per spec:
--   - observations: (instrument_id, period_end, source_document_id)
--   - _current:     (instrument_id)
-- Treasury is issuer-level — no holder dimension. ``ownership_nature``
-- pinned to ``'economic'`` (issuer-held shares are an economic
-- position; not Rule-13d-3 beneficial). Source = 'xbrl_dei'.
--
-- DEF 14A natural keys:
--   - observations: (instrument_id, holder_name, period_end, source_document_id)
--   - _current:     (instrument_id, holder_name)
-- DEF 14A's bene table holders are typically named officers /
-- directors / 5%+ owners; not all of them have a CIK on the proxy
-- itself. Identity is the canonical lower(trim(holder_name)) so
-- DEF 14A doesn't depend on the holder-name resolver running first.
-- Match-to-CIK happens at rollup-read time (#840.E).

BEGIN;

-- ---------------------------------------------------------------------
-- Treasury observations
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ownership_treasury_observations (
    instrument_id           INTEGER NOT NULL,
    ownership_nature        TEXT NOT NULL DEFAULT 'economic'
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

    treasury_shares         NUMERIC(24, 4),

    PRIMARY KEY (instrument_id, period_end, source_document_id)
) PARTITION BY RANGE (period_end);

COMMENT ON TABLE ownership_treasury_observations IS
    'Immutable per-period treasury-stock fact log from XBRL DEI / us-gaap. Source for ownership_treasury_current.';

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
            pname := FORMAT('ownership_treasury_observations_%sq%s', yr, qtr);
            EXECUTE FORMAT(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF ownership_treasury_observations FOR VALUES FROM (%L) TO (%L)',
                pname, qstart, qend
            );
        END LOOP;
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS ownership_treasury_observations_default
    PARTITION OF ownership_treasury_observations DEFAULT;

CREATE INDEX IF NOT EXISTS idx_treasury_obs_instrument_period
    ON ownership_treasury_observations (instrument_id, period_end DESC);

CREATE TABLE IF NOT EXISTS ownership_treasury_current (
    instrument_id           INTEGER NOT NULL,
    ownership_nature        TEXT NOT NULL DEFAULT 'economic'
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

    treasury_shares         NUMERIC(24, 4),

    PRIMARY KEY (instrument_id)
);

COMMENT ON TABLE ownership_treasury_current IS
    'Materialised latest-treasury snapshot per instrument. Rebuilt by refresh_treasury_current().';


-- ---------------------------------------------------------------------
-- DEF 14A observations
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ownership_def14a_observations (
    instrument_id           INTEGER NOT NULL,
    holder_name             TEXT NOT NULL,
    holder_name_key         TEXT NOT NULL GENERATED ALWAYS AS (lower(trim(holder_name))) STORED,
    holder_role             TEXT,
    ownership_nature        TEXT NOT NULL DEFAULT 'beneficial'
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
    percent_of_class        NUMERIC(8, 4),

    PRIMARY KEY (instrument_id, holder_name_key, period_end, source_document_id)
) PARTITION BY RANGE (period_end);

COMMENT ON TABLE ownership_def14a_observations IS
    'Immutable per-proxy bene-table fact log. Holder identity is normalized name (no CIK on proxy). CIK match happens at rollup-read time.';

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
            pname := FORMAT('ownership_def14a_observations_%sq%s', yr, qtr);
            EXECUTE FORMAT(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF ownership_def14a_observations FOR VALUES FROM (%L) TO (%L)',
                pname, qstart, qend
            );
        END LOOP;
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS ownership_def14a_observations_default
    PARTITION OF ownership_def14a_observations DEFAULT;

CREATE INDEX IF NOT EXISTS idx_def14a_obs_instrument_period
    ON ownership_def14a_observations (instrument_id, period_end DESC);

CREATE INDEX IF NOT EXISTS idx_def14a_obs_holder_period
    ON ownership_def14a_observations (holder_name_key, period_end DESC);


CREATE TABLE IF NOT EXISTS ownership_def14a_current (
    instrument_id           INTEGER NOT NULL,
    holder_name             TEXT NOT NULL,
    holder_name_key         TEXT NOT NULL,
    holder_role             TEXT,
    ownership_nature        TEXT NOT NULL DEFAULT 'beneficial'
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
    percent_of_class        NUMERIC(8, 4),

    -- Codex review for #840.D: include ``ownership_nature`` in the
    -- PK so a holder reporting both beneficial + voting splits on
    -- the same proxy retains both rows. Without it, the second
    -- INSERT collapses the first via UPSERT.
    PRIMARY KEY (instrument_id, holder_name_key, ownership_nature)
);

COMMENT ON TABLE ownership_def14a_current IS
    'Materialised latest-proxy-per-holder DEF 14A bene snapshot. Rebuilt by refresh_def14a_current().';

CREATE INDEX IF NOT EXISTS idx_def14a_current_holder_key
    ON ownership_def14a_current (holder_name_key);

COMMIT;
