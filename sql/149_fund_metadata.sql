-- 149_fund_metadata.sql
--
-- Issue #1171 — N-CSR / N-CSRS fund-level + class-level metadata
-- extraction. Replaces the #918 / PR #1170 synth no-op with a real
-- parser. Spec: docs/superpowers/specs/2026-05-14-n-csr-fund-metadata.md.
-- Plan: docs/superpowers/specs/2026-05-14-n-csr-fund-metadata-plan.md.
--
-- Three new tables:
--
--   1. ``fund_metadata_observations`` — append-only event log per
--      (instrument_id, source_accession). Partitioned by RANGE(period_end)
--      quarterly 2010-2030 + default. Mirrors the two-layer ownership
--      model (sql/113 ownership_insiders_observations).
--   2. ``fund_metadata_current`` — write-through materialised snapshot,
--      one row per instrument_id. Mirrors the Tier 1 + Tier 2 observation
--      column set; omits raw_facts (Tier 3) and provenance (known_*).
--   3. ``cik_refresh_mf_directory`` — companion table for the bundled
--      company_tickers_mf.json ingest. Populated by daily_cik_refresh
--      (Stage 6 extension, see T4 in the plan). Consumed by
--      _fund_class_resolver.classify_resolver_miss to discriminate
--      pending_cik_refresh vs ext_id_not_yet_written vs
--      instrument_not_in_universe.
--
-- Append-only invariant (data-engineer I6): the parser NEVER UPDATEs an
-- observation row. Parser-version rewashes mark prior rows with
-- ``known_to = NOW()`` (soft-delete supersession) and INSERT fresh rows.
-- The partial unique index ``uq_fund_metadata_observations_current``
-- enforces "at most one currently-valid row per (instrument_id,
-- source_accession, period_end)" while permitting superseded history.
--
-- ``period_end`` is included in the partial unique index ONLY because
-- PostgreSQL requires the partition key in every UNIQUE constraint
-- across a partitioned table. Functionally, ``period_end`` is determined
-- by ``source_accession`` (one DocumentPeriodEndDate per filing) so the
-- index effectively enforces uniqueness on
-- (instrument_id, source_accession) alone.
--
-- Source-priority chain for ``refresh_fund_metadata_current`` writes
-- (settled-decisions entry "Source priority for fund metadata"):
--   ORDER BY period_end DESC, filed_at DESC, source_accession DESC LIMIT 1
-- See spec §2 + §8.

BEGIN;

-- ---------------------------------------------------------------------
-- fund_metadata_observations — append-only fact log
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fund_metadata_observations (
    observation_id          BIGSERIAL,
    instrument_id           BIGINT NOT NULL REFERENCES instruments(instrument_id),
    source_accession        TEXT NOT NULL,
    filed_at                TIMESTAMPTZ NOT NULL,
    period_end              DATE NOT NULL,
    document_type           TEXT NOT NULL
        CHECK (document_type IN ('N-CSR', 'N-CSR/A', 'N-CSRS', 'N-CSRS/A')),
    amendment_flag          BOOLEAN NOT NULL DEFAULT FALSE,
    parser_version          TEXT NOT NULL,

    -- Series + class identity (per spec §5 Tier 1).
    trust_cik               TEXT NOT NULL,
    trust_name              TEXT,
    entity_inv_company_type TEXT,
    series_id               TEXT,
    series_name             TEXT,
    class_id                TEXT NOT NULL,
    class_name              TEXT,
    trading_symbol          TEXT,
    exchange                TEXT,
    inception_date          DATE,
    shareholder_report_type TEXT,

    -- Per-class economics.
    expense_ratio_pct       NUMERIC(12, 8),
    expenses_paid_amt       NUMERIC,
    net_assets_amt          NUMERIC,
    advisory_fees_paid_amt  NUMERIC,
    portfolio_turnover_pct  NUMERIC(12, 6),
    holdings_count          INTEGER,

    -- Tier 2 dimensional JSONB.
    returns_pct             JSONB,
    benchmark_returns_pct   JSONB,
    sector_allocation       JSONB,
    region_allocation       JSONB,
    credit_quality_allocation JSONB,
    growth_curve            JSONB,

    -- Material change.
    material_chng_date      DATE,
    material_chng_notice    TEXT,

    -- Contact / diligence.
    contact_phone           TEXT,
    contact_website         TEXT,
    contact_email           TEXT,
    prospectus_phone        TEXT,
    prospectus_website      TEXT,
    prospectus_email        TEXT,

    -- Tier 3 fallback.
    raw_facts               JSONB,

    -- Provenance (uniform with ownership_*_observations).
    known_from              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    known_to                TIMESTAMPTZ,
    ingest_run_id           UUID,
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    PRIMARY KEY (observation_id, period_end)
) PARTITION BY RANGE (period_end);

COMMENT ON TABLE fund_metadata_observations IS
    'Immutable per-filing-fact log for N-CSR / N-CSRS fund + class metadata. Append-only with known_to supersession; rebuild source for fund_metadata_current. Spec: docs/superpowers/specs/2026-05-14-n-csr-fund-metadata.md.';

-- Quarterly partitions 2010-2030 + default (mirrors sql/113 pattern).
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
            pname := FORMAT('fund_metadata_observations_%sq%s', yr, qtr);
            EXECUTE FORMAT(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF fund_metadata_observations FOR VALUES FROM (%L) TO (%L)',
                pname, qstart, qend
            );
        END LOOP;
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS fund_metadata_observations_default
    PARTITION OF fund_metadata_observations DEFAULT;

-- Partial unique index: at most one currently-valid row per
-- (instrument_id, source_accession, period_end). Soft-deleted rows
-- (known_to IS NOT NULL) are exempt so a parser-version rewash can
-- supersede the prior row without violating uniqueness.
--
-- Idempotency: use ``pg_index`` introspection (data-engineer §0 step 4)
-- — name-only IF NOT EXISTS misses a partial-apply where the index
-- exists but has the wrong column set or predicate. We assert:
--   - ``indisunique = TRUE``
--   - ``indkey`` matches the three expected attnums in order
--   - ``indpred`` is non-null (a partial index with the WHERE clause)
-- If any check fails we DROP + recreate so the schema converges to the
-- intended shape.
DO $$
DECLARE
    expected_attnums INT[];
    actual_attnums   INT2VECTOR;
    is_unique        BOOLEAN;
    has_predicate    BOOLEAN;
    idx_oid          OID;
BEGIN
    SELECT array_agg(a.attnum ORDER BY ord)
      INTO expected_attnums
      FROM unnest(ARRAY['instrument_id', 'source_accession', 'period_end']) WITH ORDINALITY t(name, ord)
      JOIN pg_attribute a
        ON a.attrelid = 'fund_metadata_observations'::regclass
       AND a.attname = t.name;

    SELECT i.indexrelid, i.indisunique, i.indkey, i.indpred IS NOT NULL
      INTO idx_oid, is_unique, actual_attnums, has_predicate
      FROM pg_index i
      JOIN pg_class c ON c.oid = i.indexrelid
      WHERE c.relname = 'uq_fund_metadata_observations_current';

    IF idx_oid IS NULL THEN
        CREATE UNIQUE INDEX uq_fund_metadata_observations_current
            ON fund_metadata_observations (instrument_id, source_accession, period_end)
            WHERE known_to IS NULL;
    ELSIF NOT is_unique
       OR actual_attnums::INT[] IS DISTINCT FROM expected_attnums
       OR NOT has_predicate THEN
        DROP INDEX uq_fund_metadata_observations_current;
        CREATE UNIQUE INDEX uq_fund_metadata_observations_current
            ON fund_metadata_observations (instrument_id, source_accession, period_end)
            WHERE known_to IS NULL;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_fund_metadata_observations_class_id
    ON fund_metadata_observations (class_id);
CREATE INDEX IF NOT EXISTS idx_fund_metadata_observations_period_end
    ON fund_metadata_observations (instrument_id, period_end DESC);
CREATE INDEX IF NOT EXISTS idx_fund_metadata_observations_filed_at
    ON fund_metadata_observations (filed_at DESC);

-- ---------------------------------------------------------------------
-- fund_metadata_current — materialised current-state snapshot
-- ---------------------------------------------------------------------
-- Mirrors the full Tier 1 + Tier 2 column set of the observation table.
-- Intentionally omits:
--   - raw_facts (Tier 3 audit-only data; never surfaced through reads).
--   - Provenance columns (known_from, known_to, ingest_run_id,
--     ingested_at) — _current is a projection of the currently-valid
--     observation; provenance lookup goes through the observation table.
-- Rebuilt by refresh_fund_metadata_current(instrument_id) under a
-- per-instrument pg_advisory_xact_lock (I7 invariant).

CREATE TABLE IF NOT EXISTS fund_metadata_current (
    instrument_id           BIGINT PRIMARY KEY REFERENCES instruments(instrument_id),
    source_accession        TEXT NOT NULL,
    filed_at                TIMESTAMPTZ NOT NULL,
    period_end              DATE NOT NULL,
    document_type           TEXT NOT NULL
        CHECK (document_type IN ('N-CSR', 'N-CSR/A', 'N-CSRS', 'N-CSRS/A')),
    amendment_flag          BOOLEAN NOT NULL DEFAULT FALSE,
    parser_version          TEXT NOT NULL,

    trust_cik               TEXT NOT NULL,
    trust_name              TEXT,
    entity_inv_company_type TEXT,
    series_id               TEXT,
    series_name             TEXT,
    class_id                TEXT NOT NULL,
    class_name              TEXT,
    trading_symbol          TEXT,
    exchange                TEXT,
    inception_date          DATE,
    shareholder_report_type TEXT,

    expense_ratio_pct       NUMERIC(12, 8),
    expenses_paid_amt       NUMERIC,
    net_assets_amt          NUMERIC,
    advisory_fees_paid_amt  NUMERIC,
    portfolio_turnover_pct  NUMERIC(12, 6),
    holdings_count          INTEGER,

    returns_pct             JSONB,
    benchmark_returns_pct   JSONB,
    sector_allocation       JSONB,
    region_allocation       JSONB,
    credit_quality_allocation JSONB,
    growth_curve            JSONB,

    material_chng_date      DATE,
    material_chng_notice    TEXT,

    contact_phone           TEXT,
    contact_website         TEXT,
    contact_email           TEXT,
    prospectus_phone        TEXT,
    prospectus_website      TEXT,
    prospectus_email        TEXT,

    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE fund_metadata_current IS
    'Materialised current-state snapshot of fund_metadata_observations under the source-priority chain (period_end DESC, filed_at DESC, source_accession DESC). Read by /instruments/{symbol}/fund-metadata.';

CREATE INDEX IF NOT EXISTS idx_fund_metadata_current_expense_ratio
    ON fund_metadata_current (expense_ratio_pct);
CREATE INDEX IF NOT EXISTS idx_fund_metadata_current_net_assets
    ON fund_metadata_current (net_assets_amt DESC);

-- ---------------------------------------------------------------------
-- cik_refresh_mf_directory — companion table for the bundled
-- company_tickers_mf.json ingest (T4 in the plan).
-- ---------------------------------------------------------------------
-- Populated by daily_cik_refresh (Stage 6 extension). Holds every
-- classId observed in company_tickers_mf.json regardless of whether the
-- corresponding instrument is in the eToro universe. Consumed by
-- _fund_class_resolver.classify_resolver_miss to discriminate:
--   - directory row absent → PENDING_CIK_REFRESH (transient retry).
--   - directory row present + symbol matches an instrument + no
--     external_identifiers row → EXT_ID_NOT_YET_WRITTEN (transient race).
--   - directory row present + symbol does not match any instrument →
--     INSTRUMENT_NOT_IN_UNIVERSE (deterministic tombstone).

CREATE TABLE IF NOT EXISTS cik_refresh_mf_directory (
    class_id        TEXT PRIMARY KEY,
    series_id       TEXT,
    symbol          TEXT,
    trust_cik       TEXT,
    last_seen       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE cik_refresh_mf_directory IS
    'Snapshot of company_tickers_mf.json keyed by classId. Populated by daily_cik_refresh (Stage 6 extension, #1171). Used by _fund_class_resolver.classify_resolver_miss.';

CREATE INDEX IF NOT EXISTS idx_cik_refresh_mf_directory_symbol
    ON cik_refresh_mf_directory (symbol);

COMMIT;
