-- 152_finra_short_interest.sql
--
-- Issue #915 (Phase 6 PR 11) — FINRA bimonthly short interest schema.
-- Spec: docs/superpowers/specs/2026-05-18-finra-bimonthly-short-interest.md.
--
-- Two tables:
--
--   * ``finra_short_interest_observations`` — partitioned by
--     settlement_date (range, quarterly). Append-only history per
--     #788 ownership-decomposition Phase 6 design.
--   * ``finra_short_interest_current`` — materialised snapshot,
--     one row per instrument. Refreshed by the FINRA bimonthly
--     ScheduledJob ingester (`app/services/finra_short_interest_ingest.py`).
--
-- Provenance shape mirrors the ownership-decomposition uniform block
-- (sql/113-116): ``source='finra_si'`` (single-element CHECK locks the
-- table), ``source_document_id`` = settlement_date as ``YYYYMMDD``,
-- ``source_url`` = canonical FINRA CDN URL.

BEGIN;

CREATE TABLE finra_short_interest_observations (
    instrument_id           BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    settlement_date         DATE   NOT NULL,
    source_document_id      TEXT   NOT NULL,
        -- For finra_si bimonthly: 'YYYYMMDD' (settlement date as
        -- compact string). Same source_document_id across original +
        -- revision (revisionFlag='Y') payloads — re-ingest UPSERTs the
        -- (PK) row. Latest fetch wins.

    current_short_interest      NUMERIC(20, 0) NOT NULL,
    previous_short_interest     NUMERIC(20, 0),
    average_daily_volume        NUMERIC(20, 0),
    days_to_cover               NUMERIC(10, 4),
    change_percent              NUMERIC(10, 4),
    change_previous             NUMERIC(20, 0),
    accounting_yearmonth        INTEGER,
        -- FINRA's accountingYearMonthNumber column. YYYYMM-shaped
        -- integer in publication; kept verbatim for audit.
    market_class_code           TEXT,
        -- FINRA marketClassCode: 'NYSE' | 'BZX' | 'OTC' | etc.
    exchange_code               TEXT,
        -- FINRA issuerServicesGroupExchangeCode: single letter
        -- 'A' | 'S' | 'H' | etc.
    issue_name                  TEXT,
        -- FINRA issueName free text.
    stock_split_flag            TEXT,
        -- '' or 'Y'.
    revision_flag               TEXT,
        -- '' or 'Y'. When 'Y', the row reflects a corrected FINRA
        -- snapshot; caller UPSERTs (overwrites) on PK collision.

    source                  TEXT NOT NULL CHECK (source = 'finra_si'),
        -- Single-element CHECK locks the table to FINRA short
        -- interest bimonthly. The ownership observations enum at
        -- sql/113-116 already includes 'finra_si' as a valid value.
    source_url              TEXT NOT NULL,
        -- 'https://cdn.finra.org/equity/otcmarket/biweekly/shrt{YYYYMMDD}.csv'.
    filed_at                TIMESTAMPTZ NOT NULL,
        -- Settlement date midnight UTC — the publication-time anchor.
    period_end              DATE NOT NULL,
        -- Same as settlement_date (the fact's valid-time end).
    known_from              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ingest_run_id           UUID NOT NULL,

    PRIMARY KEY (instrument_id, settlement_date, source_document_id)
) PARTITION BY RANGE (settlement_date);

-- Quarterly partitions covering the exchange-listed cohort (post-June
-- 2021) through 2027-Q1. Pre-June 2021 archive is OTC-only and out of
-- scope for v1 (spec §4.2). The DO loop creates 23 partitions inclusive
-- (2021-Q3, Q4, four per year for 2022-2026, plus 2027-Q1).

DO $$
DECLARE
    q_start DATE := '2021-07-01';
    q_end   DATE;
    q_name  TEXT;
BEGIN
    WHILE q_start < '2027-04-01' LOOP
        q_end := q_start + INTERVAL '3 months';
        q_name := format(
            'finra_short_interest_observations_p_%sq%s',
            EXTRACT(YEAR FROM q_start)::INT,
            EXTRACT(QUARTER FROM q_start)::INT
        );
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF finra_short_interest_observations '
            'FOR VALUES FROM (%L) TO (%L)',
            q_name, q_start, q_end
        );
        q_start := q_end;
    END LOOP;
END$$;

-- Operator chart queries by instrument over time.
CREATE INDEX idx_finra_si_obs_instrument_settlement
    ON finra_short_interest_observations (instrument_id, settlement_date DESC);

-- Source/audit queries.
CREATE INDEX idx_finra_si_obs_source_doc
    ON finra_short_interest_observations (source_document_id);

-- Materialised _current snapshot. One row per instrument; settled by
-- the service ingester on every UPSERT (settlement-date-wins with
-- compound predicate handling same-date revisions per spec §5.4).
CREATE TABLE finra_short_interest_current (
    instrument_id           BIGINT PRIMARY KEY REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    settlement_date         DATE   NOT NULL,
    source_document_id      TEXT   NOT NULL,

    current_short_interest      NUMERIC(20, 0) NOT NULL,
    previous_short_interest     NUMERIC(20, 0),
    average_daily_volume        NUMERIC(20, 0),
    days_to_cover               NUMERIC(10, 4),
    change_percent              NUMERIC(10, 4),
    change_previous             NUMERIC(20, 0),
    market_class_code           TEXT,
    exchange_code               TEXT,
    issue_name                  TEXT,

    source_url              TEXT NOT NULL,
    filed_at                TIMESTAMPTZ NOT NULL,
    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMIT;
