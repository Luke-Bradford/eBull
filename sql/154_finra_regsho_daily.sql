-- 154_finra_regsho_daily.sql
--
-- Issue #916 (Phase 6 PR 12) — FINRA RegSHO daily short volume schema.
-- Spec: docs/superpowers/specs/2026-05-18-finra-regsho-daily.md.
--
-- Single partitioned table ``finra_regsho_daily_observations``:
--
--   PARTITION BY RANGE (trade_date) — quarterly buckets covering
--   2024-Q1 → 2030-Q1 inclusive (25 partitions). Loop bound
--   ``q_start < '2030-04-01'`` includes 2030-Q1 as its last iteration.
--
-- No ``_current`` snapshot — the daily file IS the per-day snapshot.
-- Per-instrument latest-trade-date queries land on the partitioned
-- observations table directly via the (instrument_id, trade_date DESC)
-- index.
--
-- PK ``(instrument_id, trade_date, market, source_document_id)`` lets
-- the CNMS aggregate (``market='B,Q,N'``) coexist with per-facility
-- rows (``market='B'`` etc.) for the same (instrument, trade_date) —
-- both are distinct facts. CNMS aggregates across all reporting
-- facilities; the per-facility rows attribute volume to specific
-- facilities (FNQC / FNSQ / FNYX / FORF / FNRA).
--
-- Volume columns are NUMERIC(18, 6) — empirically verified in spike
-- §3.3 that FINRA reports per-symbol weighted aggregates to 6 decimal
-- places (e.g. AAPL ShortVolume 8714049.111124). 12-digit integer
-- runway ~10,000× the current daily peak.

BEGIN;

CREATE TABLE finra_regsho_daily_observations (
    instrument_id           BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    trade_date              DATE   NOT NULL,
        -- Trade date from the file body (matches the URL date). NOT a
        -- settlement date — RegSHO daily is per-trade-date.
    market                  TEXT   NOT NULL,
        -- Single-facility files: 'B' | 'Q' | 'N' | 'O' (FINRA's
        -- single-char facility codes).
        -- CNMS aggregate: comma-joined union (e.g. 'B,Q,N'). PK
        -- distinguishes CNMS aggregate from per-facility rows.
    source_document_id      TEXT   NOT NULL,
        -- '{PREFIX}_{YYYYMMDD}' — e.g. 'CNMS_20260515',
        -- 'FNQC_20260515'. Encodes the prefix so the audit trail
        -- distinguishes the source files. Same source_document_id
        -- across original + revision payloads — re-ingest UPSERTs the
        -- (PK) row. Latest fetch wins.

    short_volume            NUMERIC(18, 6) NOT NULL,
    short_exempt_volume     NUMERIC(18, 6) NOT NULL,
    total_volume            NUMERIC(18, 6) NOT NULL,
        -- All three reported by FINRA to 6 decimal places.

    source                  TEXT NOT NULL CHECK (source = 'finra_regsho'),
        -- Single-element CHECK locks the table to FINRA RegSHO daily.
        -- 'finra_regsho' is the short-form column value (mirrors the
        -- bimonthly's 'finra_si' / 'finra_short_interest' short/long
        -- split). Manifest source enum uses the long form
        -- 'finra_regsho_daily'.
    source_url              TEXT NOT NULL,
        -- 'https://cdn.finra.org/equity/regsho/daily/{PREFIX}shvol{YYYYMMDD}.txt'.
    filed_at                TIMESTAMPTZ NOT NULL,
        -- Trade-date midnight UTC. Publication anchor — FINRA
        -- publishes EOD ~6 PM ET.
    period_end              DATE NOT NULL,
        -- Same as trade_date (the fact's valid-time end).
    known_from              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ingest_run_id           UUID NOT NULL,

    PRIMARY KEY (instrument_id, trade_date, market, source_document_id)
) PARTITION BY RANGE (trade_date);

-- Quarterly partitions 2024-Q1 → 2030-Q1 inclusive (25 partitions).
-- Loop bound `q_start < '2030-04-01'` creates the 2030-Q1 partition as
-- its last iteration.
DO $$
DECLARE
    q_start DATE := '2024-01-01';
    q_end   DATE;
    q_name  TEXT;
BEGIN
    WHILE q_start < '2030-04-01' LOOP
        q_end := q_start + INTERVAL '3 months';
        q_name := format(
            'finra_regsho_daily_observations_p_%sq%s',
            EXTRACT(YEAR FROM q_start)::INT,
            EXTRACT(QUARTER FROM q_start)::INT
        );
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF finra_regsho_daily_observations '
            'FOR VALUES FROM (%L) TO (%L)',
            q_name, q_start, q_end
        );
        q_start := q_end;
    END LOOP;
END$$;

-- Operator chart queries: "GME daily short volume over the last 30 days."
CREATE INDEX idx_finra_regsho_obs_instrument_trade
    ON finra_regsho_daily_observations (instrument_id, trade_date DESC);

-- Source/audit queries: "every row from CNMS_20260515".
CREATE INDEX idx_finra_regsho_obs_source_doc
    ON finra_regsho_daily_observations (source_document_id);

COMMIT;
