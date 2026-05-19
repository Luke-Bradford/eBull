-- 156: financial_facts_raw partition + retention plumbing (#1208 Sub 3)
--
-- Converts the 28 GB / 62 M-row `financial_facts_raw` heap to a
-- `PARTITION BY RANGE (period_end)` shape with 86 leaf partitions:
--   - financial_facts_raw_pre2010          ['1900-01-01', '2010-01-01')
--   - financial_facts_raw_{2010..2030}q{1..4}  (84 quarterly)
--   - financial_facts_raw_default          DEFAULT (junk + 2031+)
--
-- See `docs/superpowers/specs/2026-05-19-phase3-financial-facts-raw-partition.md`
-- for the full design + Codex 1a/1b/1c iteration history. The single largest
-- gotcha: BIGSERIAL on the old `fact_id` column made the sequence OWNED BY
-- the column → DROP TABLE cascades to the sequence → the new table's
-- DEFAULT nextval(...) breaks. Step 6 detaches OWNED BY NONE before the
-- DROP; step 9 reattaches OWNED BY the renamed new table.
--
-- Migration is tx-wrapped by the runner — no explicit BEGIN/COMMIT here.
-- See sql/155 for the (different) autocommit-directive shape used only
-- for ALTER SYSTEM.

-- ── 1. Create the partitioned skeleton (no indexes yet) ──────────

CREATE TABLE financial_facts_raw_new (
    fact_id          BIGINT NOT NULL DEFAULT nextval('financial_facts_raw_fact_id_seq'),
    instrument_id    BIGINT NOT NULL REFERENCES instruments(instrument_id),
    taxonomy         TEXT NOT NULL DEFAULT 'us-gaap',
    concept          TEXT NOT NULL,
    unit             TEXT NOT NULL,
    period_start     DATE,
    period_end       DATE NOT NULL,
    val              NUMERIC(30,6) NOT NULL,
    frame            TEXT,
    accession_number TEXT NOT NULL,
    form_type        TEXT NOT NULL,
    filed_date       DATE NOT NULL,
    fiscal_year      INTEGER,
    fiscal_period    TEXT,
    decimals         TEXT,
    ingestion_run_id BIGINT REFERENCES data_ingestion_runs(ingestion_run_id),
    fetched_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (fact_id, period_end)
) PARTITION BY RANGE (period_end);

-- pre-2010 catch (1900-01-01 lower bound — pre-1900 junk → DEFAULT)
CREATE TABLE financial_facts_raw_pre2010
    PARTITION OF financial_facts_raw_new
    FOR VALUES FROM ('1900-01-01') TO ('2010-01-01');

-- Quarterly partitions 2010-Q1 → 2030-Q4 (84 leaves) via a DO block
-- so the migration stays compact + a future "extend to 2035" patch is
-- a one-line bump rather than a 16-statement block.
DO $$
DECLARE
    y           INTEGER;
    q           INTEGER;
    start_date  TEXT;
    end_date    TEXT;
    part_name   TEXT;
BEGIN
    FOR y IN 2010..2030 LOOP
        FOR q IN 1..4 LOOP
            start_date := format('%s-%s-01', y, lpad(((q - 1) * 3 + 1)::text, 2, '0'));
            IF q = 4 THEN
                end_date := format('%s-01-01', y + 1);
            ELSE
                end_date := format('%s-%s-01', y, lpad((q * 3 + 1)::text, 2, '0'));
            END IF;
            part_name := format('financial_facts_raw_%sq%s', y, q);
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF financial_facts_raw_new '
                'FOR VALUES FROM (%L) TO (%L)',
                part_name, start_date, end_date
            );
        END LOOP;
    END LOOP;
END $$;

-- DEFAULT partition — catches pre-1900 parser junk + far-future
-- (e.g. period_end=6016-06-30 from XBRL date-overflow bugs) + any
-- 2031+ filing until quarterly partitions are extended.
CREATE TABLE financial_facts_raw_default
    PARTITION OF financial_facts_raw_new DEFAULT;

-- ── 2a. Block concurrent writes to the old table ─────────────────
--
-- SHARE MODE permits concurrent SELECT but blocks every
-- INSERT/UPDATE/DELETE so late writes cannot slip in between the
-- INSERT-SELECT snapshot and the DROP TABLE in step 7. Released at
-- the runner's tx commit/rollback.

LOCK TABLE financial_facts_raw IN SHARE MODE;

-- ── 2b. Bulk copy ──────────────────────────────────────────────

INSERT INTO financial_facts_raw_new
SELECT * FROM financial_facts_raw;

-- ── 3. Build indexes on the partitioned parent ───────────────────
--
-- Indexes cascade to every leaf partition. Names use `_new` suffix
-- to avoid global-namespace collision with the old table's indexes
-- which still exist; canonical names assigned in step 8.

CREATE UNIQUE INDEX uq_facts_raw_identity_new ON financial_facts_raw_new (
    instrument_id, concept, unit,
    COALESCE(period_start, '0001-01-01'::date),
    period_end, accession_number
);

CREATE INDEX idx_facts_raw_instrument_concept_new
    ON financial_facts_raw_new (instrument_id, concept, period_end DESC);

-- ── 4. Retention-sweep supporting indexes ────────────────────────
--
-- Two indexes:
--   - retention_ranking: covers the ranking CTE's (instrument_id,
--     form_type IN ('10-K','10-K/A','10-Q','10-Q/A')) filter.
--   - retention_evict: covers the DELETE join's
--     (instrument_id, accession_number) lookup. The wider ranking
--     index alone leaves form_type as a "gap" so the DELETE has no
--     usable prefix beyond instrument_id.

CREATE INDEX idx_facts_raw_retention_ranking_new
    ON financial_facts_raw_new
    (instrument_id, form_type, accession_number, filed_date);

CREATE INDEX idx_facts_raw_retention_evict_new
    ON financial_facts_raw_new
    (instrument_id, accession_number);

-- ── 5. Drop dependent views (reverse dependency order) ───────────

DROP VIEW IF EXISTS instrument_dilution_summary;
DROP VIEW IF EXISTS instrument_share_count_latest;
DROP VIEW IF EXISTS share_count_history;

-- ── 6. Detach sequence ownership BEFORE dropping the old table ───
--
-- BIGSERIAL on the old `fact_id` made the sequence OWNED BY the
-- column → DROP TABLE would cascade to the sequence and break the
-- new table's DEFAULT nextval(...). Detach first.

ALTER SEQUENCE financial_facts_raw_fact_id_seq OWNED BY NONE;

-- ── 7. Drop old table + its indexes ──────────────────────────────

DROP TABLE financial_facts_raw;

-- ── 8. Rename new table + indexes to canonical names ─────────────
--
-- PG does NOT auto-rename the PK constraint's implicit index on
-- ALTER TABLE RENAME — must rename explicitly.

ALTER TABLE financial_facts_raw_new RENAME TO financial_facts_raw;

ALTER INDEX financial_facts_raw_new_pkey         RENAME TO financial_facts_raw_pkey;
ALTER INDEX uq_facts_raw_identity_new            RENAME TO uq_facts_raw_identity;
ALTER INDEX idx_facts_raw_instrument_concept_new RENAME TO idx_facts_raw_instrument_concept;
ALTER INDEX idx_facts_raw_retention_ranking_new  RENAME TO idx_facts_raw_retention_ranking;
ALTER INDEX idx_facts_raw_retention_evict_new    RENAME TO idx_facts_raw_retention_evict;

-- ── 9. Re-attach sequence ownership + setval to current max ──────
--
-- BIGSERIAL on the old table advanced the sequence on every insert,
-- so post-migration `nextval()` already returns a unique value for
-- normal serial inserts. BUT — the INSERT-SELECT copied explicit
-- `fact_id` values, including any row that was inserted with an
-- explicit `fact_id` greater than the sequence's last_value (manual
-- backfill, restore-from-dump, etc.). Without `setval()`, the next
-- regular insert could collide with an existing high `fact_id`.
-- `setval(..., GREATEST(max(fact_id), 1))` is the canonical safe-and-
-- idempotent guard.

ALTER SEQUENCE financial_facts_raw_fact_id_seq
    OWNED BY financial_facts_raw.fact_id;

SELECT setval(
    'financial_facts_raw_fact_id_seq',
    GREATEST((SELECT COALESCE(max(fact_id), 0) FROM financial_facts_raw), 1)
);

-- ── 10. Re-create dependent views VERBATIM from sql/052 ──────────
--
-- View bodies are pasted here so the migration is self-contained:
-- even if sql/052 is later edited, this migration applies the
-- original behaviour. Drift caught by
-- tests/test_share_count_history_views_post_swap.py.

CREATE OR REPLACE VIEW share_count_history AS
WITH latest_fact AS (
    SELECT DISTINCT ON (f.instrument_id, f.concept, f.period_end, f.period_start)
           f.instrument_id,
           f.concept,
           f.period_end,
           f.period_start,
           f.val,
           f.form_type,
           f.filed_date,
           f.fiscal_year,
           f.fiscal_period
    FROM financial_facts_raw f
    WHERE f.concept IN (
        'StockIssuedDuringPeriodSharesNewIssues',
        'StockRepurchasedDuringPeriodShares',
        'TreasuryStockSharesAcquired',
        'CommonStockSharesOutstanding',
        'EntityCommonStockSharesOutstanding'
    )
    ORDER BY f.instrument_id, f.concept, f.period_end, f.period_start,
             f.filed_date DESC, f.accession_number DESC
)
SELECT
    instrument_id,
    period_end,
    MAX(fiscal_year)    AS fiscal_year,
    MAX(fiscal_period)  AS fiscal_period,
    MAX(val) FILTER (WHERE concept = 'EntityCommonStockSharesOutstanding') AS shares_outstanding_dei,
    MAX(val) FILTER (WHERE concept = 'CommonStockSharesOutstanding')       AS shares_outstanding_gaap,
    COALESCE(
        MAX(val) FILTER (WHERE concept = 'EntityCommonStockSharesOutstanding'),
        MAX(val) FILTER (WHERE concept = 'CommonStockSharesOutstanding')
    ) AS shares_outstanding,
    MAX(val) FILTER (WHERE concept = 'StockIssuedDuringPeriodSharesNewIssues') AS shares_issued_new,
    COALESCE(
        MAX(val) FILTER (WHERE concept = 'StockRepurchasedDuringPeriodShares'),
        MAX(val) FILTER (WHERE concept = 'TreasuryStockSharesAcquired')
    ) AS buyback_shares,
    MAX(form_type)  AS latest_form_type,
    MAX(filed_date) AS latest_filed_date
FROM latest_fact
GROUP BY instrument_id, period_end;

COMMENT ON VIEW share_count_history IS
    'Per-period share-count snapshot + issuance/buyback deltas from '
    'SEC XBRL. DEI section preferred for the point-in-time count. '
    'Populated by the daily fundamentals_sync path; no new HTTP.';

CREATE OR REPLACE VIEW instrument_dilution_summary AS
WITH outstanding_only AS (
    SELECT instrument_id,
           period_end,
           shares_outstanding,
           ROW_NUMBER() OVER (
               PARTITION BY instrument_id
               ORDER BY period_end DESC
           ) AS rn
    FROM share_count_history
    WHERE shares_outstanding IS NOT NULL
),
flow_only AS (
    SELECT instrument_id,
           period_end,
           shares_issued_new,
           buyback_shares,
           ROW_NUMBER() OVER (
               PARTITION BY instrument_id
               ORDER BY period_end DESC
           ) AS rn
    FROM share_count_history
    WHERE shares_issued_new IS NOT NULL
       OR buyback_shares    IS NOT NULL
),
current_state AS (
    SELECT instrument_id, shares_outstanding AS latest_shares,
           period_end AS latest_as_of
    FROM outstanding_only
    WHERE rn = 1
),
year_ago AS (
    SELECT DISTINCT ON (instrument_id) instrument_id,
           shares_outstanding AS yoy_shares
    FROM outstanding_only
    WHERE rn BETWEEN 4 AND 6
    ORDER BY instrument_id, rn ASC
),
trailing_flow AS (
    SELECT instrument_id,
           SUM(shares_issued_new) FILTER (WHERE rn <= 4) AS ttm_shares_issued,
           SUM(buyback_shares)    FILTER (WHERE rn <= 4) AS ttm_buyback_shares
    FROM flow_only
    GROUP BY instrument_id
)
SELECT
    c.instrument_id,
    c.latest_shares,
    c.latest_as_of,
    y.yoy_shares,
    CASE
        WHEN y.yoy_shares IS NOT NULL
         AND y.yoy_shares > 0
        THEN ((c.latest_shares - y.yoy_shares) / y.yoy_shares) * 100
        ELSE NULL
    END AS net_dilution_pct_yoy,
    t.ttm_shares_issued,
    t.ttm_buyback_shares,
    COALESCE(t.ttm_shares_issued, 0) - COALESCE(t.ttm_buyback_shares, 0)
        AS ttm_net_share_change,
    CASE
        WHEN y.yoy_shares IS NOT NULL AND y.yoy_shares > 0
             AND (c.latest_shares - y.yoy_shares) / y.yoy_shares > 0.02
        THEN 'dilutive'
        WHEN y.yoy_shares IS NOT NULL AND y.yoy_shares > 0
             AND (c.latest_shares - y.yoy_shares) / y.yoy_shares < -0.02
        THEN 'buyback_heavy'
        ELSE 'stable'
    END AS dilution_posture
FROM current_state c
LEFT JOIN year_ago y      ON y.instrument_id = c.instrument_id
LEFT JOIN trailing_flow t ON t.instrument_id = c.instrument_id;

COMMENT ON VIEW instrument_dilution_summary IS
    'One row per instrument with trailing-year dilution signal. Drives '
    'the ranking-engine quality sub-score and the operator-page '
    'dilution badge. Positive net_dilution_pct_yoy = dilutive; '
    'negative = buyback-heavy.';

CREATE OR REPLACE VIEW instrument_share_count_latest AS
SELECT DISTINCT ON (instrument_id)
    instrument_id,
    shares_outstanding AS latest_shares,
    period_end         AS as_of_date,
    CASE
        WHEN shares_outstanding_dei IS NOT NULL THEN 'dei'
        WHEN shares_outstanding_gaap IS NOT NULL THEN 'us-gaap'
        ELSE 'none'
    END AS source_taxonomy
FROM share_count_history
WHERE shares_outstanding IS NOT NULL
ORDER BY instrument_id, period_end DESC;

COMMENT ON VIEW instrument_share_count_latest IS
    'Newest point-in-time share count per instrument. Drives live '
    'market-cap derivation (shares x close) — retires a yfinance '
    'call site under #432.';

-- ── 11. ANALYZE post-swap so the planner has fresh stats ─────────
--
-- The 62M-row swap leaves every partition with empty pg_statistic.
-- Autoanalyze will catch up eventually, but until it does the planner
-- runs on zero-stat partitions and can pick poor plans (e.g. seq-scan
-- a 380 MB leaf because it thinks it's empty). One explicit ANALYZE
-- on the parent cascades to every leaf and bounds the cost.

ANALYZE financial_facts_raw;
