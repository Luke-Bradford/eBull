-- 052_share_count_history_views.sql
--
-- Dilution tracker (#435). User ask 2026-04-24:
-- "how much per stock was issued over time" + "anything to bolster the
-- rankings with".
--
-- Three VIEWs on top of ``financial_facts_raw`` (already populated by
-- #430's expanded TRACKED_CONCEPTS):
--
--   1. ``share_count_history``     — per-period share count + deltas
--                                    (issued, repurchased) per instrument.
--                                    Drives the instrument-page dilution
--                                    chart.
--   2. ``instrument_dilution_summary`` — one row per instrument with
--                                    trailing-year net-dilution %, latest
--                                    share count, and buyback-heavy /
--                                    dilutive flag. Drives ranking engine
--                                    quality sub-score (#432 follow-up) +
--                                    instruments-list filter.
--   3. ``instrument_share_count_latest`` — one row per instrument with
--                                    the newest point-in-time share
--                                    count (dei > us-gaap > NULL). Used
--                                    everywhere market-cap needs a fresh
--                                    count (retires part of yfinance
--                                    get_profile — #432).
--
-- Dedupe strategy: XBRL amendments re-publish the same fact under a
-- new accession_number, so ``financial_facts_raw`` carries multiple
-- rows per (instrument, concept, period). ``DISTINCT ON`` newest
-- ``filed_date`` + ``accession_number`` picks the latest restated
-- figure.

-- ---------------------------------------------------------------------------
-- 1. share_count_history — per-quarter deltas
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW share_count_history AS
WITH latest_fact AS (
    -- For each (instrument, concept, period), keep only the newest
    -- filing. Amendments (10-K/A) re-state prior periods — we want the
    -- restated number, not the original.
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
-- Group strictly on (instrument_id, period_end) so a 10-K/A re-tag
-- of the same period_end under a different fiscal_year/fiscal_period
-- cannot produce duplicate rows. We still expose one representative
-- fiscal_year + fiscal_period via MAX() for display convenience;
-- downstream consumers treat them as hints, not keys.
SELECT
    instrument_id,
    period_end,
    MAX(fiscal_year)    AS fiscal_year,
    MAX(fiscal_period)  AS fiscal_period,
    -- Share-count snapshots — prefer DEI (entity cover page) over
    -- us-gaap when both are present, because DEI is filed by the
    -- issuer specifically as the point-in-time authoritative count.
    MAX(val) FILTER (WHERE concept = 'EntityCommonStockSharesOutstanding') AS shares_outstanding_dei,
    MAX(val) FILTER (WHERE concept = 'CommonStockSharesOutstanding')         AS shares_outstanding_gaap,
    COALESCE(
        MAX(val) FILTER (WHERE concept = 'EntityCommonStockSharesOutstanding'),
        MAX(val) FILTER (WHERE concept = 'CommonStockSharesOutstanding')
    ) AS shares_outstanding,
    -- Flow columns: period-over-period deltas. Either tag for
    -- buybacks — issuers split across them depending on how the
    -- treasury-stock accounting method was elected.
    MAX(val) FILTER (WHERE concept = 'StockIssuedDuringPeriodSharesNewIssues')     AS shares_issued_new,
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


-- ---------------------------------------------------------------------------
-- 2. instrument_dilution_summary
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW instrument_dilution_summary AS
WITH outstanding_only AS (
    -- Rank by period_end across rows that report a point-in-time
    -- count. This is what drives the "latest" + "year-ago" slots;
    -- flow TTM below uses a SEPARATE ranking so flow-only periods
    -- (filer publishes issuance without matching outstanding
    -- snapshot) still contribute to ttm totals.
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
    -- Independent ranking for flow TTM. No shares_outstanding filter
    -- so a period with only issuance / buyback facts still counts
    -- toward ttm_shares_issued / ttm_buyback_shares.
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
-- Year-ago share count (rn=5 across quarterly series, or the oldest
-- row within the last 6 periods if fewer).
year_ago AS (
    SELECT DISTINCT ON (instrument_id) instrument_id,
           shares_outstanding AS yoy_shares
    FROM outstanding_only
    WHERE rn BETWEEN 4 AND 6
    ORDER BY instrument_id, rn ASC
),
-- Trailing-4-quarter deltas from the flow series.
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
    -- Net change (positive = dilutive, negative = buyback-heavy).
    COALESCE(t.ttm_shares_issued, 0) - COALESCE(t.ttm_buyback_shares, 0)
        AS ttm_net_share_change,
    -- Flag used by the ranking-engine quality sub-score follow-up.
    -- >2% YoY dilution = penalty; buyback-heavy = reward. Threshold
    -- mirrors portfolio.py's guard patterns (simple, deterministic).
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
LEFT JOIN year_ago y ON y.instrument_id = c.instrument_id
LEFT JOIN trailing_flow t ON t.instrument_id = c.instrument_id;

COMMENT ON VIEW instrument_dilution_summary IS
    'One row per instrument with trailing-year dilution signal. Drives '
    'the ranking-engine quality sub-score and the operator-page '
    'dilution badge. Positive net_dilution_pct_yoy = dilutive; '
    'negative = buyback-heavy.';


-- ---------------------------------------------------------------------------
-- 3. instrument_share_count_latest — cheap market-cap input
-- ---------------------------------------------------------------------------

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
