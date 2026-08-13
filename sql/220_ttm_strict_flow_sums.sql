-- 220: financial_periods_ttm — strict flow sums + adjacency guard (#2008)
--
-- Spec: docs/specs/fundamentals/2026-07-12-2008-ttm-reconciliation.md.
--
-- SQL SUM skips NULL members, and is_complete_ttm counted ROWS, so a
-- latest-4 window with NULL flow members surfaced a 1-3 quarter sum as a
-- complete TTM (CAT: exactly one quarter; AMZN: exactly three — full-pop
-- 2026-07-12: 749/4,065 "complete" windows carried revenue_ttm NULL).
-- The window was also never checked for adjacency: a missing quarter
-- pulls a prior-year row in and produces a silently-wrong cross-year
-- "TTM" (#1839 class).
--
-- Two flow classes (spec §Source rules):
--   * strict (statement-core / recurring lines): value present in ALL 4
--     window rows or the TTM is NULL — absence is an extraction gap, not
--     a zero. COUNT(col) = 4 inside every CASE.
--   * sporadic (absence means "did not occur"): capex, dividends_paid,
--     dps_declared, buyback_spend — issuers emit these facts only in
--     quarters where they happen (special dividends, buyback quarters;
--     capex NULL→0 is the settled repo treatment: fcf_yield.py +
--     sql/201 fcf_ttm). Summed over present members; window-shape guard
--     still applies.
--
-- Adjacency guard = window span (newest end − oldest end) <= 330 days —
-- the settled bound from app/services/fcf_yield.py::_QUARTERLY_SQL
-- (consecutive windows span ~273-275d even on 53-week calendars; one
-- missing quarter pushes the span to ~364-365d; 330 separates cleanly).
-- Keep the two in sync.
--
-- Same column list/order/types as sql/032 → CREATE OR REPLACE, and the
-- dependent instrument_valuation view (sql/201) reads through unchanged.
-- View recreate only — no data change; the #2008 backfill re-derives
-- financial_periods rows separately.

BEGIN;

CREATE OR REPLACE VIEW financial_periods_ttm AS
WITH deduped_quarters AS (
    -- Collapse fiscal-year-rekey duplicates (two period_type rows sharing
    -- one period_end_date, #1914 class — 133 instruments on dev) to ONE
    -- row per (instrument_id, period_end_date) BEFORE ranking. Without
    -- this, ROW_NUMBER assigns both dup rows distinct rn, so a dup pair
    -- can land inside rn<=4 and double-count that quarter while
    -- under-spanning the true 4 distinct quarters — a WRONG (non-NULL)
    -- TTM instead of the strict NULL intended. Latest-filed wins (same
    -- tiebreak as the fundamentals_snapshot write-through).
    SELECT DISTINCT ON (instrument_id, period_end_date) *
    FROM financial_periods
    WHERE period_type IN ('Q1','Q2','Q3','Q4')
      AND superseded_at IS NULL
      AND normalization_status = 'normalized'
    ORDER BY instrument_id, period_end_date, filed_date DESC NULLS LAST
),
ranked_quarters AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY instrument_id
               ORDER BY period_end_date DESC
           ) AS rn
    FROM deduped_quarters
),
latest_4 AS (
    SELECT * FROM ranked_quarters WHERE rn <= 4
)
SELECT
    instrument_id,
    COUNT(*) AS quarter_count,
    SUM(COALESCE(months_covered, 3)) AS ttm_months_covered,
    COUNT(*) = 4
        AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
        AS is_complete_ttm,
    MIN(period_end_date) AS ttm_start,
    MAX(period_end_date) AS ttm_end,

    -- Flow items, strict: 4 adjacent rows AND the column present in all 4.
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(revenue) = 4
         THEN SUM(revenue) END AS revenue_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(cost_of_revenue) = 4
         THEN SUM(cost_of_revenue) END AS cost_of_revenue_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(gross_profit) = 4
         THEN SUM(gross_profit) END AS gross_profit_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(operating_income) = 4
         THEN SUM(operating_income) END AS operating_income_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(net_income) = 4
         THEN SUM(net_income) END AS net_income_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(research_and_dev) = 4
         THEN SUM(research_and_dev) END AS research_and_dev_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(sga_expense) = 4
         THEN SUM(sga_expense) END AS sga_expense_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(depreciation_amort) = 4
         THEN SUM(depreciation_amort) END AS depreciation_amort_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(interest_expense) = 4
         THEN SUM(interest_expense) END AS interest_expense_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(income_tax) = 4
         THEN SUM(income_tax) END AS income_tax_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(sbc_expense) = 4
         THEN SUM(sbc_expense) END AS sbc_expense_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(operating_cf) = 4
         THEN SUM(operating_cf) END AS operating_cf_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(investing_cf) = 4
         THEN SUM(investing_cf) END AS investing_cf_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(financing_cf) = 4
         THEN SUM(financing_cf) END AS financing_cf_ttm,

    -- Sporadic flow items: absence means "did not occur" (spec §Source
    -- rules) — sum over present members, window-shape guard only.
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
         THEN SUM(capex) END AS capex_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
         THEN SUM(dividends_paid) END AS dividends_paid_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
         THEN SUM(dps_declared) END AS dps_declared_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
         THEN SUM(buyback_spend) END AS buyback_spend_ttm,

    -- Stock items: latest quarter only (rn=1) — unchanged.
    MAX(total_assets) FILTER (WHERE rn = 1) AS total_assets,
    MAX(total_liabilities) FILTER (WHERE rn = 1) AS total_liabilities,
    MAX(shareholders_equity) FILTER (WHERE rn = 1) AS shareholders_equity,
    MAX(cash) FILTER (WHERE rn = 1) AS cash,
    MAX(long_term_debt) FILTER (WHERE rn = 1) AS long_term_debt,
    MAX(short_term_debt) FILTER (WHERE rn = 1) AS short_term_debt,
    MAX(shares_outstanding) FILTER (WHERE rn = 1) AS shares_outstanding,
    MAX(inventory) FILTER (WHERE rn = 1) AS inventory,
    MAX(receivables) FILTER (WHERE rn = 1) AS receivables,
    MAX(payables) FILTER (WHERE rn = 1) AS payables,
    MAX(goodwill) FILTER (WHERE rn = 1) AS goodwill,
    MAX(ppe_net) FILTER (WHERE rn = 1) AS ppe_net,

    -- Derived — unchanged shapes.
    MAX(reported_currency) FILTER (WHERE rn = 1) AS reported_currency,
    MAX(eps_basic) FILTER (WHERE rn = 1) AS eps_basic_latest,
    MAX(eps_diluted) FILTER (WHERE rn = 1) AS eps_diluted_latest,
    -- TTM EPS: flow sums, strict like the other statement-core lines.
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(eps_basic) = 4
         THEN SUM(eps_basic) END AS eps_basic_ttm,
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(eps_diluted) = 4
         THEN SUM(eps_diluted) END AS eps_diluted_ttm,
    MAX(shares_basic) FILTER (WHERE rn = 1) AS shares_basic,
    MAX(shares_diluted) FILTER (WHERE rn = 1) AS shares_diluted

FROM latest_4
GROUP BY instrument_id;

COMMIT;
