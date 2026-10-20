-- 508_financial_periods_ttm_dividend_window_guard.sql
--
-- #3903: dividends_paid_ttm_observed publishes only from a window that cannot hold a payment twice. Spec:
-- docs/specs/fundamentals/2026-10-19-3903-dividend-yield-total-form.md, Amendment 2.
--
-- sql/507's timed acceptance failed on KO: its window (2025-03-29 to 2026-04-03, 371 days) summed $10,971M of cash
-- dividends paid against FY2025's $8,779M, about one payment more than four, and published 2.73% against an external
-- 2.25%. The source form (JKP div12m_me) sums 12 months; a fiscal window longer than a calendar year is not that.
--
-- Three conditions join sql/507's, in this order:
--   1. every member's period_start_date is non-NULL and not after its period_end_date (NOT NULL in the schema);
--   2. no member starts on or before the previous member's period_end_date, so no stored interval is summed twice;
--   3. for a positive total, the last member's period_end_date < the first (rn = 4) member's period_start_date +
--      INTERVAL '1 year' (calendar arithmetic: an end on the anniversary is refused). Four valid, non-overlapping,
--      non-negative members summing to zero are zero over any span, so a zero total skips this test.
-- No lower bound and no gap rule: the start-date metadata is noisy, and its coverage rule belongs to the normaliser
-- (#3916). Every other column of the view, and sql/220's dedup, ranking and shape guard, are unchanged.
--
-- Columns, order and types are unchanged; instrument_valuation keeps sql/507's body. View replace only; no backfill.
-- The rollback is sql/rollback/508_financial_periods_ttm_dividend_window_guard.sql (sql/507's financial_periods_ttm
-- body), applied by a revert PR.

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
           ) AS rn,
           -- #3903 (sql/508): the previous stored quarter's end, for dividends_paid_ttm_observed's overlap test.
           LAG(period_end_date) OVER (
               PARTITION BY instrument_id
               ORDER BY period_end_date
           ) AS prev_end
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
    MAX(shares_diluted) FILTER (WHERE rn = 1) AS shares_diluted,

    -- #3903 (sql/507), appended last: the window's cash dividends paid only when all four members are observed and
    -- non-negative. dividends_paid_ttm above keeps its sporadic sum for its other readers.
    -- sql/508 (spec Amendment 2), in order: (1) every member's interval is valid; (2) no member starts on or before the
    -- previous member's end; (3) a positive total's window ends before one calendar year from the first member's
    -- start, so a fixed-date payment schedule cannot land in it twice. A zero total is zero over any span.
    CASE WHEN COUNT(*) = 4
              AND (MAX(period_end_date) - MIN(period_end_date)) <= 330
              AND COUNT(dividends_paid) = 4
              AND MIN(dividends_paid) >= 0
              AND COUNT(*) FILTER (WHERE period_start_date IS NOT NULL
                                     AND period_start_date <= period_end_date) = 4
              AND COUNT(*) FILTER (WHERE rn < 4 AND period_start_date <= prev_end) = 0
              AND (SUM(dividends_paid) = 0
                   OR MAX(period_end_date)
                      < MAX(period_start_date) FILTER (WHERE rn = 4) + INTERVAL '1 year')
         THEN SUM(dividends_paid) END AS dividends_paid_ttm_observed

FROM latest_4
GROUP BY instrument_id;

COMMENT ON VIEW financial_periods_ttm IS
    '#2008/#3903: latest four normalized quarters per instrument. Strict flow sums need all four members; sporadic '
    'ones (capex, dividends_paid, dps_declared, buyback_spend) sum present members. dividends_paid_ttm_observed '
    '(sql/507, sql/508) is the dividends_paid sum only when all four members are observed and non-negative, carry '
    'valid non-overlapping intervals, and, for a positive sum, end before one calendar year from the first start.';

COMMIT;
