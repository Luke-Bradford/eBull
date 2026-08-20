-- #2232 — a zero share count is not a share count: select the latest POSITIVE
-- figure, not the latest non-NULL one.
--
-- `share_count_history` derives its point-in-time count as
--     COALESCE(MAX(val) FILTER (concept = dei), MAX(val) FILTER (concept = us-gaap))
-- and `instrument_share_count_latest` then picks the newest period with
-- `WHERE shares_outstanding IS NOT NULL`. Zero is not NULL, so a zero-valued
-- fact wins twice over: it beats an older positive row on `period_end DESC`,
-- and inside its own period it beats a positive row of the OTHER taxonomy
-- through the COALESCE.
--
-- The zeros are the filer's own tag, not our parse. Confirmed against SEC
-- EDGAR directly for Chime (CIK 0001795586) —
--   data.sec.gov/api/xbrl/companyconcept/CIK0001795586/us-gaap/CommonStockSharesOutstanding.json
-- returns val=0 at 2025-06-30 / 2025-09-30 / 2025-12-31 and 66,950,736 at
-- 2024-12-31, matching `financial_facts_raw` row for row. Post-IPO Chime tags
-- its Class A / Class B counts dimensionally, and companyfacts strips
-- dimensional facts (sec-edgar skill §7.17), so the undimensioned line we do
-- receive is the filer's zero.
--
-- Every consumer already treats a non-positive count as unusable —
-- `ownership_rollup.get_ownership_rollup` short-circuits on
-- `outstanding is None or outstanding <= 0`, and `instrument_dilution_summary`
-- guards its YoY ratio with `yoy_shares > 0`. The defect is in SELECTION: an
-- unusable row is allowed to win over a usable one, so the usable one is never
-- offered. This migration moves the same `> 0` requirement up to where the row
-- is chosen.
--
-- Scope note: the FLOW columns (`shares_issued_new`, `buyback_shares`) keep
-- their `IS NOT NULL` treatment. Zero is a meaningful value for a flow — no
-- issuance this period — and only the point-in-time STOCK is definitionally
-- positive for a registrant with a listed class of common stock.
--
-- Full-population A/B on the dev corpus (4,641 instruments carrying a
-- denominator): 25 instruments change, all of them the zero cohort — 17 go
-- zero → positive, 8 lose the fake zero and become genuinely absent, and
-- 0 instruments whose count was already positive move at all. Reproduce the
-- control side with:
--
--   SELECT count(*) FROM instrument_share_count_latest WHERE latest_shares = 0;
--   SELECT count(*), count(DISTINCT instrument_id)
--     FROM share_count_history WHERE shares_outstanding = 0;
--
-- Both return 0 once this migration is applied (25 / 231 + 156 before it).
--
-- The single enforcement point is `share_count_history`'s two point-in-time
-- FILTERs. Once they are positive-only the derived `shares_outstanding` can
-- only be NULL or positive, so the `> 0` predicates in the two dependent views
-- are RESTATEMENTS of an invariant their base already guarantees, not extra
-- gates — a revert-probe that flips either of them back to `IS NOT NULL`
-- changes no result and no test can catch it. They are kept because they state
-- each view's own requirement locally, and `> 0` is never weaker than the
-- `IS NOT NULL` it replaces. The invariant itself is pinned by
-- tests/test_share_count_history_views_post_swap.py::TestPositiveOnlyShareCount.
--
-- `CREATE OR REPLACE VIEW` throughout: no column is added, removed, renamed or
-- retyped, so the dependent views survive the replace of their base.

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
    MAX(val) FILTER (
        WHERE concept = 'EntityCommonStockSharesOutstanding' AND val > 0
    ) AS shares_outstanding_dei,
    MAX(val) FILTER (
        WHERE concept = 'CommonStockSharesOutstanding' AND val > 0
    ) AS shares_outstanding_gaap,
    COALESCE(
        MAX(val) FILTER (
            WHERE concept = 'EntityCommonStockSharesOutstanding' AND val > 0
        ),
        MAX(val) FILTER (
            WHERE concept = 'CommonStockSharesOutstanding' AND val > 0
        )
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
    'Populated by the daily fundamentals_sync path; no new HTTP. '
    'The point-in-time count columns select POSITIVE values only (#2232) — '
    'a filer''s zero-valued undimensioned tag is not a share count. The flow '
    'columns keep every value, zero included.';

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
    WHERE shares_outstanding > 0
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
    'negative = buyback-heavy. Periods whose share count is not positive are '
    'excluded from both the latest and the year-ago slot (#2232) — a zero '
    'there rendered as a -100% buyback.';

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
WHERE shares_outstanding > 0
ORDER BY instrument_id, period_end DESC;

COMMENT ON VIEW instrument_share_count_latest IS
    'Newest point-in-time share count per instrument. Drives live '
    'market-cap derivation (shares x close) — retires a yfinance '
    'call site under #432. Newest POSITIVE count (#2232): a zero-valued '
    'filer tag no longer masks an older usable figure, and the existing '
    'staleness guard decides whether what remains is fresh enough to divide by.';
