-- #2411 — `latest_filed_date` is not the filed date of the share COUNT, and a
-- freshness gate cannot be built on it.
--
-- `share_count_history` groups five concepts per (instrument_id, period_end):
-- the two point-in-time counts and three issuance/buyback flows. Its
-- `latest_filed_date` is `MAX(filed_date)` over ALL of them, so when any other
-- concept in the same period carries a later filing — a restatement, a
-- subsequent form re-tagging a flow — the reported date belongs to that fact
-- and not to the count in `shares_outstanding`.
--
-- MAX can only move the date FORWARD, so the error is always in the fail-open
-- direction: the count looks fresher than it is. Measured on the dev corpus
-- (2026-08-08, newest positive row per instrument, 4,665 instruments): 16 rows
-- where `latest_filed_date` is newer than the count's own filed date, 0 where
-- it is older, overstatement up to 1,456 days (`HUIZ`: count filed 2020-04-24,
-- view reports 2024-04-19). Reproduce with the CTE in
-- `scripts/ab_2411_share_count_denominator.py`'s docstring, or:
--
--   SELECT count(*) FROM share_count_history
--    WHERE shares_outstanding > 0 AND latest_filed_date <> shares_outstanding_filed_date;
--
-- Seven of those 16 sit either side of the 183-day `share_count_filed` bound in
-- `thesis_break.FRESHNESS_BOUNDS`, i.e. seven stale denominators that the bound
-- would have read as fresh. That bound has one live consumer today
-- (`thesis_break_scan._short_interest_observations`) and gains a second in
-- #2411 (`instrument_analytics._short_interest_from_row`); both are corrected
-- to read the column added here.
--
-- The new column mirrors the value's own COALESCE exactly — same concepts, same
-- `val > 0` filter, same DEI-before-us-gaap preference — so it is the filed date
-- of whichever arm actually produced `shares_outstanding`, and the two cannot
-- drift because they are computed side by side from one GROUP BY.
--
-- ⚠ `latest_filed_date` is KEPT, unchanged. It has other readers and it is the
-- right answer to a different question ("when did anything in this period last
-- get filed"). This migration adds the count-specific date rather than
-- redefining the general one.
--
-- `CREATE OR REPLACE VIEW` adds `shares_outstanding_filed_date` as a TRAILING
-- column; no existing column is removed, renamed or retyped, which is what
-- PostgreSQL requires for a replace with dependent views
-- (`instrument_dilution_summary`, `instrument_share_count_latest`). Both select
-- by name and neither reads the new column.

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
    MAX(filed_date) AS latest_filed_date,
    -- #2411: the filed date OF THE COUNT. Same COALESCE order as
    -- `shares_outstanding` above, so it names the arm that won.
    COALESCE(
        MAX(filed_date) FILTER (
            WHERE concept = 'EntityCommonStockSharesOutstanding' AND val > 0
        ),
        MAX(filed_date) FILTER (
            WHERE concept = 'CommonStockSharesOutstanding' AND val > 0
        )
    ) AS shares_outstanding_filed_date
FROM latest_fact
GROUP BY instrument_id, period_end;

COMMENT ON VIEW share_count_history IS
    'Per-period share-count snapshot + issuance/buyback deltas from '
    'SEC XBRL. DEI section preferred for the point-in-time count. '
    'Populated by the daily fundamentals_sync path; no new HTTP. '
    'The point-in-time count columns select POSITIVE values only (#2232) — '
    'a filer''s zero-valued undimensioned tag is not a share count. The flow '
    'columns keep every value, zero included. '
    '``latest_filed_date`` is MAX over ALL five concepts; '
    '``shares_outstanding_filed_date`` (#2411) is the filed date of the count '
    'in ``shares_outstanding`` and is the one a freshness bound must use.';
