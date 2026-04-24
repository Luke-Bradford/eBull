-- 050_dividend_history_views.sql
--
-- Dividend surfacing on top of ``financial_periods`` (user ask 2026-04-24:
-- "especially as a way we could filter on to dividend-providing businesses,
-- understanding what the yield is, when it pays out, how much per stock was
-- issued over time").
--
-- No new underlying tables — every field below is derived from data already
-- captured under the existing ``dps_declared`` + ``dividends_paid`` TRACKED
-- CONCEPTS in ``financial_periods``. Adds two VIEWs:
--
--   1. ``dividend_history``            — long, one row per dividend-paying
--                                        period, ordered newest-first. Drives
--                                        the instrument-page dividend chart.
--   2. ``instrument_dividend_summary`` — wide, one row per dividend-paying
--                                        instrument, with TTM yield + streak
--                                        + latest-DPS shortcuts for the
--                                        instruments-list ``has_dividend``
--                                        filter.
--
-- Source-of-truth: ``financial_periods.dps_declared`` (per-share cash
-- declared, from us-gaap:CommonStockDividendsPerShareDeclared) and
-- ``financial_periods.dividends_paid`` (aggregate cash outflow, from
-- us-gaap:PaymentsOfDividends).
--
-- A row is considered "a dividend-paying period" when EITHER ``dps_declared``
-- is strictly positive OR ``dividends_paid`` is strictly positive. A zero
-- amount is explicitly non-paying — not just "no data" — so it must not
-- surface as ``latest_dps`` or leak into the chart-driving history.
--
-- Live-row guard: every read filters ``superseded_at IS NULL`` so a
-- restated or withdrawn period cannot leak into the has_dividend filter or
-- the chart. Mirrors the canonical projection in sql/032 (line 218).
--
-- FY-vs-quarterly tie-break: an issuer sometimes reports the same
-- period_end_date under BOTH ``period_type='FY'`` AND ``period_type='Q4'``.
-- History surfaces quarterly rows only (FY excluded) to avoid double-counting
-- four quarters as an extra row. The summary's ``latest_*`` resolution adds
-- a deterministic ordering (``period_type DESC`` puts 'Q?' > 'FY') so the
-- quarter-level figure wins when both exist for the same end date.

-- ---------------------------------------------------------------------------
-- 1. dividend_history
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW dividend_history AS
SELECT
    fp.instrument_id,
    fp.period_end_date,
    fp.period_type,
    fp.fiscal_year,
    fp.fiscal_quarter,
    fp.period_start_date,
    fp.months_covered,
    fp.dps_declared,
    fp.dividends_paid,
    fp.reported_currency
FROM financial_periods fp
WHERE fp.superseded_at IS NULL
  AND fp.period_type IN ('Q1', 'Q2', 'Q3', 'Q4')
  AND (
        (fp.dps_declared IS NOT NULL AND fp.dps_declared > 0)
     OR (fp.dividends_paid IS NOT NULL AND fp.dividends_paid > 0)
  );

COMMENT ON VIEW dividend_history IS
    'Per-quarter dividend record (one row per instrument per FISCAL QUARTER '
    'where dps_declared > 0 or dividends_paid > 0). FY rows excluded to '
    'prevent Q1+Q2+Q3+Q4 double-counting. Superseded rows filtered.';


-- ---------------------------------------------------------------------------
-- 2. instrument_dividend_summary
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW instrument_dividend_summary AS
WITH recent_quarters AS (
    -- Live rows only, quarterly granularity only. The streak walk below
    -- needs every quarter (not only paying ones) so it can detect a
    -- break, so this CTE does NOT pre-filter by amount.
    SELECT fp.instrument_id,
           fp.period_end_date,
           fp.period_type,
           fp.dps_declared,
           fp.dividends_paid,
           ROW_NUMBER() OVER (
               PARTITION BY fp.instrument_id
               ORDER BY fp.period_end_date DESC
           ) AS rn
    FROM financial_periods fp
    WHERE fp.superseded_at IS NULL
      AND fp.period_type IN ('Q1', 'Q2', 'Q3', 'Q4')
),
ttm AS (
    SELECT instrument_id,
           SUM(dps_declared)     FILTER (WHERE rn <= 4) AS ttm_dps,
           SUM(dividends_paid)   FILTER (WHERE rn <= 4) AS ttm_dividends_paid,
           COUNT(*)              FILTER (WHERE rn <= 4
                                         AND dps_declared IS NOT NULL
                                         AND dps_declared > 0) AS ttm_dividend_quarters
    FROM recent_quarters
    GROUP BY instrument_id
),
-- ``latest`` reports the newest DIVIDEND-PAYING quarter. Zero / NULL
-- rows are deliberately excluded so ``latest_dps`` never surfaces a
-- "we skipped the dividend this quarter" amount as though it were the
-- current payout. Tie-break on period_type DESC so Q4 wins over FY
-- when both exist for the same end date.
latest AS (
    SELECT DISTINCT ON (fp.instrument_id)
           fp.instrument_id,
           fp.dps_declared      AS latest_dps,
           fp.period_end_date   AS latest_dividend_at,
           fp.reported_currency AS dividend_currency
    FROM financial_periods fp
    WHERE fp.superseded_at IS NULL
      AND (
            (fp.dps_declared IS NOT NULL AND fp.dps_declared > 0)
         OR (fp.dividends_paid IS NOT NULL AND fp.dividends_paid > 0)
      )
    ORDER BY fp.instrument_id,
             fp.period_end_date DESC,
             fp.period_type     DESC  -- Q4 > FY, Q3 > Q2 > Q1 lexically
),
-- Streak: walk newest-back across quarterly rows and count consecutive
-- non-zero dps_declared periods until the first zero / NULL. When no
-- break exists (``first_break_rn`` is NULL), every row in the window
-- counts — otherwise it would collapse to zero and misreport an
-- uninterrupted dividend payer as a non-payer.
streaks AS (
    SELECT instrument_id,
           COUNT(*) FILTER (
               WHERE first_break_rn IS NULL OR rn < first_break_rn
           ) AS dividend_streak_q
    FROM (
        SELECT instrument_id,
               rn,
               MIN(CASE
                       WHEN dps_declared IS NULL OR dps_declared = 0
                       THEN rn
                   END) OVER (PARTITION BY instrument_id) AS first_break_rn
        FROM recent_quarters
    ) s
    GROUP BY instrument_id
),
-- Live price gate on yield. Quote row can be missing (non-tradable, demoted)
-- — LEFT JOIN to surface the dividend facts even then.
priced AS (
    SELECT instrument_id,
           COALESCE(
               NULLIF(GREATEST(last, 0), 0),
               CASE WHEN bid > 0 AND ask > 0 THEN (bid + ask) / 2 END
           ) AS price
    FROM quotes
)
SELECT
    l.instrument_id,
    ttm.ttm_dps,
    ttm.ttm_dividends_paid,
    ttm.ttm_dividend_quarters,
    l.latest_dps,
    l.latest_dividend_at,
    l.dividend_currency,
    s.dividend_streak_q,
    -- TTM yield = ttm_dps / price, percent. NULL if either input missing.
    CASE WHEN p.price IS NOT NULL
              AND p.price > 0
              AND ttm.ttm_dps IS NOT NULL
              AND ttm.ttm_dps > 0
         THEN (ttm.ttm_dps / p.price) * 100
         ELSE NULL
    END AS ttm_yield_pct,
    (
        (ttm.ttm_dps IS NOT NULL AND ttm.ttm_dps > 0)
        OR (ttm.ttm_dividends_paid IS NOT NULL AND ttm.ttm_dividends_paid > 0)
    ) AS has_dividend
FROM latest l
LEFT JOIN ttm ON ttm.instrument_id = l.instrument_id
LEFT JOIN streaks s ON s.instrument_id = l.instrument_id
LEFT JOIN priced p ON p.instrument_id = l.instrument_id;

COMMENT ON VIEW instrument_dividend_summary IS
    'One row per instrument that has ever reported a positive dividend (live '
    'rows only, superseded filtered). Drives the instruments-list has_dividend '
    'filter + per-instrument dividend summary card. Recomputed on demand.';
