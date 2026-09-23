-- #3344 — null EPS in canonical rows that no re-normalize reaches, where the row's
-- own earnings and share count refute it.
--
-- Source rule: ASC 260-10-45-10 / 45-16 — EPS = income available to common ÷
-- weighted-average shares. `_eps_facts_contradicting_identity` now drops an EPS fact
-- when |EPS × shares| ≥ 10^5 × |NetIncomeLoss| within one filing and the cover page
-- corroborates the share count. That runs only on facts still in
-- financial_facts_raw. `_canonical_merge_instrument` deliberately preserves canonical
-- rows whose facts the retention sweep has evicted, so their bad EPS (HAL 2021–2023
-- quarters at 260,000…800,000 for $0.26…$0.80) survives every re-normalize. This is
-- the same rule applied once to those rows.
--
-- A cell is nulled only when ALL of these hold, and a missing input keeps it:
--   1. source 'sec_edgar' and no financial_periods_raw row exists for the same
--      (instrument, period_type, period_end_date). A row that still has a raw source
--      is governed by the fact-level filter on its next normalize, not by this.
--   2. net_income <> 0 and |eps × weighted shares| ≥ 10^5 × |net_income| (the same
--      bound as `_EPS_IDENTITY_BOUND`).
--   3. The weighted share count is within 10× of EVERY shares_outstanding the
--      instrument holds at the nearest period_end within 400 days. A share count
--      scaled by 10^3 or 10^6 is the more common way the identity fails, and EPS is
--      correct then (LNC, NTRS, IOR, LARK). This clause keeps those rows. The row's own shares_outstanding is
--      often NULL on quarters, hence the nearest one.
--
-- Measured on dev 2026-09-23 after re-normalizing every instrument with a failing
-- canonical row: 10 rows (HAL 7, CHDN 2, SR 1 diluted-only). Reproduce: run the
-- UPDATEs in a transaction with RETURNING, then ROLLBACK.

WITH candidates AS (
    SELECT
        p.instrument_id, p.period_type, p.period_end_date, p.net_income,
        p.eps_basic, p.shares_basic, p.eps_diluted, p.shares_diluted,
        near.lo AS near_lo,
        near.hi AS near_hi
    FROM financial_periods p
    -- Every shares_outstanding at the nearest distance; ties (FY + Q4, a date on
    -- each side) must ALL corroborate, so an ambiguous nearest value keeps EPS.
    CROSS JOIN LATERAL (
        SELECT min(q.shares_outstanding) AS lo, max(q.shares_outstanding) AS hi
        FROM financial_periods q
        WHERE q.instrument_id = p.instrument_id
          AND q.shares_outstanding > 0
          AND abs(q.period_end_date - p.period_end_date) = (
              SELECT min(abs(q2.period_end_date - p.period_end_date))
              FROM financial_periods q2
              WHERE q2.instrument_id = p.instrument_id
                AND q2.shares_outstanding > 0
                AND abs(q2.period_end_date - p.period_end_date) <= 400
          )
    ) near
    WHERE p.source = 'sec_edgar'
      AND p.net_income <> 0
      AND NOT EXISTS (
          SELECT 1 FROM financial_periods_raw r
          WHERE r.instrument_id = p.instrument_id
            AND r.source = 'sec_edgar'
            AND r.period_type = p.period_type
            AND r.period_end_date = p.period_end_date
      )
)
UPDATE financial_periods fp
SET
    eps_basic = CASE
        WHEN abs(c.eps_basic * c.shares_basic) >= 1e5 * abs(c.net_income)
         AND c.shares_basic > c.near_hi / 10
         AND c.shares_basic < c.near_lo * 10
        THEN NULL ELSE fp.eps_basic END,
    eps_diluted = CASE
        WHEN abs(c.eps_diluted * c.shares_diluted) >= 1e5 * abs(c.net_income)
         AND c.shares_diluted > c.near_hi / 10
         AND c.shares_diluted < c.near_lo * 10
        THEN NULL ELSE fp.eps_diluted END
FROM candidates c
WHERE fp.instrument_id = c.instrument_id
  AND fp.period_type = c.period_type
  AND fp.period_end_date = c.period_end_date
  AND fp.source = 'sec_edgar'
  AND (
      (abs(c.eps_basic * c.shares_basic) >= 1e5 * abs(c.net_income)
       AND c.shares_basic > c.near_hi / 10
       AND c.shares_basic < c.near_lo * 10)
   OR (abs(c.eps_diluted * c.shares_diluted) >= 1e5 * abs(c.net_income)
       AND c.shares_diluted > c.near_hi / 10
       AND c.shares_diluted < c.near_lo * 10)
  );
