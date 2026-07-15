-- 229_fair_value_band_reason_earnings_nonrep.sql
--
-- #2043 (fvb_v5) — admit 'earnings_nonrepresentative' into the fair_value_band
-- reason vocabulary (fvb_obs_reason_chk / fvb_cur_reason_chk, sql/221:64/89).
-- Statused absence for a band whose ONLY synthesizable leg was the pe leg and
-- that leg's earnings denominator failed the representativeness gate (spec
-- docs/proposals/valuation/2026-07-15-fvb-v5-earnings-representativeness-gate.md §4.4).
--
-- New-value-only widening: every existing row satisfies the new CHECK, so
-- ADD CONSTRAINT validates instantly. sql/221 itself is not edited
-- (applied migrations are immutable — migration-content-drift rule).

BEGIN;

ALTER TABLE fair_value_band_observations
    DROP CONSTRAINT IF EXISTS fvb_obs_reason_chk;

ALTER TABLE fair_value_band_observations
    ADD CONSTRAINT fvb_obs_reason_chk CHECK (reason IN
        ('ok', 'no_multiple', 'currency_mismatch', 'stale_price',
         'multiclass_unavailable', 'thin_cohort', 'earnings_nonrepresentative'));

ALTER TABLE fair_value_band_current
    DROP CONSTRAINT IF EXISTS fvb_cur_reason_chk;

ALTER TABLE fair_value_band_current
    ADD CONSTRAINT fvb_cur_reason_chk CHECK (reason IN
        ('ok', 'no_multiple', 'currency_mismatch', 'stale_price',
         'multiclass_unavailable', 'thin_cohort', 'earnings_nonrepresentative'));

COMMIT;
