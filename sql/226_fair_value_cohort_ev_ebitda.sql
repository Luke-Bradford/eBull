-- 226_fair_value_cohort_ev_ebitda.sql
--
-- #2021 (#2009 v2) — admit 'ev_ebitda' into the fair_value_cohort_members
-- multiple vocabulary (fvcm_multiple_chk, sql/221:38 read 'pe','ps','pb').
--
-- The EV/EBITDA leg is peer-only (fundamentals_snapshot has no historical
-- EBITDA/debt/cash — sql/201 legacy CTE NULLs ev_ebitda), so pass-1
-- materializes its members alongside pe/ps/pb; member formula mirrors
-- sql/201:128-135 (EV = close*shares + COALESCE(debt,0) - cash over
-- OpInc + D&A), strict D&A + cash-present + debt/interest coherence gates
-- per docs/proposals/valuation/2026-07-15-fair-value-band-ev-ebitda.md §3.1.
--
-- New-value-only widening: every existing row satisfies the new CHECK, so
-- ADD CONSTRAINT validates instantly. sql/221 itself is not edited
-- (applied migrations are immutable — migration-content-drift rule).

BEGIN;

ALTER TABLE fair_value_cohort_members
    DROP CONSTRAINT IF EXISTS fvcm_multiple_chk;

ALTER TABLE fair_value_cohort_members
    ADD CONSTRAINT fvcm_multiple_chk
    CHECK (multiple IN ('pe', 'ps', 'pb', 'ev_ebitda'));

COMMIT;
