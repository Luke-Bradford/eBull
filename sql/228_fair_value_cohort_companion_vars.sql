-- 228_fair_value_cohort_companion_vars.sql
--
-- #2032 (#2009 v2 Phase 2, fvb_v4) — companion-variable columns on
-- fair_value_cohort_members for the peer comparability screen
-- (spec: docs/proposals/valuation/2026-07-15-fvb-v4-companion-screen.md §5).
--
-- Written by _MATERIALIZE_SQL (fair_value_band.py) per §4.5: net margin =
-- net_income_ttm / revenue_ttm; revenue growth YoY = strict current TTM vs
-- strict prior TTM (rn 5-8, span <= 330d, windows adjacent 1-120d); ROE =
-- net_income_ttm / latest-quarter shareholders_equity. Each NULLed at
-- materialize time when |value| >= _MAX_SANE_MULTIPLE (degenerate-ratio
-- guard). Read back by _MEMBER_SQL for the pass-2 screen predicate.
--
-- Additive + nullable: pre-v4 as_of_date rows stay NULL and are never
-- re-read (each materialize DELETE+INSERTs its own as_of_date). sql/221 and
-- sql/226 are not edited (applied migrations are immutable —
-- migration-content-drift rule).

BEGIN;

ALTER TABLE fair_value_cohort_members
    ADD COLUMN IF NOT EXISTS net_margin     numeric(18,6),
    ADD COLUMN IF NOT EXISTS rev_growth_yoy numeric(18,6),
    ADD COLUMN IF NOT EXISTS roe            numeric(18,6);

COMMIT;
