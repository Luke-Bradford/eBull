-- 323_strategy_forecast_brier_skill_range.sql
--
-- #2557 follow-up to review migration 321: Brier skill is bounded above by
-- one but unbounded below as the empirical baseline score approaches zero.
-- The service still quantizes values to eight decimal places; an unbounded
-- NUMERIC integer range prevents a large imbalanced cohort from overflowing.

ALTER TABLE strategy_forecast_assessments
    ALTER COLUMN brier_skill_score TYPE NUMERIC;
