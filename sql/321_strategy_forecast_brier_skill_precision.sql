-- 321_strategy_forecast_brier_skill_precision.sql
--
-- #2555 review: keep the proper-score skill metric at the same deterministic
-- storage precision as the Brier and calibration metrics it is derived from.

ALTER TABLE strategy_forecast_assessments
    ALTER COLUMN brier_skill_score TYPE NUMERIC(12,8);
