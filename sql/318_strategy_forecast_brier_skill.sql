-- 318_strategy_forecast_brier_skill.sql
--
-- #2555 review: absolute probability error is not enough. Capital authority
-- also requires positive skill against the recent cohort's empirical class
-- frequencies (the strongest no-feature climatology available for that cohort).

ALTER TABLE strategy_forecast_assessment_policies
    ADD COLUMN IF NOT EXISTS min_brier_skill_score NUMERIC(12,8) NOT NULL
        CHECK (min_brier_skill_score BETWEEN 0 AND 1);

ALTER TABLE strategy_forecast_assessments
    ADD COLUMN IF NOT EXISTS baseline_normalized_brier_score NUMERIC(12,8)
        CHECK (baseline_normalized_brier_score BETWEEN 0 AND 1),
    ADD COLUMN IF NOT EXISTS brier_skill_score NUMERIC
        CHECK (brier_skill_score <= 1);

ALTER TABLE strategy_forecast_assessments
    ADD CONSTRAINT strategy_forecast_assessment_baseline_shape CHECK (
        (resolved_forecasts = 0) = (baseline_normalized_brier_score IS NULL)
        AND (brier_skill_score IS NULL) = (
            resolved_forecasts = 0 OR baseline_normalized_brier_score = 0
        )
    );

COMMENT ON COLUMN strategy_forecast_assessment_policies.min_brier_skill_score IS
    'Minimum 1 - model normalized Brier / empirical-class-frequency normalized Brier; non-negative by policy.';
COMMENT ON COLUMN strategy_forecast_assessments.baseline_normalized_brier_score IS
    'Normalized multiclass Brier of the same recent cohort under its empirical class-frequency forecast.';
COMMENT ON COLUMN strategy_forecast_assessments.brier_skill_score IS
    '1 - model score / baseline score. NULL when the recent cohort is single-class and its baseline is perfect.';
