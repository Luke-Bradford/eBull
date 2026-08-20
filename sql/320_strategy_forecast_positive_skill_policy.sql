-- 320_strategy_forecast_positive_skill_policy.sql
--
-- #2555 final challenge: equality with a no-feature base-rate forecast is not
-- evidence of useful forecasting skill. Require every registered policy to
-- demand a strictly positive improvement.

ALTER TABLE strategy_forecast_assessment_policies
    DROP CONSTRAINT strategy_forecast_assessment_polici_min_brier_skill_score_check;
ALTER TABLE strategy_forecast_assessment_policies
    ADD CONSTRAINT strategy_forecast_assessment_policy_positive_skill CHECK (
        min_brier_skill_score > 0 AND min_brier_skill_score <= 1
    );
