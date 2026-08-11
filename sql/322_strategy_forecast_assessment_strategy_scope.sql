-- 322_strategy_forecast_assessment_strategy_scope.sql
--
-- #2557: a setup label is not a strategy identity. Bind prospective forecast
-- authority to the exact strategy and version that emitted the signal so two
-- strategies cannot accidentally share an assessment scope.

ALTER TABLE strategy_forecast_assessments
    ADD COLUMN strategy_id TEXT NOT NULL,
    ADD COLUMN strategy_version TEXT NOT NULL;

ALTER TABLE strategy_forecast_assessments
    DROP CONSTRAINT strategy_forecast_assessments_evidence_unique;
ALTER TABLE strategy_forecast_assessments
    ADD CONSTRAINT strategy_forecast_assessments_evidence_unique UNIQUE (
        policy_id,strategy_id,strategy_version,forecast_policy_version,model_version,
        calibration_id,setup_version,exit_policy_version,resolver_version,
        input_rule_set_version,evidence_hash
    );

DROP INDEX idx_strategy_forecast_assessments_scope;
CREATE INDEX idx_strategy_forecast_assessments_scope
    ON strategy_forecast_assessments (
        policy_id,strategy_id,strategy_version,forecast_policy_version,model_version,
        calibration_id,setup_version,exit_policy_version,resolver_version,
        input_rule_set_version,assessment_id DESC
    );

ALTER TABLE strategy_forecast_assessment_current
    ADD COLUMN strategy_id TEXT NOT NULL,
    ADD COLUMN strategy_version TEXT NOT NULL;
ALTER TABLE strategy_forecast_assessment_current
    DROP CONSTRAINT strategy_forecast_assessment_current_pkey;
ALTER TABLE strategy_forecast_assessment_current
    ADD PRIMARY KEY (
        policy_id,strategy_id,strategy_version,forecast_policy_version,model_version,
        calibration_id,setup_version,exit_policy_version,resolver_version,
        input_rule_set_version
    );

COMMENT ON COLUMN strategy_forecast_assessments.strategy_version IS
    'Exact immutable strategy identity that emitted every signal in this cohort.';
