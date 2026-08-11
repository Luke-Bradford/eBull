-- 319_strategy_forecast_assessment_calibration_scope.sql
--
-- #2555 review: model_version alone cannot prove that two immutable
-- calibration records are interchangeable. The feature has no assessment rows
-- yet, so add the exact calibration identity without inventing a backfill.

ALTER TABLE strategy_forecast_assessments
    ADD COLUMN IF NOT EXISTS calibration_id TEXT NOT NULL
        REFERENCES strategy_forecast_calibrations(calibration_id) ON DELETE RESTRICT;

ALTER TABLE strategy_forecast_assessments
    DROP CONSTRAINT strategy_forecast_assessments_policy_id_forecast_policy_ver_key;
ALTER TABLE strategy_forecast_assessments
    ADD CONSTRAINT strategy_forecast_assessments_evidence_unique UNIQUE (
        policy_id,forecast_policy_version,model_version,calibration_id,setup_version,
        exit_policy_version,resolver_version,input_rule_set_version,evidence_hash
    );

DROP INDEX IF EXISTS idx_strategy_forecast_assessments_scope;
CREATE INDEX idx_strategy_forecast_assessments_scope
    ON strategy_forecast_assessments (
        policy_id,forecast_policy_version,model_version,calibration_id,setup_version,
        exit_policy_version,resolver_version,input_rule_set_version,assessment_id DESC
    );

ALTER TABLE strategy_forecast_assessment_current
    ADD COLUMN IF NOT EXISTS calibration_id TEXT NOT NULL
        REFERENCES strategy_forecast_calibrations(calibration_id) ON DELETE RESTRICT;
ALTER TABLE strategy_forecast_assessment_current
    DROP CONSTRAINT strategy_forecast_assessment_current_pkey;
ALTER TABLE strategy_forecast_assessment_current
    ADD PRIMARY KEY (
        policy_id,forecast_policy_version,model_version,calibration_id,setup_version,
        exit_policy_version,resolver_version,input_rule_set_version
    );

COMMENT ON COLUMN strategy_forecast_assessments.calibration_id IS
    'Exact immutable historical calibration evidence used by every forecast in this prospective cohort.';
