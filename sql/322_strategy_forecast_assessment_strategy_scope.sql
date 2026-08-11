-- 322_strategy_forecast_assessment_strategy_scope.sql
--
-- #2557: a setup label is not a strategy identity. Bind prospective forecast
-- authority to the exact strategy and version that emitted the signal so two
-- strategies cannot accidentally share an assessment scope.

ALTER TABLE strategy_forecast_assessments
    ADD COLUMN strategy_id TEXT,
    ADD COLUMN strategy_version TEXT;

-- Older assessment rows did not carry strategy identity. Recover it only when
-- the frozen cohort maps to one exact strategy/version and its row count
-- reconciles. An ambiguous derived assessment must lose authority and be
-- rebuilt by the scheduler; assigning a placeholder could authorise the wrong
-- strategy.
WITH recoverable AS (
    SELECT a.assessment_id,
           min(s.strategy_id) AS strategy_id,
           min(s.strategy_version) AS strategy_version
    FROM strategy_forecast_assessments a
    JOIN strategy_opportunity_forecasts f
      ON f.forecast_policy_version=a.forecast_policy_version
     AND f.setup_version=a.setup_version
     AND f.exit_policy_version=a.exit_policy_version
     AND f.calibration_id=a.calibration_id
     AND f.decided_at::date BETWEEN a.window_start AND a.window_end
    JOIN strategy_forecast_calibrations c
      ON c.calibration_id=f.calibration_id AND c.model_version=a.model_version
    JOIN strategy_signals s ON s.signal_id=f.signal_id
    GROUP BY a.assessment_id,a.total_forecasts
    HAVING count(*)=a.total_forecasts
       AND count(DISTINCT (s.strategy_id,s.strategy_version))=1
)
UPDATE strategy_forecast_assessments a
SET strategy_id=recoverable.strategy_id,
    strategy_version=recoverable.strategy_version
FROM recoverable
WHERE recoverable.assessment_id=a.assessment_id;

DELETE FROM strategy_forecast_assessment_current current_assessment
WHERE EXISTS (
    SELECT 1 FROM strategy_forecast_assessments assessment
    WHERE assessment.assessment_id=current_assessment.assessment_id
      AND assessment.strategy_id IS NULL
);
DELETE FROM strategy_forecast_assessments WHERE strategy_id IS NULL;

ALTER TABLE strategy_forecast_assessments
    ALTER COLUMN strategy_id SET NOT NULL,
    ALTER COLUMN strategy_version SET NOT NULL;

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
    ADD COLUMN strategy_id TEXT,
    ADD COLUMN strategy_version TEXT;
UPDATE strategy_forecast_assessment_current current_assessment
SET strategy_id=assessment.strategy_id,
    strategy_version=assessment.strategy_version
FROM strategy_forecast_assessments assessment
WHERE assessment.assessment_id=current_assessment.assessment_id;
ALTER TABLE strategy_forecast_assessment_current
    ALTER COLUMN strategy_id SET NOT NULL,
    ALTER COLUMN strategy_version SET NOT NULL;
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
