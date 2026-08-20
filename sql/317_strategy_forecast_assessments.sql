-- 317_strategy_forecast_assessments.sql
--
-- #2555: preregistered recent prospective probability assessment. Immutable
-- evidence rows deduplicate unchanged cohorts; one bounded current pointer per
-- forecast scope can be refreshed without appending scheduler heartbeats.

CREATE TABLE IF NOT EXISTS strategy_forecast_assessment_policies (
    policy_id                      TEXT PRIMARY KEY CHECK (policy_id <> ''),
    effective_from                 TIMESTAMPTZ NOT NULL UNIQUE,
    recent_window_days             INTEGER NOT NULL CHECK (recent_window_days BETWEEN 20 AND 365),
    minimum_resolved_forecasts     INTEGER NOT NULL CHECK (minimum_resolved_forecasts >= 30),
    adaptive_calibration_bins      INTEGER NOT NULL CHECK (adaptive_calibration_bins BETWEEN 2 AND 20),
    max_normalized_brier_score     NUMERIC(12,8) NOT NULL CHECK (max_normalized_brier_score BETWEEN 0 AND 1),
    max_classwise_calibration_error NUMERIC(12,8) NOT NULL CHECK (max_classwise_calibration_error BETWEEN 0 AND 1),
    max_ambiguous_rate             NUMERIC(12,8) NOT NULL CHECK (max_ambiguous_rate BETWEEN 0 AND 1),
    max_unresolved_rate            NUMERIC(12,8) NOT NULL CHECK (max_unresolved_rate BETWEEN 0 AND 1),
    max_pending_rate               NUMERIC(12,8) NOT NULL CHECK (max_pending_rate BETWEEN 0 AND 1),
    max_assessment_age_days        INTEGER NOT NULL CHECK (max_assessment_age_days BETWEEN 1 AND 7),
    evidence_ref                   TEXT NOT NULL CHECK (evidence_ref <> ''),
    created_at                     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS strategy_forecast_assessments (
    assessment_id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    policy_id                      TEXT NOT NULL
        REFERENCES strategy_forecast_assessment_policies(policy_id) ON DELETE RESTRICT,
    forecast_policy_version        TEXT NOT NULL CHECK (forecast_policy_version <> ''),
    model_version                  TEXT NOT NULL CHECK (model_version <> ''),
    setup_version                  TEXT NOT NULL CHECK (setup_version <> ''),
    exit_policy_version            TEXT NOT NULL CHECK (exit_policy_version <> ''),
    resolver_version               TEXT NOT NULL CHECK (resolver_version <> ''),
    input_rule_set_version         TEXT NOT NULL CHECK (input_rule_set_version <> ''),
    window_start                   DATE NOT NULL,
    window_end                     DATE NOT NULL CHECK (window_end >= window_start),
    evidence_hash                  TEXT NOT NULL CHECK (evidence_hash <> ''),
    total_forecasts                INTEGER NOT NULL CHECK (total_forecasts >= 0),
    resolved_forecasts             INTEGER NOT NULL CHECK (resolved_forecasts >= 0),
    target_first_count             INTEGER NOT NULL CHECK (target_first_count >= 0),
    stop_first_count               INTEGER NOT NULL CHECK (stop_first_count >= 0),
    timeout_count                  INTEGER NOT NULL CHECK (timeout_count >= 0),
    ambiguous_count                INTEGER NOT NULL CHECK (ambiguous_count >= 0),
    unresolved_count               INTEGER NOT NULL CHECK (unresolved_count >= 0),
    pending_count                  INTEGER NOT NULL CHECK (pending_count >= 0),
    normalized_brier_score         NUMERIC(12,8) CHECK (normalized_brier_score BETWEEN 0 AND 1),
    max_classwise_calibration_error NUMERIC(12,8) CHECK (max_classwise_calibration_error BETWEEN 0 AND 1),
    ambiguous_rate                 NUMERIC(12,8) NOT NULL CHECK (ambiguous_rate BETWEEN 0 AND 1),
    unresolved_rate                NUMERIC(12,8) NOT NULL CHECK (unresolved_rate BETWEEN 0 AND 1),
    pending_rate                   NUMERIC(12,8) NOT NULL CHECK (pending_rate BETWEEN 0 AND 1),
    passed                         BOOLEAN NOT NULL,
    reason_codes                   JSONB NOT NULL CHECK (jsonb_typeof(reason_codes)='array'),
    recorded_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (
        policy_id,forecast_policy_version,model_version,setup_version,
        exit_policy_version,resolver_version,input_rule_set_version,evidence_hash
    ),
    CHECK (resolved_forecasts = target_first_count + stop_first_count + timeout_count),
    CHECK (total_forecasts = resolved_forecasts + ambiguous_count + unresolved_count + pending_count),
    CHECK ((resolved_forecasts = 0) = (normalized_brier_score IS NULL)),
    CHECK ((resolved_forecasts = 0) = (max_classwise_calibration_error IS NULL))
);

CREATE INDEX IF NOT EXISTS idx_strategy_forecast_assessments_scope
    ON strategy_forecast_assessments (
        policy_id,forecast_policy_version,model_version,setup_version,exit_policy_version,
        resolver_version,input_rule_set_version,assessment_id DESC
    );

CREATE TABLE IF NOT EXISTS strategy_forecast_assessment_current (
    policy_id                      TEXT NOT NULL
        REFERENCES strategy_forecast_assessment_policies(policy_id) ON DELETE RESTRICT,
    forecast_policy_version        TEXT NOT NULL,
    model_version                  TEXT NOT NULL,
    setup_version                  TEXT NOT NULL,
    exit_policy_version            TEXT NOT NULL,
    resolver_version               TEXT NOT NULL,
    input_rule_set_version         TEXT NOT NULL,
    assessment_id                  BIGINT NOT NULL
        REFERENCES strategy_forecast_assessments(assessment_id) ON DELETE RESTRICT,
    checked_at                     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (
        policy_id,forecast_policy_version,model_version,setup_version,
        exit_policy_version,resolver_version,input_rule_set_version
    )
);

COMMENT ON TABLE strategy_forecast_assessment_policies IS
    'Immutable operator-preregistered prospective forecast trust thresholds; deliberately has no passing seed default.';
COMMENT ON TABLE strategy_forecast_assessments IS
    'Deduplicated recent-cohort probability evidence. Normalized multiclass Brier = sum squared class errors / (2N).';
COMMENT ON COLUMN strategy_forecast_assessments.max_classwise_calibration_error IS
    'Maximum adaptive-bin expected calibration error across target, stop and timeout classes.';
COMMENT ON TABLE strategy_forecast_assessment_current IS
    'One bounded freshness pointer per exact forecast scope; scheduler checks do not append heartbeat evidence.';
