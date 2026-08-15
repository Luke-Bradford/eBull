-- #2769 -- durable, structural-first evidence for the preregistered MT-1/S-8
-- four-arm controlled experiment.
--
-- One immutable strategy version gets one structural attempt. A passed attempt
-- must own the complete ambiguity x quarantine fan (four cells); a refused
-- attempt owns none and contains no performance columns. Only a passed,
-- complete structural attempt may own an outcome bundle, and that bundle must
-- atomically own the same exact four cells. Deferred constraint triggers make
-- partial fan commits impossible even for a writer that bypasses Python.

BEGIN;

CREATE OR REPLACE FUNCTION strategy_mt1_numeric_is_finite(value NUMERIC)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
STRICT
AS $$
    SELECT value NOT IN ('NaN'::NUMERIC, 'Infinity'::NUMERIC, '-Infinity'::NUMERIC)
$$;

CREATE TABLE strategy_mt1_structural_attempts (
    structural_attempt_id       BIGSERIAL PRIMARY KEY,
    mt1_declaration_id          BIGINT NOT NULL,
    s8_declaration_id           BIGINT NOT NULL,
    mt1_strategy_id             TEXT NOT NULL CHECK (
        mt1_strategy_id = 'mt1-capped-volatility-managed-relative-strength-v1'
    ),
    mt1_strategy_version        TEXT NOT NULL CHECK (btrim(mt1_strategy_version) <> ''),
    s8_strategy_id              TEXT NOT NULL CHECK (
        s8_strategy_id = 'mt1-s8-capped-volatility-negative-control-v1'
    ),
    s8_strategy_version         TEXT NOT NULL CHECK (btrim(s8_strategy_version) <> ''),
    mt1_source_strategy_version TEXT NOT NULL CHECK (btrim(mt1_source_strategy_version) <> ''),
    s8_source_strategy_version  TEXT NOT NULL CHECK (btrim(s8_source_strategy_version) <> ''),
    universe_basis              TEXT NOT NULL CHECK (universe_basis = 'survivorship_free'),
    corpus_version              TEXT NOT NULL CHECK (btrim(corpus_version) <> ''),
    cost_model_id               TEXT NOT NULL CHECK (btrim(cost_model_id) <> ''),
    trial_register_version      TEXT NOT NULL CHECK (btrim(trial_register_version) <> ''),
    trial_contract_version      TEXT NOT NULL CHECK (btrim(trial_contract_version) <> ''),
    book_rule_version           TEXT NOT NULL CHECK (btrim(book_rule_version) <> ''),
    evaluator_version           TEXT NOT NULL CHECK (btrim(evaluator_version) <> ''),
    metric_axis_rule_version    TEXT NOT NULL CHECK (
        metric_axis_rule_version = 'full-namespace-panel-v1'
    ),
    metric_axis_dates           DATE[] NOT NULL,
    metric_axis_start           DATE NOT NULL,
    metric_axis_end             DATE NOT NULL CHECK (metric_axis_end < DATE '2021-06-29'),
    metric_axis_digest          TEXT NOT NULL CHECK (metric_axis_digest ~ '^[0-9a-f]{64}$'),
    opportunity_set_digest      TEXT NOT NULL CHECK (opportunity_set_digest ~ '^[0-9a-f]{64}$'),
    passed                      BOOLEAN NOT NULL,
    refusal_code                TEXT CHECK (refusal_code = 'structural_gate_refused'),
    refusal_detail              TEXT CHECK (
        refusal_detail IS NULL OR char_length(btrim(refusal_detail)) BETWEEN 1 AND 2000
    ),
    structural_evidence_sha256  TEXT NOT NULL CHECK (structural_evidence_sha256 ~ '^[0-9a-f]{64}$'),
    structural_evidence_json    JSONB NOT NULL CHECK (jsonb_typeof(structural_evidence_json) = 'object'),
    assessed_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT strategy_mt1_structural_attempt_declarations_distinct CHECK (
        mt1_declaration_id <> s8_declaration_id
    ),
    CONSTRAINT strategy_mt1_structural_attempt_axis CHECK (
        strategy_metric_axis_is_valid(metric_axis_dates, metric_axis_start, metric_axis_end)
    ),
    CONSTRAINT strategy_mt1_structural_attempt_state CHECK (
        (passed AND refusal_code IS NULL AND refusal_detail IS NULL)
        OR (NOT passed AND refusal_code IS NOT NULL AND refusal_detail IS NOT NULL)
    ),
    CONSTRAINT strategy_mt1_structural_attempt_once UNIQUE (
        mt1_strategy_version, s8_strategy_version
    ),
    CONSTRAINT strategy_mt1_structural_mt1_declaration_fk FOREIGN KEY (
        mt1_declaration_id, mt1_strategy_id, mt1_strategy_version
    ) REFERENCES strategy_preregistration_declarations (
        declaration_id, strategy_id, strategy_version
    ) ON DELETE RESTRICT,
    CONSTRAINT strategy_mt1_structural_s8_declaration_fk FOREIGN KEY (
        s8_declaration_id, s8_strategy_id, s8_strategy_version
    ) REFERENCES strategy_preregistration_declarations (
        declaration_id, strategy_id, strategy_version
    ) ON DELETE RESTRICT
);

CREATE TABLE strategy_mt1_structural_cells (
    structural_attempt_id       BIGINT NOT NULL
        REFERENCES strategy_mt1_structural_attempts(structural_attempt_id) ON DELETE RESTRICT,
    ambiguity_arm               TEXT NOT NULL CHECK (ambiguity_arm IN ('best_case', 'worst_case')),
    quarantine_arm              TEXT NOT NULL CHECK (quarantine_arm IN ('masked', 'admitted')),
    mt1_decision_dates          DATE[] NOT NULL,
    s8_decision_dates           DATE[] NOT NULL,
    mt1_annualised_turnover     NUMERIC NOT NULL CHECK (
        mt1_annualised_turnover >= 0 AND mt1_annualised_turnover <= 6
    ),
    s8_annualised_turnover      NUMERIC NOT NULL CHECK (
        s8_annualised_turnover >= 0 AND s8_annualised_turnover <= 6
    ),
    mt1_traded_notional         NUMERIC NOT NULL CHECK (mt1_traded_notional >= 0),
    s8_traded_notional          NUMERIC NOT NULL CHECK (s8_traded_notional >= 0),
    exposure_reconciled         BOOLEAN NOT NULL CHECK (exposure_reconciled),
    evidence_sha256             TEXT NOT NULL CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
    evidence_json               JSONB NOT NULL CHECK (jsonb_typeof(evidence_json) = 'object'),
    PRIMARY KEY (structural_attempt_id, ambiguity_arm, quarantine_arm),
    CONSTRAINT strategy_mt1_structural_cell_metrics_finite CHECK (
        strategy_mt1_numeric_is_finite(mt1_annualised_turnover)
        AND strategy_mt1_numeric_is_finite(s8_annualised_turnover)
        AND strategy_mt1_numeric_is_finite(mt1_traded_notional)
        AND strategy_mt1_numeric_is_finite(s8_traded_notional)
    ),
    CONSTRAINT strategy_mt1_structural_cell_clock CHECK (
        cardinality(mt1_decision_dates) > 0
        AND mt1_decision_dates = s8_decision_dates
        AND strategy_metric_axis_is_valid(
            mt1_decision_dates,
            mt1_decision_dates[1],
            mt1_decision_dates[cardinality(mt1_decision_dates)]
        )
    )
);

CREATE TABLE strategy_mt1_trial_results (
    mt1_trial_result_id         BIGSERIAL PRIMARY KEY,
    structural_attempt_id       BIGINT NOT NULL UNIQUE
        REFERENCES strategy_mt1_structural_attempts(structural_attempt_id) ON DELETE RESTRICT,
    historical_conjuncts_pass  BOOLEAN NOT NULL,
    evidence_sha256             TEXT NOT NULL CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
    evidence_json               JSONB NOT NULL CHECK (jsonb_typeof(evidence_json) = 'object'),
    evaluated_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION strategy_mt1_month_axis_is_valid(months DATE[])
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
STRICT
AS $$
    SELECT cardinality(months) >= 120
       AND strategy_metric_axis_is_valid(
            months,
            months[1],
            months[cardinality(months)]
       )
       AND NOT EXISTS (
            SELECT 1 FROM unnest(months) AS month
            WHERE extract(day FROM month) <> 1
       )
$$;

CREATE TABLE strategy_mt1_trial_result_cells (
    mt1_trial_result_id             BIGINT NOT NULL
        REFERENCES strategy_mt1_trial_results(mt1_trial_result_id) ON DELETE RESTRICT,
    ambiguity_arm                   TEXT NOT NULL CHECK (ambiguity_arm IN ('best_case', 'worst_case')),
    quarantine_arm                  TEXT NOT NULL CHECK (quarantine_arm IN ('masked', 'admitted')),
    common_months                   DATE[] NOT NULL CHECK (cardinality(common_months) >= 120),
    excluded_months_by_arm          INTEGER[] NOT NULL CHECK (
        cardinality(excluded_months_by_arm) = 4
        AND 0 <= ALL(excluded_months_by_arm)
    ),
    mt1_scaled_certainty_equivalent NUMERIC NOT NULL,
    mt1_scaled_maximum_drawdown     NUMERIC NOT NULL CHECK (
        mt1_scaled_maximum_drawdown BETWEEN 0 AND 1
    ),
    mt1_scaled_expected_shortfall_5 NUMERIC NOT NULL,
    mt1_unscaled_certainty_equivalent NUMERIC NOT NULL,
    mt1_unscaled_maximum_drawdown   NUMERIC NOT NULL CHECK (
        mt1_unscaled_maximum_drawdown BETWEEN 0 AND 1
    ),
    mt1_unscaled_expected_shortfall_5 NUMERIC NOT NULL,
    s8_scaled_certainty_equivalent  NUMERIC NOT NULL,
    s8_scaled_maximum_drawdown      NUMERIC NOT NULL CHECK (
        s8_scaled_maximum_drawdown BETWEEN 0 AND 1
    ),
    s8_scaled_expected_shortfall_5  NUMERIC NOT NULL,
    s8_unscaled_certainty_equivalent NUMERIC NOT NULL,
    s8_unscaled_maximum_drawdown    NUMERIC NOT NULL CHECK (
        s8_unscaled_maximum_drawdown BETWEEN 0 AND 1
    ),
    s8_unscaled_expected_shortfall_5 NUMERIC NOT NULL,
    mt1_delta_cer                   NUMERIC NOT NULL,
    s8_delta_cer                    NUMERIC NOT NULL,
    primary_difference_in_differences NUMERIC NOT NULL,
    mt1_interval_low                NUMERIC NOT NULL,
    mt1_interval_high               NUMERIC NOT NULL CHECK (mt1_interval_high >= mt1_interval_low),
    primary_interval_low            NUMERIC NOT NULL,
    primary_interval_high           NUMERIC NOT NULL CHECK (
        primary_interval_high >= primary_interval_low
    ),
    primary_lower_bound_positive    BOOLEAN NOT NULL,
    mt1_lower_bound_positive        BOOLEAN NOT NULL,
    mt1_drawdown_improved           BOOLEAN NOT NULL,
    mt1_expected_shortfall_improved BOOLEAN NOT NULL,
    historical_conjuncts_pass       BOOLEAN NOT NULL,
    evidence_sha256                 TEXT NOT NULL CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
    evidence_json                   JSONB NOT NULL CHECK (jsonb_typeof(evidence_json) = 'object'),
    PRIMARY KEY (mt1_trial_result_id, ambiguity_arm, quarantine_arm),
    CONSTRAINT strategy_mt1_result_metrics_finite CHECK (
        strategy_mt1_numeric_is_finite(mt1_scaled_certainty_equivalent)
        AND strategy_mt1_numeric_is_finite(mt1_scaled_maximum_drawdown)
        AND strategy_mt1_numeric_is_finite(mt1_scaled_expected_shortfall_5)
        AND strategy_mt1_numeric_is_finite(mt1_unscaled_certainty_equivalent)
        AND strategy_mt1_numeric_is_finite(mt1_unscaled_maximum_drawdown)
        AND strategy_mt1_numeric_is_finite(mt1_unscaled_expected_shortfall_5)
        AND strategy_mt1_numeric_is_finite(s8_scaled_certainty_equivalent)
        AND strategy_mt1_numeric_is_finite(s8_scaled_maximum_drawdown)
        AND strategy_mt1_numeric_is_finite(s8_scaled_expected_shortfall_5)
        AND strategy_mt1_numeric_is_finite(s8_unscaled_certainty_equivalent)
        AND strategy_mt1_numeric_is_finite(s8_unscaled_maximum_drawdown)
        AND strategy_mt1_numeric_is_finite(s8_unscaled_expected_shortfall_5)
        AND strategy_mt1_numeric_is_finite(mt1_delta_cer)
        AND strategy_mt1_numeric_is_finite(s8_delta_cer)
        AND strategy_mt1_numeric_is_finite(primary_difference_in_differences)
        AND strategy_mt1_numeric_is_finite(mt1_interval_low)
        AND strategy_mt1_numeric_is_finite(mt1_interval_high)
        AND strategy_mt1_numeric_is_finite(primary_interval_low)
        AND strategy_mt1_numeric_is_finite(primary_interval_high)
    ),
    CONSTRAINT strategy_mt1_result_common_month_clock CHECK (
        strategy_mt1_month_axis_is_valid(common_months)
    )
);

CREATE OR REPLACE FUNCTION enforce_strategy_mt1_structural_fan()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    owner_id BIGINT := COALESCE(NEW.structural_attempt_id, OLD.structural_attempt_id);
    owner_passed BOOLEAN;
    owner_axis DATE[];
    cell_count INTEGER;
BEGIN
    SELECT passed, metric_axis_dates INTO owner_passed, owner_axis
      FROM strategy_mt1_structural_attempts
     WHERE structural_attempt_id = owner_id;
    IF owner_passed IS NULL THEN
        RETURN NULL;
    END IF;
    SELECT count(*) INTO cell_count
      FROM strategy_mt1_structural_cells
     WHERE structural_attempt_id = owner_id;
    IF (owner_passed AND cell_count <> 4) OR (NOT owner_passed AND cell_count <> 0) THEN
        RAISE EXCEPTION
            'MT-1 structural attempt % passed=% requires exactly % cells, found %',
            owner_id, owner_passed, CASE WHEN owner_passed THEN 4 ELSE 0 END, cell_count;
    END IF;
    IF owner_passed AND EXISTS (
        SELECT 1
          FROM strategy_mt1_structural_cells cell,
               unnest(cell.mt1_decision_dates) AS decision_date
         WHERE cell.structural_attempt_id = owner_id
           AND NOT decision_date = ANY(owner_axis)
    ) THEN
        RAISE EXCEPTION
            'MT-1 structural attempt % has a decision outside its frozen metric axis', owner_id;
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER trg_strategy_mt1_structural_attempt_fan
AFTER INSERT ON strategy_mt1_structural_attempts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION enforce_strategy_mt1_structural_fan();

CREATE CONSTRAINT TRIGGER trg_strategy_mt1_structural_cell_fan
AFTER INSERT ON strategy_mt1_structural_cells
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION enforce_strategy_mt1_structural_fan();

CREATE OR REPLACE FUNCTION enforce_strategy_mt1_result_fan()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    result_id BIGINT := COALESCE(NEW.mt1_trial_result_id, OLD.mt1_trial_result_id);
    attempt_id BIGINT;
    attempt_passed BOOLEAN;
    structural_count INTEGER;
    result_count INTEGER;
    declared_pass BOOLEAN;
    derived_pass BOOLEAN;
    distinct_common_axes INTEGER;
BEGIN
    SELECT r.structural_attempt_id, a.passed, r.historical_conjuncts_pass
      INTO attempt_id, attempt_passed, declared_pass
      FROM strategy_mt1_trial_results r
      JOIN strategy_mt1_structural_attempts a
        ON a.structural_attempt_id = r.structural_attempt_id
     WHERE r.mt1_trial_result_id = result_id;
    IF attempt_id IS NULL THEN
        RETURN NULL;
    END IF;
    SELECT count(*) INTO structural_count
      FROM strategy_mt1_structural_cells WHERE structural_attempt_id = attempt_id;
    SELECT count(*), bool_and(historical_conjuncts_pass)
      INTO result_count, derived_pass
      FROM strategy_mt1_trial_result_cells WHERE mt1_trial_result_id = result_id;
    SELECT count(DISTINCT common_months) INTO distinct_common_axes
      FROM strategy_mt1_trial_result_cells WHERE mt1_trial_result_id = result_id;
    IF NOT attempt_passed OR structural_count <> 4 OR result_count <> 4 THEN
        RAISE EXCEPTION
            'MT-1 result % requires one passed four-cell structural attempt and four result cells', result_id;
    END IF;
    IF declared_pass IS DISTINCT FROM derived_pass THEN
        RAISE EXCEPTION
            'MT-1 result % declares pass=% but its four-cell conjunction is %',
            result_id, declared_pass, derived_pass;
    END IF;
    IF distinct_common_axes <> 1 OR EXISTS (
        SELECT 1
          FROM strategy_mt1_trial_result_cells cell
          JOIN strategy_mt1_trial_results result
            ON result.mt1_trial_result_id = cell.mt1_trial_result_id
          JOIN strategy_mt1_structural_attempts attempt
            ON attempt.structural_attempt_id = result.structural_attempt_id,
               unnest(cell.common_months) AS common_month
         WHERE cell.mt1_trial_result_id = result_id
           AND NOT EXISTS (
               SELECT 1 FROM unnest(attempt.metric_axis_dates) AS axis_date
                WHERE date_trunc('month', axis_date)::DATE = common_month
           )
    ) THEN
        RAISE EXCEPTION
            'MT-1 result % common-month axes differ or escape the frozen metric axis', result_id;
    END IF;
    IF EXISTS (
        SELECT ambiguity_arm, quarantine_arm
          FROM strategy_mt1_structural_cells
         WHERE structural_attempt_id = attempt_id
        EXCEPT
        SELECT ambiguity_arm, quarantine_arm
          FROM strategy_mt1_trial_result_cells
         WHERE mt1_trial_result_id = result_id
    ) OR EXISTS (
        SELECT ambiguity_arm, quarantine_arm
          FROM strategy_mt1_trial_result_cells
         WHERE mt1_trial_result_id = result_id
        EXCEPT
        SELECT ambiguity_arm, quarantine_arm
          FROM strategy_mt1_structural_cells
         WHERE structural_attempt_id = attempt_id
    ) THEN
        RAISE EXCEPTION 'MT-1 result % fan keys differ from its structural attempt', result_id;
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER trg_strategy_mt1_result_header_fan
AFTER INSERT ON strategy_mt1_trial_results
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION enforce_strategy_mt1_result_fan();

CREATE CONSTRAINT TRIGGER trg_strategy_mt1_result_cell_fan
AFTER INSERT ON strategy_mt1_trial_result_cells
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION enforce_strategy_mt1_result_fan();

CREATE OR REPLACE FUNCTION reject_strategy_mt1_evidence_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'MT-1 controlled-trial evidence is immutable; mint a new strategy version';
END;
$$;

CREATE TRIGGER trg_strategy_mt1_structural_attempt_immutable
BEFORE UPDATE OR DELETE ON strategy_mt1_structural_attempts
FOR EACH ROW EXECUTE FUNCTION reject_strategy_mt1_evidence_change();
CREATE TRIGGER trg_strategy_mt1_structural_cell_immutable
BEFORE UPDATE OR DELETE ON strategy_mt1_structural_cells
FOR EACH ROW EXECUTE FUNCTION reject_strategy_mt1_evidence_change();
CREATE TRIGGER trg_strategy_mt1_result_immutable
BEFORE UPDATE OR DELETE ON strategy_mt1_trial_results
FOR EACH ROW EXECUTE FUNCTION reject_strategy_mt1_evidence_change();
CREATE TRIGGER trg_strategy_mt1_result_cell_immutable
BEFORE UPDATE OR DELETE ON strategy_mt1_trial_result_cells
FOR EACH ROW EXECUTE FUNCTION reject_strategy_mt1_evidence_change();

COMMENT ON TABLE strategy_mt1_structural_attempts IS
    'One immutable pre-performance MT-1/S-8 structural assessment per exact strategy-version pair.';
COMMENT ON TABLE strategy_mt1_structural_cells IS
    'The complete four-cell ambiguity/quarantine structural fan; contains no return or performance statistic.';
COMMENT ON TABLE strategy_mt1_trial_results IS
    'One immutable outcome bundle authorised by a committed passed four-cell structural attempt.';
COMMENT ON TABLE strategy_mt1_trial_result_cells IS
    'Four conjunctive MT-1 controlled-result cells; no favourable ambiguity/quarantine cell is selectable.';

COMMIT;
