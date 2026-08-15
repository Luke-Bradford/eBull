-- #2737 — compose withheld evidence with its exact in-sample harness control.
--
-- A 1,000-member random-entry cohort is deliberately never run over hold-out
-- data: that would turn one audited evaluation into 1,000 outcome looks.  The
-- hold-out row consequently keeps synthetic_control_* NULL.  This view derives
-- the one in-sample result that can support it without copying that control onto
-- the withheld row or allowing a caller to choose a favourable result id.

CREATE OR REPLACE VIEW strategy_result_control_support AS
WITH candidates AS (
    SELECT h.result_id AS holdout_result_id,
           i.result_id AS control_result_id
    FROM strategy_results_store h
    JOIN strategy_result_universe h_universe ON h_universe.result_id = h.result_id
    JOIN strategy_results_store i
      ON i.strategy_id = h.strategy_id
     AND i.strategy_version = h.strategy_version
     AND i.result_scope = h.result_scope
     AND i.namespace = 'in_sample'
     AND i.ambiguity_arm = h.ambiguity_arm
     AND i.quarantine_arm = h.quarantine_arm
     AND i.universe_basis IS NOT DISTINCT FROM h.universe_basis
     AND i.corpus_version = h.corpus_version
     AND i.cost_model_id = h.cost_model_id
     AND i.carry_unmodelled = h.carry_unmodelled
     AND i.fx_unmodelled = h.fx_unmodelled
     AND i.sizing_rule = h.sizing_rule
     AND i.benchmark_rule = h.benchmark_rule
     AND i.return_basis = h.return_basis
     AND i.position_rule_set_version = h.position_rule_set_version
     AND i.outcome_rule_set_version = h.outcome_rule_set_version
     AND i.input_rule_set_version = h.input_rule_set_version
     AND i.metric_set_id = h.metric_set_id
    JOIN strategy_result_universe i_universe
      ON i_universe.result_id = i.result_id
     -- Declared recent windows legitimately evaluate a subset of the names
     -- seen by the full in-sample run.  Bind the immutable universe definition,
     -- not the window-dependent evaluated subset.
     AND i_universe.universe_rule_version = h_universe.universe_rule_version
     AND i_universe.validated_universe_ids = h_universe.validated_universe_ids
     -- The public runner permits custom windows only on the audited hold-out
     -- side.  These two bounds identify its one full-corpus in-sample result
     -- without baking either vendor's changing end date into the view.
     AND i.window_start = DATE '1962-01-02'
     AND i.window_end >= DATE '2021-06-29'
    WHERE h.namespace = 'hold_out'
)
SELECT h.result_id AS holdout_result_id,
       count(c.control_result_id)::integer AS candidate_count,
       CASE WHEN count(c.control_result_id) = 1 THEN min(c.control_result_id) END AS control_result_id
FROM strategy_results_store h
LEFT JOIN candidates c ON c.holdout_result_id = h.result_id
WHERE h.namespace = 'hold_out'
GROUP BY h.result_id;

COMMENT ON VIEW strategy_result_control_support IS
    'Exact derived in-sample synthetic-control support for each hold-out result (#2737). '
    'candidate_count must equal one; callers never choose control_result_id.';
