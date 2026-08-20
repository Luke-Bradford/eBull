-- #2697 -- a corrected hold-out row cannot borrow criterion 9's synthetic
-- control from a legacy in-sample result measured on a position-selected axis.

BEGIN;

SET LOCAL lock_timeout = '5s';

-- #2737's exact in-sample control may support a control-free hold-out only
-- within one metric-axis derivation generation. ``=`` is deliberate: NULL
-- legacy rules do not match each other and can never support a current row.
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
     AND i.ambiguity_rule_version = h.ambiguity_rule_version
     AND i.metric_axis_rule_version = h.metric_axis_rule_version
     AND i.position_rule_set_version = h.position_rule_set_version
     AND i.outcome_rule_set_version = h.outcome_rule_set_version
     AND i.input_rule_set_version = h.input_rule_set_version
     AND i.metric_set_id = h.metric_set_id
    JOIN strategy_result_universe i_universe
      ON i_universe.result_id = i.result_id
     AND i_universe.universe_rule_version = h_universe.universe_rule_version
     AND i_universe.validated_universe_ids = h_universe.validated_universe_ids
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

COMMIT;
