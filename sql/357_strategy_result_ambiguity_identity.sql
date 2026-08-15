-- #2747 -- §3.4's ambiguity comparison rule changes what a result means.
-- Store it on the result itself and include it in ResultIdentity; the auxiliary
-- immutable record must describe the same version as its owner.

BEGIN;

ALTER TABLE strategy_results_store
    ADD COLUMN IF NOT EXISTS ambiguity_rule_version TEXT;

-- Every existing result predates the matched-control implementation. Preserve
-- that fact rather than restamping history with today's rule.
UPDATE strategy_results_store
   SET ambiguity_rule_version = 'ambiguity-verdict-2026-08-13-v1-no-cohort-threshold'
 WHERE ambiguity_rule_version IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'strategy_results_store'::regclass
          AND conname = 'strategy_results_ambiguity_rule_not_blank'
    ) THEN
        ALTER TABLE strategy_results_store
            ADD CONSTRAINT strategy_results_ambiguity_rule_not_blank
            CHECK (btrim(ambiguity_rule_version) <> '');
    END IF;
END
$$;

ALTER TABLE strategy_results_store
    ALTER COLUMN ambiguity_rule_version SET NOT NULL;

COMMENT ON COLUMN strategy_results_store.ambiguity_rule_version IS
    'Versioned §3.4 ambiguity-arm comparison semantics; a rule change mints a new result identity.';

-- PostgreSQL requires a referenced unique key. result_id is already the PK,
-- but the exact composite key makes the cross-table version equality declarative.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'strategy_results_store'::regclass
          AND conname = 'strategy_results_id_ambiguity_rule_unique'
    ) THEN
        ALTER TABLE strategy_results_store
            ADD CONSTRAINT strategy_results_id_ambiguity_rule_unique
            UNIQUE (result_id, ambiguity_rule_version);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'strategy_result_ambiguity'::regclass
          AND conname = 'strategy_result_ambiguity_owner_rule_matches'
    ) THEN
        ALTER TABLE strategy_result_ambiguity
            ADD CONSTRAINT strategy_result_ambiguity_owner_rule_matches
            FOREIGN KEY (result_id, ambiguity_rule_version)
            REFERENCES strategy_results_store (result_id, ambiguity_rule_version)
            ON DELETE RESTRICT
            NOT VALID;
    END IF;
END
$$;

ALTER TABLE strategy_result_ambiguity
    VALIDATE CONSTRAINT strategy_result_ambiguity_owner_rule_matches;

-- #2737's hold-out control composition must not cross an ambiguity-rule
-- boundary. The remaining predicates are copied byte-for-byte from migration
-- 356; this migration adds exactly the new identity member.
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

CREATE OR REPLACE VIEW strategy_results AS
    SELECT *
    FROM strategy_results_store
    WHERE namespace = 'in_sample';

ALTER VIEW strategy_results SET (check_option = 'cascaded');

COMMIT;
