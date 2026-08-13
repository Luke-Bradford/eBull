-- #2450: an explicit live-promotion request must remain auditable even when
-- the operator has not registered a gate policy for that strategy version.

ALTER TABLE strategy_live_gate_assessments
    ADD COLUMN IF NOT EXISTS strategy_id TEXT,
    ADD COLUMN IF NOT EXISTS strategy_version TEXT;

UPDATE strategy_live_gate_assessments a
SET strategy_id = p.strategy_id,
    strategy_version = p.strategy_version
FROM strategy_live_gate_policies p
WHERE p.live_gate_policy_id = a.live_gate_policy_id
  AND (a.strategy_id IS NULL OR a.strategy_version IS NULL);

ALTER TABLE strategy_live_gate_assessments
    ALTER COLUMN strategy_id SET NOT NULL,
    ALTER COLUMN strategy_version SET NOT NULL,
    ALTER COLUMN live_gate_policy_id DROP NOT NULL;

ALTER TABLE strategy_live_gate_assessments
    ADD CONSTRAINT strategy_live_gate_assessment_identity_non_empty
    CHECK (strategy_id <> '' AND strategy_version <> ''),
    ADD CONSTRAINT strategy_live_gate_assessment_policy_or_missing_refusal
    CHECK (
        live_gate_policy_id IS NOT NULL
        OR (NOT passed AND refusal_codes @> ARRAY['live_gate_policy_missing']::TEXT[])
    );

CREATE INDEX IF NOT EXISTS idx_strategy_live_gate_assessment_strategy_recent
    ON strategy_live_gate_assessments (strategy_id, strategy_version, assessed_at DESC);

COMMENT ON TABLE strategy_live_gate_assessments IS
    'One compact evidence snapshot per explicit operator promotion attempt, '
    'including policy-less refusals. Periodic health reads write no rows.';
