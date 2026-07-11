-- 219: split LLM writer/critic model knobs + critic-run provenance (#1995)
--
-- Spec: docs/specs/thesis/2026-07-09-byo-llm-thesis-live.md (§2 config,
-- amended in the same PR). Operator direction 2026-07-10 (#1995): the
-- bulk writer and the adversarial critic may run different local models
-- (deepseek-r1 writer / qwen3 critic candidate), so the single
-- `llm_model` knob splits into `llm_model_writer` + `llm_model_critic`.
--
-- Altered tables:
--   runtime_config       — llm_model RENAMED to llm_model_writer;
--                          new llm_model_critic seeded from the same
--                          value (behaviour-preserving: both roles keep
--                          running whatever the old knob said)
--   runtime_config_audit — field CHECK extended; legacy 'llm_model'
--                          value RETAINED (historical audit rows from
--                          218's flip + boot recovery still satisfy it)
--   thesis_runs          — critic_model provenance column (critic_json
--                          vanishes on best-effort critic failure, so
--                          the configured critic model must be recorded
--                          on the run row to stay auditable)

BEGIN;

-- Idempotent rename: only when the old column still exists and the new
-- one does not (re-running on a half-applied dev DB is a no-op). The
-- critic seed lives in the same branch so it runs exactly once, before
-- any operator PATCH can diverge the two knobs.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name = 'runtime_config' AND column_name = 'llm_model')
       AND NOT EXISTS (SELECT 1 FROM information_schema.columns
                       WHERE table_name = 'runtime_config' AND column_name = 'llm_model_writer')
    THEN
        ALTER TABLE runtime_config RENAME COLUMN llm_model TO llm_model_writer;
        ALTER TABLE runtime_config
            ADD COLUMN llm_model_critic TEXT NOT NULL DEFAULT 'qwen3:14b';
        UPDATE runtime_config SET llm_model_critic = llm_model_writer;
    END IF;
END $$;

-- Extend audit field CHECK (precedent: 023, 218). 'llm_model' stays —
-- historical rows carry it; new writes use the split names only.
ALTER TABLE runtime_config_audit
    DROP CONSTRAINT IF EXISTS runtime_config_audit_field_check;
ALTER TABLE runtime_config_audit
    ADD CONSTRAINT runtime_config_audit_field_check
    CHECK (field IN ('enable_auto_trading', 'enable_live_trading',
                     'kill_switch', 'display_currency',
                     'llm_provider', 'llm_base_url', 'llm_model',
                     'llm_model_writer', 'llm_model_critic'));

-- Critic provenance on the run row: the CONFIGURED critic model (the
-- critic may fail before any provider response exists — same contract
-- as thesis_runs.model for the writer). Nullable: historical rows stay
-- NULL.
ALTER TABLE thesis_runs
    ADD COLUMN IF NOT EXISTS critic_model TEXT;

COMMIT;
