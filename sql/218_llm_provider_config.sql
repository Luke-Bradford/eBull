-- 218: BYO-LLM provider config + thesis audit columns + thesis_runs (#1919 PR-A)
--
-- Spec: docs/specs/thesis/2026-07-09-byo-llm-thesis-live.md
--
-- Altered tables:
--   runtime_config       — llm_provider / llm_base_url / llm_model knobs
--                          (local-first default per operator mandate 2026-07-09;
--                          keys stay env-only, NEVER in this table — the audit
--                          table stores old/new values in plaintext)
--   runtime_config_audit — extend field CHECK for the three new knobs
--   theses               — model / provider / prompt_version audit columns
--                          (nullable; dev has 0 historical rows)
--
-- New tables:
--   thesis_runs          — one row per generation attempt (all trigger paths);
--                          in-flight indicator + failure surface for #1902/#1901

BEGIN;

-- Operator-editable LLM provider knobs (singleton pattern per 015/023).
ALTER TABLE runtime_config
    ADD COLUMN IF NOT EXISTS llm_provider TEXT NOT NULL DEFAULT 'openai_compatible',
    ADD COLUMN IF NOT EXISTS llm_base_url TEXT NOT NULL DEFAULT 'http://localhost:11434/v1',
    ADD COLUMN IF NOT EXISTS llm_model    TEXT NOT NULL DEFAULT 'qwen3:14b';

ALTER TABLE runtime_config
    DROP CONSTRAINT IF EXISTS runtime_config_llm_provider_check;
ALTER TABLE runtime_config
    ADD CONSTRAINT runtime_config_llm_provider_check
    CHECK (llm_provider IN ('openai_compatible', 'anthropic'));

-- Extend audit field CHECK (precedent: 023 display_currency).
ALTER TABLE runtime_config_audit
    DROP CONSTRAINT IF EXISTS runtime_config_audit_field_check;
ALTER TABLE runtime_config_audit
    ADD CONSTRAINT runtime_config_audit_field_check
    CHECK (field IN ('enable_auto_trading', 'enable_live_trading',
                     'kill_switch', 'display_currency',
                     'llm_provider', 'llm_base_url', 'llm_model'));

-- Thesis provenance: which provider/model/prompt produced each memo
-- ("version model outputs where required"). Nullable — historical rows
-- (none exist on dev) stay NULL. No model_version semantics touched.
ALTER TABLE theses
    ADD COLUMN IF NOT EXISTS model          TEXT,
    ADD COLUMN IF NOT EXISTS provider       TEXT,
    ADD COLUMN IF NOT EXISTS prompt_version TEXT;

-- One row per generation attempt, all three trigger paths. Replaces
-- log-only failure handling; feeds #1902 (in-flight/failure column)
-- and #1901 (cockpit) with zero further backend work.
CREATE TABLE IF NOT EXISTS thesis_runs (
    run_id        BIGSERIAL PRIMARY KEY,
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    trigger       TEXT NOT NULL CHECK (trigger IN ('manual', 'cascade', 'scheduled')),
    started_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at   TIMESTAMPTZ,
    status        TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'ok', 'failed')),
    error         TEXT,                -- failure text + finish_reason on failure
    provider      TEXT,
    model         TEXT,
    thesis_id     BIGINT REFERENCES theses(thesis_id)
);

-- #1902 reads "latest run per instrument" for the status column.
CREATE INDEX IF NOT EXISTS idx_thesis_runs_instrument_started
    ON thesis_runs(instrument_id, started_at DESC);

COMMIT;
