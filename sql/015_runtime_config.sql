-- Migration 015: runtime config — DB-backed source of truth for trading flags
--
-- Replaces the env-sourced settings.enable_auto_trading / enable_live_trading.
-- Pattern mirrors kill_switch (010): a typed singleton row guarded by a CHECK.
-- Missing row at runtime is treated as configuration corruption (fail closed)
-- by execution_guard, order_client, and the /config endpoints.
--
-- runtime_config_audit captures every mutation of either runtime_config or
-- kill_switch.  One row per changed field.  No reuse of decision_audit
-- (which is reserved for trade-decision auditability per settled-decisions).

CREATE TABLE IF NOT EXISTS runtime_config (
    id                  BOOLEAN PRIMARY KEY DEFAULT TRUE,
    enable_auto_trading BOOLEAN     NOT NULL,
    enable_live_trading BOOLEAN     NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by          TEXT        NOT NULL,
    reason              TEXT        NOT NULL,
    CONSTRAINT runtime_config_single_row CHECK (id = TRUE)
);

-- Seed singleton with safe defaults: both flags off.  Operators must
-- explicitly enable via PATCH /config; the env file no longer controls these.
INSERT INTO runtime_config (id, enable_auto_trading, enable_live_trading, updated_by, reason)
VALUES (TRUE, FALSE, FALSE, 'migration', 'initial seed')
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS runtime_config_audit (
    audit_id    BIGSERIAL PRIMARY KEY,
    changed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    changed_by  TEXT        NOT NULL,
    reason      TEXT        NOT NULL,
    field       TEXT        NOT NULL,   -- 'enable_auto_trading' | 'enable_live_trading' | 'kill_switch'
    old_value   TEXT,                   -- nullable: first-ever rows / kill_switch first activation
    new_value   TEXT        NOT NULL,
    CONSTRAINT runtime_config_audit_field_check
        CHECK (field IN ('enable_auto_trading', 'enable_live_trading', 'kill_switch'))
);

CREATE INDEX IF NOT EXISTS idx_runtime_config_audit_changed_at
    ON runtime_config_audit(changed_at DESC);

CREATE INDEX IF NOT EXISTS idx_runtime_config_audit_field
    ON runtime_config_audit(field, changed_at DESC);
