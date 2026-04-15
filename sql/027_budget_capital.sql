-- Migration 027: budget and capital management tables
--
-- capital_events: operator-facing ledger for cash injections, withdrawals,
--   and system-managed tax provisions/releases.  amount is always positive;
--   event_type carries the directional semantics.
--
-- budget_config: singleton row (same pattern as runtime_config) holding
--   operator-level budget preferences: cash buffer percentage and the CGT
--   scenario used for tax provisioning.
--
-- budget_config_audit: per-field audit trail for every mutation of
--   budget_config, mirroring runtime_config_audit.

CREATE TABLE IF NOT EXISTS capital_events (
    event_id    BIGSERIAL    PRIMARY KEY,
    event_time  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    event_type  TEXT         NOT NULL,
    amount      NUMERIC(18,6) NOT NULL,
    currency    TEXT         NOT NULL DEFAULT 'USD',
    source      TEXT         NOT NULL DEFAULT 'operator',
    note        TEXT,
    created_by  TEXT
);

-- Idempotent CHECK constraints for capital_events.
DO $$
BEGIN
    ALTER TABLE capital_events
        DROP CONSTRAINT IF EXISTS chk_capital_events_event_type;
    ALTER TABLE capital_events
        ADD CONSTRAINT chk_capital_events_event_type
        CHECK (event_type IN ('injection', 'withdrawal', 'tax_provision', 'tax_release'));

    ALTER TABLE capital_events
        DROP CONSTRAINT IF EXISTS chk_capital_events_amount;
    ALTER TABLE capital_events
        ADD CONSTRAINT chk_capital_events_amount
        CHECK (amount > 0);

    ALTER TABLE capital_events
        DROP CONSTRAINT IF EXISTS chk_capital_events_currency;
    ALTER TABLE capital_events
        ADD CONSTRAINT chk_capital_events_currency
        CHECK (currency IN ('USD', 'GBP'));

    ALTER TABLE capital_events
        DROP CONSTRAINT IF EXISTS chk_capital_events_source;
    ALTER TABLE capital_events
        ADD CONSTRAINT chk_capital_events_source
        CHECK (source IN ('operator', 'system', 'broker_sync'));
END $$;

CREATE INDEX IF NOT EXISTS idx_capital_events_event_time
    ON capital_events (event_time DESC);

CREATE INDEX IF NOT EXISTS idx_capital_events_event_type
    ON capital_events (event_type, event_time DESC);

-- Singleton budget config.  id BOOLEAN PRIMARY KEY DEFAULT TRUE + CHECK id = TRUE
-- enforces at most one row, matching the runtime_config pattern.
CREATE TABLE IF NOT EXISTS budget_config (
    id               BOOLEAN      PRIMARY KEY DEFAULT TRUE,
    cash_buffer_pct  NUMERIC(5,4) NOT NULL DEFAULT 0.05,
    cgt_scenario     TEXT         NOT NULL DEFAULT 'higher',
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_by       TEXT         NOT NULL DEFAULT 'system',
    reason           TEXT         NOT NULL DEFAULT 'initial seed',
    CONSTRAINT budget_config_single_row CHECK (id = TRUE)
);

-- Idempotent CHECK constraints for budget_config.
DO $$
BEGIN
    ALTER TABLE budget_config
        DROP CONSTRAINT IF EXISTS chk_budget_config_cash_buffer_pct;
    ALTER TABLE budget_config
        ADD CONSTRAINT chk_budget_config_cash_buffer_pct
        CHECK (cash_buffer_pct >= 0 AND cash_buffer_pct <= 0.50);

    ALTER TABLE budget_config
        DROP CONSTRAINT IF EXISTS chk_budget_config_cgt_scenario;
    ALTER TABLE budget_config
        ADD CONSTRAINT chk_budget_config_cgt_scenario
        CHECK (cgt_scenario IN ('basic', 'higher'));
END $$;

-- Seed singleton row with safe defaults.
INSERT INTO budget_config (id)
VALUES (TRUE)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS budget_config_audit (
    audit_id    BIGSERIAL    PRIMARY KEY,
    changed_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    changed_by  TEXT         NOT NULL,
    field       TEXT         NOT NULL,
    old_value   TEXT,
    new_value   TEXT,
    reason      TEXT
);

CREATE INDEX IF NOT EXISTS idx_budget_config_audit_changed_at
    ON budget_config_audit (changed_at DESC);

CREATE INDEX IF NOT EXISTS idx_budget_config_audit_field
    ON budget_config_audit (field, changed_at DESC);
