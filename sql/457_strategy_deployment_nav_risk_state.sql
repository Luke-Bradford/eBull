-- 457_strategy_deployment_nav_risk_state.sql
-- #3541 slice 3: each paper deployment's drawdown is measured on its OWN book, not the
-- demo account. Same time-weighted NAV index as the engine pot (sql/456,
-- app/services/engine_pot_risk.py; spec
-- docs/proposals/execution/2026-10-02-3541-deployment-nav-risk.md), per deployment:
-- NAV = capital_limit in force + realised + unrealised over its allocated trades.
--
-- strategy_paper_deployment_risk_state (sql/290) is left in place and no longer written
-- or read. Its high water is in ACCOUNT dollars and cannot seed a deployment index.

CREATE TABLE IF NOT EXISTS strategy_deployment_nav_risk_state (
    deployment_id       BIGINT PRIMARY KEY
        REFERENCES strategy_deployments(deployment_id) ON DELETE RESTRICT,
    nav_index           NUMERIC(30,16) NOT NULL CHECK (nav_index > 0),
    index_high_water    NUMERIC(30,16) NOT NULL CHECK (index_high_water > 0),
    last_nav            NUMERIC(18,6) NOT NULL CHECK (last_nav > 0),
    last_pnl            NUMERIC(18,6) NOT NULL,
    last_drawdown_pct   NUMERIC(12,8) NOT NULL CHECK (last_drawdown_pct >= 0),
    max_drawdown_pct    NUMERIC(12,8) NOT NULL,
    last_refusal        TEXT CHECK (last_refusal IS NULL OR char_length(last_refusal) BETWEEN 1 AND 1000),
    observed_at         TIMESTAMPTZ NOT NULL,
    CONSTRAINT strategy_deployment_nav_risk_state_high_water CHECK (index_high_water >= nav_index),
    CONSTRAINT strategy_deployment_nav_risk_state_max CHECK (max_drawdown_pct >= last_drawdown_pct)
);

COMMENT ON TABLE strategy_deployment_nav_risk_state IS
    'One update-in-place NAV index per paper deployment over its own allocated book; '
    'max_drawdown_pct is the paper-period maximum the live gate reads. A non-NULL '
    'last_refusal means the latest observation failed and the row is not evidence.';

COMMENT ON TABLE strategy_paper_deployment_risk_state IS
    'Superseded by strategy_deployment_nav_risk_state (#3541 slice 3): account-equity '
    'basis, no longer written or read.';

COMMENT ON COLUMN strategy_entry_preflights.account_drawdown_pct IS
    'Engine-pot drawdown percent at sizing since #3541 slice 1 (85e666c0); the name '
    'predates it. Not the demo account''s drawdown.';
