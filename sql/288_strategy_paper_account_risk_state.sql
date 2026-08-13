-- 288_strategy_paper_account_risk_state.sql
-- #2449: one rolling demo-account high-water mark. It starts when paper
-- automation is configured and updates in place; no account-value series.

CREATE TABLE IF NOT EXISTS strategy_paper_account_risk_state (
    id                  BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    equity_high_water   NUMERIC(18,6) NOT NULL CHECK (equity_high_water > 0),
    last_equity         NUMERIC(18,6) NOT NULL CHECK (last_equity > 0),
    last_drawdown_pct   NUMERIC(12,8) NOT NULL CHECK (last_drawdown_pct >= 0),
    observed_at         TIMESTAMPTZ NOT NULL
);
