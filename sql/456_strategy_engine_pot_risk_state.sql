-- 456_strategy_engine_pot_risk_state.sql
-- #3541 slice 1: the drawdown gate measures the ENGINE POT, not the demo account.
-- One rolling time-weighted NAV index over the exact-owned book
-- (app/services/engine_pot_risk.py; spec
-- docs/proposals/execution/2026-10-02-3541-engine-pot-drawdown.md).
--
-- strategy_paper_account_risk_state (sql/288) is left in place and no longer
-- written. Its high water is in ACCOUNT dollars and cannot seed a pot index.

CREATE TABLE IF NOT EXISTS strategy_engine_pot_risk_state (
    id                  BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    epoch_started_at    TIMESTAMPTZ NOT NULL,
    nav_index           NUMERIC(30,16) NOT NULL CHECK (nav_index > 0),
    index_high_water    NUMERIC(30,16) NOT NULL,
    last_nav            NUMERIC(18,6) NOT NULL CHECK (last_nav > 0),
    last_pnl            NUMERIC(18,6) NOT NULL,
    last_drawdown_pct   NUMERIC(12,8) NOT NULL CHECK (last_drawdown_pct >= 0),
    observed_at         TIMESTAMPTZ NOT NULL,
    CONSTRAINT strategy_engine_pot_risk_state_high_water CHECK (index_high_water >= nav_index)
);
