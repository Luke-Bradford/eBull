-- 311_strategy_portfolio_mandate.sql
--
-- #2541 attaches one measurable, versioned portfolio-risk mandate to each
-- material shared-pot revision.  This is event-shaped storage: one row per
-- operator change, never a scheduler heartbeat.  Existing rows are explicitly
-- legacy-unconfigured and therefore cannot authorise new entries.

ALTER TABLE strategy_paper_pool_events
    ADD COLUMN IF NOT EXISTS mandate_policy_version TEXT NOT NULL
        DEFAULT 'portfolio-mandate-unconfigured',
    ADD COLUMN IF NOT EXISTS risk_profile TEXT NOT NULL DEFAULT 'unconfigured',
    ADD COLUMN IF NOT EXISTS target_volatility_pct NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS max_portfolio_drawdown_pct NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS max_loss_per_position_pct NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS max_daily_loss_pct NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS active_risk_budget_pct NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS cash_reserve_pct NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS max_concurrent_positions INTEGER,
    ADD COLUMN IF NOT EXISTS shorts_allowed BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS leverage_allowed BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE strategy_paper_pool_events
    DROP CONSTRAINT IF EXISTS strategy_paper_pool_enabled_has_mandate;

-- Do not place an enabled=>configured constraint on this append-only audit
-- table: a legacy enabled event must remain readable after deployment.  The
-- API and executor reject an unconfigured latest event for new authority.
ALTER TABLE strategy_paper_pool_events
    DROP CONSTRAINT IF EXISTS strategy_paper_pool_mandate_shape;
ALTER TABLE strategy_paper_pool_events
    ADD CONSTRAINT strategy_paper_pool_mandate_shape CHECK (
        (
            risk_profile = 'unconfigured'
            AND mandate_policy_version = 'portfolio-mandate-unconfigured'
            AND target_volatility_pct IS NULL
            AND max_portfolio_drawdown_pct IS NULL
            AND max_loss_per_position_pct IS NULL
            AND max_daily_loss_pct IS NULL
            AND active_risk_budget_pct IS NULL
            AND cash_reserve_pct IS NULL
            AND max_concurrent_positions IS NULL
            AND NOT shorts_allowed
            AND NOT leverage_allowed
        )
        OR
        (
            risk_profile IN ('cautious', 'balanced', 'growth')
            AND mandate_policy_version = 'portfolio-mandate-v1'
            AND target_volatility_pct > 0 AND target_volatility_pct <= 100
            AND max_portfolio_drawdown_pct > 0 AND max_portfolio_drawdown_pct < 100
            AND max_loss_per_position_pct > 0 AND max_loss_per_position_pct <= 100
            AND max_daily_loss_pct > 0 AND max_daily_loss_pct <= 100
            AND active_risk_budget_pct > 0 AND active_risk_budget_pct <= 100
            AND cash_reserve_pct >= 0 AND cash_reserve_pct < 100
            AND active_risk_budget_pct + cash_reserve_pct <= 100
            AND max_concurrent_positions > 0
            AND NOT shorts_allowed
            AND NOT leverage_allowed
        )
    );

COMMENT ON COLUMN strategy_paper_pool_events.risk_profile IS
    'Operator-facing mandate label. Exact policy limits are stored beside it; the label is not a return forecast.';
COMMENT ON COLUMN strategy_paper_pool_events.active_risk_budget_pct IS
    'Maximum share of effective pot capital available to active alpha risk; not a target allocation.';
COMMENT ON COLUMN strategy_paper_pool_events.cash_reserve_pct IS
    'Minimum uncommitted cash share of the effective pot under the selected mandate.';
COMMENT ON COLUMN strategy_paper_pool_events.shorts_allowed IS
    'False for portfolio-mandate-v1; requires an independently validated broker shortability/cost contract before a later policy may enable it.';
COMMENT ON COLUMN strategy_paper_pool_events.leverage_allowed IS
    'False for portfolio-mandate-v1; no current mandate may authorise leverage.';
