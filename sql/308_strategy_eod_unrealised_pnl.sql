-- 308_strategy_eod_unrealised_pnl.sql
--
-- F-0 portfolio truth: preserve the USD unrealised P&L already calculated
-- while the broker position exists.  Exact strategy ownership can then join
-- this compact, once-per-session evidence without copying quotes, features or
-- broker payloads into another strategy-specific time series.

ALTER TABLE portfolio_eod_position_snapshots
    ADD COLUMN IF NOT EXISTS unrealised_pnl_usd NUMERIC(20,4);

COMMENT ON COLUMN portfolio_eod_position_snapshots.unrealised_pnl_usd IS
    'Causal EOD mark minus open basis in broker USD, using the position open conversion rate. NULL means the mark was unavailable; it must never be read as zero.';
