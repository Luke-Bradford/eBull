-- 444_broker_account_position_marks_total_fees.sql
--
-- #3540 (spec docs/proposals/execution/2026-10-01-3540-engine-nav-bridge.md, "Fees and
-- distributions"). The engine NAV bridge needs the broker's fee-and-distribution counter per
-- position PER DAY; today it exists only on the current-state `broker_positions` row, so a
-- dividend or financing charge between two snapshots is invisible.
--
-- Source rule: `TradingDemoApi_Position.totalFees` -- "Total overnight fees and dividends
-- charged/paid on the position in USD. Negative amount represents refund"
-- (tests/fixtures/etoro/openapi_v1.375.0.json), read from the same `/pnl` position row the
-- mark already comes from.
--
-- Nullable and never backfilled: a historical value cannot be inferred from today's (refunds
-- make the counter non-monotone), so history stays NULL and the bridge names it
-- `fees_unobserved`.

ALTER TABLE broker_account_position_marks ADD COLUMN IF NOT EXISTS total_fees NUMERIC;

COMMENT ON COLUMN broker_account_position_marks.total_fees IS
    '#3540 broker totalFees (overnight fees AND distributions, signed, USD) at this snapshot. NULL = unobserved.';
