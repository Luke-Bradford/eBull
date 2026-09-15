-- 383_broker_account_position_marks.sql
--
-- #3068: retain the OFFICIAL per-position terms of each account-equity snapshot, so a
-- reconciliation can price our book at the broker's own mark instead of at a mark struck
-- at a different time.
--
-- The defect this exists for.  `account_reconciliation_check` (#2844 clause 3) compares
-- `official_direct_long_market_value + available_cash` -- taken from the P&L endpoint at
-- 23:55 UTC, inside the US extended-hours session -- against `portfolio_eod_snapshots`,
-- which values the same holdings at the REGULAR-SESSION close.  The tolerance
-- (`account_equity_evidence.RECONCILIATION_RULE_VERSION = 'f0-reconcile-v1'`) is one cent
-- per unit held plus one cent of cash: it models rounding of one mark against the SAME
-- mark, and carries no term for the two sides being marked at different instants.
-- Measured 2026-09-14, the countdown's first session: `diverged`, difference -204.65
-- against tolerance 31.56, no incomplete reasons -- and the whole of it attributed to two
-- evening-quoted names (GME -195.00, QQQ -9.66) against eToro's own intraday bars at the
-- snapshot instant.  Full write-up: #3068 and
-- `docs/proposals/ops/2026-09-15-3068-reconciliation-mark-instant.md`.
--
-- Source rule.  No regulator fixes a broker-reconciliation tolerance (searched and
-- recorded at `RECONCILIATION_RULE_VERSION`; an earlier draft's SEC Reg NMS Rule 612
-- citation was withdrawn as off-point).  The FIELD semantics are not exempt from citation
-- and are governed by eToro's published contracts.  Every column below comes from
-- `TradingRealAdminApi_Position`, the item schema of `ClientPortfolio.positions` in BOTH
-- `PortfolioResponse` and `PortfolioResponseWithPnl` (committed
-- `tests/fixtures/etoro/openapi_v1.375.0.json`):
--   units                 "Number of units in the position"      -- CURRENT quantity, and
--                         distinct from `initialUnits`, which the same schema documents
--                         as not changing
--   amount                "USD amount allocated to the position"
--   unrealizedPnL.pnL     "only present in PnL endpoint"
--   isBuy                 "true for long (buy) positions"
--   isPartiallyAltered    "whether this position was partially closed"
--
-- ⚠⚠ This deliberately crosses a boundary the parent table states, so it is stated back.
-- `broker_account_equity_snapshots` comments that "per-position facts are deliberately not
-- retained", and #2559 scoped it as "no raw payload and no per-position duplication …
-- without creating a second position/price warehouse".  That intent is DUPLICATION, and
-- this is not duplication: `broker_positions` carries `units`, `amount` and `open_rate`
-- but no P&L and no market value, and every sync overwrites it -- so the official mark at
-- a past snapshot instant is recoverable from nowhere.  It is also not a warehouse: one
-- row per open direct position per day, seven on the current account, which keeps #2559's
-- "realistic annual storage remains in kilobytes" acceptance true.  No raw payload is
-- stored here either.
--
-- ⚠ `instrument_id` is NOT foreign-keyed to `instruments`.  The broker can report a
-- holding in an instrument our universe has not synced, and refusing the write would
-- discard official evidence to protect a join.  The same posture the parent takes with an
-- undocumented `account_currency_id` (sql/341).
--
-- ⚠ No backfill, and none is possible: the payload is not retained and the marks are
-- instantaneous.  Snapshots written before this migration have no child rows, which is
-- MISSING EVIDENCE and must read as a refusal at the consumer, never as an empty book --
-- an empty official book would reconcile against an empty local one and manufacture a
-- green.
--
-- ⚠⚠ "No child rows" is AMBIGUOUS on its own: a legacy snapshot and an account that
-- genuinely holds nothing both store zero.  The discriminator is already on the parent
-- and no column is added for it -- a post-migration row's child count MUST equal
-- `official_direct_long_positions + official_direct_short_positions` (the writer inserts
-- one row per direct position, shorts included).  So:
--     children = 0 AND long+short = 0        -> a real empty book
--     children = 0 AND (long+short > 0 OR either IS NULL) -> missing evidence, refuse
-- It is not a CHECK because the two are written to different tables in one transaction;
-- it is the consumer's precondition, asserted in this slice's tests.
--
-- Population at authoring time (dev, the only database):
--   select count(*), count(official_direct_long_positions) from broker_account_equity_snapshots;
--   -- (20, 9): 20 demo rows 2026-08-11..09-15, of which 11 predate the count columns
--   --          (sql/363) and are therefore missing evidence under the rule above.
--
-- This migration changes NO verdict.  It is the evidence the fix needs; the comparand and
-- the `RECONCILIATION_RULE_VERSION` bump are a separate slice, so this one cannot
-- accidentally decide a countdown day.

CREATE TABLE broker_account_position_marks (
    environment         TEXT NOT NULL,
    snapshot_date       DATE NOT NULL,
    position_id         BIGINT NOT NULL,
    instrument_id       INTEGER NOT NULL,
    is_buy              BOOLEAN NOT NULL,
    units               NUMERIC(20,8) NOT NULL CHECK (units > 0),
    amount              NUMERIC(20,6) NOT NULL,
    unrealized_pnl      NUMERIC(20,6) NOT NULL,
    market_value        NUMERIC(20,6) NOT NULL,
    is_partially_altered BOOLEAN NOT NULL,
    PRIMARY KEY (environment, snapshot_date, position_id),
    FOREIGN KEY (environment, snapshot_date)
        REFERENCES broker_account_equity_snapshots (environment, snapshot_date)
        ON DELETE CASCADE,
    -- The identity the derived mark rests on. Stored rather than recomputed so a row
    -- that violates it can never be read back as a mark.
    CHECK (abs(market_value - (amount + unrealized_pnl)) <= 0.000001)
);

COMMENT ON TABLE broker_account_position_marks IS
    'Official per-position terms of one account-equity snapshot. (amount + unrealized_pnl) / units is that position''s mark AT the snapshot instant (#3068). Child rows are replaced wholesale exactly when the parent snapshot accepts a write. Zero children is ambiguous on its own: it is a real empty book only when the parent''s official_direct_long_positions + official_direct_short_positions is also 0, and missing evidence otherwise.';

COMMENT ON COLUMN broker_account_position_marks.units IS
    'Current quantity (portal: TradingRealAdminApi_Position.units). NOT initialUnits, which the portal documents as unchanging. Divisor of the derived mark, hence CHECK (units > 0).';
