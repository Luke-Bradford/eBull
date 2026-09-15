-- 384_portfolio_eod_position_direction.sql
--
-- #3068 slice 2: record the DIRECTION of each local end-of-day position, so the
-- reconciliation can re-price that position at the broker's own mark without assuming
-- which way it points.
-- Writer: app/services/portfolio_eod.py (`PositionResult`, `_write_snapshot`).
-- Reader: app/services/account_equity_evidence.py (`load_account_equity_evidence`).
--
-- WHY A COLUMN AND NOT THE OFFICIAL ROW
-- ---------------------------------------------------------------------------
-- The v2 comparand prices our book at the broker's per-position mark (sql/383) by
-- correcting the stored local value:
--
--     amount + units * (mark_official - open_rate)
--       = [amount + units * (close - open_rate)]  +  units * (mark_official - close)
--         ^ already in portfolio_eod_snapshots.positions_value    ^ the correction
--
-- and for a SHORT the correction is `units * (close - mark_official)` instead. So the
-- sign is load-bearing, and `portfolio_eod_position_snapshots` did not carry it --
-- `portfolio_eod.compute_eod_equity` branches on `PositionInput.is_buy`, stores the
-- resulting value, and discards the flag.
--
-- ⚠⚠ `broker_account_position_marks.is_buy` is NOT an acceptable substitute. Reading the
-- direction off the OFFICIAL row to sign a LOCAL value assumes the two endpoints agree on
-- the direction of a position -- an unstated premise, in a slice whose entire subject is
-- an unstated premise (the two sides were being marked at different instants and the
-- tolerance had no term for it; #3068). With the column, local `is_buy` against official
-- `is_buy` is a real assertion and a disagreement refuses the day.
--
-- ⚠ FORWARD ONLY. NOTHING IS BACKFILLED. The direction could be re-read from
-- `broker_positions` today -- and that would be a RECONSTRUCTION, not an observation:
-- that table is overwritten by every `sync_portfolio` and carries no snapshot date, so a
-- position re-opened the other way since would hand a historical snapshot a direction it
-- never used. Rows predating this migration read NULL and REFUSE at the consumer
-- (`local_eod_position_direction_not_recorded`); they are never read as long. Same
-- posture as `unrealised_pnl_usd` (sql/308) and `mark_price_date` (sql/350).
--
-- ⚠ No CHECK ties `is_buy` to any other column. A short row is
-- `units > 0 AND is_buy = false` -- the sign lives in the flag, not in the quantity
-- (`portfolio_eod._read_positions` filters `units > 0` for both directions), so there is
-- no arithmetic relation between the two to constrain.

ALTER TABLE portfolio_eod_position_snapshots
    ADD COLUMN IF NOT EXISTS is_buy BOOLEAN;

COMMENT ON COLUMN portfolio_eod_position_snapshots.is_buy IS
    'Direction of the position as the local book valued it (broker_positions.is_buy at '
    'compute time): true = long. Signs the mark-substitution correction in the #3068 v2 '
    'reconciliation comparand, and is compared against broker_account_position_marks.is_buy '
    'as a two-endpoint check. NULL means the row predates sql/384 -- missing evidence, '
    'refused by the reader, never read as long.';
