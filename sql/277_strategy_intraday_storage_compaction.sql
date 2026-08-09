-- 277_strategy_intraday_storage_compaction.sql
--
-- #2448 benchmark correction. The first candidate measured 247.9 bytes/row
-- including indexes, projecting 2.99 GB at the declared 24m/12m/30d caps.
-- Two indexes carried the same three keys in different orders. Keep one primary
-- key ordered for the real consumer (timeframe + instrument + time range), and
-- use fixed-width doubles for observational OHLCV values. Strategy maths still
-- converts to Decimal at its decision boundary; this table is not an order or
-- accounting ledger.

DROP INDEX IF EXISTS idx_strategy_intraday_bars_instrument_time;

ALTER TABLE strategy_intraday_bars
    DROP CONSTRAINT IF EXISTS strategy_intraday_bars_pkey;

ALTER TABLE strategy_intraday_bars
    ALTER COLUMN open TYPE DOUBLE PRECISION USING open::double precision,
    ALTER COLUMN high TYPE DOUBLE PRECISION USING high::double precision,
    ALTER COLUMN low TYPE DOUBLE PRECISION USING low::double precision,
    ALTER COLUMN close TYPE DOUBLE PRECISION USING close::double precision,
    ALTER COLUMN volume TYPE DOUBLE PRECISION USING volume::double precision;

ALTER TABLE strategy_intraday_bars
    ADD CONSTRAINT strategy_intraday_bars_pkey
    PRIMARY KEY (timeframe, instrument_id, bar_time);

COMMENT ON CONSTRAINT strategy_intraday_bars_pkey ON strategy_intraday_bars IS
    'Single uniqueness/read index: timeframe + instrument range scans ordered by bar_time. #2448 measured replacement for two-key-order indexes.';
