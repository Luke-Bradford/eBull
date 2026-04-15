-- Migration 025: add technical analysis indicator columns to price_daily
--
-- These columns store the latest-only computed TA values for each instrument.
-- Values are recomputed on every daily candle refresh (tail end of
-- _compute_and_store_features). Only the most recent price_date row per
-- instrument carries values; historical rows remain NULL.
--
-- Formula variants pinned for auditability:
--   RSI: Wilder smoothing (alpha = 1/period)
--   ATR: Wilder smoothing
--   EMA: seeded with SMA of first `period` values
--   Bollinger: population stddev (ddof=0)
--   Stochastic %K: (close - low14) / (high14 - low14) * 100
--   Stochastic %D: SMA(3) of %K

-- Trend indicators
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS sma_20 NUMERIC(18,6);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS sma_50 NUMERIC(18,6);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS sma_200 NUMERIC(18,6);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS ema_12 NUMERIC(18,6);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS ema_26 NUMERIC(18,6);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS macd_line NUMERIC(18,6);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS macd_signal NUMERIC(18,6);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS macd_histogram NUMERIC(18,6);

-- Momentum indicators
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS rsi_14 NUMERIC(10,4);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS stoch_k NUMERIC(10,4);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS stoch_d NUMERIC(10,4);

-- Volatility indicators
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS bb_upper NUMERIC(18,6);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS bb_lower NUMERIC(18,6);
ALTER TABLE price_daily ADD COLUMN IF NOT EXISTS atr_14 NUMERIC(18,6);
