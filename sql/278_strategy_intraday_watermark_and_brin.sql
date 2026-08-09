-- 278_strategy_intraday_watermark_and_brin.sql
--
-- #2448 second measured correction. One btree primary key still projected the
-- declared retained tiers at ~2.28 GB (117 bytes heap + 72 bytes index/row).
-- A per-instrument monotonic watermark gives the single writer replay/duplicate
-- protection in O(number of instruments), while a BRIN index stays proportional
-- to page ranges rather than to the 12.05M retained bars. The writer inserts in
-- instrument/time order so the BRIN ranges remain selective.

ALTER TABLE strategy_intraday_bars
    DROP CONSTRAINT IF EXISTS strategy_intraday_bars_pkey;

CREATE INDEX IF NOT EXISTS idx_strategy_intraday_bars_instrument_time_brin
    ON strategy_intraday_bars USING BRIN (instrument_id, bar_time)
    WITH (pages_per_range = 16);

CREATE TABLE IF NOT EXISTS strategy_intraday_watermarks (
    timeframe      TEXT NOT NULL CHECK (timeframe IN ('30m', '5m', '1m')),
    instrument_id  BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    last_bar_time  TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (timeframe, instrument_id)
);

COMMENT ON TABLE strategy_intraday_watermarks IS
    'Monotonic per-tier/instrument frontier. The bounded intraday writer refuses bars at or behind it before append. #2448.';
