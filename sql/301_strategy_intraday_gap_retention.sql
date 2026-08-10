-- 301_strategy_intraday_gap_retention.sql
--
-- #2477 -- gap metadata follows the same tier horizon as the bars it
-- describes. This compact index supports the daily bounded delete; bars still
-- expire by whole partition and retain their BRIN-only read shape.

CREATE INDEX IF NOT EXISTS idx_strategy_intraday_gaps_timeframe_end
    ON strategy_intraday_gaps (timeframe, gap_end);
