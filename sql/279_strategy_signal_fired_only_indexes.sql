-- 279_strategy_signal_fired_only_indexes.sql
--
-- #2448: after sql/276 the durable strategy_signals writer stores FIRED rows
-- only. The per-instrument replay index has no production predicate consumer,
-- and exclusion counts now read strategy_signal_daily_counts. Replace the
-- strategy/version/date index with its fired-only form and remove the two write
-- amplification paths. The PK and immutable-decision UNIQUE constraint remain.

DROP INDEX IF EXISTS idx_strategy_signals_version_bar;
DROP INDEX IF EXISTS idx_strategy_signals_instrument_bar;
DROP INDEX IF EXISTS idx_strategy_signals_reason;

CREATE INDEX IF NOT EXISTS idx_strategy_signals_fired_version_bar
    ON strategy_signals (strategy_id, strategy_version, signal_bar_date)
    WHERE verdict = 'fired';

COMMENT ON INDEX idx_strategy_signals_fired_version_bar IS
    'Fired-only outcome/pending-result scan. Routine verdict counts moved to strategy_signal_daily_counts in #2448.';
