-- 345_price_daily_price_date_index.sql
--
-- #2624 scope 3.  `strategy_scan_freshness` needs the most recent N distinct
-- trading dates on every `/system/status` poll, and `price_daily`'s only
-- price_date-bearing index is the PK `(instrument_id, price_date)` -- wrong
-- leading column, so a date-first query cannot use it.
--
-- Measured on dev BEFORE this index (6,755,721 rows, 1,995 distinct dates):
--
--   EXPLAIN (ANALYZE, BUFFERS)
--   SELECT price_date FROM (
--       SELECT DISTINCT price_date FROM price_daily ORDER BY price_date DESC LIMIT 30
--   ) r ORDER BY price_date;
--
--   ->  Parallel Seq Scan on price_daily  (actual rows=2,251,907 loops=3)
--       Buffers: shared hit=335 read=69830
--   Execution Time: 279.348 ms
--
-- A full-corpus seq scan on an operator health endpoint that is polled is a
-- regression that grows with the corpus.  Raised by Codex at checkpoint 2.
--
-- ⚠ THE INDEX ALONE DOES NOT FIX IT, and measuring the remedy is what showed
-- that.  With the index the same query becomes Limit -> Unique -> Index Scan
-- Backward, which stops after the 30th distinct date but still walks EVERY ROW
-- of each of those dates -- ~3,400 instruments each:
--
--   Buffers: shared hit=83781 read=9953   Execution: 57 ms warm / 523 ms cold
--
-- i.e. it trades a cost that grows with the corpus for one that grows with
-- instruments-per-date, which is the same regression a season later.  The
-- consumer therefore uses a LOOSE INDEX SCAN (a recursive `max()` walk, see
-- `strategy_scan_freshness._RECENT_TRADING_DATES`), which does exactly N probes:
--
--   Buffers: shared hit=129 read=1        Execution: 1.0 ms
--
-- This index is what makes those probes index-only; the recursion is what makes
-- them 30 instead of 100,000.  Neither works without the other.
--
-- Deliberately NOT a covering/INCLUDE index and not composite: the only consumer
-- reads the date alone, and every extra column would be paid on every
-- price_daily write for nothing.  `price_daily` is append-mostly on a daily
-- cadence, so the write cost of one narrow index is a single-digit-percent
-- overhead on the daily refresh, not a per-tick cost.
--
-- ⚠ NOT `CREATE INDEX CONCURRENTLY`: this runner applies each migration inside a
-- transaction (app/db/migrations.py:194) and CONCURRENTLY cannot run in one.
-- The blocking build is acceptable at this size -- measured 3.4s on dev -- and
-- the alternative is the `-- runner: autocommit` directive, which trades the
-- migration's own atomicity for a lock this table does not need protecting from
-- at the point migrations run (boot, before the schedulers start).

CREATE INDEX IF NOT EXISTS idx_price_daily_price_date
    ON price_daily (price_date);

COMMENT ON INDEX idx_price_daily_price_date IS
    'Date-leading companion to the (instrument_id, price_date) PK. Added for '
    '#2624 scope 3, whose recent-distinct-trading-dates probe runs on every '
    '/system/status poll and seq-scanned the whole corpus without it (279ms, '
    'measured). Also usable by any corpus-frontier query that leads on date.';
