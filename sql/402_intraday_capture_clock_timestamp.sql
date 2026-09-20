-- #2840 — `strategy_intraday_bars.captured_at` must bound the OBSERVATION, not the
-- transaction that happened to write it.
--
-- `sql/276` declared `captured_at TIMESTAMPTZ NOT NULL DEFAULT now()`, and `now()` is
-- `transaction_timestamp()` — the start of the enclosing transaction. `store_intraday_bars`
-- opens `conn.transaction()`, which is only a SAVEPOINT when the caller already holds one,
-- so a caller whose outer transaction began before the fetch stamps every bar with a time
-- EARLIER than it was observed.
--
-- ⚠⚠ THE DIRECTION IS THE UNSAFE ONE, WHICH IS WHY THIS IS A MIGRATION AND NOT A COMMENT.
-- `app/services/bar_capture_certificate.nominality_bucket` certifies a bar's level as
-- nominal when no session OPEN falls between the bar and its capture. A stamp earlier than
-- the true observation can place a post-open observation before the open, and the
-- certificate then says "no corporate action could have re-based this" about a bar for
-- which one could have. A stamp LATER than the observation can only refuse. So the column
-- must hold an upper bound on the observation instant, and `clock_timestamp()` — the actual
-- statement time, which necessarily follows the fetch — is one.
--
-- ⚠ Existing rows are NOT rewritten and cannot be: the true observation instant of a bar
-- already stored is not recoverable. Measured before this change (dev DB, 2026-09-20):
-- 29,234 rows over 7,183 distinct `captured_at`, batch writes sharing one timestamp across
-- up to 1,000 rows and 122 bar-days — the shape a transaction timestamp makes — and ZERO
-- rows whose `captured_at` precedes their own bar's completion. Reproduce with:
--
--   SELECT count(*), count(DISTINCT captured_at) FROM strategy_intraday_bars;
--   SELECT count(*) FROM strategy_intraday_bars b
--     JOIN (VALUES ('1m',1),('5m',5),('30m',30)) t(tf,mins) ON t.tf = b.timeframe
--    WHERE b.captured_at < b.bar_time + (t.mins || ' minutes')::interval;
--
-- ⚠ That second query is the ONLY detectable form of the defect and it is clean. The
-- undetectable form — a stamp between bar completion and the next open for an observation
-- that actually followed the open — is exactly why the fix is structural and not a
-- measurement, and why this migration is not accompanied by a backfill.
--
-- ⚠ `clock_timestamp()` is non-deterministic, so it is legal in a DEFAULT and illegal in an
-- index or a generated column. Nothing here uses it in either.
--
-- Refs #2840, #2437. Schema it amends: sql/276_strategy_observation_storage.sql.

-- ⚠⚠ BOUNDED LOCK WAIT, and it is not optional on this relation. `ALTER TABLE ... SET
-- DEFAULT` takes `ACCESS EXCLUSIVE`, and a PENDING `AccessExclusiveLock` queues AHEAD OF
-- NEW READERS — so a migration that is merely waiting stops every subsequent `SELECT` on
-- `strategy_intraday_bars` from starting. The intraday harvester and the census both hold
-- read locks on it, and the FastAPI lifespan runs migrations, so an unbounded wait hangs
-- app boot rather than failing it. `lock_timeout` converts that into a clean, retryable
-- `LockNotAvailable` bounded to five seconds per attempt; apply in a quiet window.
-- Precedent and the full mechanism: `docs/review-prevention-log.md`, *"A migration that is
-- WAITING for a lock is not passive"* (#2363), and `sql/335`.
SET LOCAL lock_timeout = '5s';

ALTER TABLE strategy_intraday_bars
    ALTER COLUMN captured_at SET DEFAULT clock_timestamp();

COMMENT ON COLUMN strategy_intraday_bars.captured_at IS
    'Upper bound on when this bar was first observed by our collector. clock_timestamp(), '
    'not now(): a transaction timestamp can precede the observation and would let '
    'bar_capture_certificate certify a post-open observation as pre-open. #2840.';
