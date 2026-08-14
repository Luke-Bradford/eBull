-- 347_strategy_result_holding_period.sql
--
-- #2623 gap 1 — the operator's "expected turnaround" number.
-- Spec: docs/proposals/ta/2026-08-14-strategy-holding-period.md.
-- Derivation: app/services/strategy_statistics.py (`_hold_percentiles`).
-- Writer + reader: app/services/result_ledger.py.
--
--
-- ⚠⚠ POPULATE FORWARD ONLY. THIS MIGRATION DELIBERATELY BACKFILLS NOTHING.
-- ---------------------------------------------------------------------------
-- `strategy_results_store` holds 324 rows, every one written before the
-- holding period was carried through `TradeReturns`. There is no way to derive
-- it from a stored row — the per-trade exit dates were never persisted, only
-- aggregated away — so a backfill would mean RE-RUNNING the backtests. A re-run
-- mints results under current pins, which is the trial-register-charging path
-- #2599's declaration contract and #2616's pre-cutoff rerun gate govern. It is
-- not something a migration may trigger and not something an unattended run may
-- open. Existing rows read NULL, permanently, and that is correct.
--
--
-- ⚠⚠ WHICH IS WHY `metric_set_id` MOVED TO `criterion7-v2`.
-- ---------------------------------------------------------------------------
-- Without the bump, a NULL holding period on a row written TOMORROW would be
-- indistinguishable from a legitimate legacy NULL, so a writer defect would be
-- permanently invisible. With it the rule is exact, and the CHECK below is what
-- makes it enforceable rather than a convention:
--
--   * `criterion7-v1` row  -> NULL triple, legitimate and permanent.
--   * `criterion7-v2` row with realised trades -> the triple is REQUIRED.
--   * `criterion7-v2` row with `trade_count = 0` -> NULL triple, legitimate.
--
-- The bump is also just true: a version denotes a RULE SET, not a row
-- population (#2670), and a row carrying three metrics that `criterion7-v1`
-- never defined cannot honestly keep that stamp. Verified before bumping that
-- nothing gates on the VALUE — `metric_set_id` is written by
-- strategy_result.py, read back by result_ledger.py, and constrained only by
-- `CHECK (metric_set_id <> '')` in sql/263. No promotion rule, index or
-- comparison reads it.
--
-- ⚠ The CHECK is written with `<>` against a NOT NULL column (sql/263 sets it),
-- so the `NULL = 'x'` trap that #2603 item 2 admitted does not apply here. Said
-- out loud because that is exactly the shape that failed before: a constraint
-- passes unless its expression is FALSE, and `NULL <> 'x'` is NULL.
--
--
-- ⚠ NULLABLE WITH NO DEFAULT, WHICH IS WHAT MAKES THIS ROLLING-SAFE.
-- `strategy_backtest_run` is a live job (sql/335 measured it), so between this
-- migration applying and the daemon picking up the new code an old writer will
-- INSERT without these columns. That is legal against a nullable column, and
-- the CHECK still holds for it because that writer stamps `criterion7-v1`.

BEGIN;

-- ⚠ sql/335's lesson, inherited rather than rediscovered: a PENDING
-- AccessExclusiveLock queues AHEAD of new readers, so an ALTER merely waiting
-- behind the live backtest job blocks every subsequent SELECT on the relation.
-- A bounded timeout turns that into a clean, retryable failure.
SET LOCAL lock_timeout = '5s';

ALTER TABLE strategy_results_store
    ADD COLUMN IF NOT EXISTS median_hold_days NUMERIC,
    ADD COLUMN IF NOT EXISTS hold_days_p25    NUMERIC,
    ADD COLUMN IF NOT EXISTS hold_days_p75    NUMERIC;

COMMENT ON COLUMN strategy_results_store.median_hold_days IS
    'Median CALENDAR days held per REALISED trade (#2623 gap 1). ⚠ Calendar, not '
    'bars: strategy_outcomes.bars_held is the competing unit and is deliberately '
    'not followed, because five bars is a week or a fortnight depending on halts '
    'and holidays. Matches the unit the live path already reports in '
    'strategy_monitoring''s median_days_to_outcome — same unit, DIFFERENT '
    'population, so the two must never be labelled as one number. '
    '⚠⚠ RIGHT-CENSORED: positions still open at the window end contribute '
    'nothing, and the direction of that bias is NOT determinable a priori. Render '
    'open_trade_count AND unpriced_trade_count beside it, never the median alone.';

COMMENT ON COLUMN strategy_results_store.hold_days_p25 IS
    '25th percentile of calendar days held, linear interpolation (numpy '
    '"linear" == Postgres percentile_cont, chosen so this and the live '
    'median_days_to_outcome cannot disagree on identical data). Same censoring '
    'caveat as median_hold_days.';

COMMENT ON COLUMN strategy_results_store.hold_days_p75 IS
    '75th percentile of calendar days held. Same method and caveat as '
    'hold_days_p25.';

-- All-or-nothing: a partial triple means the derivation half-ran, which would
-- otherwise render as a single plausible number with no tell.
ALTER TABLE strategy_results_store
    ADD CONSTRAINT strategy_results_hold_all_or_nothing CHECK (
        num_nulls(median_hold_days, hold_days_p25, hold_days_p75) IN (0, 3)
    );

-- Ordered and non-negative. A same-day close holds for 0 days, which is legal.
ALTER TABLE strategy_results_store
    ADD CONSTRAINT strategy_results_hold_ordered CHECK (
        median_hold_days IS NULL
        OR (0 <= hold_days_p25 AND hold_days_p25 <= median_hold_days
            AND median_hold_days <= hold_days_p75)
    );

-- The provenance rule the metric_set_id bump exists to make enforceable.
ALTER TABLE strategy_results_store
    ADD CONSTRAINT strategy_results_hold_required_from_v2 CHECK (
        metric_set_id <> 'criterion7-v2'
        OR trade_count = 0
        OR median_hold_days IS NOT NULL
    );

-- ⚠ SELECT * is expanded at creation, so the view must be recreated to expose
-- the new columns, and the check option restored — CREATE OR REPLACE drops it
-- (sql/335's sequence, same reason).
CREATE OR REPLACE VIEW strategy_results AS
    SELECT *
    FROM strategy_results_store
    WHERE namespace = 'in_sample';

ALTER VIEW strategy_results SET (check_option = 'cascaded');

COMMIT;
