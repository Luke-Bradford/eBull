-- 263_strategy_result_metrics.sql
--
-- Phase 5d — criterion 7's metric set on the result row.
-- Spec: docs/proposals/ta/2026-08-07-bounded-backtester.md §8 (stage 5d), §5.4
-- (three levels, exposure, the return denominator), §3.4 (what an ambiguous or
-- open position does to each statistic). Parent criterion 7. Engine:
-- app/services/equity_curve.py + app/services/strategy_statistics.py.
-- The row this extends: sql/262.
--
--
-- ⚠⚠ COLUMNS, NOT A JSONB BLOB — sql/262's header already committed to this
-- ---------------------------------------------------------------------------
-- Criterion 7 names TWELVE metrics and says "a result missing any of the twelve
-- is incomplete". A JSONB column cannot express that: a partial metric set
-- would store silently and read as a complete result. So each metric is its own
-- column, and every one whose value is always computable is NOT NULL.
--
--
-- ⚠⚠ THE THREE NULLABLE METRICS, AND WHY EACH NULL IS A DIFFERENT STATEMENT
-- ---------------------------------------------------------------------------
-- effective_sample_size  — criterion 3's block bootstrap is STAGE 5e (spec §8).
--   Null here, and strategy_result.check_promotable refuses on it. ⚠ Filling it
--   with a nominal n would be worse than leaving it null: criterion 3 says "no
--   bare percentage and no nominal n is reported anywhere", so an overlap-
--   ignoring count would be the exact number the criterion forbids, wearing the
--   name of the one it requires.
-- profit_factor          — null exactly when there was no LOSING TRADE, so the
--   denominator is empty. Tied to losing_trade_count by CHECK.
-- sortino                — null exactly when there was no LOSING PERIOD, so the
--   downside deviation has no observations. Tied to losing_period_count by
--   CHECK.
--
-- The two CHECKs are the point: without them "not computed" and "denominator
-- empty" are the same NULL, and a reader cannot tell a missing measurement from
-- a real one.
--
--
-- ⚠ WHY THE SUPPORTING COUNTS ARE HERE AND ARE NOT "EXTRA"
-- ---------------------------------------------------------------------------
-- losing_trade_count / losing_period_count make the two CHECKs above
-- expressible. open_trade_count / unpriced_trade_count are §3.4's "excluded,
-- counted" — an ambiguous close and a position open at the window end are out
-- of the win rate and out of expectancy but IN exposure and ON the curve, so a
-- trade_count read without them understates the capital that was committed.
-- periods_per_year is the measured annualisation every annualised column is a
-- function of, and a reader cannot re-derive it from the row.
--
--
-- ⚠ NO DEFAULT ON ANY NOT NULL COLUMN, and that is sql/262's clause-2 argument
-- applied one layer down: "a column with a default is a column a writer can
-- forget". Measured 2026-08-07 before this migration: strategy_results holds
-- 0 rows, so NOT NULL costs nothing today and would cost an invented history
-- after 5d starts writing.

ALTER TABLE strategy_results
    -- Criterion 7's twelve. Percent where the name says pct; a bare ratio
    -- otherwise (sharpe, sortino, profit_factor, turnover are unitless).
    ADD COLUMN IF NOT EXISTS expectancy_per_trade_pct   NUMERIC,
    ADD COLUMN IF NOT EXISTS profit_factor              NUMERIC,
    ADD COLUMN IF NOT EXISTS cagr_pct                   NUMERIC,
    ADD COLUMN IF NOT EXISTS annualised_volatility_pct  NUMERIC,
    ADD COLUMN IF NOT EXISTS sharpe                     NUMERIC,
    ADD COLUMN IF NOT EXISTS sortino                    NUMERIC,
    ADD COLUMN IF NOT EXISTS max_drawdown_pct           NUMERIC,
    ADD COLUMN IF NOT EXISTS exposure_time_pct          NUMERIC,
    ADD COLUMN IF NOT EXISTS turnover_annualised        NUMERIC,
    ADD COLUMN IF NOT EXISTS trade_count                INTEGER,
    ADD COLUMN IF NOT EXISTS effective_sample_size      NUMERIC,
    ADD COLUMN IF NOT EXISTS return_vs_buy_and_hold_pct NUMERIC,
    -- The supporting record.
    ADD COLUMN IF NOT EXISTS losing_trade_count         INTEGER,
    ADD COLUMN IF NOT EXISTS losing_period_count        INTEGER,
    ADD COLUMN IF NOT EXISTS open_trade_count           INTEGER,
    ADD COLUMN IF NOT EXISTS unpriced_trade_count       INTEGER,
    ADD COLUMN IF NOT EXISTS periods_per_year           NUMERIC,
    ADD COLUMN IF NOT EXISTS total_return_pct           NUMERIC,
    ADD COLUMN IF NOT EXISTS buy_and_hold_return_pct    NUMERIC,
    -- strategy_statistics.METRIC_SET_ID. ⚠ A change to any metric DEFINITION is
    -- a change to what a stored number means, and a reader holding an old row
    -- needs to know which definition produced it. Same argument as
    -- cost_model_id on sql/262.
    ADD COLUMN IF NOT EXISTS metric_set_id              TEXT;

-- ⚠ SET NOT NULL as a SECOND statement, deliberately: `ADD COLUMN ... NOT NULL`
-- with no default fails outright on a populated table, so the two-step form is
-- the one that stays correct if this ever runs against rows. It is a no-op
-- today (0 rows) and remains the honest shape.
ALTER TABLE strategy_results
    ALTER COLUMN expectancy_per_trade_pct   SET NOT NULL,
    ALTER COLUMN cagr_pct                   SET NOT NULL,
    ALTER COLUMN annualised_volatility_pct  SET NOT NULL,
    ALTER COLUMN sharpe                     SET NOT NULL,
    ALTER COLUMN max_drawdown_pct           SET NOT NULL,
    ALTER COLUMN exposure_time_pct          SET NOT NULL,
    ALTER COLUMN turnover_annualised        SET NOT NULL,
    ALTER COLUMN trade_count                SET NOT NULL,
    ALTER COLUMN return_vs_buy_and_hold_pct SET NOT NULL,
    ALTER COLUMN losing_trade_count         SET NOT NULL,
    ALTER COLUMN losing_period_count        SET NOT NULL,
    ALTER COLUMN open_trade_count           SET NOT NULL,
    ALTER COLUMN unpriced_trade_count       SET NOT NULL,
    ALTER COLUMN periods_per_year           SET NOT NULL,
    ALTER COLUMN total_return_pct           SET NOT NULL,
    ALTER COLUMN buy_and_hold_return_pct    SET NOT NULL,
    ALTER COLUMN metric_set_id              SET NOT NULL;

DO $$
BEGIN
    -- ⚠ A DRAWDOWN IS REPORTED AS A NON-POSITIVE NUMBER so a sign flip cannot
    -- read as a good result. The engine already refuses it; the table refuses
    -- it too, because a second writer would not go through the engine.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_drawdown_non_positive') THEN
        ALTER TABLE strategy_results ADD CONSTRAINT strategy_results_drawdown_non_positive
            CHECK (max_drawdown_pct <= 0);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_exposure_share') THEN
        ALTER TABLE strategy_results ADD CONSTRAINT strategy_results_exposure_share
            CHECK (exposure_time_pct >= 0 AND exposure_time_pct <= 100);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_counts_non_negative') THEN
        ALTER TABLE strategy_results ADD CONSTRAINT strategy_results_counts_non_negative
            CHECK (
                trade_count >= 0
                AND losing_trade_count >= 0
                AND losing_period_count >= 0
                AND open_trade_count >= 0
                AND unpriced_trade_count >= 0
                AND losing_trade_count <= trade_count
            );
    END IF;

    -- ⚠⚠ THE TWO CHECKS THAT MAKE A NULL MEAN SOMETHING. Without them a null
    -- profit factor could equally be "no losing trade" or "nobody computed it",
    -- and the second is the state #2288 clause 2 exists to refuse.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_profit_factor_denominator') THEN
        ALTER TABLE strategy_results ADD CONSTRAINT strategy_results_profit_factor_denominator
            CHECK ((profit_factor IS NULL) = (losing_trade_count = 0));
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_sortino_denominator') THEN
        ALTER TABLE strategy_results ADD CONSTRAINT strategy_results_sortino_denominator
            CHECK ((sortino IS NULL) = (losing_period_count = 0));
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_annualisation_positive') THEN
        ALTER TABLE strategy_results ADD CONSTRAINT strategy_results_annualisation_positive
            CHECK (periods_per_year > 0);
    END IF;

    -- Criterion 3's, when 5e fills it. Null is the fail-closed state the
    -- promotion gate refuses on; zero is not a sample size anybody measured.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_ess_positive') THEN
        ALTER TABLE strategy_results ADD CONSTRAINT strategy_results_ess_positive
            CHECK (effective_sample_size IS NULL OR effective_sample_size > 0);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_metric_set_non_empty') THEN
        ALTER TABLE strategy_results ADD CONSTRAINT strategy_results_metric_set_non_empty
            CHECK (metric_set_id <> '');
    END IF;
END $$;

COMMENT ON COLUMN strategy_results.effective_sample_size IS
    'Criterion 3''s overlap-corrected sample size, from a block bootstrap over '
    'calendar blocks with errors clustered by date. ⚠ NULL until stage 5e '
    'computes it, and the promotion gate refuses on the null. A nominal n is '
    'NOT an acceptable stand-in — criterion 3 forbids reporting one anywhere.';

COMMENT ON COLUMN strategy_results.periods_per_year IS
    'The MEASURED annualisation, (len(dates)-1) / (span_days / 365.25), derived '
    'off the evaluation window''s own trading-date axis. ⚠ Not the 252 '
    'convention: the corpus spans 1962-2026 and its per-year trading-date count '
    'is not constant. Every annualised column here is a function of this one.';

COMMENT ON COLUMN strategy_results.turnover_annualised IS
    'Total notional traded (entries + exits + rebalance trades), halved into '
    'round trips, over the mean allocated pot, per year. 1.0 means the pot '
    'turned over once a year.';

COMMENT ON COLUMN strategy_results.return_vs_buy_and_hold_pct IS
    'total_return_pct minus buy_and_hold_return_pct. ⚠ The benchmark is one leg '
    'per evaluated instrument, opened at its first usable bar in the window and '
    'closed at its last, run through the SAME equity-curve engine under the '
    'SAME sizing rule and cost model — a benchmark computed by different '
    'machinery would attribute the machinery''s difference to the strategy. '
    'Criterion 7: a strategy failing to beat buy-and-hold after costs is '
    'reported as not a strategy.';
