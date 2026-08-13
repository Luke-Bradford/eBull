-- 265_strategy_results_block_bootstrap.sql
--
-- Phase 5e-2 — criterion 3's block bootstrap: the interval beside the effective
-- sample size, and the provenance that makes both re-runnable.
-- Spec: docs/proposals/ta/2026-08-07-bounded-backtester.md §8 (stage 5e-2),
-- acceptance C3. Parent:
-- docs/proposals/ta/strategy-catalogue-and-backtest-validity.md criterion 3.
-- Producer: app/services/block_bootstrap.py. Carrier:
-- app/services/strategy_statistics.py::StrategyMetrics.
--
--
-- ⚠⚠ CRITERION 3 ASKS FOR TWO NUMBERS, AND sql/263 SHIPPED ONE COLUMN.
-- ---------------------------------------------------------------------------
-- The criterion is "report the effective sample size AND confidence interval —
-- not a bare percentage". `effective_sample_size` already exists (sql/263) and
-- has been NULL since, because stage 5e-2 had not run. The interval had nowhere
-- to go at all.
--
-- A corrected sample size with no interval is only half the correction: it says
-- how much evidence there is without saying what the evidence bounds. So the
-- two are stored together and CONSTRAINED together below — a row may carry the
-- whole block-bootstrap set or none of it, and never a part.
--
--
-- ⚠⚠ THE TARGET IS `strategy_results_store`, NOT `strategy_results`.
-- ---------------------------------------------------------------------------
-- sql/263 altered `strategy_results` while it was still a TABLE. sql/264 then
-- renamed it to `strategy_results_store` and put a namespace-filtering VIEW at
-- the old name (criterion 5 — a view filters for the superuser this app
-- connects as, which RLS measurably does not). Altering the view name here
-- would fail; altering the store and forgetting the view would be worse, which
-- is what the next block is about.
--
--
-- ⚠⚠ `SELECT *` IS EXPANDED AT CREATION, SO THE VIEW MUST BE RE-CREATED.
-- ---------------------------------------------------------------------------
-- sql/264 defines the view as `SELECT * FROM strategy_results_store WHERE
-- namespace = 'in_sample'`. Postgres resolves that star ONCE, at creation, and
-- freezes the column list. Adding a column to the store does not add it to the
-- view: every in-sample read would silently lose the new columns, with no error
-- anywhere. The `CREATE OR REPLACE VIEW` at the foot of this file is therefore
-- load-bearing, not tidying, and
-- tests/test_strategy_holdout_namespace.py's store-vs-view column-parity test
-- is what catches a future migration that forgets it.

ALTER TABLE strategy_results_store
    -- The 95% interval on `expectancy_per_trade_pct`, from a circular block
    -- bootstrap over date clusters. ⚠ On EXPECTANCY specifically: that is
    -- criterion 7's trade-level headline and therefore the "bare percentage"
    -- criterion 3 refuses to let stand unqualified.
    ADD COLUMN IF NOT EXISTS expectancy_ci_low_pct     NUMERIC,
    ADD COLUMN IF NOT EXISTS expectancy_ci_high_pct    NUMERIC,
    -- Declared inputs, stored because criterion 11 makes them part of what the
    -- number MEANS. None is re-derivable from the row.
    ADD COLUMN IF NOT EXISTS bootstrap_block_length    INTEGER,
    ADD COLUMN IF NOT EXISTS bootstrap_cluster_count   INTEGER,
    ADD COLUMN IF NOT EXISTS bootstrap_resamples       INTEGER,
    ADD COLUMN IF NOT EXISTS bootstrap_seed            BIGINT,
    ADD COLUMN IF NOT EXISTS bootstrap_design_effect   NUMERIC,
    ADD COLUMN IF NOT EXISTS bootstrap_model_id        TEXT;

DO $$
BEGIN
    -- ⚠⚠ ALL-OR-NOTHING. `num_nulls` counts NULL arguments, so the set is
    -- either wholly present (0 nulls) or wholly absent (9). Any other count is
    -- a partial write: a corrected sample size whose correction cannot be
    -- judged, or an interval with no sample size behind it. `effective_sample_size`
    -- is IN the set — sql/263's own "NULL until 5e computes it" is exactly the
    -- all-absent case, so existing rows still satisfy this.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_bootstrap_all_or_nothing') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_bootstrap_all_or_nothing
            CHECK (num_nulls(
                effective_sample_size, expectancy_ci_low_pct, expectancy_ci_high_pct,
                bootstrap_block_length, bootstrap_cluster_count, bootstrap_resamples,
                bootstrap_seed, bootstrap_design_effect, bootstrap_model_id
            ) IN (0, 9));
    END IF;

    -- An inverted interval is not a wide interval, it is a swapped write.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_bootstrap_ci_ordered') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_bootstrap_ci_ordered
            CHECK (expectancy_ci_low_pct IS NULL OR expectancy_ci_low_pct <= expectancy_ci_high_pct);
    END IF;

    -- ⚠ A block of 0 clusters is not a block, and a block longer than the axis
    -- it was measured on means the length came from somewhere other than that
    -- axis. Both are caught here rather than trusted to the writer.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_bootstrap_block_within_axis') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_bootstrap_block_within_axis
            CHECK (bootstrap_block_length IS NULL
                   OR (bootstrap_block_length >= 1
                       AND bootstrap_cluster_count >= 1
                       AND bootstrap_block_length <= bootstrap_cluster_count));
    END IF;

    -- ⚠ 1,000 is Efron & Tibshirani's floor for INTERVAL estimation (a point
    -- estimate's standard error needs far fewer). A row computed under fewer
    -- carries an interval whose ends are noise, so the floor is enforced here
    -- and not left to the default in `block_bootstrap.RESAMPLES`.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_bootstrap_resamples_floor') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_bootstrap_resamples_floor
            CHECK (bootstrap_resamples IS NULL OR bootstrap_resamples >= 1000);
    END IF;

    -- The design effect is a ratio of two variances; zero or negative is not a
    -- weak correction, it is an arithmetic failure upstream.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_bootstrap_deff_positive') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_bootstrap_deff_positive
            CHECK (bootstrap_design_effect IS NULL OR bootstrap_design_effect > 0);
    END IF;

    -- ⚠ A BLANK model id is PRESENT and meaningless — the same trap sql/263
    -- guards `metric_set_id` against. It would satisfy the all-or-nothing
    -- constraint above while naming no construction at all.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_bootstrap_model_non_empty') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_bootstrap_model_non_empty
            CHECK (bootstrap_model_id IS NULL OR bootstrap_model_id <> '');
    END IF;
END $$;

COMMENT ON COLUMN strategy_results_store.expectancy_ci_low_pct IS
    'Lower end of the 95% circular-block-bootstrap interval on '
    'expectancy_per_trade_pct, with errors clustered by entry fill date '
    '(criterion 3). ⚠ Percentile method (Efron & Tibshirani 1993 ch.13), which '
    'is first-order accurate only — BCa is not computed.';

COMMENT ON COLUMN strategy_results_store.bootstrap_block_length IS
    'Block length in CLUSTERS (active dates, not calendar days), MEASURED by '
    'Politis & White (2004) with the Patton, Politis & White (2009) correction '
    '— never declared. ⚠ The 4/3 constant is the circular scheme''s; 2 is the '
    'stationary bootstrap''s, and crossing them mis-sizes the block silently.';

COMMENT ON COLUMN strategy_results_store.bootstrap_design_effect IS
    'Kish (1965) design effect: Var_bootstrap(mean) / Var_iid(mean). '
    'effective_sample_size = trade_count / design_effect. ⚠ Its DIRECTION is '
    'the finding — above 1 the overlap cost sample size (the expected case, and '
    'why criterion 3 exists); below 1 it did not, which is why an effective '
    'sample size above the nominal trade count is reported rather than clipped.';

COMMENT ON COLUMN strategy_results_store.bootstrap_seed IS
    'The RNG seed. ⚠ A DECLARED input under criterion 11, not an implementation '
    'detail: without it a stored interval cannot be reproduced, and two runs of '
    '"the same" evaluation could differ in a number nobody chose.';

-- ---------------------------------------------------------------------------
-- ⚠ Re-created because `SELECT *` was expanded at sql/264's creation time and
-- would otherwise not carry the eight columns added above. See the header.
CREATE OR REPLACE VIEW strategy_results AS
    SELECT *
    FROM strategy_results_store
    WHERE namespace = 'in_sample';

ALTER VIEW strategy_results SET (check_option = 'cascaded');
