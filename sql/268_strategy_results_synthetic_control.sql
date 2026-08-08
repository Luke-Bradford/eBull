-- 268_strategy_results_synthetic_control.sql
--
-- Phase 5e-5b — §9's 1,000-strategy random-entry synthetic control, given the
-- columns that make its two acceptance thresholds a stored, checkable fact
-- rather than a line in a run log.
-- Spec: docs/proposals/ta/2026-08-07-bounded-backtester.md §8 (stage 5e-5b) and
-- §9 ("the harness itself"). Parent:
-- docs/proposals/ta/strategy-catalogue-and-backtest-validity.md §5, the block
-- headed "Acceptance for the harness itself".
-- Producer: app/services/random_entry_cohort.py.
--
--
-- ⚠⚠ WHY THIS IS ON THE RESULT ROW AND NOT IN A TABLE OF ITS OWN.
-- ---------------------------------------------------------------------------
-- The control is not a second measurement standing beside the result — it is
-- the NULL DISTRIBUTION the result's own Sharpe is read against, so a result
-- separated from its control is a number with no scale. Criterion 11's argument
-- applies unchanged: the same strategy against a cohort built under a different
-- model id, size or seed is a different measurement, and all three therefore
-- travel on the row that quotes the comparison.
--
--
-- ⚠⚠ THE STRATEGY'S OWN SHARPE AND RETURN ARE **NOT** RE-STORED HERE.
-- ---------------------------------------------------------------------------
-- Both already exist on this table (`sharpe`, `total_return_pct`) and a second
-- copy is how two copies of one number diverge — the defect sql/266's
-- effective-sample-size binding was added to stop. The comparison is therefore
-- expressed as a CHECK over the columns already present plus the thresholds
-- added below, so `synthetic_control_passed` cannot disagree with the inputs
-- that produce it.
--
--
-- ⚠ ALL-OR-NOTHING, and NULL is the fail-closed default. A result with no
-- control is refused by `check_promotable` (`synthetic_control_not_run`), never
-- treated as unmeasured-but-fine. That is the same posture as sql/265's
-- bootstrap block and sql/266's DSR block, and for the same reason: a partial
-- set reports a corrected number whose correction cannot be judged.
--
--
-- ⚠⚠ THE TARGET IS `strategy_results_store`, AND THE VIEW MUST BE RE-CREATED.
-- sql/264's `strategy_results` is a VIEW whose `SELECT *` was expanded at
-- creation time, so a column added to the store does not appear in it. The
-- CREATE OR REPLACE at the foot is load-bearing; the store-vs-view column
-- parity test in tests/test_strategy_holdout_namespace.py catches a future
-- migration that forgets it.

ALTER TABLE strategy_results_store
    -- The placement measure and the matching rule, both of which are OURS
    -- (§9 fixes the cohort SIZE and the two thresholds and nothing else).
    ADD COLUMN IF NOT EXISTS synthetic_control_model_id TEXT,
    -- ⚠ STORED, never assumed to be 1000. A run that lost members to a refusal
    -- estimates the 95th percentile from fewer order statistics, and a reader
    -- must see that on the row rather than infer it from silence.
    ADD COLUMN IF NOT EXISTS synthetic_control_size INTEGER,
    ADD COLUMN IF NOT EXISTS synthetic_control_root_seed BIGINT,
    ADD COLUMN IF NOT EXISTS synthetic_control_mean_return_pct NUMERIC,
    ADD COLUMN IF NOT EXISTS synthetic_control_mean_return_ci_low_pct NUMERIC,
    ADD COLUMN IF NOT EXISTS synthetic_control_mean_return_ci_high_pct NUMERIC,
    ADD COLUMN IF NOT EXISTS synthetic_control_sharpe_percentile NUMERIC,
    ADD COLUMN IF NOT EXISTS synthetic_control_sharpe_threshold NUMERIC,
    -- ⚠ REPORTED, NOT GATED. Where the strategy's own return falls in the
    -- cohort's return distribution — the statistic the Monte-Carlo permutation
    -- literature uses (Aronson 2006 ch. 6; Masters 2018). §9's thresholds are
    -- the two above; this column exists so a reader can tell "no edge" from
    -- "the threshold is measuring the wrong quantity" without re-running 1,000
    -- strategies to find out.
    ADD COLUMN IF NOT EXISTS synthetic_control_return_threshold_pct NUMERIC,
    ADD COLUMN IF NOT EXISTS synthetic_control_passed BOOLEAN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_synthetic_all_or_nothing') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_synthetic_all_or_nothing CHECK (
            (
                synthetic_control_model_id IS NULL
                AND synthetic_control_size IS NULL
                AND synthetic_control_root_seed IS NULL
                AND synthetic_control_mean_return_pct IS NULL
                AND synthetic_control_mean_return_ci_low_pct IS NULL
                AND synthetic_control_mean_return_ci_high_pct IS NULL
                AND synthetic_control_sharpe_percentile IS NULL
                AND synthetic_control_sharpe_threshold IS NULL
                AND synthetic_control_return_threshold_pct IS NULL
                AND synthetic_control_passed IS NULL
            ) OR (
                synthetic_control_model_id IS NOT NULL
                AND synthetic_control_size IS NOT NULL
                AND synthetic_control_root_seed IS NOT NULL
                AND synthetic_control_mean_return_pct IS NOT NULL
                AND synthetic_control_mean_return_ci_low_pct IS NOT NULL
                AND synthetic_control_mean_return_ci_high_pct IS NOT NULL
                AND synthetic_control_sharpe_percentile IS NOT NULL
                AND synthetic_control_sharpe_threshold IS NOT NULL
                AND synthetic_control_return_threshold_pct IS NOT NULL
                AND synthetic_control_passed IS NOT NULL
            )
        );
    END IF;

    -- §9's cohort size, and the percentile it reads off it.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_synthetic_shape') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_synthetic_shape CHECK (
            (synthetic_control_size IS NULL OR synthetic_control_size >= 1)
            AND (synthetic_control_model_id IS NULL OR length(btrim(synthetic_control_model_id)) > 0)
            AND (
                synthetic_control_sharpe_percentile IS NULL
                OR (synthetic_control_sharpe_percentile > 0 AND synthetic_control_sharpe_percentile < 100)
            )
            AND (
                synthetic_control_mean_return_ci_low_pct IS NULL
                OR synthetic_control_mean_return_ci_low_pct <= synthetic_control_mean_return_ci_high_pct
            )
        );
    END IF;

    -- ⚠⚠ THE VERDICT IS DERIVED FROM THE STORED INPUTS, NOT ASSERTED BESIDE
    -- THEM. §9: "acceptance is BOTH parent thresholds" — the cohort's mean net
    -- return inside its own interval of zero, AND this strategy's Sharpe above
    -- the cohort's threshold. Without this CHECK a writer could store a `true`
    -- that the columns next to it contradict, which is exactly the state an
    -- operator reading the row would have no way to detect.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_synthetic_verdict_derived') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_synthetic_verdict_derived CHECK (
            synthetic_control_passed IS NULL
            OR synthetic_control_passed = (
                synthetic_control_mean_return_ci_low_pct <= 0
                AND synthetic_control_mean_return_ci_high_pct >= 0
                AND sharpe > synthetic_control_sharpe_threshold
            )
        );
    END IF;
END $$;

COMMENT ON COLUMN strategy_results_store.synthetic_control_model_id IS
    'The random-entry cohort construction this result was read against '
    '(app/services/random_entry_cohort.py::COHORT_MODEL_ID). ⚠ The placement '
    'measure and the matching rule are OURS — §9 fixes only the cohort size and '
    'the two thresholds — so a number computed under a different model id is not '
    'comparable with one computed under this.';

COMMENT ON COLUMN strategy_results_store.synthetic_control_size IS
    'How many cohort members actually produced a metric set. ⚠ §9 asks for '
    '1,000; this records what was achieved, because a smaller cohort estimates '
    'the 95th percentile from fewer order statistics.';

COMMENT ON COLUMN strategy_results_store.synthetic_control_sharpe_threshold IS
    'The cohort Sharpe at synthetic_control_sharpe_percentile. §9: each real '
    'strategy''s Sharpe must EXCEED it "to count as evidence at all".';

COMMENT ON COLUMN strategy_results_store.synthetic_control_return_threshold_pct IS
    'The cohort total-net-return at the same percentile. ⚠ REPORTED, NOT GATED '
    '— it is the statistic a Monte-Carlo permutation test uses, kept beside §9''s '
    'two thresholds so a reader can tell "no edge" from "the threshold is '
    'measuring the wrong quantity".';

COMMENT ON COLUMN strategy_results_store.synthetic_control_passed IS
    '§9''s conjunction, DERIVED from the columns beside it by '
    'strategy_results_synthetic_verdict_derived. A stored true that the inputs '
    'do not produce is unrepresentable.';

-- ---------------------------------------------------------------------------
-- ⚠ Re-created because `SELECT *` was expanded at sql/264's creation time and
-- would otherwise not carry the columns added above. See the header.
CREATE OR REPLACE VIEW strategy_results AS
    SELECT *
    FROM strategy_results_store
    WHERE namespace = 'in_sample';

ALTER VIEW strategy_results SET (check_option = 'cascaded');
