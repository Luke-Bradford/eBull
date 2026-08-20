-- #2437 — causal, immutable per-regime trade cohorts for each backtest result.
-- The parent row remains the promotion unit and owns portfolio path metrics.
-- These children answer where its realised trades occurred without pretending
-- that a filtered list of closed returns can reconstruct drawdown.

CREATE TABLE IF NOT EXISTS strategy_result_regime_cohorts (
    result_id BIGINT NOT NULL REFERENCES strategy_results_store(result_id) ON DELETE RESTRICT,
    regime TEXT NOT NULL CHECK (regime IN (
        'bull_quiet', 'bull_volatile', 'bear_quiet', 'bear_volatile', 'unclassified'
    )),
    trade_count INTEGER NOT NULL CHECK (trade_count > 0),
    instrument_count INTEGER NOT NULL CHECK (instrument_count BETWEEN 1 AND trade_count),
    decision_date_count INTEGER NOT NULL CHECK (decision_date_count BETWEEN 1 AND trade_count),
    losing_trade_count INTEGER NOT NULL CHECK (losing_trade_count BETWEEN 0 AND trade_count),
    expectancy_pct NUMERIC NOT NULL,
    profit_factor NUMERIC,
    worst_trade_pct NUMERIC NOT NULL,
    effective_sample_size NUMERIC,
    expectancy_ci_low_pct NUMERIC,
    expectancy_ci_high_pct NUMERIC,
    bootstrap_block_length INTEGER,
    bootstrap_cluster_count INTEGER,
    bootstrap_resamples INTEGER,
    bootstrap_seed BIGINT,
    bootstrap_design_effect NUMERIC,
    bootstrap_model_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (result_id, regime),
    CHECK ((profit_factor IS NULL) = (losing_trade_count = 0)),
    CHECK (expectancy_pct > '-Infinity'::numeric AND expectancy_pct < 'Infinity'::numeric),
    CHECK (worst_trade_pct > '-Infinity'::numeric AND worst_trade_pct < 'Infinity'::numeric),
    CHECK (worst_trade_pct <= expectancy_pct),
    CHECK (profit_factor IS NULL OR (
        profit_factor >= 0 AND profit_factor < 'Infinity'::numeric
    )),
    CHECK (expectancy_ci_low_pct IS NULL OR expectancy_ci_low_pct <= expectancy_ci_high_pct),
    CHECK (effective_sample_size IS NULL OR effective_sample_size > 0),
    CHECK (expectancy_ci_low_pct IS NULL OR (
        expectancy_ci_low_pct > '-Infinity'::numeric AND expectancy_ci_low_pct < 'Infinity'::numeric AND
        expectancy_ci_high_pct > '-Infinity'::numeric AND expectancy_ci_high_pct < 'Infinity'::numeric
    )),
    CHECK (bootstrap_block_length IS NULL OR bootstrap_block_length > 0),
    CHECK (bootstrap_cluster_count IS NULL OR bootstrap_cluster_count > 0),
    CHECK (bootstrap_resamples IS NULL OR bootstrap_resamples > 0),
    CHECK (bootstrap_seed IS NULL OR bootstrap_seed >= 0),
    CHECK (bootstrap_design_effect IS NULL OR bootstrap_design_effect > 0),
    CHECK (bootstrap_model_id IS NULL OR btrim(bootstrap_model_id) <> ''),
    CHECK (num_nonnulls(
        effective_sample_size, expectancy_ci_low_pct, expectancy_ci_high_pct,
        bootstrap_block_length, bootstrap_cluster_count, bootstrap_resamples,
        bootstrap_seed, bootstrap_design_effect, bootstrap_model_id
    ) IN (0, 9))
);

COMMENT ON TABLE strategy_result_regime_cohorts IS
    'Immutable causal signal-date regime breakdown of a parent strategy result (#2437). '
    'Cohort trade counts must reconcile to the parent realised trade count. Portfolio path metrics remain on parent.';

CREATE OR REPLACE FUNCTION prevent_strategy_result_regime_cohort_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'strategy result regime cohorts are immutable; create a new result identity';
END;
$$;

DROP TRIGGER IF EXISTS trg_strategy_result_regime_cohort_immutable
    ON strategy_result_regime_cohorts;
CREATE TRIGGER trg_strategy_result_regime_cohort_immutable
BEFORE UPDATE OR DELETE ON strategy_result_regime_cohorts
FOR EACH ROW EXECUTE FUNCTION prevent_strategy_result_regime_cohort_mutation();
