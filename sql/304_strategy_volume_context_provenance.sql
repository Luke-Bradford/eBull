-- #2508 — complete volume provenance for point-in-time decision contexts.
-- Mean ADV is the capacity convention; median volume is the robust typical-day
-- baseline. Both need the causal lookback and coverage record to be comparable.

ALTER TABLE strategy_decision_contexts
    ADD COLUMN IF NOT EXISTS volume_lookback_sessions INTEGER,
    ADD COLUMN IF NOT EXISTS trailing_mean_share_volume NUMERIC,
    ADD COLUMN IF NOT EXISTS trailing_mean_dollar_volume NUMERIC,
    ADD COLUMN IF NOT EXISTS zero_volume_frequency NUMERIC,
    ADD COLUMN IF NOT EXISTS intraday_coverage NUMERIC;

ALTER TABLE strategy_decision_contexts
    ADD CONSTRAINT strategy_decision_context_volume_provenance_valid
    CHECK (
        (volume_lookback_sessions IS NULL OR volume_lookback_sessions > 0)
        AND (trailing_mean_share_volume IS NULL OR trailing_mean_share_volume >= 0)
        AND (trailing_mean_dollar_volume IS NULL OR trailing_mean_dollar_volume >= 0)
        AND (zero_volume_frequency IS NULL OR zero_volume_frequency BETWEEN 0 AND 1)
        AND (intraday_coverage IS NULL OR intraday_coverage BETWEEN 0 AND 1)
    );

ALTER TABLE strategy_decision_contexts
    DROP CONSTRAINT strategy_decision_context_eligible_complete;

ALTER TABLE strategy_decision_contexts
    ADD CONSTRAINT strategy_decision_context_eligible_complete
    CHECK (
        candidate_verdict = 'refused' OR (
            classification_effective_from IS NOT NULL
            AND security_type IS NOT NULL AND security_type <> 'unknown'
            AND primary_listing_market IS NOT NULL AND primary_listing_market <> 'unknown'
            AND as_traded_price IS NOT NULL AND price_band IS NOT NULL
            AND volume_lookback_sessions IS NOT NULL
            AND trailing_mean_share_volume IS NOT NULL
            AND trailing_median_share_volume IS NOT NULL
            AND trailing_mean_dollar_volume IS NOT NULL
            AND trailing_median_dollar_volume IS NOT NULL
            AND dollar_volume_band IS NOT NULL
            AND zero_volume_frequency IS NOT NULL
            AND intraday_coverage IS NOT NULL
            AND relative_volume IS NOT NULL AND spread_bps IS NOT NULL
            AND realised_volatility IS NOT NULL AND gap_pct IS NOT NULL
            AND market_sector_residual_z IS NOT NULL AND vix IS NOT NULL
        )
    );

COMMENT ON COLUMN strategy_decision_contexts.volume_lookback_sessions IS
    'Completed causal sessions used by both mean and median volume baselines; '
    'versioned by strategy_decision_context.CONTEXT_VERSION.';

COMMENT ON COLUMN strategy_decision_contexts.intraday_coverage IS
    'Observed / expected completed intraday bars in the declared lookback, 0-1; '
    'an eligible context cannot hide partial feed coverage.';
