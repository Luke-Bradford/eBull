-- #2508 / #2400 — prove the price level used for candidate cohorts.
--
-- A split-adjusted research close is valid for continuous price returns but is
-- not the nominal price that could have traded on the historical date.  Price
-- and dollar-volume cohorts therefore require either a directly observed
-- unadjusted level or a reconstruction backed by point-in-time adjustments.

ALTER TABLE strategy_decision_contexts
    ADD COLUMN IF NOT EXISTS as_traded_price_basis TEXT
        CHECK (as_traded_price_basis IN (
            'observed_unadjusted', 'reconstructed_unadjusted', 'unknown'
        ));

ALTER TABLE strategy_decision_contexts
    DROP CONSTRAINT strategy_decision_context_eligible_complete;

ALTER TABLE strategy_decision_contexts
    ADD CONSTRAINT strategy_decision_context_eligible_complete
    CHECK (
        candidate_verdict = 'refused' OR (
            classification_effective_from IS NOT NULL
            AND security_type IS NOT NULL AND security_type <> 'unknown'
            AND primary_listing_market IS NOT NULL AND primary_listing_market <> 'unknown'
            AND as_traded_price IS NOT NULL
            AND as_traded_price_basis IN ('observed_unadjusted', 'reconstructed_unadjusted')
            AND price_band IS NOT NULL
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

COMMENT ON COLUMN strategy_decision_contexts.as_traded_price_basis IS
    'Provenance of the nominal decision-time price. Split-adjusted research '
    'levels are not eligible for price/liquidity cohort attribution.';
