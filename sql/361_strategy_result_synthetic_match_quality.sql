-- #2772 -- durable, fail-closed structural match evidence for §9's random-entry control.
--
-- No source defines a tolerance for "matched ... on exposure and turnover".
-- The first version therefore records and admits exact equality only. A future
-- tolerance must carry a new policy id; it cannot silently make an existing
-- control easier to pass. Legacy controls keep this block NULL and are refused
-- by the application as `synthetic_control_match_evidence_missing`.

BEGIN;

SET LOCAL lock_timeout = '5s';

ALTER TABLE strategy_results_store
    ADD COLUMN IF NOT EXISTS synthetic_control_match_policy_id TEXT,
    ADD COLUMN IF NOT EXISTS synthetic_control_placement_space_id TEXT,
    ADD COLUMN IF NOT EXISTS synthetic_control_matchable_trade_count BIGINT,
    ADD COLUMN IF NOT EXISTS synthetic_control_cohort_mean_trade_count NUMERIC,
    ADD COLUMN IF NOT EXISTS synthetic_control_unmatchable_count BIGINT,
    ADD COLUMN IF NOT EXISTS synthetic_control_unmatchable_by_reason JSONB,
    ADD COLUMN IF NOT EXISTS synthetic_control_no_slack_series INTEGER,
    ADD COLUMN IF NOT EXISTS synthetic_control_series_placed INTEGER,
    ADD COLUMN IF NOT EXISTS synthetic_control_strategy_exposure_time_pct NUMERIC,
    ADD COLUMN IF NOT EXISTS synthetic_control_cohort_mean_exposure_time_pct NUMERIC,
    ADD COLUMN IF NOT EXISTS synthetic_control_strategy_turnover_annualised NUMERIC,
    ADD COLUMN IF NOT EXISTS synthetic_control_cohort_mean_turnover_annualised NUMERIC,
    ADD COLUMN IF NOT EXISTS synthetic_control_match_passed BOOLEAN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_synthetic_match_all_or_nothing'
    ) THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_synthetic_match_all_or_nothing CHECK (
            (
                synthetic_control_match_policy_id IS NULL
                AND synthetic_control_placement_space_id IS NULL
                AND synthetic_control_matchable_trade_count IS NULL
                AND synthetic_control_cohort_mean_trade_count IS NULL
                AND synthetic_control_unmatchable_count IS NULL
                AND synthetic_control_unmatchable_by_reason IS NULL
                AND synthetic_control_no_slack_series IS NULL
                AND synthetic_control_series_placed IS NULL
                AND synthetic_control_strategy_exposure_time_pct IS NULL
                AND synthetic_control_cohort_mean_exposure_time_pct IS NULL
                AND synthetic_control_strategy_turnover_annualised IS NULL
                AND synthetic_control_cohort_mean_turnover_annualised IS NULL
                AND synthetic_control_match_passed IS NULL
            ) OR (
                synthetic_control_match_policy_id IS NOT NULL
                AND synthetic_control_placement_space_id IS NOT NULL
                AND synthetic_control_matchable_trade_count IS NOT NULL
                AND synthetic_control_cohort_mean_trade_count IS NOT NULL
                AND synthetic_control_unmatchable_count IS NOT NULL
                AND synthetic_control_unmatchable_by_reason IS NOT NULL
                AND synthetic_control_no_slack_series IS NOT NULL
                AND synthetic_control_series_placed IS NOT NULL
                AND synthetic_control_strategy_exposure_time_pct IS NOT NULL
                AND synthetic_control_cohort_mean_exposure_time_pct IS NOT NULL
                AND synthetic_control_strategy_turnover_annualised IS NOT NULL
                AND synthetic_control_cohort_mean_turnover_annualised IS NOT NULL
                AND synthetic_control_match_passed IS NOT NULL
            )
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_synthetic_match_shape') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_synthetic_match_shape CHECK (
            synthetic_control_match_policy_id IS NULL OR (
                synthetic_control_model_id IS NOT NULL
                AND length(btrim(synthetic_control_match_policy_id)) > 0
                AND length(btrim(synthetic_control_placement_space_id)) > 0
                AND synthetic_control_matchable_trade_count >= 0
                AND synthetic_control_cohort_mean_trade_count >= 0
                AND synthetic_control_unmatchable_count >= 0
                AND jsonb_typeof(synthetic_control_unmatchable_by_reason) = 'object'
                AND synthetic_control_no_slack_series >= 0
                AND synthetic_control_series_placed >= 1
                AND synthetic_control_strategy_exposure_time_pct BETWEEN 0 AND 100
                AND synthetic_control_cohort_mean_exposure_time_pct BETWEEN 0 AND 100
                AND synthetic_control_strategy_turnover_annualised >= 0
                AND synthetic_control_cohort_mean_turnover_annualised >= 0
            )
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_synthetic_match_derived') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_synthetic_match_derived CHECK (
            synthetic_control_match_passed IS NULL
            OR synthetic_control_match_passed = (
                synthetic_control_matchable_trade_count = synthetic_control_cohort_mean_trade_count
                AND synthetic_control_unmatchable_count = 0
                AND synthetic_control_no_slack_series = 0
                AND synthetic_control_strategy_exposure_time_pct = synthetic_control_cohort_mean_exposure_time_pct
                AND synthetic_control_strategy_turnover_annualised = synthetic_control_cohort_mean_turnover_annualised
            )
        );
    END IF;
END $$;

COMMENT ON COLUMN strategy_results_store.synthetic_control_match_policy_id IS
    'Versioned rule deciding whether the random cohort structurally matches its sleeve. '
    'The initial policy admits exact population, exposure and turnover equality only.';
COMMENT ON COLUMN strategy_results_store.synthetic_control_unmatchable_by_reason IS
    'Structural census of realised in-sample positions excluded from the random-entry population. '
    'The separately stored total is re-derived and checked by the application on read.';
COMMENT ON COLUMN strategy_results_store.synthetic_control_match_passed IS
    'Derived exact-match verdict. NULL means no durable match evidence and promotion fails closed.';

-- `strategy_results` was created from SELECT *; refresh its expanded columns.
CREATE OR REPLACE VIEW strategy_results AS
    SELECT *
    FROM strategy_results_store
    WHERE namespace = 'in_sample';

ALTER VIEW strategy_results SET (check_option = 'cascaded');

COMMIT;
