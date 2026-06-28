-- Migration 209: CONSIDERED recommendations + data-completeness gate (#1820, P0 of #1815)
--
-- Two independent additive changes:
--
-- 1. scores.data_completeness + scores.completeness_tier
--    Evidence columns recording the #1815 §4 data-completeness score C (0-1)
--    and its tier (insufficient_data / thin_data / full). Additive + nullable:
--    pre-migration rows keep NULL ("not computed then"). The headline
--    total_score math is UNCHANGED, so model_version is NOT bumped (consistent
--    with the risk_v1 evidence-layer blessing in settled-decisions: additive
--    nullable evidence under a stable version, do not bump).
--
-- 2. trade_recommendations status vocabulary += 'considered'
--    The portfolio review now persists blocked unheld BUY candidates as a
--    CONSIDERED action so the operator can see the universe was evaluated
--    (previously the buy_reason was silently discarded — #1820 primary bug).
--    These rows MUST carry status='considered' (never 'proposed') so they are
--    invisible to every execution selector (scheduler Phase 0/1 + work-signal
--    all filter status='proposed' / status IN ('proposed','approved')).
--
--    The CHECK is rebuilt with the FULL live vocabulary. The current
--    constraint (verified via pg_get_constraintdef on dev) is the sql/028
--    8-value set and is MISSING 'execution_pending', which order_client.py:1115
--    writes on a broker-pending live order — a latent constraint gap. This
--    migration adds 'execution_pending' (gap fix) and 'considered'.

-- ---------------------------------------------------------------------------
-- 1. scores completeness columns
-- ---------------------------------------------------------------------------
ALTER TABLE scores
    ADD COLUMN IF NOT EXISTS data_completeness NUMERIC(10,4),
    ADD COLUMN IF NOT EXISTS completeness_tier TEXT;

DO $$
BEGIN
    ALTER TABLE scores DROP CONSTRAINT IF EXISTS chk_scores_completeness_tier;
    ALTER TABLE scores
        ADD CONSTRAINT chk_scores_completeness_tier
        CHECK (
            completeness_tier IS NULL
            OR completeness_tier IN ('insufficient_data', 'thin_data', 'full')
        );
END $$;

-- ---------------------------------------------------------------------------
-- 2. trade_recommendations status vocabulary
-- ---------------------------------------------------------------------------
-- NOT VALID + VALIDATE in separate statements: avoids an ACCESS EXCLUSIVE
-- full-table scan on ADD, and surfaces any out-of-vocabulary legacy row as a
-- clean error rather than rolling back the whole migration. Idempotent.
DO $$
BEGIN
    ALTER TABLE trade_recommendations
        DROP CONSTRAINT IF EXISTS chk_recommendation_status;
    ALTER TABLE trade_recommendations
        ADD CONSTRAINT chk_recommendation_status
        CHECK (status IN (
            'proposed', 'approved', 'rejected', 'executed',
            'execution_pending', 'execution_failed',
            'timing_deferred', 'timing_expired',
            'cancelled', 'considered'
        ))
        NOT VALID;
END $$;

ALTER TABLE trade_recommendations VALIDATE CONSTRAINT chk_recommendation_status;
