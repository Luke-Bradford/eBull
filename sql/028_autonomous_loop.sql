-- Migration 028: autonomous operation loop — deferred retry tracking
--
-- timing_retry_count: how many times a timing_deferred rec has been
--   re-evaluated. Starts at 0, incremented on each retry attempt.
--   Used to cap retries (max 3 per cycle) and for observability.
--
-- timing_deferred_at: when the rec was first deferred. Used to expire
--   stale deferred recs (>24h old) so they don't retry indefinitely.
--
-- deferred_recommendation_id: when a deferred rec expires, a new
--   recommendation may be generated in the next morning cycle. This
--   FK links the retry lineage for auditability.

ALTER TABLE trade_recommendations
    ADD COLUMN IF NOT EXISTS timing_retry_count      INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS timing_deferred_at       TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS deferred_recommendation_id BIGINT
        REFERENCES trade_recommendations(recommendation_id);

-- Add timing_expired to the status vocabulary.
-- No existing CHECK constraint on status (001_init.sql uses bare TEXT),
-- so we add one now for the expanded set used by the autonomous loop.
-- Use NOT VALID to avoid a full-table scan on ADD, then VALIDATE in a
-- separate statement so existing out-of-vocabulary rows (if any) surface
-- as a clear error rather than rolling back the entire migration.
-- Wrapped in DO $$ for idempotency.
DO $$
BEGIN
    ALTER TABLE trade_recommendations
        DROP CONSTRAINT IF EXISTS chk_recommendation_status;
    ALTER TABLE trade_recommendations
        ADD CONSTRAINT chk_recommendation_status
        CHECK (status IN (
            'proposed', 'approved', 'rejected', 'executed',
            'execution_failed', 'timing_deferred', 'timing_expired',
            'cancelled'
        ))
        NOT VALID;
END $$;

-- Validate separately — this does a sequential scan but will not hold
-- an ACCESS EXCLUSIVE lock (only SHARE UPDATE EXCLUSIVE), and will fail
-- cleanly if any legacy rows violate the constraint.
ALTER TABLE trade_recommendations VALIDATE CONSTRAINT chk_recommendation_status;
