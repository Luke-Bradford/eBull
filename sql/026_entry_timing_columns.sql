-- Migration 026: add entry timing columns to trade_recommendations
--
-- stop_loss_rate: ATR-based stop-loss price computed by the entry timing
--   service. Passed through to the broker as OrderParams.stop_loss_rate.
--   NULL for EXIT recs and for recs created before this migration.
--
-- take_profit_rate: thesis base_value (target price) at recommendation time.
--   Passed through to the broker as OrderParams.take_profit_rate.
--   NULL for EXIT recs and for recs created before this migration.
--
-- timing_verdict: outcome of the entry timing evaluation.
--   'pass' = conditions favorable, proceed to guard.
--   'defer' = conditions unfavorable, skip this cycle.
--   'skip' = not evaluated (EXIT recs, or pre-migration recs).
--   NULL for recs created before this migration.
--
-- timing_rationale: human-readable explanation of the timing verdict.
--   Records which conditions passed/failed and the computed SL/TP values.

ALTER TABLE trade_recommendations
    ADD COLUMN IF NOT EXISTS stop_loss_rate    NUMERIC(18,6),
    ADD COLUMN IF NOT EXISTS take_profit_rate  NUMERIC(18,6),
    ADD COLUMN IF NOT EXISTS timing_verdict    TEXT,
    ADD COLUMN IF NOT EXISTS timing_rationale  TEXT;

-- Constrain timing_verdict to known values.  NULL is allowed for
-- pre-migration rows.  'error' marks recs where timing evaluation
-- raised an exception and the rec was deferred as a safety fallback.
DO $$
BEGIN
    ALTER TABLE trade_recommendations
        ADD CONSTRAINT chk_timing_verdict
        CHECK (timing_verdict IN ('pass', 'defer', 'skip', 'error'));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;
