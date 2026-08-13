-- Migration 037: cascade_retry_queue — durable outbox for cascade failures.
--
-- K.2 of #276 cascade stream. Post-K.1, SEC watermarks commit before
-- cascade runs, so a cascade failure never triggers a re-plan of the
-- same CIK on the next daily_financial_facts cycle. This table is the
-- durable signal that makes retries deterministic and observable.
--
-- Semantics:
--   - Row per instrument (PK on instrument_id; UPSERT pattern).
--   - attempt_count = failed thesis attempts observed so far.
--       * INSERT on first thesis failure sets count=1.
--       * UPDATE on subsequent thesis failure increments count+1.
--       * Rerank-only failure inserts/updates with last_error='RERANK_NEEDED'
--         and attempt_count=0 — does NOT consume thesis retry budget.
--       * K.3 LOCKED_BY_SIBLING inserts fresh with count=0 or leaves
--         existing count unchanged.
--   - drain_retry_queue returns rows with attempt_count < ATTEMPT_CAP (5).
--     Rows at or above cap are left in place for admin inspection
--     (surfaced in Chunk H / K.4).
--   - Clear on success: DELETE the row after compute_rankings succeeds
--     (deferred so rerank failure preserves the durable signal).
--
-- ON DELETE CASCADE: defensive — eBull does not hard-delete instruments
-- today (is_tradable=false instead), but the FK makes the retention
-- invariant explicit at the schema level.

CREATE TABLE IF NOT EXISTS cascade_retry_queue (
    instrument_id     BIGINT PRIMARY KEY
        REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    enqueued_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_attempted_at TIMESTAMPTZ,
    attempt_count     INTEGER NOT NULL DEFAULT 0,
    last_error        TEXT NOT NULL,
    CONSTRAINT cascade_retry_queue_attempt_count_nonneg
        CHECK (attempt_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_cascade_retry_queue_enqueued
    ON cascade_retry_queue(enqueued_at);
