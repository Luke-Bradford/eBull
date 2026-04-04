-- Migration 005: add url and snippet columns to news_events, and a
-- per-instrument uniqueness constraint on url_hash.
--
-- url_hash is NOT globally unique because one article can legitimately be
-- attached to multiple instruments (e.g. a sector-wide announcement).
-- The uniqueness constraint is (instrument_id, url_hash) only.

ALTER TABLE news_events
    ADD COLUMN IF NOT EXISTS url     TEXT,
    ADD COLUMN IF NOT EXISTS snippet TEXT;

-- Drop the old bare index on url_hash if it exists (created as a plain index
-- in earlier migrations), then create the correct unique constraint.
DROP INDEX IF EXISTS idx_news_events_url_hash;

ALTER TABLE news_events
    DROP CONSTRAINT IF EXISTS uq_news_events_instrument_url_hash;

ALTER TABLE news_events
    ADD CONSTRAINT uq_news_events_instrument_url_hash
    UNIQUE (instrument_id, url_hash);
