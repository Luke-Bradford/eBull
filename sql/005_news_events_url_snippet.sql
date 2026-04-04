-- Migration 005: add url, snippet, and sentiment_raw_json columns to
-- news_events, and a per-instrument uniqueness constraint on url_hash.
--
-- url_hash is NOT globally unique because one article can legitimately be
-- attached to multiple instruments (e.g. a sector-wide announcement).
-- The uniqueness constraint is (instrument_id, url_hash) only.
--
-- url_hash is NOT NULL so the unique constraint is meaningful — rows without
-- a hash are not accepted by the service layer.
--
-- raw_payload_json stores the pristine provider payload.
-- sentiment_raw_json stores the scorer output separately (label + magnitude),
-- keeping provider data and derived data in distinct columns.

ALTER TABLE news_events
    ADD COLUMN IF NOT EXISTS url               TEXT,
    ADD COLUMN IF NOT EXISTS snippet           TEXT,
    ADD COLUMN IF NOT EXISTS sentiment_raw_json JSONB;

-- Make url_hash NOT NULL so the unique constraint is meaningful.
-- Existing rows (if any) must have a hash before this runs.
ALTER TABLE news_events
    ALTER COLUMN url_hash SET NOT NULL;

-- Drop the old bare index on url_hash if it exists (created as a plain index
-- in earlier migrations), then create the correct unique constraint.
DROP INDEX IF EXISTS idx_news_events_url_hash;

ALTER TABLE news_events
    DROP CONSTRAINT IF EXISTS uq_news_events_instrument_url_hash;

ALTER TABLE news_events
    ADD CONSTRAINT uq_news_events_instrument_url_hash
    UNIQUE (instrument_id, url_hash);
