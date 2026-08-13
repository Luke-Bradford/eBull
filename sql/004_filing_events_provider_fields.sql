-- Migration 004: add provider-scoped uniqueness to filing_events
--
-- filing_events previously had no way to prevent duplicate ingestion of the
-- same filing on re-runs. This migration adds:
--   - provider           TEXT NOT NULL  (e.g. 'sec', 'companies_house')
--   - provider_filing_id TEXT NOT NULL  (provider-native unique ID)
--   - primary_document_url TEXT         (canonical link to the document)
--
-- A UNIQUE constraint on (provider, provider_filing_id) enables idempotent
-- upserts keyed off ON CONFLICT (provider, provider_filing_id).
--
-- Provider-specific ID conventions:
--   SEC:              accession number, e.g. "0000320193-24-000001"
--   Companies House:  transaction ID from the filing history API
--   FMP:              native document ID if available; otherwise a deterministic
--                     hash of provider + symbol + filing_date + filing_type

ALTER TABLE filing_events
    ADD COLUMN IF NOT EXISTS provider TEXT,
    ADD COLUMN IF NOT EXISTS provider_filing_id TEXT,
    ADD COLUMN IF NOT EXISTS primary_document_url TEXT;

-- Back-fill existing rows with a placeholder so we can add NOT NULL later
-- (safe to apply to an empty table in greenfield; revisit if applied to live data)
UPDATE filing_events
    SET provider = 'unknown',
        provider_filing_id = 'legacy-' || filing_event_id::text
    WHERE provider IS NULL;

ALTER TABLE filing_events
    ALTER COLUMN provider SET NOT NULL,
    ALTER COLUMN provider_filing_id SET NOT NULL;

ALTER TABLE filing_events
    ADD CONSTRAINT uq_filing_events_provider_unique
        UNIQUE (provider, provider_filing_id);
