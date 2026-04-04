-- Migration 003: external provider identifier mapping

-- Stores provider-native identifiers for instruments.
-- The service layer uses this table to resolve the correct external ID before
-- calling a provider. Providers remain pure HTTP clients with no DB access.
--
-- Examples:
--   provider='sec',             identifier_type='cik',            identifier_value='0000320193'
--   provider='companies_house', identifier_type='company_number', identifier_value='00102498'
--   provider='fmp',             identifier_type='symbol',         identifier_value='AAPL'

CREATE TABLE IF NOT EXISTS external_identifiers (
    external_identifier_id BIGSERIAL PRIMARY KEY,
    instrument_id          BIGINT NOT NULL REFERENCES instruments(instrument_id),
    provider               TEXT NOT NULL,
    identifier_type        TEXT NOT NULL,
    identifier_value       TEXT NOT NULL,
    is_primary             BOOLEAN NOT NULL DEFAULT TRUE,
    last_verified_at       TIMESTAMPTZ,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- A given (provider, identifier_type, identifier_value) triple is globally unique
    CONSTRAINT uq_external_identifiers_provider_value
        UNIQUE (provider, identifier_type, identifier_value),

    -- At most one primary identifier per instrument per provider per type
    CONSTRAINT uq_external_identifiers_primary
        UNIQUE (instrument_id, provider, identifier_type, is_primary)
);

CREATE INDEX IF NOT EXISTS idx_external_identifiers_instrument
    ON external_identifiers(instrument_id);
