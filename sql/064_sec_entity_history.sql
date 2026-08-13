-- 064_sec_entity_history.sql
--
-- SEC submissions.json long-tail normalisation (#463 / #452 Phase B).
-- Captures the remaining structured fields from submissions.json that
-- the #427 migration left on disk, plus a unified change-log for
-- every mutable entity field so the operator can answer "when did
-- this issuer's sector / address / fiscal-year / organisation
-- change" without re-walking historical raw dumps.
--
-- Fields added to ``instrument_sec_profile`` (latest-value columns):
--   - phone                 — registrant contact phone number
--   - entity_type           — "operating" / "investment" / etc.
--   - flags                 — SEC free-text marker (rarely populated)
--   - address_business      — JSONB of the current business address
--   - address_mailing       — JSONB of the current mailing address
--
-- New table ``sec_entity_change_log`` records every detected change
-- to a tracked entity field. One row per (instrument, field, detection
-- timestamp). Enables queries like:
--   - "show every sector re-classification in the last year"
--   - "which issuers moved headquarters this quarter"
--   - "when did APPL's fiscal year last change"
--
-- Change detection runs in the ingester (``sec_entity_profile``
-- service) on every submissions.json fetch: compare the incoming
-- value against the stored row, emit a log entry when they differ.
-- Initial seed is skipped — the first ingest writes the profile row
-- without synthesising a spurious "changed from NULL" event.

-- ---------------------------------------------------------------------
-- instrument_sec_profile — extend with submissions.json long-tail
-- ---------------------------------------------------------------------

ALTER TABLE instrument_sec_profile
    ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE instrument_sec_profile
    ADD COLUMN IF NOT EXISTS entity_type TEXT;
ALTER TABLE instrument_sec_profile
    ADD COLUMN IF NOT EXISTS flags TEXT;
ALTER TABLE instrument_sec_profile
    ADD COLUMN IF NOT EXISTS address_business JSONB;
ALTER TABLE instrument_sec_profile
    ADD COLUMN IF NOT EXISTS address_mailing JSONB;

COMMENT ON COLUMN instrument_sec_profile.entity_type IS
    'SEC entityType classifier — "operating", "investment", "general-purpose '
    'acquisition company", etc. Rare to change; kept on the latest-value '
    'row and change events logged to sec_entity_change_log.';

COMMENT ON COLUMN instrument_sec_profile.address_business IS
    'Current business address as a JSONB object with fields '
    '{street1, street2, city, state_or_country, state_or_country_description, '
    'zip_code, country, country_code, is_foreign_location, foreign_state_territory}. '
    'Historical address changes land in sec_entity_change_log.';

-- ---------------------------------------------------------------------
-- sec_entity_change_log — unified change log for entity fields
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sec_entity_change_log (
    id                 BIGSERIAL   PRIMARY KEY,
    instrument_id      BIGINT      NOT NULL
                           REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    -- Denormalised for readable queries. Matches
    -- instrument_sec_profile.cik for the same instrument.
    cik                TEXT        NOT NULL,
    -- Name of the submissions.json field that changed. Known values:
    --   sic, sic_description, owner_org, description, website,
    --   investor_website, fiscal_year_end, category, state_of_incorporation,
    --   state_of_incorporation_desc, entity_type, phone, flags,
    --   address_business, address_mailing, exchanges.
    -- Free-text to allow future fields without a schema migration.
    field_name         TEXT        NOT NULL,
    -- JSON-serialised snapshot of the value before / after the
    -- detected change. TEXT rather than JSONB because some fields
    -- store primitive strings; TEXT keeps the comparison + storage
    -- path uniform across primitive and object-valued fields.
    prev_value         TEXT,
    new_value          TEXT        NOT NULL,
    -- Timestamp when the ingester noticed the delta — NOT the date
    -- the change actually happened at the issuer (SEC doesn't
    -- publish that for most fields). The detection timestamp is an
    -- upper bound; the true change landed somewhere between this
    -- row and the previous change-log entry for the same field.
    detected_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Accession that was being processed when the change was
    -- detected, if the caller had one in scope. Useful for linking
    -- a sector-change log back to the 10-K that triggered it.
    source_accession   TEXT
);

CREATE INDEX IF NOT EXISTS idx_sec_entity_change_log_instrument
    ON sec_entity_change_log (instrument_id, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_sec_entity_change_log_field
    ON sec_entity_change_log (field_name, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_sec_entity_change_log_cik
    ON sec_entity_change_log (cik);

COMMENT ON TABLE sec_entity_change_log IS
    'Append-only log of detected changes to entity fields on '
    'instrument_sec_profile. Populated by the SEC entity-profile '
    'ingester when a newly-fetched submissions.json field differs '
    'from the stored latest value. Distinct from formerNames '
    '(SEC-published name history) — this captures sector, address, '
    'fiscal year, organisation, and the rest of the mutable fields '
    'that do not have a SEC-side historical feed.';

COMMENT ON COLUMN sec_entity_change_log.detected_at IS
    'Timestamp when the ingester noticed the field delta. Upper '
    'bound for when the true change landed at SEC — if you need a '
    'tighter timestamp, correlate with the preceding log entry '
    'for the same field.';
