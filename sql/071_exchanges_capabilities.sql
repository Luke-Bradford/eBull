-- Migration 071 — exchanges.capabilities JSONB column (#515 PR 3a).
--
-- Adds the per-exchange_id capability defaults the per-instrument
-- ``resolve_capabilities()`` helper unions with
-- ``external_identifiers`` facts to drive frontend panel gating.
-- See workstream 3 of
-- docs/superpowers/specs/2026-04-26-complete-coverage-spec.md.
--
-- Shape: ``capabilities`` is a JSONB object keyed by capability
-- name (one of the 11 v1 keys) → list of provider tags from the
-- ``CAPABILITY_PROVIDERS`` enum (see app/services/capabilities.py).
-- Empty list = "no source picked" (covers both "no public source
-- available" AND "available but not decision-relevant").
--
-- The CHECK constraint validates only the OUTER shape — that
-- ``capabilities`` is a JSONB object, not an array or scalar. The
-- enum-membership check on the values lives in Python at the
-- service boundary (app/services/capabilities.py); enforcing it
-- in SQL would require a CHECK that walks every list value via
-- a JSONB function and would still need updating each time the
-- enum grows. Python-side guard is cheaper to maintain.
--
-- Seed: every exchange row gets a default capability set for its
-- asset_class, sourced from the workstream 2 matrix at
-- docs/per-exchange-capability-matrix.md. Operator can override
-- per row directly via the admin UI (PR 3b).
--
-- ``us_equity`` rows get the SEC + FMP coverage that's already
-- wired in eBull. Every other asset_class lands with a mostly-
-- empty default per the matrix's pre-decided cells; the per-region
-- investigation tickets (#516-#523) fill in the rest.

BEGIN;

ALTER TABLE exchanges
    ADD COLUMN IF NOT EXISTS capabilities JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE exchanges
    DROP CONSTRAINT IF EXISTS exchanges_capabilities_is_object;

ALTER TABLE exchanges
    ADD CONSTRAINT exchanges_capabilities_is_object
        CHECK (jsonb_typeof(capabilities) = 'object');

COMMENT ON COLUMN exchanges.capabilities IS
    'Per-exchange_id capability defaults (#515 PR 3). JSONB '
    'object keyed by capability name (filings / fundamentals / '
    'dividends / insider / analyst / ratings / esg / ownership / '
    'corporate_events / business_summary / officers) → list of '
    'provider tags from CAPABILITY_PROVIDERS. Empty list = no '
    'source picked. resolve_capabilities() unions this with '
    'external_identifiers per instrument at API time.';

-- ---------------------------------------------------------------
-- Seed defaults from the workstream 2 matrix.
-- ---------------------------------------------------------------
--
-- ``us_equity`` venues get the full SEC + FMP coverage map.
-- Every other asset_class lands with the matrix's pre-decided
-- empties (most cells empty, rationale recorded in the matrix
-- doc; per-region tickets fill the rest later).
--
-- Idempotency: only seed rows where ``capabilities`` is still
-- the empty default ``'{}'::jsonb``. An operator override or a
-- prior migration run is preserved.

UPDATE exchanges
   SET capabilities = jsonb_build_object(
           'filings',          jsonb_build_array('sec_edgar'),
           'fundamentals',     jsonb_build_array('sec_xbrl', 'fmp'),
           'dividends',        jsonb_build_array('sec_dividend_summary'),
           'insider',          jsonb_build_array('sec_form4'),
           'analyst',          jsonb_build_array('fmp'),
           'ratings',          jsonb_build_array(),
           'esg',              jsonb_build_array(),
           'ownership',        jsonb_build_array('sec_13f', 'sec_13d_13g'),
           'corporate_events', jsonb_build_array('sec_8k_events'),
           'business_summary', jsonb_build_array('sec_10k_item1'),
           'officers',         jsonb_build_array()
       ),
       updated_at = NOW()
 WHERE asset_class = 'us_equity'
   AND capabilities = '{}'::jsonb;

-- Non-us_equity rows: seed the empty-but-correctly-shaped object
-- so the resolve helper doesn't have to special-case missing
-- keys. Each per-region ticket UPDATEs its venues with the
-- decided provider lists once investigation lands.
UPDATE exchanges
   SET capabilities = jsonb_build_object(
           'filings',          jsonb_build_array(),
           'fundamentals',     jsonb_build_array(),
           'dividends',        jsonb_build_array(),
           'insider',          jsonb_build_array(),
           'analyst',          jsonb_build_array(),
           'ratings',          jsonb_build_array(),
           'esg',              jsonb_build_array(),
           'ownership',        jsonb_build_array(),
           'corporate_events', jsonb_build_array(),
           'business_summary', jsonb_build_array(),
           'officers',         jsonb_build_array()
       ),
       updated_at = NOW()
 WHERE asset_class <> 'us_equity'
   AND capabilities = '{}'::jsonb;

COMMIT;
