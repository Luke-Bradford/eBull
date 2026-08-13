-- Migration 067 — exchanges metadata table (#503 PR 3).
--
-- eToro returns ``exchangeId`` as an opaque integer on every
-- instrument record. The dev DB has 35+ distinct ids today;
-- nothing in our schema interprets them. The SEC ingester
-- carries a hardcoded list (``exchange IN ('2', '4', '5', '6',
-- '7', '19', '20')`` per ``app/workers/scheduler.py:1123`` per #496)
-- which works but is brittle: a new exchange id eToro adds
-- silently won't be classified, and there's no single source the
-- router can read from to pick the right data source per region.
--
-- This migration introduces the ``exchanges`` table — one row per
-- eToro ``exchangeId`` with operator-curated semantic columns
-- (``country``, ``asset_class``). The scheduler's SEC filter
-- migrates to ``WHERE exchange_id IN (SELECT exchange_id FROM
-- exchanges WHERE asset_class = 'us_equity')`` so adding /
-- correcting an exchange's classification is a single row update,
-- not a code change.
--
-- ``description`` is sourced from eToro's
-- ``/api/v1/market-data/exchanges`` endpoint (deferred follow-up:
-- a periodic refresh job seeds + updates this column from the
-- API without touching ``country`` / ``asset_class``, which stay
-- operator-curated).
--
-- ``asset_class`` controlled vocabulary (constrained via CHECK):
--
--   us_equity      — US-listed equities + ETFs (SEC EDGAR coverage)
--   crypto         — crypto coins / pairs (no SEC; CoinGecko etc.)
--   eu_equity      — EU-listed equities (no SEC; ESMA / national)
--   uk_equity      — LSE-listed (Companies House)
--   asia_equity    — TSE / HKEX / SGX etc.
--   commodity      — futures / commodities
--   fx             — currency pairs
--   index          — composite indices
--   unknown        — id observed but not yet classified
--
-- Initial seed pins the eight US-equity exchange ids the SEC
-- mapper has been using since #496 (a no-functional-change swap
-- of the magic-numbers filter into table form), plus ``8 =
-- crypto`` so the operator can see the BTC family doesn't go
-- through the SEC mapper. Every other id seen in ``instruments``
-- on the dev DB lands as ``asset_class = 'unknown'``; the
-- operator updates as eToro descriptions are reviewed.

BEGIN;

CREATE TABLE IF NOT EXISTS exchanges (
    exchange_id   TEXT PRIMARY KEY,
    description   TEXT,                       -- from eToro; null until refresh job runs
    country       TEXT,                       -- ISO 3166-1 alpha-2 or NULL when not yet curated
    asset_class   TEXT NOT NULL DEFAULT 'unknown',
    seeded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT exchanges_asset_class_check CHECK (asset_class IN (
        'us_equity', 'crypto', 'eu_equity', 'uk_equity', 'asia_equity',
        'commodity', 'fx', 'index', 'unknown'
    ))
);

CREATE INDEX IF NOT EXISTS idx_exchanges_asset_class ON exchanges (asset_class);

COMMENT ON TABLE exchanges IS
    'Operator-curated mapping of eToro exchangeId → semantic class. '
    'Drives SEC ingester scope (asset_class = ''us_equity'') and '
    'per-region routing for filings / fundamentals data sources.';
COMMENT ON COLUMN exchanges.description IS
    'Sourced from eToro /api/v1/market-data/exchanges. NULL until the '
    'refresh job populates it; safe to query with NULL description.';
COMMENT ON COLUMN exchanges.asset_class IS
    'Controlled vocabulary. Determines which data sources the router '
    'consults for an instrument (SEC EDGAR for us_equity, none for '
    'crypto, Companies House for uk_equity, etc.).';

-- ---------------------------------------------------------------
-- Seed: pin the 8 ids the SEC mapper currently uses + crypto.
-- Every other id observed on dev becomes ``unknown`` so the
-- operator can audit and curate. ``ON CONFLICT DO NOTHING`` so
-- re-running this migration on a manually-curated DB doesn't
-- overwrite operator decisions.
-- ---------------------------------------------------------------

-- US-equity exchange ids the SEC mapper has used since #496.
INSERT INTO exchanges (exchange_id, country, asset_class) VALUES
    ('2',  'US', 'us_equity'),
    ('4',  'US', 'us_equity'),
    ('5',  'US', 'us_equity'),
    ('6',  'US', 'us_equity'),
    ('7',  'US', 'us_equity'),
    ('19', 'US', 'us_equity'),
    ('20', 'US', 'us_equity'),
    ('8',  NULL, 'crypto')
ON CONFLICT (exchange_id) DO NOTHING;

-- Backfill every observed exchange id from instruments as 'unknown'
-- so the operator can see the full set in a single SELECT and
-- reclassify as eToro descriptions are reviewed.
INSERT INTO exchanges (exchange_id, asset_class)
SELECT DISTINCT exchange, 'unknown'
FROM instruments
WHERE exchange IS NOT NULL
ON CONFLICT (exchange_id) DO NOTHING;

COMMIT;
