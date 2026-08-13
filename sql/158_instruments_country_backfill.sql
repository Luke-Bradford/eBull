-- 158: instruments.country — backfill from exchanges.country via instrument.exchange = exchange_id (#1233 §6.1)
--
-- Pre-existing `instruments.country` column (sql/001_init.sql) is 100% NULL
-- because the eToro instruments endpoint does not expose country
-- (see app/providers/implementations/etoro.py:350 — `country=None`).
--
-- The `exchanges` table (sql/067 + sql/068) already carries
-- operator-curated `country` (ISO 3166-1 alpha-2) per exchange_id.
-- This migration derives `instruments.country` from `exchanges.country`
-- via `instruments.exchange = exchanges.exchange_id`.
--
-- Forward-compat: `app/services/universe.py` is updated in the same PR
-- to derive country from the exchanges join on every upsert, so future
-- universe syncs keep the value fresh as new exchanges land. This
-- migration handles the one-shot backfill for rows already present.
--
-- Effect (dev DB at writing): ~10k rows updated (~80% of universe,
-- matches the curated us_equity + non-equity asset_class set). Crypto /
-- FX / index instruments stay NULL because exchanges.country is NULL
-- for those asset classes (intentional — they have no country).
--
-- Migration is tx-wrapped by the runner — no explicit BEGIN/COMMIT.

UPDATE instruments i
SET country = e.country
FROM exchanges e
WHERE i.exchange = e.exchange_id
  AND e.country IS NOT NULL
  AND i.country IS DISTINCT FROM e.country;

-- Index supports the cross-cutting "SEC ingest filters US-only via
-- instruments.country='US'" pattern surfaced by #1233 §6.1. The
-- universe filter (`is_tradable = TRUE`) already has its own index
-- (idx_instruments_tradable from sql/001).
CREATE INDEX IF NOT EXISTS idx_instruments_country
    ON instruments (country);

COMMENT ON COLUMN instruments.country IS
    'ISO 3166-1 alpha-2 country code derived from exchanges.country '
    '(via instruments.exchange = exchanges.exchange_id). NULL when the '
    'exchange has no curated country (crypto, FX, index). Set by '
    'sql/158 backfill + maintained by app/services/universe.py upsert. '
    '#1233 §6.1.';
