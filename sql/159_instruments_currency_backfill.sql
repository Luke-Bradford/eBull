-- 159: instruments.currency — operator-curated exchanges.currency + backfill (#1431)
--
-- Pre-existing `instruments.currency` column (sql/001_init.sql) is 100% NULL
-- across the whole universe (12,530 rows checked) because the eToro
-- instruments endpoint does not expose currency (see
-- app/providers/implementations/etoro.py — `currency=None`) and the
-- "enriched separately by FMP" path referenced in old comments never
-- shipped (no third-party fundamentals provider — settled-decisions
-- "Fundamentals provider posture").
--
-- Consequence: get_portfolio defaults native currency to 'USD'
-- (app/services/portfolio.py:200), so US listings render correctly but any
-- non-USD listing (~40% of the universe: GBP / EUR / CHF / Nordic / HKD /
-- AUD / JPY / AED) is silently treated as USD and NOT FX-converted into the
-- operator's display currency — a latent valuation bug.
--
-- Fix mirrors sql/158 (country): the trading currency of a listing is a
-- property of its exchange, so it lives on the operator-curated `exchanges`
-- table and is derived onto each instrument via
-- `instruments.exchange = exchanges.exchange_id`. `app/services/universe.py`
-- is updated in the same PR to derive currency from the exchanges join on
-- every upsert (same shape as country), so future syncs stay fresh. This
-- migration adds the column, seeds it per curated exchange, and backfills
-- the rows already present.
--
-- Migration is tx-wrapped by the runner — no explicit BEGIN/COMMIT.

-- ---------------------------------------------------------------
-- 1. New column on the existing `exchanges` table.
--    ADD COLUMN IF NOT EXISTS (not CREATE IF NOT EXISTS) so re-apply
--    on a DB where the column already exists is a no-op (prevention-log
--    "new column in new-table migration" trap — here the table already
--    exists, so the ALTER is the correct idempotent form).
--    CHECK enforces ISO-4217 shape (3 uppercase letters) rather than a
--    tight allow-list, so the operator can curate a new market's
--    currency without a schema change, while garbage is still rejected
--    (prevention-log "New TEXT columns in migrations need CHECK
--    constraints"). NULL is permitted for asset classes with no single
--    fiat (crypto / FX / index / commodity) and uncurated exchanges.
-- ---------------------------------------------------------------
ALTER TABLE exchanges ADD COLUMN IF NOT EXISTS currency TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'exchanges_currency_iso4217_chk'
          AND conrelid = 'exchanges'::regclass
    ) THEN
        ALTER TABLE exchanges
            ADD CONSTRAINT exchanges_currency_iso4217_chk
            CHECK (currency IS NULL OR currency ~ '^[A-Z]{3}$');
    END IF;
END $$;

COMMENT ON COLUMN exchanges.currency IS
    'ISO 4217 trading currency for listings on this exchange, '
    'operator-curated. NULL when the exchange has no single fiat '
    '(crypto / FX / index / commodity) or is not yet curated. Derived '
    'onto instruments.currency via instruments.exchange = '
    'exchanges.exchange_id (sql/159 backfill + app/services/universe.py '
    'upsert). #1431.';

-- ---------------------------------------------------------------
-- 2. Seed currency per exchange, keyed on exchange_id (stable) and
--    grouped by ISO-4217 code. Covers every exchange whose trading
--    currency is unambiguous from its eToro `description`, INCLUDING
--    venues still classified `asset_class='unknown'` with a NULL
--    `country` (Tokyo-dup / Tadawul / Warsaw / Shenzhen / …): currency
--    is independent FX metadata, so curating it does not depend on the
--    operator first reclassifying the exchange's asset_class/country.
--    Leaving these NULL would keep portfolio.py defaulting their
--    listings to USD (the #1431 bug) for the ~170 instruments on them.
--
--    `WHERE currency IS NULL` guards an operator hand-edit from being
--    clobbered on re-apply (same spirit as sql/067's ON CONFLICT DO
--    NOTHING). Genuinely no-single-fiat venues stay NULL: crypto (8),
--    FX (1), index/CFD (3), commodity (2), CME futures (40).
-- ---------------------------------------------------------------
UPDATE exchanges SET currency = 'USD' WHERE currency IS NULL AND exchange_id IN ('4','5','19','20','33');   -- Nasdaq / NYSE / OTC / CBOE / RTH
UPDATE exchanges SET currency = 'GBP' WHERE currency IS NULL AND exchange_id IN ('7','42','43','44');        -- LSE + LSE AIM
UPDATE exchanges SET currency = 'EUR' WHERE currency IS NULL AND exchange_id IN
    ('6','9','10','11','17','22','23','30','38','32','34','51','52','53');                                    -- FRA/Paris/Madrid/Milan/Helsinki/Lisbon/Brussels/Amsterdam/Xetra + Vienna/Dublin/Tallinn/Vilnius/Riga
UPDATE exchanges SET currency = 'CHF' WHERE currency IS NULL AND exchange_id = '12';                          -- SIX (Switzerland)
UPDATE exchanges SET currency = 'NOK' WHERE currency IS NULL AND exchange_id = '14';                          -- Oslo
UPDATE exchanges SET currency = 'SEK' WHERE currency IS NULL AND exchange_id = '15';                          -- Stockholm
UPDATE exchanges SET currency = 'DKK' WHERE currency IS NULL AND exchange_id = '16';                          -- Copenhagen
UPDATE exchanges SET currency = 'ISK' WHERE currency IS NULL AND exchange_id = '50';                          -- Nasdaq Iceland
UPDATE exchanges SET currency = 'HKD' WHERE currency IS NULL AND exchange_id = '21';                          -- Hong Kong
UPDATE exchanges SET currency = 'AUD' WHERE currency IS NULL AND exchange_id = '31';                          -- Sydney
UPDATE exchanges SET currency = 'JPY' WHERE currency IS NULL AND exchange_id IN ('56','13');                  -- Tokyo / TYO
UPDATE exchanges SET currency = 'AED' WHERE currency IS NULL AND exchange_id IN ('39','41');                  -- Dubai / Abu Dhabi
UPDATE exchanges SET currency = 'CAD' WHERE currency IS NULL AND exchange_id IN ('18','48');                  -- Toronto / TSX Venture
UPDATE exchanges SET currency = 'SAR' WHERE currency IS NULL AND exchange_id = '24';                          -- Tadawul (Saudi)
UPDATE exchanges SET currency = 'CZK' WHERE currency IS NULL AND exchange_id = '35';                          -- Prague
UPDATE exchanges SET currency = 'PLN' WHERE currency IS NULL AND exchange_id = '36';                          -- Warsaw
UPDATE exchanges SET currency = 'HUF' WHERE currency IS NULL AND exchange_id = '37';                          -- Budapest
UPDATE exchanges SET currency = 'CNY' WHERE currency IS NULL AND exchange_id IN ('45','46');                  -- Shenzhen / Shanghai
UPDATE exchanges SET currency = 'INR' WHERE currency IS NULL AND exchange_id = '47';                          -- NSE India
UPDATE exchanges SET currency = 'SGD' WHERE currency IS NULL AND exchange_id = '49';                          -- Singapore
UPDATE exchanges SET currency = 'KRW' WHERE currency IS NULL AND exchange_id = '54';                          -- Korea
UPDATE exchanges SET currency = 'TWD' WHERE currency IS NULL AND exchange_id = '55';                          -- Taiwan

-- ---------------------------------------------------------------
-- 3. One-shot backfill of instruments already present. Future syncs
--    keep the value fresh via the exchanges join in universe.py.
-- ---------------------------------------------------------------
UPDATE instruments i
SET currency = e.currency
FROM exchanges e
WHERE i.exchange = e.exchange_id
  AND e.currency IS NOT NULL
  AND i.currency IS DISTINCT FROM e.currency;

COMMENT ON COLUMN instruments.currency IS
    'ISO 4217 native trading currency, derived from exchanges.currency '
    'via instruments.exchange = exchanges.exchange_id. NULL when the '
    'exchange has no curated currency (crypto / FX / index / uncurated). '
    'Consumed by app/services/portfolio.py for FX conversion to the '
    'operator display currency. Set by sql/159 backfill + maintained by '
    'app/services/universe.py upsert. #1431.';
