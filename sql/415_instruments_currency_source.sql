-- #3322 — per-instrument price currency from the broker's conversion rate.
--
-- Spec: docs/specs/etl/2026-09-23-instrument-price-currency.md.
--
-- `instruments.currency` was the venue default (sql/159). eToro's rates endpoint
-- documents `conversionRateAsk/Bid` as the rate "from instrument's currency to USD";
-- on the LSE it shows most lines priced in pence (GBX) and some in USD/EUR. The
-- nightly universe sync now derives the currency from that rate
-- (app/services/instrument_price_currency.py) and records where it came from, so the
-- venue-default upsert does not overwrite it.
--
-- `currency` stays `^[A-Z]{3}$`-shaped: GBX (1/100 GBP) is the market convention for
-- pence and fits. It is NOT ISO 4217; app/services/fx.py normalises it.
--
-- Reproduce the population figures in the spec with
-- `PYTHONPATH=. uv run python -m scripts.census_3322_instrument_price_currency`.

ALTER TABLE instruments
    ADD COLUMN IF NOT EXISTS currency_source TEXT NOT NULL DEFAULT 'exchange';

ALTER TABLE instruments
    DROP CONSTRAINT IF EXISTS instruments_currency_source_chk;
ALTER TABLE instruments
    ADD CONSTRAINT instruments_currency_source_chk
    CHECK (currency_source IN ('exchange', 'broker_rate'));

-- Quote timestamp of the conversion rate the current value was derived from.
-- NULL = never derived (venue default only).
ALTER TABLE instruments
    ADD COLUMN IF NOT EXISTS currency_derived_at TIMESTAMPTZ;

COMMENT ON COLUMN instruments.currency_source IS
    '#3322: exchange = venue default (exchanges.currency); broker_rate = derived from '
    'the eToro conversion rate and differs from the venue default. The universe upsert '
    'keeps a broker_rate currency while the exchange is unchanged.';
