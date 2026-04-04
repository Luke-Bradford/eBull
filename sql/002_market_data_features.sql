-- Migration 002: add computed price features to price_daily, add quotes table

ALTER TABLE price_daily
    ADD COLUMN IF NOT EXISTS return_1w  NUMERIC(10,6),  -- 1-week total return
    ADD COLUMN IF NOT EXISTS return_1m  NUMERIC(10,6),  -- 1-month total return
    ADD COLUMN IF NOT EXISTS return_3m  NUMERIC(10,6),  -- 3-month total return
    ADD COLUMN IF NOT EXISTS return_6m  NUMERIC(10,6),  -- 6-month total return
    ADD COLUMN IF NOT EXISTS return_1y  NUMERIC(10,6),  -- 1-year total return
    ADD COLUMN IF NOT EXISTS volatility_30d NUMERIC(10,6); -- 30-day realised volatility (annualised)

-- Current quote snapshot per instrument (overwritten on each refresh)
CREATE TABLE IF NOT EXISTS quotes (
    instrument_id  BIGINT PRIMARY KEY REFERENCES instruments(instrument_id),
    quoted_at      TIMESTAMPTZ NOT NULL,
    bid            NUMERIC(18,6) NOT NULL,
    ask            NUMERIC(18,6) NOT NULL,
    last           NUMERIC(18,6),
    spread_pct     NUMERIC(10,6),    -- (ask - bid) / mid * 100
    spread_flag    BOOLEAN NOT NULL DEFAULT FALSE  -- TRUE if spread exceeds policy threshold
);
