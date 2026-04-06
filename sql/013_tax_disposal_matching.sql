-- 013: Tax disposal matching tables
-- Supports issue #11: UK disposal matching and year-to-date tax view

-- A. FX rates table for GBP conversion
CREATE TABLE IF NOT EXISTS fx_rates (
    rate_date       DATE NOT NULL,
    from_currency   TEXT NOT NULL,
    to_currency     TEXT NOT NULL,
    rate            NUMERIC(18,10) NOT NULL,
    PRIMARY KEY (rate_date, from_currency, to_currency)
);

-- B. Extend tax_lots with direction and FX columns
ALTER TABLE tax_lots
    ADD COLUMN IF NOT EXISTS direction TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS original_currency TEXT,
    ADD COLUMN IF NOT EXISTS fx_rate_to_gbp NUMERIC(18,10),
    ADD COLUMN IF NOT EXISTS amount_gbp NUMERIC(18,6);

-- Idempotent fill ingestion: one tax_lot per fill
CREATE UNIQUE INDEX IF NOT EXISTS idx_tax_lots_fill_unique
    ON tax_lots(reference_fill_id) WHERE reference_fill_id IS NOT NULL;

-- C. Disposal matches table
CREATE TABLE IF NOT EXISTS disposal_matches (
    match_id              BIGSERIAL PRIMARY KEY,
    instrument_id         BIGINT NOT NULL REFERENCES instruments(instrument_id),
    disposal_tax_lot_id   BIGINT NOT NULL REFERENCES tax_lots(tax_lot_id),
    acquisition_tax_lot_id BIGINT REFERENCES tax_lots(tax_lot_id),
    matching_rule         TEXT NOT NULL,
    matched_units         NUMERIC(18,6) NOT NULL,
    acquisition_cost_gbp  NUMERIC(18,6) NOT NULL,
    disposal_proceeds_gbp NUMERIC(18,6) NOT NULL,
    gain_or_loss_gbp      NUMERIC(18,6) NOT NULL,
    disposal_uk_date      DATE NOT NULL,
    tax_year              TEXT NOT NULL,
    matched_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_disposal_matches_instrument
    ON disposal_matches(instrument_id, tax_year);
CREATE INDEX IF NOT EXISTS idx_disposal_matches_disposal_lot
    ON disposal_matches(disposal_tax_lot_id);

-- D. Section 104 pool state (one row per instrument, upserted)
CREATE TABLE IF NOT EXISTS s104_pool (
    pool_id           BIGSERIAL PRIMARY KEY,
    instrument_id     BIGINT NOT NULL REFERENCES instruments(instrument_id),
    pool_units        NUMERIC(18,6) NOT NULL,
    pool_cost_gbp     NUMERIC(18,6) NOT NULL,
    pool_avg_cost_gbp NUMERIC(18,6) NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (instrument_id)
);
