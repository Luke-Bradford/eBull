-- 024: Per-position tracking for direct holdings.
--
-- The existing `positions` table aggregates by instrument_id (one row per
-- ticker). This migration adds `broker_positions` which stores individual
-- eToro positions with per-position SL/TP, leverage, fees, and the full
-- raw payload — mirroring what `copy_mirror_positions` already does for
-- copy-trading.
--
-- After this migration, `positions` becomes a derived summary refreshed
-- from `broker_positions` during each portfolio sync cycle. The sync
-- function upserts into `broker_positions` first, then derives the
-- per-instrument aggregate.

CREATE TABLE IF NOT EXISTS broker_positions (
    position_id              BIGINT PRIMARY KEY,                   -- eToro positionID
    instrument_id            BIGINT NOT NULL REFERENCES instruments(instrument_id),
    is_buy                   BOOLEAN NOT NULL,
    units                    NUMERIC(20, 8) NOT NULL,
    initial_units            NUMERIC(20, 8),                       -- detect partial closes (isPartiallyAltered)
    amount                   NUMERIC(20, 4) NOT NULL,              -- current amount (margin-adjusted)
    initial_amount_in_dollars NUMERIC(20, 4) NOT NULL,             -- original investment
    open_rate                NUMERIC(20, 6) NOT NULL,
    open_conversion_rate     NUMERIC(20, 10) NOT NULL,
    open_date_time           TIMESTAMPTZ NOT NULL,
    stop_loss_rate           NUMERIC(20, 6),
    take_profit_rate         NUMERIC(20, 6),
    is_no_stop_loss          BOOLEAN NOT NULL DEFAULT TRUE,        -- "SL disabled" vs "SL rate is null"
    is_no_take_profit        BOOLEAN NOT NULL DEFAULT TRUE,        -- "TP disabled" vs "TP rate is null"
    leverage                 INTEGER NOT NULL DEFAULT 1,
    is_tsl_enabled           BOOLEAN NOT NULL DEFAULT FALSE,       -- trailing stop loss
    total_fees               NUMERIC(20, 4) NOT NULL DEFAULT 0,
    source                   TEXT NOT NULL DEFAULT 'broker_sync',  -- 'ebull' | 'broker_sync'
    raw_payload              JSONB NOT NULL,
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS broker_positions_instrument_id_idx
    ON broker_positions (instrument_id);
