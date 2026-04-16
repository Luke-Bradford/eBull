-- Migration 031: transaction cost model
--
-- cost_model: per-instrument fee schedules with temporal validity.
--   One active row per instrument (valid_to IS NULL).
--   Supports spread, overnight rate, and FX markup components.
--
-- transaction_cost_config: singleton operator-level cost thresholds.
--   Same pattern as budget_config (id BOOLEAN PK + CHECK id=TRUE).
--
-- trade_cost_record: links estimated costs (at guard time) to actual
--   costs (at fill time) for post-trade reconciliation.

-- Per-instrument fee schedule
CREATE TABLE IF NOT EXISTS cost_model (
    cost_model_id    BIGSERIAL      PRIMARY KEY,
    instrument_id    BIGINT         NOT NULL REFERENCES instruments(instrument_id),
    spread_bps       NUMERIC(10,4)  NOT NULL,
    overnight_rate   NUMERIC(10,6)  NOT NULL DEFAULT 0,  -- bps per day
    fx_pair          TEXT,
    fx_markup_bps    NUMERIC(10,4)  NOT NULL DEFAULT 0,
    valid_from       TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    valid_to         TIMESTAMPTZ,
    source           TEXT           NOT NULL DEFAULT 'computed',
    CONSTRAINT chk_cost_model_source
        CHECK (source IN ('manual', 'computed', 'api'))
);

CREATE INDEX IF NOT EXISTS idx_cost_model_instrument_active
    ON cost_model(instrument_id, valid_from DESC)
    WHERE valid_to IS NULL;

-- Enforce one active row per instrument at the DB level.
CREATE UNIQUE INDEX IF NOT EXISTS idx_cost_model_one_active
    ON cost_model(instrument_id)
    WHERE valid_to IS NULL;

-- Singleton cost config
CREATE TABLE IF NOT EXISTS transaction_cost_config (
    id                        BOOLEAN       PRIMARY KEY DEFAULT TRUE,
    max_total_cost_bps        NUMERIC(8,2)  NOT NULL DEFAULT 150,
    min_return_vs_cost_ratio  NUMERIC(6,2)  NOT NULL DEFAULT 3.0,
    default_hold_days         INTEGER       NOT NULL DEFAULT 90,
    updated_at                TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_by                TEXT          NOT NULL DEFAULT 'system',
    reason                    TEXT          NOT NULL DEFAULT 'initial seed',
    CONSTRAINT transaction_cost_config_single_row CHECK (id = TRUE)
);

DO $$
BEGIN
    ALTER TABLE transaction_cost_config
        DROP CONSTRAINT IF EXISTS chk_tcc_max_cost_bps;
    ALTER TABLE transaction_cost_config
        ADD CONSTRAINT chk_tcc_max_cost_bps
        CHECK (max_total_cost_bps > 0 AND max_total_cost_bps <= 1000);

    ALTER TABLE transaction_cost_config
        DROP CONSTRAINT IF EXISTS chk_tcc_min_ratio;
    ALTER TABLE transaction_cost_config
        ADD CONSTRAINT chk_tcc_min_ratio
        CHECK (min_return_vs_cost_ratio >= 1.0);

    ALTER TABLE transaction_cost_config
        DROP CONSTRAINT IF EXISTS chk_tcc_hold_days;
    ALTER TABLE transaction_cost_config
        ADD CONSTRAINT chk_tcc_hold_days
        CHECK (default_hold_days > 0 AND default_hold_days <= 365);
END $$;

INSERT INTO transaction_cost_config (id)
VALUES (TRUE)
ON CONFLICT DO NOTHING;

-- Per-fill cost tracking
CREATE TABLE IF NOT EXISTS trade_cost_record (
    cost_record_id       BIGSERIAL      PRIMARY KEY,
    order_id             BIGINT         NOT NULL REFERENCES orders(order_id),
    recommendation_id    BIGINT         NOT NULL REFERENCES trade_recommendations(recommendation_id),
    instrument_id        BIGINT         NOT NULL REFERENCES instruments(instrument_id),
    estimated_spread_bps NUMERIC(10,4),
    estimated_carry_bps  NUMERIC(10,4),
    estimated_fx_bps     NUMERIC(10,4),
    estimated_total_bps  NUMERIC(10,4)  NOT NULL,
    actual_spread_bps    NUMERIC(10,4),
    actual_total_bps     NUMERIC(10,4),
    cost_breakdown       JSONB          NOT NULL,
    recorded_at          TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_trade_cost_record_order
    ON trade_cost_record(order_id);
