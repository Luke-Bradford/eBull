-- 023: Live pricing and currency conversion schema
--
-- New tables:
--   live_fx_rates    — real-time FX rates for display conversion
--   broker_events    — WebSocket event audit log (populated later)
--   price_intraday   — 1-min OHLCV bars (populated later)
--
-- Altered tables:
--   instruments      — add currency_enriched_at for FMP enrichment tracking
--   runtime_config   — add display_currency for operator currency preference
--   runtime_config_audit — extend field CHECK for display_currency

BEGIN;

-- Live FX rates for display conversion (separate from fx_rates used by tax)
CREATE TABLE IF NOT EXISTS live_fx_rates (
    from_currency TEXT NOT NULL,
    to_currency   TEXT NOT NULL,
    rate          NUMERIC(18,10) NOT NULL,
    quoted_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (from_currency, to_currency)
);

-- Broker WebSocket events for reconciliation audit
CREATE TABLE IF NOT EXISTS broker_events (
    event_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    broker_event_type TEXT NOT NULL,
    broker_ref        TEXT UNIQUE,
    raw_payload       JSONB NOT NULL,
    received_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    reconciled        BOOLEAN NOT NULL DEFAULT FALSE
);

-- Intraday price bars (1-min OHLCV, populated by WebSocket tick aggregation)
CREATE TABLE IF NOT EXISTS price_intraday (
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    candle_time   TIMESTAMPTZ NOT NULL,
    open          NUMERIC(18,6) NOT NULL,
    high          NUMERIC(18,6) NOT NULL,
    low           NUMERIC(18,6) NOT NULL,
    close         NUMERIC(18,6) NOT NULL,
    volume        BIGINT,
    PRIMARY KEY (instrument_id, candle_time)
);

-- Track when instrument currency was last enriched by FMP
ALTER TABLE instruments
    ADD COLUMN IF NOT EXISTS currency_enriched_at TIMESTAMPTZ;

-- Operator display currency preference
ALTER TABLE runtime_config
    ADD COLUMN IF NOT EXISTS display_currency TEXT NOT NULL DEFAULT 'GBP';

-- Extend audit field CHECK to include display_currency
ALTER TABLE runtime_config_audit
    DROP CONSTRAINT IF EXISTS runtime_config_audit_field_check;
ALTER TABLE runtime_config_audit
    ADD CONSTRAINT runtime_config_audit_field_check
    CHECK (field IN ('enable_auto_trading', 'enable_live_trading',
                     'kill_switch', 'display_currency'));

COMMIT;
