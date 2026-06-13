-- 194_trade_events.sql
--
-- #1593 (spec docs/proposals/etl/2026-06-13-etoro-trade-ledger.md §4)
-- — append-only broker-observed trade ledger + closed-position archive.
--
-- trade_events: one row per position OPEN or CLOSE event, written only
-- by sync_portfolio (open events from the portfolio-payload diff,
-- open+close events from the eToro trade-history fetch). Immutable —
-- no UPDATE path; conflicts are ON CONFLICT DO NOTHING (first
-- observation wins, disagreement logged loudly by the ingest service).
--
-- Units contract (spec §1.7): open.units = the position's ORIGINAL
-- opened units (initialUnits / Σ history slices); each close.units =
-- that slice's delta. eToro partial closes reduce the SAME positionId
-- (sql/024:19 isPartiallyAltered), so a position may carry one open
-- and N closes.
--
-- position_id >= 0: eBull-originated orders write synthetic NEGATIVE
-- broker_positions ids (-order_id, #227) that are replaced by the real
-- broker id at next sync — synthetic ids must never enter the ledger.
--
-- instrument_id is nullable on purpose: deep-history trades can
-- reference instruments absent from the current universe; the raw
-- etoro_instrument_id is always kept and a runbook re-resolves later
-- (no silent drops).
--
-- Not partitioned: single account, O(hundreds) rows/year.

BEGIN;

CREATE TABLE IF NOT EXISTS trade_events (
    event_id            BIGSERIAL PRIMARY KEY,
    position_id         BIGINT NOT NULL CHECK (position_id >= 0),
    etoro_instrument_id BIGINT NOT NULL,
    instrument_id       BIGINT REFERENCES instruments(instrument_id),
    event_kind          TEXT NOT NULL CHECK (event_kind IN ('open', 'close')),
    side                TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    units               NUMERIC(20, 8) NOT NULL CHECK (units > 0),
    price               NUMERIC(20, 8) CHECK (price > 0),
    executed_at         TIMESTAMPTZ NOT NULL,
    fees_usd            NUMERIC(20, 4),
    realized_pnl_usd    NUMERIC(20, 4) CHECK (event_kind = 'close' OR realized_pnl_usd IS NULL),
    investment_usd      NUMERIC(20, 4),
    order_id            BIGINT,
    social_trade_id     BIGINT,
    parent_position_id  BIGINT,
    source              TEXT NOT NULL CHECK (source IN ('etoro_sync', 'etoro_history')),
    raw_payload         JSONB NOT NULL,
    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One open per position (first observation wins; portfolio-diff opens
-- carry initialUnits and beat later history-derived opens).
CREATE UNIQUE INDEX IF NOT EXISTS uq_trade_events_open
    ON trade_events (position_id) WHERE event_kind = 'open';

-- One close per (position, close timestamp) — partial closes land as
-- distinct rows; a same-ms collision is counted as conflict_anomaly by
-- the ingest service (spec §15), never silently merged.
CREATE UNIQUE INDEX IF NOT EXISTS uq_trade_events_close
    ON trade_events (position_id, executed_at) WHERE event_kind = 'close';

CREATE INDEX IF NOT EXISTS idx_trade_events_instrument_time
    ON trade_events (instrument_id, executed_at);

-- Archive for broker positions that disappeared from the broker payload
-- (closed externally: eToro UI, SL/TP trigger). Evidence copy of the
-- last-seen row, written immediately before the disappeared-DELETE
-- sweep in _upsert_broker_positions. Real ids only (position_id >= 0);
-- synthetic eBull handoff rows are not archived.
CREATE TABLE IF NOT EXISTS broker_positions_closed (
    LIKE broker_positions INCLUDING DEFAULTS,
    closed_detected_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (position_id, closed_detected_at)
);

COMMIT;
