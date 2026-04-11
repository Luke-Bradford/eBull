-- Migration 022: copy trading ingestion (Track 1a)
--
-- Adds three sibling tables so the eToro /portfolio payload's
-- clientPortfolio.mirrors[] data can be ingested first-class:
--
--   copy_traders          — one row per eToro trader identity
--   copy_mirrors          — one row per copy relationship (mirror_id)
--   copy_mirror_positions — one row per nested position inside a mirror
--
-- Existing tables (positions, cash_ledger, positions.source) are
-- untouched. The execution guard's rule queries continue to read
-- FROM positions only; mirrors inflate AUM via a separate query in
-- Track 1b (#187) — this migration is the schema prerequisite.
--
-- Soft-close semantics: copy_mirrors.active / closed_at columns let
-- a mirror that disappears from the payload be marked closed rather
-- than deleted. Nested positions are retained on soft-closed mirrors
-- for audit. See spec §1 and §2.3.4.
--
-- Issue: #183

BEGIN;

CREATE TABLE copy_traders (
    parent_cid      BIGINT PRIMARY KEY,
    parent_username TEXT   NOT NULL,

    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX copy_traders_username_idx ON copy_traders (parent_username);

CREATE TABLE copy_mirrors (
    mirror_id  BIGINT PRIMARY KEY,
    parent_cid BIGINT NOT NULL REFERENCES copy_traders(parent_cid),

    initial_investment          NUMERIC(20, 4) NOT NULL,
    deposit_summary             NUMERIC(20, 4) NOT NULL,
    withdrawal_summary          NUMERIC(20, 4) NOT NULL,
    available_amount            NUMERIC(20, 4) NOT NULL,
    closed_positions_net_profit NUMERIC(20, 4) NOT NULL,
    stop_loss_percentage        NUMERIC(10, 4),
    stop_loss_amount            NUMERIC(20, 4),
    mirror_status_id            INTEGER,
    mirror_calculation_type     INTEGER,
    pending_for_closure         BOOLEAN NOT NULL DEFAULT FALSE,
    started_copy_date           TIMESTAMPTZ NOT NULL,

    active      BOOLEAN     NOT NULL DEFAULT TRUE,
    closed_at   TIMESTAMPTZ NULL,

    raw_payload JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX copy_mirrors_parent_cid_idx ON copy_mirrors (parent_cid);
CREATE INDEX copy_mirrors_active_idx     ON copy_mirrors (active) WHERE active;

CREATE TABLE copy_mirror_positions (
    mirror_id   BIGINT NOT NULL REFERENCES copy_mirrors(mirror_id) ON DELETE CASCADE,
    position_id BIGINT NOT NULL,
    PRIMARY KEY (mirror_id, position_id),

    parent_position_id BIGINT NOT NULL,
    instrument_id      BIGINT NOT NULL,

    is_buy                    BOOLEAN         NOT NULL,
    units                     NUMERIC(20, 8)  NOT NULL,
    amount                    NUMERIC(20, 4)  NOT NULL,
    initial_amount_in_dollars NUMERIC(20, 4)  NOT NULL,
    open_rate                 NUMERIC(20, 6)  NOT NULL,
    open_conversion_rate      NUMERIC(20, 10) NOT NULL,
    open_date_time            TIMESTAMPTZ     NOT NULL,
    take_profit_rate          NUMERIC(20, 6),
    stop_loss_rate            NUMERIC(20, 6),
    total_fees                NUMERIC(20, 4)  NOT NULL DEFAULT 0,
    leverage                  INTEGER         NOT NULL DEFAULT 1,

    raw_payload JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX copy_mirror_positions_instrument_id_idx
    ON copy_mirror_positions (instrument_id);

COMMIT;
