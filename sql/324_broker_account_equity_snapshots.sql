-- 324_broker_account_equity_snapshots.sql
--
-- #2559: compact prospective F-0 account evidence from eToro's official P&L
-- endpoint. One row per environment/UTC day; no raw broker payload, positions,
-- prices, indicators or polling history are duplicated here.

CREATE TABLE broker_account_equity_snapshots (
    environment       TEXT NOT NULL CHECK (environment IN ('demo','real')),
    snapshot_date     DATE NOT NULL,
    observed_at       TIMESTAMPTZ NOT NULL,
    source_version    TEXT NOT NULL CHECK (source_version <> ''),
    currency          TEXT NOT NULL CHECK (currency = 'USD'),
    available_cash    NUMERIC(20,6) NOT NULL CHECK (available_cash >= 0),
    total_invested    NUMERIC(20,6) NOT NULL CHECK (total_invested >= 0),
    unrealised_pnl    NUMERIC(20,6) NOT NULL,
    equity            NUMERIC(20,6) NOT NULL CHECK (equity > 0),
    recorded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (environment,snapshot_date),
    CHECK (snapshot_date = (observed_at AT TIME ZONE 'UTC')::date),
    CHECK (abs(equity - (available_cash + total_invested + unrealised_pnl)) <= 0.000001)
);

COMMENT ON TABLE broker_account_equity_snapshots IS
    'One official prospective account-equity observation per UTC day. Raw P&L payload and per-position facts are deliberately not retained.';
