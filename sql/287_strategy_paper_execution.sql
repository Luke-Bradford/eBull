-- 287_strategy_paper_execution.sql
--
-- #2449 / #2437 demo-only allocator.  Policy is current state plus a bounded
-- audit history; each fired signal gets at most one compact preflight row.
-- Broker payloads, quote histories and polling heartbeats are deliberately not
-- copied here.

CREATE TABLE IF NOT EXISTS strategy_execution_policies (
    deployment_id                   BIGINT PRIMARY KEY
        REFERENCES strategy_deployments(deployment_id) ON DELETE RESTRICT,
    revision                        BIGINT NOT NULL CHECK (revision >= 1),
    ticket_fraction                 NUMERIC(12,8) NOT NULL CHECK (ticket_fraction > 0 AND ticket_fraction <= 1),
    max_ticket_amount               NUMERIC(18,6) NOT NULL CHECK (max_ticket_amount > 0),
    stop_loss_pct                   NUMERIC(12,8) NOT NULL CHECK (stop_loss_pct > 0 AND stop_loss_pct < 100),
    take_profit_pct                 NUMERIC(12,8) NOT NULL CHECK (take_profit_pct > 0),
    max_quote_age_seconds           INTEGER NOT NULL CHECK (max_quote_age_seconds > 0),
    max_scan_age_seconds            INTEGER NOT NULL CHECK (max_scan_age_seconds > 0),
    max_halt_feed_age_seconds       INTEGER NOT NULL CHECK (max_halt_feed_age_seconds > 0),
    max_cost_age_seconds            INTEGER NOT NULL CHECK (max_cost_age_seconds > 0),
    max_reconciliation_age_seconds  INTEGER NOT NULL CHECK (max_reconciliation_age_seconds > 0),
    max_instrument_exposure_pct     NUMERIC(12,8) NOT NULL CHECK (
        max_instrument_exposure_pct > 0 AND max_instrument_exposure_pct <= 100
    ),
    max_portfolio_exposure_pct      NUMERIC(12,8) NOT NULL CHECK (
        max_portfolio_exposure_pct > 0 AND max_portfolio_exposure_pct <= 100
    ),
    max_drawdown_pct                NUMERIC(12,8) NOT NULL CHECK (max_drawdown_pct > 0 AND max_drawdown_pct < 100),
    min_net_expectancy_pct          NUMERIC(12,8) NOT NULL,
    cost_stress_multiplier          NUMERIC(12,8) NOT NULL CHECK (cost_stress_multiplier >= 1),
    updated_by                      TEXT NOT NULL CHECK (updated_by <> '' AND length(updated_by) <= 200),
    reason                          TEXT NOT NULL CHECK (reason <> '' AND length(reason) <= 1000),
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS strategy_execution_policy_events (
    policy_event_id                 BIGSERIAL PRIMARY KEY,
    deployment_id                  BIGINT NOT NULL
        REFERENCES strategy_deployments(deployment_id) ON DELETE RESTRICT,
    revision                       BIGINT NOT NULL CHECK (revision >= 1),
    ticket_fraction                NUMERIC(12,8) NOT NULL,
    max_ticket_amount              NUMERIC(18,6) NOT NULL,
    stop_loss_pct                  NUMERIC(12,8) NOT NULL,
    take_profit_pct                NUMERIC(12,8) NOT NULL,
    max_quote_age_seconds          INTEGER NOT NULL,
    max_scan_age_seconds           INTEGER NOT NULL,
    max_halt_feed_age_seconds      INTEGER NOT NULL,
    max_cost_age_seconds           INTEGER NOT NULL,
    max_reconciliation_age_seconds INTEGER NOT NULL,
    max_instrument_exposure_pct    NUMERIC(12,8) NOT NULL,
    max_portfolio_exposure_pct     NUMERIC(12,8) NOT NULL,
    max_drawdown_pct               NUMERIC(12,8) NOT NULL,
    min_net_expectancy_pct         NUMERIC(12,8) NOT NULL,
    cost_stress_multiplier         NUMERIC(12,8) NOT NULL,
    changed_by                     TEXT NOT NULL CHECK (changed_by <> '' AND length(changed_by) <= 200),
    reason                         TEXT NOT NULL CHECK (reason <> '' AND length(reason) <= 1000),
    changed_at                     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (deployment_id, revision)
);

-- One current feed observation, updated in place every successful fetch.
CREATE TABLE IF NOT EXISTS strategy_halt_feed_state (
    source          TEXT PRIMARY KEY CHECK (source = 'nasdaq_trader_rss'),
    fetched_at      TIMESTAMPTZ NOT NULL,
    source_pub_at   TIMESTAMPTZ NOT NULL,
    item_count      INTEGER NOT NULL CHECK (item_count >= 0),
    payload_sha256  TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$')
);

-- Provider-native halt identity. Rows are upserted as Nasdaq adds resumption
-- fields; a routine deletes rows older than 90 days.
CREATE TABLE IF NOT EXISTS strategy_market_halts (
    source              TEXT NOT NULL CHECK (source = 'nasdaq_trader_rss'),
    symbol              TEXT NOT NULL CHECK (symbol <> '' AND length(symbol) <= 40),
    halt_at             TIMESTAMPTZ NOT NULL,
    market              TEXT NOT NULL CHECK (market <> '' AND length(market) <= 80),
    reason_code         TEXT NOT NULL CHECK (reason_code <> '' AND length(reason_code) <= 20),
    resumed_at          TIMESTAMPTZ,
    observed_at         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source, symbol, halt_at),
    CHECK (resumed_at IS NULL OR resumed_at >= halt_at)
);

CREATE INDEX IF NOT EXISTS idx_strategy_market_halts_active_symbol
    ON strategy_market_halts (symbol, halt_at DESC) WHERE resumed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_strategy_market_halts_retention
    ON strategy_market_halts (halt_at);

-- Decision-bearing facts only. This is a one-row shadow/allocated arm for a
-- durable fired signal and does not retain raw eligibility/cost/portfolio data.
CREATE TABLE IF NOT EXISTS strategy_entry_preflights (
    signal_id                    BIGINT PRIMARY KEY
        REFERENCES strategy_signals(signal_id) ON DELETE RESTRICT,
    deployment_id                BIGINT
        REFERENCES strategy_deployments(deployment_id) ON DELETE RESTRICT,
    policy_revision              BIGINT,
    verdict                      TEXT NOT NULL CHECK (verdict IN ('allocated', 'rejected')),
    reason_code                  TEXT NOT NULL CHECK (reason_code <> '' AND length(reason_code) <= 100),
    evaluated_at                 TIMESTAMPTZ NOT NULL,
    quote_at                     TIMESTAMPTZ,
    scan_at                      TIMESTAMPTZ,
    halt_feed_at                 TIMESTAMPTZ,
    eligibility_checked_at       TIMESTAMPTZ,
    costs_at                     TIMESTAMPTZ,
    broker_available_cash        NUMERIC(18,6),
    account_equity               NUMERIC(18,6),
    account_invested             NUMERIC(18,6),
    instrument_invested          NUMERIC(18,6),
    account_drawdown_pct         NUMERIC(12,8),
    allocated_amount             NUMERIC(18,6),
    gross_expectancy_ci_low_pct  NUMERIC(12,8),
    stressed_cost_amount         NUMERIC(18,6),
    net_expectancy_pct           NUMERIC(12,8),
    stop_loss_rate               NUMERIC(18,6),
    take_profit_rate             NUMERIC(18,6),
    CHECK (
        (verdict = 'allocated' AND deployment_id IS NOT NULL AND policy_revision IS NOT NULL
         AND allocated_amount > 0 AND stop_loss_rate > 0 AND take_profit_rate > 0)
        OR
        (verdict = 'rejected' AND allocated_amount IS NULL)
    )
);

COMMENT ON TABLE strategy_entry_preflights IS
    'One compact decision-bearing preflight per fired entry signal. Rejections '
    'are the unfunded shadow arm; raw broker/feed payloads are not persisted.';
