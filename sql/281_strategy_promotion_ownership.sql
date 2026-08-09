-- 281_strategy_promotion_ownership.sql
--
-- #2454 / #2437 control-plane slice 5.  This migration deliberately adds no
-- broker writer.  It records the operator decisions that may eventually
-- authorise one, and the exact broker position identity that every later
-- strategy mutation must present.

-- A promotion is an append-only event.  Current state is derived from the
-- newest event; there is no mutable `promotable` boolean for a metric refresh
-- to flip behind an operator's back.
CREATE TABLE IF NOT EXISTS strategy_promotions (
    promotion_id       BIGSERIAL PRIMARY KEY,
    strategy_id        TEXT NOT NULL,
    strategy_version   TEXT NOT NULL,
    from_stage         TEXT,
    to_stage           TEXT NOT NULL,
    gate_version       TEXT NOT NULL,
    evidence_ref       TEXT,
    promoted_by        TEXT NOT NULL,
    reason             TEXT NOT NULL,
    promoted_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT strategy_promotions_from_stage_check CHECK (
        from_stage IS NULL OR from_stage IN (
            'research_candidate', 'historical_validated',
            'forward_observation', 'paper_enabled', 'live_enabled',
            'paused', 'retired'
        )
    ),
    CONSTRAINT strategy_promotions_to_stage_check CHECK (to_stage IN (
        'research_candidate', 'historical_validated',
        'forward_observation', 'paper_enabled', 'live_enabled',
        'paused', 'retired'
    )),
    CONSTRAINT strategy_promotions_non_empty CHECK (
        strategy_id <> '' AND strategy_version <> '' AND gate_version <> ''
        AND promoted_by <> '' AND reason <> ''
        AND (evidence_ref IS NULL OR evidence_ref <> '')
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_promotions_one_initial
    ON strategy_promotions (strategy_id, strategy_version)
    WHERE from_stage IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_promotions_one_successor
    ON strategy_promotions (strategy_id, strategy_version, from_stage)
    WHERE from_stage IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_strategy_promotions_current
    ON strategy_promotions (strategy_id, strategy_version, promotion_id DESC);

-- A promotion may pin several result arms/folds.  Real FKs are used rather
-- than a JSON/array of ids so deleting or mistyping evidence cannot leave a
-- promotion that merely looks evidenced.
CREATE TABLE IF NOT EXISTS strategy_promotion_results (
    promotion_id BIGINT NOT NULL
        REFERENCES strategy_promotions(promotion_id) ON DELETE RESTRICT,
    result_id BIGINT NOT NULL
        REFERENCES strategy_results_store(result_id) ON DELETE RESTRICT,
    PRIMARY KEY (promotion_id, result_id)
);

-- Current operator capital ceilings.  These are maxima, never instructions to
-- spend the full amount.  Mutations are mirrored to the event table below.
CREATE TABLE IF NOT EXISTS strategy_deployments (
    deployment_id      BIGSERIAL PRIMARY KEY,
    strategy_id        TEXT NOT NULL,
    strategy_version   TEXT NOT NULL,
    mode                TEXT NOT NULL CHECK (mode IN ('paper', 'live')),
    capital_limit       NUMERIC(18,6) NOT NULL CHECK (capital_limit >= 0),
    currency            TEXT NOT NULL DEFAULT 'USD',
    enabled             BOOLEAN NOT NULL DEFAULT false,
    revision            BIGINT NOT NULL DEFAULT 1 CHECK (revision >= 1),
    updated_by          TEXT NOT NULL,
    reason              TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT strategy_deployments_unique UNIQUE
        (strategy_id, strategy_version, mode),
    CONSTRAINT strategy_deployments_non_empty CHECK (
        strategy_id <> '' AND strategy_version <> '' AND currency <> ''
        AND updated_by <> '' AND reason <> ''
    )
);

CREATE TABLE IF NOT EXISTS strategy_deployment_events (
    deployment_event_id BIGSERIAL PRIMARY KEY,
    deployment_id       BIGINT NOT NULL
        REFERENCES strategy_deployments(deployment_id) ON DELETE RESTRICT,
    revision             BIGINT NOT NULL CHECK (revision >= 1),
    capital_limit        NUMERIC(18,6) NOT NULL CHECK (capital_limit >= 0),
    currency             TEXT NOT NULL,
    enabled              BOOLEAN NOT NULL,
    changed_by           TEXT NOT NULL,
    reason               TEXT NOT NULL,
    changed_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT strategy_deployment_events_unique
        UNIQUE (deployment_id, revision),
    CONSTRAINT strategy_deployment_events_non_empty CHECK (
        currency <> '' AND changed_by <> '' AND reason <> ''
    )
);

CREATE INDEX IF NOT EXISTS idx_strategy_deployment_events_recent
    ON strategy_deployment_events (deployment_id, revision DESC);

-- Exactly one durable verdict for each fired signal.  A rejection consumes no
-- capital and names no deployment.  This is intentionally narrow: rationale
-- is a bounded code plus a short operator/debug detail, not a market snapshot.
CREATE TABLE IF NOT EXISTS strategy_funding_decisions (
    funding_decision_id BIGSERIAL PRIMARY KEY,
    signal_id           BIGINT NOT NULL UNIQUE
        REFERENCES strategy_signals(signal_id) ON DELETE RESTRICT,
    deployment_id       BIGINT
        REFERENCES strategy_deployments(deployment_id) ON DELETE RESTRICT,
    verdict              TEXT NOT NULL CHECK (verdict IN ('allocated', 'rejected')),
    amount               NUMERIC(18,6),
    reason_code          TEXT NOT NULL,
    detail               TEXT,
    decided_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT strategy_funding_decisions_shape CHECK (
        (verdict = 'allocated' AND deployment_id IS NOT NULL AND amount > 0)
        OR (verdict = 'rejected' AND deployment_id IS NULL AND amount IS NULL)
    ),
    CONSTRAINT strategy_funding_decisions_non_empty CHECK (
        reason_code <> '' AND (detail IS NULL OR detail <> '')
    )
);

CREATE INDEX IF NOT EXISTS idx_strategy_funding_decisions_recent
    ON strategy_funding_decisions (decided_at DESC);

-- A strategy trade is born from one allocated funding decision.  Later slices
-- extend the lifecycle, but ownership is already explicit and durable here.
CREATE TABLE IF NOT EXISTS strategy_trades (
    strategy_trade_id   BIGSERIAL PRIMARY KEY,
    funding_decision_id BIGINT NOT NULL UNIQUE
        REFERENCES strategy_funding_decisions(funding_decision_id) ON DELETE RESTRICT,
    instrument_id       BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE RESTRICT,
    status              TEXT NOT NULL DEFAULT 'planned'
        CHECK (status IN ('planned', 'submitted', 'open', 'closing', 'closed',
                          'failed', 'reconcile_required')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Existing/manual order creation keeps its behaviour through this default.
-- Automated code must opt in explicitly, and the link below supplies the
-- strategy trade and purpose.
ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS execution_origin TEXT NOT NULL DEFAULT 'manual';

ALTER TABLE orders
    DROP CONSTRAINT IF EXISTS orders_execution_origin_check;

ALTER TABLE orders
    ADD CONSTRAINT orders_execution_origin_check
    CHECK (execution_origin IN ('manual', 'strategy'));

CREATE INDEX IF NOT EXISTS idx_orders_strategy_origin
    ON orders (created_at DESC) WHERE execution_origin = 'strategy';

CREATE TABLE IF NOT EXISTS strategy_trade_orders (
    strategy_trade_order_id BIGSERIAL PRIMARY KEY,
    strategy_trade_id       BIGINT NOT NULL
        REFERENCES strategy_trades(strategy_trade_id) ON DELETE RESTRICT,
    order_id                BIGINT NOT NULL UNIQUE
        REFERENCES orders(order_id) ON DELETE RESTRICT,
    purpose                 TEXT NOT NULL CHECK (purpose IN (
        'entry', 'exit', 'stop_loss', 'take_profit', 'stop_ratchet', 'reconcile'
    )),
    linked_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT strategy_trade_orders_one_purpose
        UNIQUE (strategy_trade_id, purpose, order_id)
);

CREATE INDEX IF NOT EXISTS idx_strategy_trade_orders_trade
    ON strategy_trade_orders (strategy_trade_id, linked_at);

-- Deliberately no FK to broker_positions: the broker snapshot is current-state
-- storage and legitimately removes a closed position.  Ownership must survive
-- that removal for audit and reconciliation.  A broker id is never recycled
-- between strategy trades; release records history rather than deleting it.
CREATE TABLE IF NOT EXISTS strategy_position_ownership (
    ownership_id       BIGSERIAL PRIMARY KEY,
    strategy_trade_id  BIGINT NOT NULL
        REFERENCES strategy_trades(strategy_trade_id) ON DELETE RESTRICT,
    broker_position_id BIGINT NOT NULL UNIQUE CHECK (broker_position_id > 0),
    status             TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'released')),
    claimed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at        TIMESTAMPTZ,
    release_reason     TEXT,
    CONSTRAINT strategy_position_ownership_release_shape CHECK (
        (status = 'active' AND released_at IS NULL AND release_reason IS NULL)
        OR (status = 'released' AND released_at IS NOT NULL AND release_reason <> '')
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_position_one_active_trade
    ON strategy_position_ownership (strategy_trade_id)
    WHERE status = 'active';

COMMENT ON TABLE strategy_position_ownership IS
    'Durable exact broker position ownership for automated strategy trades. '
    'Strategy mutation services require (strategy_trade_id, broker_position_id); '
    'instrument/FIFO inference is forbidden. Manual positions have no row.';

COMMENT ON COLUMN orders.execution_origin IS
    'manual by default to preserve existing UI/order behaviour; strategy is '
    'set only by the strategy executor and requires a strategy_trade_orders link.';
