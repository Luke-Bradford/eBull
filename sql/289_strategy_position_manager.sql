-- 289_strategy_position_manager.sql
--
-- #2452 / #2437 exact-position paper manager.  The schema stores only
-- operator policy, registered ratchet variants, and material broker mutations.
-- Polls and unchanged bars update no rows.

CREATE TABLE IF NOT EXISTS strategy_ratchet_variants (
    ratchet_variant_id       BIGSERIAL PRIMARY KEY,
    strategy_id              TEXT NOT NULL CHECK (strategy_id <> '' AND length(strategy_id) <= 200),
    strategy_version         TEXT NOT NULL CHECK (strategy_version <> '' AND length(strategy_version) <= 200),
    promotion_id             BIGINT NOT NULL
        REFERENCES strategy_promotions(promotion_id) ON DELETE RESTRICT,
    rule_version             TEXT NOT NULL UNIQUE CHECK (rule_version <> '' AND length(rule_version) <= 200),
    break_atr_multiple       NUMERIC(12,8) NOT NULL CHECK (break_atr_multiple > 0),
    chandelier_atr_multiple  NUMERIC(12,8) NOT NULL CHECK (chandelier_atr_multiple > 0),
    structure_atr_multiple   NUMERIC(12,8) NOT NULL CHECK (structure_atr_multiple > 0),
    registered_by            TEXT NOT NULL CHECK (registered_by <> '' AND length(registered_by) <= 200),
    reason                   TEXT NOT NULL CHECK (reason <> '' AND length(reason) <= 1000),
    registered_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (strategy_id, strategy_version, promotion_id)
);

COMMENT ON TABLE strategy_ratchet_variants IS
    'A ratchet is a separately identified, promoted backtest variant. The '
    'manager cannot enable an unregistered formula or tune constants in place.';

CREATE TABLE IF NOT EXISTS strategy_position_manager_policies (
    deployment_id              BIGINT PRIMARY KEY
        REFERENCES strategy_deployments(deployment_id) ON DELETE RESTRICT,
    revision                   BIGINT NOT NULL CHECK (revision >= 1),
    max_position_age_seconds   INTEGER CHECK (max_position_age_seconds IS NULL OR max_position_age_seconds > 0),
    ratchet_variant_id         BIGINT
        REFERENCES strategy_ratchet_variants(ratchet_variant_id) ON DELETE RESTRICT,
    updated_by                 TEXT NOT NULL CHECK (updated_by <> '' AND length(updated_by) <= 200),
    reason                     TEXT NOT NULL CHECK (reason <> '' AND length(reason) <= 1000),
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS strategy_position_manager_policy_events (
    policy_event_id            BIGSERIAL PRIMARY KEY,
    deployment_id              BIGINT NOT NULL
        REFERENCES strategy_deployments(deployment_id) ON DELETE RESTRICT,
    revision                   BIGINT NOT NULL CHECK (revision >= 1),
    max_position_age_seconds   INTEGER,
    ratchet_variant_id         BIGINT
        REFERENCES strategy_ratchet_variants(ratchet_variant_id) ON DELETE RESTRICT,
    changed_by                 TEXT NOT NULL CHECK (changed_by <> '' AND length(changed_by) <= 200),
    reason                     TEXT NOT NULL CHECK (reason <> '' AND length(reason) <= 1000),
    changed_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (deployment_id, revision),
    CHECK (max_position_age_seconds IS NULL OR max_position_age_seconds > 0)
);

-- Low cardinality: one row per actual PATCH/close intent. A
-- manager heartbeat or a completed bar which leaves the stop unchanged writes
-- nothing. request_id is committed before broker I/O and is immutable.
CREATE TABLE IF NOT EXISTS strategy_position_operations (
    position_operation_id  BIGSERIAL PRIMARY KEY,
    ownership_id           BIGINT NOT NULL
        REFERENCES strategy_position_ownership(ownership_id) ON DELETE RESTRICT,
    order_id               BIGINT UNIQUE
        REFERENCES orders(order_id) ON DELETE RESTRICT,
    operation_type         TEXT NOT NULL CHECK (operation_type IN ('fixed_exit_repair', 'stop_ratchet', 'close')),
    trigger_code           TEXT NOT NULL CHECK (trigger_code IN ('entry_exit_gap', 'causal_resistance_break',
                                                                  'timeout', 'strategy_exit', 'emergency_risk')),
    request_id             UUID NOT NULL UNIQUE,
    status                 TEXT NOT NULL CHECK (status IN (
        'intent_persisted', 'submitted', 'applied', 'rejected', 'reconcile_required'
    )),
    prior_stop_rate        NUMERIC(20,6),
    desired_stop_rate      NUMERIC(20,6),
    desired_take_profit_rate NUMERIC(20,6),
    completed_bar_at       TIMESTAMPTZ,
    level_known_at         TIMESTAMPTZ,
    close_rate             NUMERIC(20,6),
    highest_close_since_entry NUMERIC(20,6),
    atr_rate               NUMERIC(20,6),
    resistance_rate        NUMERIC(20,6),
    ratchet_variant_id     BIGINT
        REFERENCES strategy_ratchet_variants(ratchet_variant_id) ON DELETE RESTRICT,
    broker_operation_id    UUID,
    broker_order_ref       BIGINT CHECK (broker_order_ref IS NULL OR broker_order_ref > 0),
    broker_response_json   JSONB,
    last_error_code        TEXT CHECK (last_error_code IS NULL OR (last_error_code <> '' AND length(last_error_code) <= 100)),
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    submitted_at           TIMESTAMPTZ,
    resolved_at            TIMESTAMPTZ,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        (operation_type = 'close' AND order_id IS NOT NULL
         AND desired_stop_rate IS NULL AND desired_take_profit_rate IS NULL)
        OR
        (operation_type IN ('fixed_exit_repair', 'stop_ratchet')
         AND order_id IS NULL AND desired_stop_rate > 0)
    ),
    CHECK (
        (operation_type = 'stop_ratchet' AND ratchet_variant_id IS NOT NULL
         AND completed_bar_at IS NOT NULL AND level_known_at IS NOT NULL
         AND close_rate > 0 AND highest_close_since_entry > 0 AND atr_rate > 0 AND resistance_rate > 0)
        OR operation_type <> 'stop_ratchet'
    ),
    CHECK (
        operation_type <> 'stop_ratchet'
        OR prior_stop_rate IS NULL
        OR desired_stop_rate > prior_stop_rate
    ),
    CHECK (level_known_at IS NULL OR completed_bar_at IS NULL OR level_known_at <= completed_bar_at),
    CHECK (
        (status IN ('intent_persisted', 'submitted') AND resolved_at IS NULL)
        OR (status IN ('applied', 'rejected', 'reconcile_required') AND resolved_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_position_one_unresolved_operation
    ON strategy_position_operations (ownership_id)
    WHERE status IN ('intent_persisted', 'submitted');

CREATE INDEX IF NOT EXISTS idx_strategy_position_operations_trade_recent
    ON strategy_position_operations (ownership_id, position_operation_id DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_position_operation_material_identity
    ON strategy_position_operations (
        ownership_id, operation_type, desired_stop_rate,
        desired_take_profit_rate, completed_bar_at
    ) NULLS NOT DISTINCT
    WHERE operation_type IN ('fixed_exit_repair', 'stop_ratchet');

COMMENT ON TABLE strategy_position_operations IS
    'Material exact-position PATCH/close intents only. One latest bounded-shape '
    'broker response may be retained; polls and unchanged checks grow no rows.';
