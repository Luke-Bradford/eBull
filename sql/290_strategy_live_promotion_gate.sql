-- 290_strategy_live_promotion_gate.sql
--
-- #2450 / #2437 final promotion slice.  Live thresholds are immutable and
-- must be registered before paper evidence is observed.  The tables retain
-- operator decisions, five material drill results, explicit promotion attempts
-- and one rolling risk row per deployment; market ticks, health heartbeats and
-- P&L snapshots stay out of SQL.

CREATE TABLE IF NOT EXISTS strategy_live_gate_policies (
    live_gate_policy_id              BIGSERIAL PRIMARY KEY,
    strategy_id                      TEXT NOT NULL,
    strategy_version                 TEXT NOT NULL,
    min_forward_resolved_signals     INTEGER NOT NULL CHECK (min_forward_resolved_signals > 0),
    min_forward_days                 INTEGER NOT NULL CHECK (min_forward_days > 0),
    min_paper_closed_trades          INTEGER NOT NULL CHECK (min_paper_closed_trades > 0),
    min_paper_days                   INTEGER NOT NULL CHECK (min_paper_days > 0),
    max_reconciliation_age_seconds   INTEGER NOT NULL CHECK (max_reconciliation_age_seconds > 0),
    min_shadow_alpha_pct             NUMERIC(12,8) NOT NULL,
    max_cost_drift_pct               NUMERIC(12,8) NOT NULL CHECK (max_cost_drift_pct >= 0),
    max_average_slippage_pct         NUMERIC(12,8) NOT NULL CHECK (max_average_slippage_pct >= 0),
    max_drawdown_pct                 NUMERIC(12,8) NOT NULL CHECK (max_drawdown_pct > 0 AND max_drawdown_pct < 100),
    max_scan_age_seconds             INTEGER NOT NULL CHECK (max_scan_age_seconds > 0),
    max_quote_age_seconds            INTEGER NOT NULL CHECK (max_quote_age_seconds > 0),
    max_broker_health_age_seconds    INTEGER NOT NULL CHECK (max_broker_health_age_seconds > 0),
    max_live_capital                 NUMERIC(18,6) NOT NULL CHECK (max_live_capital > 0),
    currency                         TEXT NOT NULL CHECK (currency = 'USD'),
    leverage                         SMALLINT NOT NULL CHECK (leverage = 1),
    registered_by                    TEXT NOT NULL CHECK (char_length(registered_by) BETWEEN 1 AND 200),
    reason                           TEXT NOT NULL CHECK (char_length(reason) BETWEEN 1 AND 1000),
    registered_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT strategy_live_gate_policy_unique UNIQUE (strategy_id, strategy_version),
    CONSTRAINT strategy_live_gate_policy_identity CHECK (
        char_length(strategy_id) BETWEEN 1 AND 200
        AND char_length(strategy_version) BETWEEN 1 AND 200
    )
);

CREATE TABLE IF NOT EXISTS strategy_paper_deployment_risk_state (
    deployment_id       BIGINT PRIMARY KEY
        REFERENCES strategy_deployments(deployment_id) ON DELETE RESTRICT,
    equity_high_water   NUMERIC(18,6) NOT NULL CHECK (equity_high_water > 0),
    last_equity         NUMERIC(18,6) NOT NULL CHECK (last_equity > 0),
    last_drawdown_pct   NUMERIC(12,8) NOT NULL CHECK (last_drawdown_pct >= 0),
    max_drawdown_pct    NUMERIC(12,8) NOT NULL CHECK (max_drawdown_pct >= 0),
    observed_at         TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS strategy_kill_drill_events (
    kill_drill_event_id BIGSERIAL PRIMARY KEY,
    live_gate_policy_id BIGINT NOT NULL
        REFERENCES strategy_live_gate_policies(live_gate_policy_id) ON DELETE RESTRICT,
    drill_kind          TEXT NOT NULL CHECK (drill_kind IN (
        'quote_lag', 'scan_lag', 'broker_outage',
        'reconciliation_backlog', 'drawdown'
    )),
    entry_block_observed    BOOLEAN NOT NULL,
    state_restored          BOOLEAN NOT NULL,
    passed                  BOOLEAN GENERATED ALWAYS AS (
        entry_block_observed AND state_restored
    ) STORED,
    run_by              TEXT NOT NULL CHECK (char_length(run_by) BETWEEN 1 AND 200),
    reason              TEXT NOT NULL CHECK (char_length(reason) BETWEEN 1 AND 1000),
    run_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_strategy_kill_drill_latest
    ON strategy_kill_drill_events (live_gate_policy_id, drill_kind, run_at DESC);

CREATE TABLE IF NOT EXISTS strategy_live_gate_assessments (
    live_gate_assessment_id BIGSERIAL PRIMARY KEY,
    live_gate_policy_id     BIGINT NOT NULL
        REFERENCES strategy_live_gate_policies(live_gate_policy_id) ON DELETE RESTRICT,
    promotion_id            BIGINT
        REFERENCES strategy_promotions(promotion_id) ON DELETE RESTRICT,
    requested_capital       NUMERIC(18,6) NOT NULL CHECK (requested_capital > 0),
    passed                  BOOLEAN NOT NULL,
    refusal_codes           TEXT[] NOT NULL,
    evidence_sha256         TEXT NOT NULL CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
    evidence_json           JSONB NOT NULL CHECK (jsonb_typeof(evidence_json) = 'object'),
    assessed_by             TEXT NOT NULL CHECK (char_length(assessed_by) BETWEEN 1 AND 200),
    reason                  TEXT NOT NULL CHECK (char_length(reason) BETWEEN 1 AND 1000),
    assessed_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT strategy_live_gate_assessment_shape CHECK (
        (passed AND cardinality(refusal_codes) = 0 AND promotion_id IS NOT NULL)
        OR (NOT passed AND cardinality(refusal_codes) > 0 AND promotion_id IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_strategy_live_gate_assessment_recent
    ON strategy_live_gate_assessments (live_gate_policy_id, assessed_at DESC);

CREATE TABLE IF NOT EXISTS strategy_paper_pool_events (
    strategy_paper_pool_event_id BIGSERIAL PRIMARY KEY,
    enabled                      BOOLEAN NOT NULL,
    capital_limit                NUMERIC(18,6) NOT NULL CHECK (capital_limit >= 0),
    currency                     TEXT NOT NULL CHECK (currency = 'USD'),
    changed_by                   TEXT NOT NULL CHECK (char_length(changed_by) BETWEEN 1 AND 200),
    reason                       TEXT NOT NULL CHECK (char_length(reason) BETWEEN 1 AND 1000),
    changed_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT strategy_paper_pool_enabled_has_capital CHECK (NOT enabled OR capital_limit > 0)
);

COMMENT ON TABLE strategy_live_gate_policies IS
    'Immutable, pre-paper live-promotion thresholds. No defaults: policy values '
    'are preregistered operator decisions, not universal market constants.';

COMMENT ON TABLE strategy_paper_deployment_risk_state IS
    'One update-in-place paper-period high-water and maximum drawdown row per '
    'deployment; never an account-value observation series.';

COMMENT ON TABLE strategy_live_gate_assessments IS
    'One compact evidence snapshot per explicit operator promotion attempt. '
    'Periodic health reads do not write assessment rows.';

COMMENT ON TABLE strategy_paper_pool_events IS
    'Material operator revisions to the shared paper-only strategy capital '
    'ceiling. The latest row is current state; no scheduler heartbeat writes.';
