-- 458_strategy_entry_tickets.sql
-- #3542 (gap register P3) slice 1: every engine ENTRY order carries a trade ticket --
-- rule id, evidence id, rationale class, exit rule (or not_applicable with a reason),
-- expected cost -- written in the path's authority transaction. Exits and protective
-- orders never get one (settled: an exit is never blocked). Enforcement (a deferred
-- constraint trigger) and immutability are slice 2. Spec:
-- docs/proposals/execution/2026-10-02-3542-entry-trade-ticket.md.

CREATE TABLE IF NOT EXISTS strategy_entry_tickets (
    order_id            BIGINT PRIMARY KEY REFERENCES orders(order_id) ON DELETE RESTRICT,
    strategy_trade_id   BIGINT NOT NULL REFERENCES strategy_trades(strategy_trade_id) ON DELETE RESTRICT,
    rationale_class     TEXT NOT NULL CHECK (rationale_class IN ('signal', 'rebalance', 'experiment')),
    rule_id             TEXT NOT NULL CHECK (char_length(rule_id) BETWEEN 1 AND 200 AND btrim(rule_id) <> ''),
    evidence_kind       TEXT NOT NULL CHECK (evidence_kind IN (
        'strategy_promotion', 'ai_trial_declaration', 'ranking_pot_declaration', 'core_mandate_event'
    )),
    evidence_id         BIGINT NOT NULL CHECK (evidence_id > 0),
    why_now             TEXT NOT NULL CHECK (char_length(why_now) BETWEEN 1 AND 1000 AND btrim(why_now) <> ''),
    exit_rule           TEXT NOT NULL CHECK (
        char_length(exit_rule) BETWEEN 1 AND 1000
        AND btrim(exit_rule) <> ''
        AND (exit_rule NOT LIKE 'not\_applicable%' OR exit_rule ~ '^not_applicable: .*\S')
    ),
    -- `'NaN' >= 0` is TRUE in Postgres, so NaN is refused explicitly.
    expected_cost_usd   NUMERIC(18,6) NOT NULL CHECK (expected_cost_usd >= 0 AND expected_cost_usd <> 'NaN'),
    cost_basis          TEXT NOT NULL CHECK (char_length(cost_basis) BETWEEN 1 AND 200 AND btrim(cost_basis) <> ''),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_strategy_entry_tickets_trade ON strategy_entry_tickets (strategy_trade_id);

COMMENT ON TABLE strategy_entry_tickets IS
    'One trade ticket per engine entry order, stated at the point of action (#3542). Never '
    'back-filled: entries before sql/458 have none.';
