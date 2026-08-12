-- #2577 / #2523 — a VIX number without its as-known date and source identity
-- is not auditable decision context.  Add two compact scalars only to the
-- fired/refused context table; rolling values remain in the bounded Cboe store.

ALTER TABLE strategy_decision_contexts
    ADD COLUMN IF NOT EXISTS vix_bar_date DATE,
    ADD COLUMN IF NOT EXISTS vix_source_version TEXT;

ALTER TABLE strategy_decision_contexts
    ADD CONSTRAINT strategy_decision_context_v3_vix_complete
    CHECK (
        context_version NOT LIKE 'decision-context-v3:%'
        OR candidate_verdict = 'refused'
        OR (
            vix IS NOT NULL
            AND vix_bar_date IS NOT NULL
            AND vix_source_version IS NOT NULL
            AND vix_source_version <> ''
        )
    );

COMMENT ON COLUMN strategy_decision_contexts.vix_bar_date IS
    'Cboe VIX close date causally available at this fired/refused decision. '
    'Decision-context-v3 eligibility requires the prior NYSE session.';

COMMENT ON COLUMN strategy_decision_contexts.vix_source_version IS
    'Frozen source/parser contract that produced vix and vix_bar_date; no '
    'rolling VIX feature history is copied into decision contexts.';
