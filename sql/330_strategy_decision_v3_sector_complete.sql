-- #2577 — migration 328 scoped prospective-sector completeness to context v2.
-- Carry that existing invariant over the v3 identity bump; a new context
-- version must never silently shed a prior eligibility requirement.

ALTER TABLE strategy_decision_contexts
    DROP CONSTRAINT strategy_decision_context_v2_sector_complete;

ALTER TABLE strategy_decision_contexts
    ADD CONSTRAINT strategy_decision_context_v2_v3_sector_complete
    CHECK (
        candidate_verdict = 'refused'
        OR (
            context_version NOT LIKE 'decision-context-v2:%'
            AND context_version NOT LIKE 'decision-context-v3:%'
        )
        OR provider_industry_id IS NOT NULL
    );
