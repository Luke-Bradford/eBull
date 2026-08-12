-- #2577 — mirror DecisionVix's complete-or-refused shape at the database
-- boundary.  Refused v3 rows carry either the complete observed triplet or
-- three NULLs plus their named refusal; partial provenance is never valid.

ALTER TABLE strategy_decision_contexts
    ADD CONSTRAINT strategy_decision_context_v3_vix_triplet
    CHECK (
        context_version NOT LIKE 'decision-context-v3:%'
        OR (
            vix IS NULL
            AND vix_bar_date IS NULL
            AND vix_source_version IS NULL
        )
        OR (
            vix IS NOT NULL
            AND vix_bar_date IS NOT NULL
            AND vix_source_version IS NOT NULL
            AND vix_source_version <> ''
        )
    );
