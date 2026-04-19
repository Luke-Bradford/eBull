-- 041_sync_runs_scope_behind.sql
-- Extends sync_runs.scope CHECK to allow 'behind' (scoped resync —
-- DEGRADED + ACTION_NEEDED layers plus their non-HEALTHY upstreams).

ALTER TABLE sync_runs
    DROP CONSTRAINT IF EXISTS sync_runs_scope_check;
ALTER TABLE sync_runs
    ADD CONSTRAINT sync_runs_scope_check
    CHECK (scope IN ('full', 'layer', 'high_frequency', 'job', 'behind'));
