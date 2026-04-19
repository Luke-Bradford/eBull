-- 041_sync_runs_scope_behind.sql
-- Extends sync_runs.scope CHECK to allow 'behind' (scoped resync —
-- DEGRADED + ACTION_NEEDED layers plus their non-HEALTHY upstreams).

-- DROP without IF EXISTS so the migration fails loudly if the
-- constraint name has drifted (e.g. a fresh-schema environment that
-- auto-named it differently). A silent drop-and-re-add would leave
-- the stale constraint in place on such a DB and reject 'behind'
-- inserts post-migration. The constraint name is pinned by
-- sql/033_sync_orchestrator.sql:15-22 which defines the table.
ALTER TABLE sync_runs
    DROP CONSTRAINT sync_runs_scope_check;
ALTER TABLE sync_runs
    ADD CONSTRAINT sync_runs_scope_check
    CHECK (scope IN ('full', 'layer', 'high_frequency', 'job', 'behind'));
