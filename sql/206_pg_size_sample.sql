-- #1564 — daily on-disk DB size sample, for the db_size_growth_7d trend
-- signal on /system/postgres-health.
--
-- Source rule: N/A (ops/infra). pg_database_size(current_database()) is the
-- authoritative on-disk size — the same function the existing _q_db_size
-- probe (app/services/postgres_health.py) and the pre-push bloat-warn hook
-- already use. Single source of truth.
--
-- Standalone table: no FK, not a partition parent, never TRUNCATEd by the
-- planner-tables reset (prevention-log L969 N/A). One row per calendar day;
-- the sampler upserts (ON CONFLICT) so a same-day re-run / boot catch-up
-- refreshes rather than duplicates.
CREATE TABLE IF NOT EXISTS pg_size_sample (
    sampled_on    DATE PRIMARY KEY,
    db_size_bytes BIGINT NOT NULL,
    sampled_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE pg_size_sample IS
    '#1564 — daily pg_database_size(current_database()) snapshot. Read by '
    'postgres_health._q_db_size_growth_7d_baseline to compute the 7-day '
    'on-disk growth trend (informational, no alarm). One row per calendar '
    'day; the pg_size_sample scheduled job upserts on sampled_on.';
