-- 185_job_first_seen.sql
--
-- #1508 C6 — persisted per-job first-seen anchor for the never-started verdict.
-- Volatile process-start cannot anchor it (resets the grace window every
-- restart -> long-cadence never-run jobs lie green). One row per job_name,
-- written once on first registry load.
--
-- No explicit BEGIN/COMMIT: the migration runner wraps the body + the
-- schema_migrations INSERT in one transaction (app/db/migrations.run_migrations);
-- an inline COMMIT would split them (prevention-log: tx-bound migrations).
CREATE TABLE IF NOT EXISTS job_first_seen (
    job_name   text PRIMARY KEY,
    first_seen timestamptz NOT NULL DEFAULT now()
);
