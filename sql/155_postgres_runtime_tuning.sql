-- runner: autocommit
-- 155: Postgres runtime tuning for partitioned-ownership workload (#1208 Sub 1)
--
-- This file is applied with autocommit=True (see directive on line 1 + the
-- migration-runner change in #1208 Phase 1). ``ALTER SYSTEM`` cannot run
-- inside a transaction block (PG limitation), so the usual single-tx
-- migration shape would fail with ``ERROR: ALTER SYSTEM cannot run inside
-- a transaction block``.
--
-- Container defaults (PG17) are too small for eBull's partitioned-ownership
-- schema. ``max_wal_size=1024 MB`` triggers WAL PANIC under autovacuum
-- bursts on the 28 GB unpartitioned ``financial_facts_raw`` table.
-- ``shared_buffers=128 MB`` is laughable for a 46 GB DB.
-- ``wal_compression=off`` leaves an easy win on the table for
-- partition-heavy churn.
--
-- ``ALTER SYSTEM SET`` persists to ``postgresql.auto.conf`` so the values
-- survive container restarts. ``pg_reload_conf()`` applies everything
-- except ``shared_buffers`` (which requires a container restart). The
-- operator runbook on issue #1208 covers the restart sequencing.
--
-- Idempotency: every ``ALTER SYSTEM SET`` overwrites the line in
-- ``postgresql.auto.conf``. Re-running the migration is harmless. Tracked
-- in ``schema_migrations`` so it runs exactly once per DB; if the
-- post-body INSERT into ``schema_migrations`` fails in autocommit mode,
-- the next boot replays the body — safe because each statement is
-- idempotent.

ALTER SYSTEM SET max_wal_size = '4GB';
ALTER SYSTEM SET min_wal_size = '512MB';
ALTER SYSTEM SET wal_compression = 'on';
ALTER SYSTEM SET checkpoint_completion_target = '0.9';
ALTER SYSTEM SET shared_buffers = '2GB';            -- restart required
ALTER SYSTEM SET maintenance_work_mem = '512MB';
ALTER SYSTEM SET effective_cache_size = '4GB';
ALTER SYSTEM SET work_mem = '32MB';

SELECT pg_reload_conf();
