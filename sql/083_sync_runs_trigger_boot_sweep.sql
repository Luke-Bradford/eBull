-- 083_sync_runs_trigger_boot_sweep.sql
--
-- Issue #649A — boot-time freshness sweep.
--
-- Adds 'boot_sweep' as a valid sync_runs.trigger value. Boot sweep
-- fires once per app startup as a non-blocking asyncio task; uses
-- the existing scope='behind' planner which is idempotent and only
-- refreshes layers that are past their freshness target. Distinct
-- from 'manual' (operator click) and 'scheduled' (APScheduler cron)
-- so the audit trail in `/sync/runs` shows the recovery cause when
-- triaging "what brought the data back to current after the
-- restart".

ALTER TABLE sync_runs DROP CONSTRAINT IF EXISTS sync_runs_trigger_check;

ALTER TABLE sync_runs
    ADD CONSTRAINT sync_runs_trigger_check
    CHECK (trigger IN ('manual', 'scheduled', 'catch_up', 'boot_sweep'));
