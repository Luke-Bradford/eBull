-- 170_bootstrap_runs_boot_resume_attempts.sql
--
-- #1233 / #1296 — auto-resume an in-flight bootstrap after jobs-process
-- restart.
--
-- Adds ``bootstrap_runs.boot_resume_attempts INT NOT NULL DEFAULT 0``.
-- Each time the jobs entrypoint observes a ``running`` bootstrap_state
-- at boot, it increments this counter on the in-flight bootstrap_run
-- and self-enqueues a ``bootstrap_orchestrator`` job request so the
-- orchestrator's PR-6 reaper (``reap_orphaned_running_stages``) can
-- reset stuck ``running`` stages back to ``pending`` and the
-- dispatcher resumes.
--
-- Why a counter and not a boolean
-- ===============================
-- If the jobs process crashes AGAIN mid-resume (e.g. the same code
-- path that killed it last time fires again), an unbounded resume
-- loop would loop forever. The counter caps attempts at
-- ``_MAX_BOOT_RESUMES`` (default 1, configurable at the call site).
-- Above the cap, the existing ``bootstrap_state.reap_orphaned_running``
-- path fires — terminate the run as ``partial_error`` so the operator
-- can retry-failed from the admin panel.
--
-- Why NOT NULL DEFAULT 0
-- ======================
-- Existing runs predate this column. The default makes the existing
-- rows safely "never resumed" without a backfill.
--
-- Idempotent.

BEGIN;

ALTER TABLE bootstrap_runs
    ADD COLUMN IF NOT EXISTS boot_resume_attempts INT NOT NULL DEFAULT 0;

COMMENT ON COLUMN bootstrap_runs.boot_resume_attempts IS
    '#1296 — count of jobs-process restarts that auto-enqueued an orchestrator '
    'resume for this run. Bounded by _MAX_BOOT_RESUMES in '
    'app/services/bootstrap_state.py (default 1) so a crash-during-resume '
    'cannot loop forever.';

COMMIT;
