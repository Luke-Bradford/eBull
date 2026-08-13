-- 081_sync_layer_progress_forensics.sql
--
-- Issue #645 — orchestrator forensics (MVP slice).
--
-- Adds three optional columns to `sync_layer_progress` so an operator
-- triaging a red banner can see WHAT broke, not just the coarse
-- `error_category` enum. Today an "Unclassified error" banner means
-- the exception did not match the small set of recognised types in
-- `exception_classifier.py` and the actual exception text is gone —
-- investigating any failure required an in-process repro.
--
-- Columns:
--   error_message     -- repr(exc)[:1000], one-line summary for the banner
--   error_traceback   -- traceback.format_exc()[:8000], full chain for triage
--   error_fingerprint -- sha1 of normalised traceback for grouping repeats
--
-- Also extends the `status` CHECK to allow `'cancelled'`. Today the
-- reaper (and `_fail_unfinished_layers`) flips `pending`/`running`
-- rows to `'failed'` with `error_category='orchestrator_crash'` on
-- the next boot. Rows that never started (uvicorn --reload killed
-- the worker before the adapter ran, `started_at IS NULL`) get
-- counted as real failures and inflate the consecutive-failure
-- streak in the admin banner — the user-visible "140 candles
-- failures" was almost entirely reaper noise from dev iteration,
-- not real adapter failures.
--
-- The new `'cancelled'` status lets the reaper distinguish
-- never-started from started-and-died, and the existing
-- `consecutive_failures` query naturally treats `'cancelled'` as a
-- streak break (it only counts `status='failed'`).
--
-- All ALTERs are idempotent. Re-running the migration is a no-op.

ALTER TABLE sync_layer_progress
    ADD COLUMN IF NOT EXISTS error_message     TEXT,
    ADD COLUMN IF NOT EXISTS error_traceback   TEXT,
    ADD COLUMN IF NOT EXISTS error_fingerprint TEXT;

-- Replace the status CHECK constraint to include 'cancelled'. The
-- original constraint name in 033_sync_orchestrator.sql is the
-- Postgres-default `sync_layer_progress_status_check`. Drop it if
-- present (it always is on databases that ran 033 cleanly), then
-- re-add with the extended set.
ALTER TABLE sync_layer_progress
    DROP CONSTRAINT IF EXISTS sync_layer_progress_status_check;

ALTER TABLE sync_layer_progress
    ADD CONSTRAINT sync_layer_progress_status_check
    CHECK (status IN ('pending', 'running', 'complete', 'failed', 'skipped', 'partial', 'cancelled'));
