-- Migration 142 — bootstrap_stages.status add 'cancelled'
--
-- Issue #1093 (PR3c #1064 follow-up sequence). Operator audit
-- 2026-05-10: when a bootstrap run is cancelled, the running and
-- pending stages today get swept to ``status='error'`` by
-- ``mark_run_cancelled`` and ``reap_orphaned_running``. The Timeline
-- can't tell those apart from genuine errors, so they all render red
-- with the ``cancelled by operator`` message buried in ``last_error``.
--
-- Operator-correct shape: stages cancelled by operator action carry
-- their own status value so the Timeline tones them gray (or amber)
-- rather than red. Genuine errors (parse failures, SEC 5xx, schema
-- regression) keep ``status='error'`` for the existing red-tone path.
--
-- This migration extends the CHECK constraint to allow ``cancelled``;
-- the application code in ``bootstrap_state.py`` flips the UPDATE
-- targets to write ``cancelled`` instead of ``error`` for the cancel
-- path.
--
-- Idempotent. Existing rows untouched (the migration adds an allowed
-- value; previously-committed ``error`` rows that were really cancels
-- stay as-is — backfilling them risks rewriting genuine error rows
-- and is not worth the ambiguity. Future cancels carry the new
-- value).

ALTER TABLE bootstrap_stages
    DROP CONSTRAINT IF EXISTS bootstrap_stages_status_check;

ALTER TABLE bootstrap_stages
    ADD CONSTRAINT bootstrap_stages_status_check
    CHECK (status IN ('pending', 'running', 'success', 'error', 'skipped', 'blocked', 'cancelled'));

COMMENT ON COLUMN bootstrap_stages.status IS
    'Stage lifecycle. pending → running → (success | error | skipped | cancelled). '
    'blocked = upstream requires-stage failed. cancelled (#1093) = operator-cancelled '
    'mid-run; distinct from error so the Timeline can tone gray instead of red.';
