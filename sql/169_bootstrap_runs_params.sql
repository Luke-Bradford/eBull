-- 169_bootstrap_runs_params.sql
--
-- #1233 PR-5a (spec §9) — operator-facing param dict on bootstrap_runs.
--
-- ## What this adds
--
-- One JSONB column on ``bootstrap_runs``:
--   * ``params`` — opaque JSON dict the API caller passes through to
--     run-time prelude logic. Today the only consumer is the manifest
--     reset prelude (``reset_manifest_for_run``); the column is sized
--     for future per-run operator knobs (e.g. "skip stage X this run",
--     "force re-download bulk archive") without churning the schema.
--
-- ## Why JSONB (not discrete columns)
--
-- Adding one BOOLEAN per knob would scale linearly with prelude
-- features; every new knob ships a migration + an audit-table change
-- + a backfill story. JSONB defaults to ``'{}'::jsonb`` so legacy rows
-- (every existing bootstrap_runs row pre-#1233) read as "no overrides"
-- without a one-shot UPDATE.
--
-- ## Why NOT NULL DEFAULT '{}'
--
-- Keeps the read path branch-free. Every consumer can call
-- ``row['params'].get('reset_failed_manifest', True)`` without first
-- distinguishing NULL from empty-object. The empty-object default is
-- semantically equivalent to NULL but eliminates the dual-state code
-- path. CHECK constraint enforces JSONB object shape (not array, not
-- scalar) so a future bug that writes ``null`` or ``[]`` trips the DB
-- guard instead of silently passing through ``.get()``.
--
-- ## Idempotent
--
-- ADD COLUMN IF NOT EXISTS guards repeat application. CHECK constraint
-- adds via NOT VALID + VALIDATE pattern on re-application would be
-- harmless on an empty new column; we use a single ADD CONSTRAINT IF
-- NOT EXISTS gated by ``DO $$`` so a re-applied migration is safe.

BEGIN;

-- #1233 PR-5a — concurrency hardening for the manifest-reset prelude.
--
-- The reset predicate filters ``last_attempted_at < bootstrap_runs.triggered_at``
-- to leave concurrent live cron writes alone (Codex pre-push HIGH).
-- That predicate is sound only if ``last_attempted_at`` reflects when
-- the worker statement ACTUALLY executed — not when its transaction
-- began. PG's ``NOW()`` is ``transaction_timestamp()`` (fixed at tx
-- start); a long-running worker tx begun BEFORE the bootstrap run
-- triggered, but committing AFTER, would write a stale stamp that
-- survives the reset predicate and gets erroneously flipped.
--
-- ``clock_timestamp()`` is per-statement wall-clock — its value
-- always reflects when the UPDATE actually ran inside the worker tx.
-- Combined with the parallel ``last_attempted_at = clock_timestamp()``
-- swap in ``app/services/sec_manifest.py::transition_status``, the
-- watermark predicate becomes race-safe.
--
-- Idempotent: CREATE OR REPLACE rewrites the trigger body. The trigger
-- definition itself is unchanged (BEFORE UPDATE, per-row).
CREATE OR REPLACE FUNCTION sec_filing_manifest_touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

ALTER TABLE bootstrap_runs
    ADD COLUMN IF NOT EXISTS params JSONB NOT NULL DEFAULT '{}'::jsonb;

-- Pin the JSONB-object shape so future writers can't sneak in a
-- top-level array / scalar / null without tripping the DB guard.
-- ``jsonb_typeof`` is stable + cheap; CHECK runs only on INSERT /
-- UPDATE of the column.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'bootstrap_runs_params_object_chk'
           AND conrelid = 'bootstrap_runs'::regclass
    ) THEN
        ALTER TABLE bootstrap_runs
            ADD CONSTRAINT bootstrap_runs_params_object_chk
            CHECK (jsonb_typeof(params) = 'object');
    END IF;
END
$$;

COMMIT;
