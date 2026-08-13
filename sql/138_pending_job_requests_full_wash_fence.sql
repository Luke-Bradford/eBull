-- 138_pending_job_requests_full_wash_fence.sql
--
-- Issue #1065 (umbrella #1064) — admin control hub rewrite, PR1 of 10.
--
-- Spec: docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md
--       §Schema migrations / sql/138 + §Full-wash execution fence.
--
-- ## Why
--
-- Full-wash trigger resets a process's watermark and re-runs from
-- epoch. Without a fence, a scheduled run that starts in the gap
-- between full-wash COMMIT and worker-start would race the reset.
--
-- The fence is a persistent queue row: the full-wash handler INSERTs
-- a `pending_job_requests` row with `mode='full_wash'`. Scheduled
-- runs and Iterate workers gate on the row's existence inside their
-- start-of-work transaction. The advisory lock
-- `pg_advisory_xact_lock(hashtext(process_id)::bigint)` serialises
-- the prelude of all paths so the fence-check + active-marker-publish
-- happen atomically (Codex round 4 R4-B1 + round 5 R5-W1/W2).
--
-- ## Migration
--
-- Adds two columns + a UNIQUE partial index:
--
--   * process_id TEXT — process key; nullable for legacy rows.
--   * mode       TEXT — 'iterate' | 'full_wash'; nullable for legacy.
--   * UNIQUE partial idx — at most one ACTIVE full-wash row per
--                          process_id (Codex round 3 R3-B1).
--
-- ## Worker finalisation
--
-- Workers transition the fence row to `status='completed'` (success
-- path) or `status='rejected'` (failure path) — never DELETE
-- (preserves audit; matches existing pending_job_requests lifecycle —
-- Codex round 3 R3-W1).
--
-- ## Boot recovery
--
-- Stuck dispatched fence rows older than 6h are swept to `rejected`
-- (verified at sql/084:23 — 'failed' is not in the CHECK set —
-- Codex round 3 R3-B2).

BEGIN;

ALTER TABLE pending_job_requests
    ADD COLUMN IF NOT EXISTS process_id TEXT,
    ADD COLUMN IF NOT EXISTS mode       TEXT;

-- Add CHECK on `mode` only when not already present. CHECKs cannot
-- use IF NOT EXISTS, so try-add-then-DROP-IF-EXISTS-first to keep
-- the migration idempotent on re-apply.
ALTER TABLE pending_job_requests
    DROP CONSTRAINT IF EXISTS pending_job_requests_mode_check;
ALTER TABLE pending_job_requests
    ADD CONSTRAINT pending_job_requests_mode_check
    CHECK (mode IS NULL OR mode IN ('iterate', 'full_wash'));

-- (Codex pre-push WARNING) NULL process_id would bypass the
-- partial-unique fence index because PG treats NULLs as distinct.
-- Require process_id when mode='full_wash' so a buggy insert can't
-- silently squeeze past the fence.
ALTER TABLE pending_job_requests
    DROP CONSTRAINT IF EXISTS pending_job_requests_full_wash_requires_process_id;
ALTER TABLE pending_job_requests
    ADD CONSTRAINT pending_job_requests_full_wash_requires_process_id
    CHECK (mode IS DISTINCT FROM 'full_wash' OR process_id IS NOT NULL);

-- UNIQUE partial: at most one ACTIVE full-wash queue row per process.
-- Concurrent full-wash POSTs racing past the fence-check are caught
-- here as UniqueViolation; handler maps to 409 (Codex round 3 R3-B1).
CREATE UNIQUE INDEX IF NOT EXISTS pending_job_requests_active_full_wash_idx
    ON pending_job_requests (process_id)
    WHERE mode = 'full_wash' AND status IN ('pending', 'claimed', 'dispatched');

COMMIT;
