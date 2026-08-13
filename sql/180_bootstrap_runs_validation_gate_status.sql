-- 180_bootstrap_runs_validation_gate_status.sql
--
-- Bootstrap ETL redesign P4 (#1419): load-time validation gate verdict
-- column on bootstrap_runs.
--
-- WHAT IT ADDS
-- ------------
--   * ``validation_gate_status`` TEXT — verdict of the terminal
--     ``bootstrap_validation`` stage (``app/services/bootstrap_validation.py``).
--     The stage runs three checks (absolute row-count floors, per-slice-
--     tolerant panel render, offline cross-source reconciliation). Status
--     values:
--       - NULL                  : validation never ran for this run.
--       - 'pending'             : stage started, not yet decided.
--       - 'passed'              : every check passed clean.
--       - 'warned'              : passed but soft warnings recorded (e.g. a
--                                 single panel instrument unresolved, mild
--                                 oversubscription) — the gate still opens.
--       - 'failed_<check_id>'   : a HARD-floor breach; suffix names the
--                                 failing check (row_floor / panel /
--                                 reconciliation). The stage ALSO errors, so
--                                 ``finalize_run`` terminalises the run as
--                                 ``partial_error`` (the gate stays closed) —
--                                 this column is the operator-facing reason,
--                                 NOT the gate mechanism.
--
-- WHY A COLUMN, NOT A NEW STATUS ENUM
-- -----------------------------------
--   'partial_complete' was deliberately rejected (sql/167 + Codex v3): the
--   validation verdict lives in a column, the run status stays the existing
--   {pending,running,complete,partial_error,cancelled} set. A hard failure
--   maps to ``partial_error`` via the stage-error → finalize_run path; this
--   column adds the *which-check* detail without widening the status enum.
--
--   CHECK uses LIKE 'failed\_%' ESCAPE '\' — literal underscore, not a LIKE
--   wildcard (mirrors the sql/173 stream_c_gate_status precedent). Without
--   ESCAPE, ``failedX...`` would match and admit garbage. Single backslash
--   under PG default standard_conforming_strings=on; do NOT double-escape.
--
-- NULLABLE — historical bootstrap_runs rows pre-migration carry NULL; new
-- rows default NULL (populated only when the validation stage runs).
--
-- Idempotent: ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` + named
-- ``DROP CONSTRAINT IF EXISTS`` + ``ADD CONSTRAINT``. Safe to re-apply.
--
-- Spec: docs/proposals/etl/2026-06-01-bootstrap-etl-redesign-design.md §4.4.
-- Plan: docs/superpowers/plans/2026-06-01-bootstrap-etl-redesign.md Phase 4.

BEGIN;

ALTER TABLE bootstrap_runs
    ADD COLUMN IF NOT EXISTS validation_gate_status TEXT;

ALTER TABLE bootstrap_runs
    DROP CONSTRAINT IF EXISTS bootstrap_runs_validation_gate_status_check;

ALTER TABLE bootstrap_runs
    ADD CONSTRAINT bootstrap_runs_validation_gate_status_check
    CHECK (
        validation_gate_status IS NULL
        OR validation_gate_status IN ('pending', 'passed', 'warned')
        OR (
            -- Literal underscore + >=1 trailing char. Rejects bare 'failed_'.
            -- ESCAPE '\' makes '\_' a literal underscore, not a LIKE wildcard.
            validation_gate_status LIKE 'failed\_%' ESCAPE '\'
            AND length(validation_gate_status) > length('failed_')
        )
    );

COMMENT ON COLUMN bootstrap_runs.validation_gate_status IS
    'Verdict of the terminal bootstrap_validation stage (#1419 P4). NULL = never run; pending / passed / warned / failed_<check_id> (literal underscore via ESCAPE; bare "failed_" rejected). A failed_* verdict accompanies a stage error → partial_error run; this column is the operator-facing reason, not the gate mechanism.';

COMMIT;
