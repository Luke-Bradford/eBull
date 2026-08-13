-- 173_bootstrap_runs_stream_c_gate.sql
--
-- Stream A PR-B T1.3 / §1.8 (#1233): operator-attestation columns
-- on bootstrap_runs for the Stream-C correctness gate runbook.
--
-- WHAT IT ADDS
-- ------------
--   * ``stream_c_gate_status`` TEXT — status of the 7-check
--     post-bootstrap acceptance runbook (``app/runbooks/
--     stream_a_stream_c_gate.py``, ships in PR-D). Note: PR-D path
--     renamed from ``app/cli/runbooks/`` (spec v2.3) to
--     ``app/runbooks/`` (spec v2.4) to avoid shadowing the existing
--     ``app/cli.py`` break-glass operator CLI. Status values:
--       - NULL                  : gate never run for this bootstrap.
--       - 'pending'             : gate started, not yet decided.
--       - 'passed'              : all 7 checks passed (or warned).
--       - 'failed_<check_id>'   : one specific check failed; suffix
--                                 names the failing C1-C7 check.
--     CHECK uses LIKE 'failed\_%' ESCAPE '\' — literal underscore,
--     not LIKE wildcard (Codex 1 re-pass BLOCKING). Without the
--     ESCAPE clause, ``failedX...`` would match and admit garbage.
--
--   * ``coverage_floor_ratio`` NUMERIC(5,4) — measured CUSIP
--     coverage ratio at S13 OpenFIGI sweep completion. The
--     existing ``coverage_floor_met`` BOOLEAN (sql/167) is a
--     point-in-time decision against a hard-coded 0.80 threshold;
--     storing the ratio alongside the boolean lets operators
--     retroactively answer "what was coverage at Run-N?" after any
--     future threshold revision without re-running bootstrap.
--
-- BOTH COLUMNS ARE NULLABLE — historical bootstrap_runs rows pre-
-- migration carry NULL; new rows default NULL (populated only when
-- the relevant pipeline step runs).
--
-- Idempotent: ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` + named
-- ``DROP CONSTRAINT IF EXISTS`` + ``ADD CONSTRAINT`` for the CHECK.
-- Safe to re-apply.
--
-- Spec: docs/proposals/etl/stream-a-run-8-fixes.md v2.3 §4 + §16 + §1.8
-- (Stream A v2.3, post-Codex-1 re-pass + 3-lens code review 2026-05-24).

BEGIN;

ALTER TABLE bootstrap_runs
    ADD COLUMN IF NOT EXISTS stream_c_gate_status TEXT;

ALTER TABLE bootstrap_runs
    ADD COLUMN IF NOT EXISTS coverage_floor_ratio NUMERIC(5,4);

ALTER TABLE bootstrap_runs
    DROP CONSTRAINT IF EXISTS bootstrap_runs_stream_c_gate_status_check;

ALTER TABLE bootstrap_runs
    ADD CONSTRAINT bootstrap_runs_stream_c_gate_status_check
    CHECK (
        stream_c_gate_status IS NULL
        OR stream_c_gate_status IN ('pending', 'passed')
        OR (
            -- Literal underscore + ≥1 trailing char. Rejects bare 'failed_'
            -- (Codex 2 LOW pre-push review). ESCAPE '\' makes '\_' a
            -- literal underscore, not a LIKE wildcard. (Single backslash
            -- under PG default standard_conforming_strings=on; do NOT
            -- double-escape — bot review iter 1 nitpick.)
            stream_c_gate_status LIKE 'failed\_%' ESCAPE '\'
            AND length(stream_c_gate_status) > length('failed_')
        )
    );

ALTER TABLE bootstrap_runs
    DROP CONSTRAINT IF EXISTS bootstrap_runs_coverage_floor_ratio_range_check;

ALTER TABLE bootstrap_runs
    ADD CONSTRAINT bootstrap_runs_coverage_floor_ratio_range_check
    CHECK (
        coverage_floor_ratio IS NULL
        OR (coverage_floor_ratio >= 0 AND coverage_floor_ratio <= 1)
    );

COMMENT ON COLUMN bootstrap_runs.stream_c_gate_status IS
    'Status of the Stream-C correctness gate runbook (#1233 Stream A §1.8). NULL = never run; pending / passed / failed_<check_id> (literal underscore via ESCAPE; bare "failed_" rejected).';

COMMENT ON COLUMN bootstrap_runs.coverage_floor_ratio IS
    'Measured CUSIP coverage ratio at S13 OpenFIGI sweep completion (NUMERIC(5,4); range CHECK enforces [0, 1]). Sibling of coverage_floor_met BOOLEAN (sql/167); preserved for retroactive threshold-change analysis without re-running bootstrap.';

COMMIT;
