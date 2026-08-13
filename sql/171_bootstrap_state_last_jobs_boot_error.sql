-- 171_bootstrap_state_last_jobs_boot_error.sql
--
-- Stream A PR-A T1.8 (#1233): boot-failure breadcrumb columns on
-- bootstrap_state singleton.
--
-- The jobs-process boot guard ``_check_operator_exists_with_cleanup``
-- (app/jobs/__main__.py) persists an operator-actionable error
-- message + timestamp here on hard-fail. ``GET /system/status``
-- (app/api/system.py) surfaces the breadcrumb so an operator can
-- triage "jobs unhealthy because <reason> at <when>" without
-- grepping stderr.
--
-- SINGLE-WRITER contract: only the operator-existence guard touches
-- these columns. Sibling ``_ensure_*_with_cleanup`` helpers do NOT
-- write here — they recover by re-INSERTing missing default
-- singletons, not by persisting a breadcrumb. Adding a second writer
-- requires explicit conflict-key + audit-trail design.
--
-- NULL semantics: "last boot succeeded, OR jobs has never run since
-- this migration applied". A successful boot CLEARs both columns
-- (``UPDATE ... WHERE last_jobs_boot_error IS NOT NULL``) so a
-- recovered boot does not leave a stale error breadcrumb.
--
-- ``last_jobs_boot_error_at`` is stamped via ``clock_timestamp()``
-- at write time (NOT ``transaction_timestamp()``/``NOW()``) per
-- ``.claude/skills/data-engineer/SKILL.md`` §6.5.8 — the wrapper's
-- breadcrumb write is its own short autocommit transaction so the
-- difference does not matter today, but using ``clock_timestamp()``
-- keeps the convention consistent if a future caller hoists the
-- write into a longer-running transaction.
--
-- CHECK length cap (≤ 8192 chars): the current writer pins a
-- ~200-char constant, but the cap pre-empts a future "let's append
-- the stack trace" footgun that would bloat the singleton row.
--
-- Idempotent: ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` is safe
-- to re-apply. CHECK constraint named explicitly so re-application
-- is also idempotent.
--
-- Spec: docs/proposals/etl/stream-a-run-8-fixes.md §4 + §16 + §1 T1.8
-- (Stream A v2.3, post-Codex-1 re-pass + 3-lens code review 2026-05-24).

BEGIN;

ALTER TABLE bootstrap_state
    ADD COLUMN IF NOT EXISTS last_jobs_boot_error    TEXT;

ALTER TABLE bootstrap_state
    ADD COLUMN IF NOT EXISTS last_jobs_boot_error_at TIMESTAMPTZ;

-- Length cap on the breadcrumb (defence-in-depth, single writer
-- today). Idempotent: named CHECK + DROP IF EXISTS pattern.
ALTER TABLE bootstrap_state
    DROP CONSTRAINT IF EXISTS bootstrap_state_last_jobs_boot_error_len_check;

ALTER TABLE bootstrap_state
    ADD CONSTRAINT bootstrap_state_last_jobs_boot_error_len_check
    CHECK (last_jobs_boot_error IS NULL OR length(last_jobs_boot_error) <= 8192);

COMMENT ON COLUMN bootstrap_state.last_jobs_boot_error IS
    'Operator-actionable string set by the operator-existence boot guard (_check_operator_exists_with_cleanup, #1233 Stream A PR-A) on hard-fail; cleared by successful boot. SINGLE-WRITER — sibling _ensure_*_with_cleanup helpers do NOT touch this column.';

COMMENT ON COLUMN bootstrap_state.last_jobs_boot_error_at IS
    'Wall-clock UTC at which last_jobs_boot_error was set (clock_timestamp() at write time). NULL iff last_jobs_boot_error IS NULL. Lets the operator see WHEN the failure happened, not only WHAT.';

COMMIT;
