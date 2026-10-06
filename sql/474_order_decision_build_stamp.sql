-- 474_order_decision_build_stamp.sql
--
-- #3614 item 2: every order and decision row names the code that wrote it.
--
-- The columns default from session settings that `app/db/build_stamp.py`
-- puts on every connection through PGOPTIONS at process start, so no insert
-- site passes them and a future writer cannot forget to. A connection opened
-- without the stamp (a script, a test, a psql session) writes NULL, which
-- means "not recorded".
--
-- ⚠ ADD COLUMN and SET DEFAULT are separate statements on purpose. A
-- non-volatile default given in ADD COLUMN is evaluated ONCE and applied to
-- every existing row, so every row already present would be stamped with the
-- MIGRATING process's commit, which did not write them (checked on Postgres
-- with a temp table before choosing this shape). Split, existing rows stay NULL.

BEGIN;

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS code_commit TEXT,
    ADD COLUMN IF NOT EXISTS code_dirty BOOLEAN,
    ADD COLUMN IF NOT EXISTS uv_lock_sha256 TEXT;

ALTER TABLE orders
    ALTER COLUMN code_commit SET DEFAULT NULLIF(current_setting('ebull.code_commit', true), ''),
    ALTER COLUMN code_dirty SET DEFAULT NULLIF(current_setting('ebull.code_dirty', true), '')::boolean,
    ALTER COLUMN uv_lock_sha256 SET DEFAULT NULLIF(current_setting('ebull.uv_lock_sha256', true), '');

ALTER TABLE decision_audit
    ADD COLUMN IF NOT EXISTS code_commit TEXT,
    ADD COLUMN IF NOT EXISTS code_dirty BOOLEAN,
    ADD COLUMN IF NOT EXISTS uv_lock_sha256 TEXT;

ALTER TABLE decision_audit
    ALTER COLUMN code_commit SET DEFAULT NULLIF(current_setting('ebull.code_commit', true), ''),
    ALTER COLUMN code_dirty SET DEFAULT NULLIF(current_setting('ebull.code_dirty', true), '')::boolean,
    ALTER COLUMN uv_lock_sha256 SET DEFAULT NULLIF(current_setting('ebull.uv_lock_sha256', true), '');

COMMENT ON COLUMN orders.code_commit IS
    'HEAD of the checkout whose process wrote this row, read once at process start (#3614). NULL = not recorded.';
COMMENT ON COLUMN orders.code_dirty IS
    'Whether a tracked file differed from code_commit at process start (#3614). NULL = not recorded.';
COMMENT ON COLUMN orders.uv_lock_sha256 IS
    'sha256 of uv.lock at process start (#3614). NULL = not recorded.';
COMMENT ON COLUMN decision_audit.code_commit IS
    'HEAD of the checkout whose process wrote this row, read once at process start (#3614). NULL = not recorded.';
COMMENT ON COLUMN decision_audit.code_dirty IS
    'Whether a tracked file differed from code_commit at process start (#3614). NULL = not recorded.';
COMMENT ON COLUMN decision_audit.uv_lock_sha256 IS
    'sha256 of uv.lock at process start (#3614). NULL = not recorded.';

COMMIT;
