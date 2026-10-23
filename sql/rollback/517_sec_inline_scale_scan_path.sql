-- rollback/517_sec_inline_scale_scan_path.sql
--
-- #3947 Amendment 2 path column rollback. NOT applied by the migration runner, which reads sql/*.sql only. To roll
-- back, one revert PR adds this body as the next sql/NNN migration, after the code that writes `path` is reverted.

BEGIN;

ALTER TABLE IF EXISTS sec_inline_scale_scan DROP CONSTRAINT IF EXISTS sec_inline_scale_scan_path_needs_generation;
ALTER TABLE IF EXISTS sec_inline_scale_scan DROP COLUMN IF EXISTS path;

COMMIT;
