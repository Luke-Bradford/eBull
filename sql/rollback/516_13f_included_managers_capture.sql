-- rollback/516_13f_included_managers_capture.sql
--
-- #3227 tier (IM) capture rollback. NOT applied by the migration runner, which reads sql/*.sql only. To roll back, one
-- revert PR adds this body as the next sql/NNN migration, after the (IM) rollup tier is reverted.

BEGIN;

ALTER TABLE IF EXISTS ownership_institutions_observations DROP COLUMN IF EXISTS thirteen_f_segments;
DROP TABLE IF EXISTS institutional_13f_other_managers;
DROP TABLE IF EXISTS institutional_13f_captures;

COMMIT;
