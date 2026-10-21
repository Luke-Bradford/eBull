-- rollback/514_sec_inline_dps_scale.sql
--
-- #3947 rollback step 5 (docs/specs/fundamentals/2026-10-21-3947-dps-inline-scale.md §Rollback): run only after
-- EBULL_DPS_INLINE_SCALE_ENABLED is off and every DPS instrument has been re-normalised and verified (steps 1-4).
-- NOT applied by the migration runner, which reads sql/*.sql only. To roll back, one revert PR adds this body as
-- the next sql/NNN migration.

BEGIN;

DROP TABLE IF EXISTS sec_inline_scale_renormalise;
DROP SEQUENCE IF EXISTS sec_inline_scale_renormalise_revision;
DROP TABLE IF EXISTS sec_inline_dps_occurrence;
DROP TABLE IF EXISTS sec_inline_scale_scan;

COMMIT;
