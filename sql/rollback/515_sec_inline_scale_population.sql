-- rollback/515_sec_inline_scale_population.sql
--
-- #3947 rollback step 5 (docs/specs/fundamentals/2026-10-21-3947-dps-inline-scale.md §Amendment 1, "Rollback"):
-- run with rollback/514 only after EBULL_DPS_INLINE_SCALE_ENABLED is off and steps 1-4 are verified. NOT applied by
-- the migration runner, which reads sql/*.sql only. To roll back, one revert PR adds this body, then 514's, as the
-- next sql/NNN migrations.

BEGIN;

DROP TABLE IF EXISTS sec_inline_scale_discovery;
DROP TABLE IF EXISTS sec_inline_scale_population;
ALTER TABLE IF EXISTS sec_inline_scale_renormalise DROP COLUMN IF EXISTS drained_revision;

COMMIT;
