-- 469_prereg_power_check.sql
--
-- #3610 — the pre-declaration power check, stored on the frozen declaration it gated
-- (`trial_register.TrialRegister.freeze_power_record`): track, trial count, critical t,
-- required years, power and the design inputs. NULL on rows frozen before the rule and on
-- supersession successors, which repair a policy string and cannot change terms. Stored rather
-- than recomputed because the register's M grows after a freeze.
-- Adding a NULL column fires no row trigger, so sql/333's immutability trigger is untouched.

BEGIN;

ALTER TABLE strategy_preregistration_declarations
    ADD COLUMN IF NOT EXISTS power_check JSONB
    CONSTRAINT strategy_preregistration_power_check_object
    CHECK (power_check IS NULL OR jsonb_typeof(power_check) = 'object');

COMMIT;
