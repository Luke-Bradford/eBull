-- #2709 — stamp the exact symbol-identity rule used by each strategy entry
-- preflight. Existing rows remain NULL: they were evaluated with the old exact
-- `upper(instruments.symbol)` comparison and must not be rewritten to claim the
-- suffix-aware rule ran historically.

BEGIN;

ALTER TABLE strategy_entry_preflights
    ADD COLUMN IF NOT EXISTS halt_identity_rule_version TEXT;

COMMENT ON COLUMN strategy_entry_preflights.halt_identity_rule_version IS
    'Version of the Nasdaq/eToro symbol identity rule actually evaluated. NULL on '
    'pre-#2709 rows and early refusals that did not reach the halt observation.';

COMMIT;
