-- 477_kill_switch_drill_target_defect.sql
--
-- #3614 item 4 slice 1 review — the operator's rule is broker-side SL AND TP on every
-- engine position, so a missing target is a book defect as a missing stop is.
-- `kill_switch_drill_events` is append-only (sql/476); a column added with no default
-- rewrites no row and fires no row trigger.

BEGIN;

ALTER TABLE kill_switch_drill_events
    ADD COLUMN IF NOT EXISTS positions_without_target INTEGER CHECK (positions_without_target >= 0);

COMMENT ON COLUMN kill_switch_drill_events.positions_without_target IS
    '#3614 mapped engine positions whose cached broker row has no take-profit; NULL when no snapshot.';

COMMIT;
