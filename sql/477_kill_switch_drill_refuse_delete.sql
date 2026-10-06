-- 477_kill_switch_drill_refuse_delete.sql
--
-- #3614 item 4 slice 1 — sql/476's append-only triggers fired on UPDATE only, so the
-- drill evidence could still be deleted. Recreate all five as BEFORE UPDATE OR DELETE
-- (precedent: sql/299, sql/327). A fix-forward: 476 is already applied and hash-locked.

BEGIN;

DROP TRIGGER IF EXISTS kill_switch_drill_events_append_only ON kill_switch_drill_events;
CREATE TRIGGER kill_switch_drill_events_append_only
    BEFORE UPDATE OR DELETE ON kill_switch_drill_events
    FOR EACH ROW EXECUTE FUNCTION kill_switch_drill_append_only();

DROP TRIGGER IF EXISTS kill_switch_drill_chokepoints_append_only ON kill_switch_drill_chokepoints;
CREATE TRIGGER kill_switch_drill_chokepoints_append_only
    BEFORE UPDATE OR DELETE ON kill_switch_drill_chokepoints
    FOR EACH ROW EXECUTE FUNCTION kill_switch_drill_append_only();

DROP TRIGGER IF EXISTS kill_switch_drill_positions_append_only ON kill_switch_drill_positions;
CREATE TRIGGER kill_switch_drill_positions_append_only
    BEFORE UPDATE OR DELETE ON kill_switch_drill_positions
    FOR EACH ROW EXECUTE FUNCTION kill_switch_drill_append_only();

DROP TRIGGER IF EXISTS kill_switch_drill_authority_append_only ON kill_switch_drill_authority;
CREATE TRIGGER kill_switch_drill_authority_append_only
    BEFORE UPDATE OR DELETE ON kill_switch_drill_authority
    FOR EACH ROW EXECUTE FUNCTION kill_switch_drill_append_only();

DROP TRIGGER IF EXISTS kill_switch_drill_close_samples_append_only ON kill_switch_drill_close_samples;
CREATE TRIGGER kill_switch_drill_close_samples_append_only
    BEFORE UPDATE OR DELETE ON kill_switch_drill_close_samples
    FOR EACH ROW EXECUTE FUNCTION kill_switch_drill_append_only();

COMMIT;
