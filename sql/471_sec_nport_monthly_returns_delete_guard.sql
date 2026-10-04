-- 471_sec_nport_monthly_returns_delete_guard.sql
--
-- #3619 slice 2b, review round 1: sql/470 guarded UPDATE only while calling the table append-only. Row DELETE is
-- now refused as well. TRUNCATE stays allowed on purpose: it is the explicit whole-table rebuild path (and the test
-- harness's reset), not a row edit, and row triggers never fire on it.

BEGIN;

DROP TRIGGER IF EXISTS sec_nport_monthly_returns_append_only ON sec_nport_monthly_returns;
CREATE TRIGGER sec_nport_monthly_returns_append_only
    BEFORE UPDATE OR DELETE ON sec_nport_monthly_returns
    FOR EACH ROW EXECUTE FUNCTION sec_nport_monthly_returns_append_only();

COMMENT ON TABLE sec_nport_monthly_returns IS
    'Form N-PORT Item B.5.a monthly total returns per class, percent as filed, one row per data-set row and month '
    'position (#3619 slice 2b). Row UPDATE and DELETE are refused; a rebuild is an explicit TRUNCATE and reload. '
    'The latest filing per (class, month) is resolved at read time.';

COMMIT;
