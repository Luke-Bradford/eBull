-- 475_build_stamp_dirty_default_never_raises.sql
--
-- #3614 review: sql/474 defaulted code_dirty to `NULLIF(…)::boolean`, which
-- RAISES on a session value that is not a boolean, aborting the INSERT it
-- rides on. An audit stamp must never be able to block an order or decision
-- write, so anything but the two values build_stamp writes is NULL ("not
-- recorded"). sql/474 is already applied on dev, so it is corrected here.

BEGIN;

ALTER TABLE orders
    ALTER COLUMN code_dirty SET DEFAULT (
        CASE current_setting('ebull.code_dirty', true) WHEN 'true' THEN true WHEN 'false' THEN false END
    );

ALTER TABLE decision_audit
    ALTER COLUMN code_dirty SET DEFAULT (
        CASE current_setting('ebull.code_dirty', true) WHEN 'true' THEN true WHEN 'false' THEN false END
    );

COMMIT;
