-- 101_insider_initial_holdings_value_owned.sql
--
-- Recovery for migration 093 schema drift. The migration recorded a
-- ``CREATE TABLE IF NOT EXISTS insider_initial_holdings`` that defined
-- a ``value_owned NUMERIC(18, 6)`` column, but the live DB already had
-- a pre-existing ``insider_initial_holdings`` table from a parallel
-- experiment. ``CREATE TABLE IF NOT EXISTS`` no-ops on a name conflict
-- and does NOT add missing columns — the migration was recorded as
-- applied but the column never landed. Every read of
-- ``insider_initial_holdings.value_owned`` errors with "column does
-- not exist".
--
-- Recovery is column-add via ``ALTER TABLE ... ADD COLUMN IF NOT
-- EXISTS``, the only safe shape for a column add to an existing
-- table. Idempotent — re-running on a DB that already has the column
-- is a no-op. ``tests/smoke/test_app_boots.py`` gains an explicit
-- column-existence assertion in the same PR; ``tests/smoke/
-- test_schema_drift.py`` lands alongside as the long-term prevention
-- for this class of drift (B5 of #797 pulled forward).
--
-- See migration 093 for the column's role in the Form 3 baseline
-- reader (`get_insider_summary`).

ALTER TABLE insider_initial_holdings
    ADD COLUMN IF NOT EXISTS value_owned NUMERIC(18, 6),
    ADD COLUMN IF NOT EXISTS underlying_value NUMERIC(18, 6);

COMMENT ON COLUMN insider_initial_holdings.value_owned IS
    'Form 3 valueOwnedFollowingTransaction alternative to shares. SEC '
    'allows EITHER shares OR value (fractional-undivided-interest '
    'securities use the value branch). Recovery for migration 093 '
    'CREATE TABLE IF NOT EXISTS no-op on pre-existing schema.';

COMMENT ON COLUMN insider_initial_holdings.underlying_value IS
    'Form 3 valueOwnedFollowingTransaction alternative for derivative '
    'underlyings. Some derivative grants (performance / dollar-'
    'denominated awards) express the underlying as a value not a '
    'share count. Recovery for migration 093 CREATE TABLE IF NOT '
    'EXISTS no-op on pre-existing schema. Surfaced by the schema-'
    'drift smoke gate (B5 of #797 pulled forward into Batch 1).';
