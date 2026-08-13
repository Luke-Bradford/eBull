-- 075_business_sections_tables_json.sql
--
-- #559 Phase 1: persist embedded <table> blocks from 10-K Item 1
-- prose so the renderer can show them as real tables instead of
-- stripped whitespace runs.
--
-- Nullable column. Existing rows stay NULL until the next parse
-- via bootstrap_business_summaries (post-deploy).
ALTER TABLE instrument_business_summary_sections
    ADD COLUMN IF NOT EXISTS tables_json JSONB;

COMMENT ON COLUMN instrument_business_summary_sections.tables_json IS
    'Array of {order:int, headers:[str], rows:[[str]]} for embedded '
    '<table> blocks parsed from this section. NULL = not yet re-parsed; '
    'empty array = re-parsed and section had no tables.';
