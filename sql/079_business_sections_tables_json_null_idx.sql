-- 079_business_sections_tables_json_null_idx.sql
--
-- #560: Partial index supporting the candidate-query EXISTS clause
-- in ``ingest_business_summaries`` that surfaces rows whose child
-- sections have ``tables_json IS NULL`` (pre-migration-075 ingest).
--
-- Without an index the EXISTS subquery scans the full sections
-- table on every candidate evaluation — fine while the backfill is
-- in flight (~178k rows) but slow once the candidate count grows.
-- Partial WHERE clause keeps the index tiny in steady state: rows
-- with populated ``tables_json`` are not indexed at all, and once
-- the backfill completes the index is empty.
--
-- Idempotent.

CREATE INDEX IF NOT EXISTS
    instrument_business_summary_sections_tables_json_null_idx
    ON instrument_business_summary_sections (instrument_id, source_accession)
    WHERE tables_json IS NULL;

COMMENT ON INDEX instrument_business_summary_sections_tables_json_null_idx IS
    '#560: supports the tables_json-backfill candidate-query EXISTS '
    'clause; partial filter on tables_json IS NULL keeps the index '
    'empty once backfill completes so steady-state cost is zero.';
