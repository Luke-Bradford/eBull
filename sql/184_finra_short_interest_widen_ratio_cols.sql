-- 184_finra_short_interest_widen_ratio_cols.sql
--
-- Issue: #1516 (surfaced while diagnosing epic #1508).
--
-- ## Why
--
-- finra_short_interest_refresh aborts every settlement file that contains an
-- edge instrument whose FINRA-supplied days_to_cover or change_percent is
-- >= 10^6: both columns are NUMERIC(10,4) (max 999,999.9999) in
-- finra_short_interest_observations AND finra_short_interest_current
-- (sql/152_finra_short_interest.sql:34-35,115-116). FINRA publishes such
-- values legitimately — near-zero ADV yields a huge days-to-cover, near-zero
-- prior short interest yields a huge percent change. The whole-file COPY/UPSERT
-- aborts with:
--
--     NumericValueOutOfRange: numeric field overflow
--     DETAIL: A field with precision 10, scale 4 must round to an absolute
--             value less than 10^6.
--
-- Three dev settlement files fail repeatedly: 2025-09-15, 2025-11-28,
-- 2026-04-15. The values are valid FINRA data, just wider than the column —
-- so the fix is to widen, not to clamp/parse. Shares columns are already
-- NUMERIC(20,0) (correct); only the two ratio columns are too narrow.
--
-- Widen days_to_cover + change_percent to NUMERIC(20,4) on both tables.
-- finra_short_interest_observations is partitioned by settlement_date —
-- ALTER COLUMN ... TYPE on the partitioned parent propagates to every
-- partition (PG14+).
--
-- ## Lock impact
--
-- Verified empirically on PG17 (dev): widening NUMERIC(10,4) -> NUMERIC(20,4)
-- keeps the same precision-class storage and does NOT rewrite the table
-- (pg_relation_filenode unchanged across the ALTER). Metadata-only, sub-second.
-- It still takes a brief ACCESS EXCLUSIVE lock on the parent + each partition +
-- the current table while it runs, but with no rewrite the lock is held only for
-- the catalog update — negligible for these tables.
--
-- ## Idempotency
--
-- The migration runner records each file once in schema_migrations, so this
-- runs once. The DO-block shape guards (numeric_precision check on the parent
-- tables) make a manual re-run a no-op anyway, matching the issue's request.
--
-- ## ETL clauses
--
-- CLAUDE.md ETL clauses 8-11: this is a column-width fix, not a parser/identity
-- change — no re-parse needed. The DoD backfill (re-run the 3 failed settlement
-- dates and confirm they upsert) is recorded in the PR body (clause 12).
--
-- No explicit BEGIN/COMMIT: the migration runner wraps the body + the
-- schema_migrations INSERT in one transaction (app/db/migrations.run_migrations);
-- an inline COMMIT would split them (prevention-log: tx-bound migrations).

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'finra_short_interest_observations'
          AND column_name = 'days_to_cover'
          AND numeric_precision = 10
    ) THEN
        ALTER TABLE finra_short_interest_observations
            ALTER COLUMN days_to_cover  TYPE NUMERIC(20, 4),
            ALTER COLUMN change_percent TYPE NUMERIC(20, 4);
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'finra_short_interest_current'
          AND column_name = 'days_to_cover'
          AND numeric_precision = 10
    ) THEN
        ALTER TABLE finra_short_interest_current
            ALTER COLUMN days_to_cover  TYPE NUMERIC(20, 4),
            ALTER COLUMN change_percent TYPE NUMERIC(20, 4);
    END IF;
END$$;
