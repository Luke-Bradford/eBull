-- 161_drop_blockholder_filer_seeds.sql
--
-- Drop dormant filer-seed table (sql/096 from #766).
-- PR11 (#1233) Task 8.5 — runs AFTER Tasks 8.1-8.4 removed every live
-- reference. Migration ordering rationale (Codex 1a HIGH 2026-05-21):
-- applying this drop earlier in the PR would leave intermediate
-- commits in a state where live resolver / ingester paths query a
-- missing table.

DROP INDEX IF EXISTS idx_blockholder_filer_seeds_active;
DROP TABLE IF EXISTS blockholder_filer_seeds;
