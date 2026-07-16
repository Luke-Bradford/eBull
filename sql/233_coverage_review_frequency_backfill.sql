-- #1996 — backfill coverage.review_frequency for rows predating the writer.
--
-- The column exists since 001_init.sql:82 and is READ by thesis staleness
-- (find_stale_instruments rule 2: missing → stale('missing_frequency')),
-- the coverage freshness label, and the execution-guard freshness gate —
-- but until #1996 NO code path wrote it. Every pre-existing row is NULL
-- (12,607/12,607 on dev at discovery, 2026-07-10), which made every
-- thesis permanently "stale" and churned the hourly thesis_refresh on
-- held names.
--
-- Settled mapping (#1996, docs/settled-decisions.md): T1='weekly',
-- T2='monthly', T3='monthly'. Filing-event triggers (#273) cover
-- real-news regen instantly; the age window is only a drift catch-all.
-- Values mirror TIER_REVIEW_FREQUENCY in app/services/coverage.py — the
-- single writer source for all future assignments (seed, bootstrap
-- gap-filler, promote/demote/override).
--
-- Realigns ALL rows disagreeing with the mapping, not just NULLs: the
-- 2026-07-10 interim dev seed assigned by tier, but promotions since then
-- had no writer, so a row promoted after the seed keeps its old frequency
-- (full-pop dev check 2026-07-16: one T1 row carried 'monthly'). No other
-- writer exists (no operator surface sets this column), so tier-derived
-- realignment cannot clobber a deliberate per-instrument value.
-- Idempotent: IS DISTINCT FROM makes re-runs no-ops.

UPDATE coverage
SET review_frequency = CASE WHEN coverage_tier = 1 THEN 'weekly' ELSE 'monthly' END
WHERE review_frequency IS DISTINCT FROM
      (CASE WHEN coverage_tier = 1 THEN 'weekly' ELSE 'monthly' END);
