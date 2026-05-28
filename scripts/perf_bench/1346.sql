-- #1346 EXPLAIN fixture — SELECT proxy over the partitioned observations
-- table demonstrating the 125-leaf planner walk that motivates `SET LOCAL
-- jit = off` in every ownership_*_current helper transaction.
--
-- Why SELECT, not MERGE: a MERGE with `WHEN NOT MATCHED BY SOURCE THEN
-- DELETE` mutates `_current` even when run for measurement. Codex 2 (#1346
-- pre-push) flagged: "outside seeded range" is a convention not enforced
-- by the fixture — a stray sentinel-collision row would be deleted by every
-- `_time_trials` invocation. SELECT is fixture-safe by construction.
--
-- The cost driver the PR cancels (planner overhead + JIT compile on the
-- partition-pruned MERGE) lives in the USING-source side of the MERGE,
-- which a SELECT over the same partitioned `_observations` table exercises
-- identically: the planner walks all 125 leaves of sql/177 + (when total
-- plan cost crosses `jit_above_cost`) JIT compiles per-partition probes.
-- Empirical 1.86× helper-call speedup remains validated by the prod-sized
-- receipts at #1345.
--
-- Fixture-safety: instrument_id = 1999999999 has zero rows in
-- ownership_institutions_observations on the bench DB (the seeder writes
-- only to `_current` for sentinel ids in [1_000_000_000, 1_000_001_000)).
-- Even if a sentinel collision were ever present, SELECT cannot mutate.

SELECT count(*)
FROM ownership_institutions_observations
WHERE instrument_id = 1999999999
  AND known_to IS NULL
