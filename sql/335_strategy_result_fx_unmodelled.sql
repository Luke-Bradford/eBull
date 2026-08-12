-- 335_strategy_result_fx_unmodelled.sql
--
-- #2363 item 1 — split the coupled cost refusal into its two components.
-- Spec: docs/proposals/ta/2026-08-12-carry-fx-refusal-split.md.
-- Rule + vocabulary: app/services/strategy_result.py. Writer:
-- app/services/result_ledger.py.
--
--
-- ⚠⚠ WHY A SECOND COLUMN RATHER THAN A RE-READ OF THE FIRST.
-- ---------------------------------------------------------------------------
-- `carry_unmodelled` has always been computed as `CARRY_BPS is None OR FX_BPS
-- is None`, so a stored `true` says "at least one of the two was missing" and
-- cannot say which. Carry closes on a per-order eToro product-eligibility
-- response proving underlying-at-x1; FX closes on the account's native currency
-- and a measured conversion markup. Different evidence, different owners,
-- different arrival dates — and coupled, whichever lands first cannot be
-- banked. The gate still needs BOTH clear to promote; what the split buys is
-- that an operator can see WHICH one is blocking.
--
--
-- ⚠ THE BACKFILL IS MEASURED, NOT IMPUTED.
-- ---------------------------------------------------------------------------
-- `DEFAULT TRUE` would ordinarily be a fail-closed guess. Here it is provable:
-- `git log -S"CARRY_BPS" -- app/services/cost_model.py` returns exactly one
-- commit (c3ee15f0, the phase-5b introduction), so no version of that module
-- has ever held a non-NULL carry or FX. Every stored row — across every
-- cost_model_id present — was therefore computed with BOTH components missing,
-- and `fx_unmodelled = true` is the fact rather than the safe assumption. No
-- row count is written here on purpose: `strategy_backtest_run` is a live job,
-- so any figure would be stale by the time it is read. Run this instead — the
-- claim holds when `carry_unmodelled` is TRUE for every group:
--
--     select cost_model_id, carry_unmodelled, count(*)
--       from strategy_results_store group by 1,2;
--
-- ⚠⚠ THE DEFAULT IS KEPT, WHICH IS A DEPARTURE FROM THE OTHER STAMP COLUMNS.
-- The first draft dropped it, so that a writer had to state the stamp rather
-- than inherit it. Measured before committing to that: `strategy_backtest_run`
-- has 13 rows in `job_runs`, the most recent 2026-08-12T18:56Z — it is a LIVE
-- job, so between this migration applying and the daemon picking up the new
-- code, an old writer would insert without the column and hit a NOT NULL
-- violation. Keeping the default makes the migration rolling-safe, and it costs
-- nothing that matters: TRUE is the FAIL-CLOSED value (unmodelled → refused),
-- an old writer genuinely does not charge FX so TRUE is truthful for it, and
-- explicitness is enforced where it belongs — `StrategyResult.fx_unmodelled` is
-- a required, undefaulted field, so every writer going through Python must
-- still state it. The database default only ever catches a raw-SQL writer, and
-- for that caller fail-closed beats a constraint error.
--
--
-- ⚠ ADD COLUMN ... DEFAULT is a catalog-only change on PG11+: no table
-- rewrite, no per-row UPDATE, and so no row trigger fires. That matters on
-- `strategy_preregistration_declarations`, which carries an immutability
-- trigger on UPDATE — this migration is population-independent and would
-- behave identically against an environment that already holds declarations.
-- The index recreate takes a brief ACCESS EXCLUSIVE lock; the relation is small
-- (`select count(*) from strategy_results_store` — low hundreds, and it grows
-- only as backtests run) and this is a single-node stack, so it is not run
-- CONCURRENTLY.

BEGIN;

-- ⚠⚠ FAIL FAST RATHER THAN QUEUE, AND THIS WAS LEARNED HERE, NOT ANTICIPATED.
-- The first attempt at this migration sat blocked for ten minutes behind pid
-- 31499 — `ebull-job-body:strategy_backtest_run`, which holds AccessShareLock
-- on `strategy_results_store` for the life of its transaction. That is bad on
-- its own, but the harm is worse than waiting: a PENDING AccessExclusiveLock
-- queues AHEAD of new readers, so an ALTER that is merely waiting blocks every
-- subsequent SELECT on the relation behind it. The migration was stalling the
-- running dev stack, not just itself. A bounded lock_timeout turns that into a
-- clean, retryable failure during a quiet window.
SET LOCAL lock_timeout = '5s';

-- The stamp. ⚠ On the STORE: `strategy_results` is a VIEW over it (sql/264),
-- not a second table.
ALTER TABLE strategy_results_store
    ADD COLUMN IF NOT EXISTS fx_unmodelled BOOLEAN NOT NULL DEFAULT TRUE;

COMMENT ON COLUMN strategy_results_store.fx_unmodelled IS
    'True when the FX conversion component was not charged (cost_model.FX_BPS '
    'is None, not zero). ⚠ Stamped AS AT COMPUTE TIME and never re-derived, for '
    'the reason carry_unmodelled is: a gate reading today''s module constant '
    'would silently promote a row that never charged it. Closes on the account '
    'native currency + a measured conversion markup (#2363), which is separate '
    'evidence from carry''s product eligibility.';

COMMENT ON COLUMN strategy_results_store.carry_unmodelled IS
    'True when the overnight/financing component was not charged '
    '(cost_model.CARRY_BPS is None, not zero). ⚠ NARROWED by #2363: this used '
    'to mean "carry AND/OR FX", which could not say which was missing. FX now '
    'has its own column. Promotion requires BOTH false.';

-- SELECT * is expanded at creation, so the view must be recreated to expose the
-- new column, and the check option restored — CREATE OR REPLACE drops it
-- (sql/326's sequence, same reason).
CREATE OR REPLACE VIEW strategy_results AS
    SELECT *
    FROM strategy_results_store
    WHERE namespace = 'in_sample';

ALTER VIEW strategy_results SET (check_option = 'cascaded');

-- The promotion sweep's candidate filter. ⚠ It is a COARSE prefilter and not
-- the gate — most refusals are deliberately absent from it — but leaving the
-- old predicate would let it offer rows the structural rule now refuses.
DROP INDEX IF EXISTS idx_strategy_results_promotable_basis;

CREATE INDEX IF NOT EXISTS idx_strategy_results_promotable_basis
    ON strategy_results_store (strategy_id, strategy_version)
    WHERE universe_basis = 'survivorship_free'
      AND NOT carry_unmodelled
      AND NOT fx_unmodelled;

-- The declared twin. A declaration that cannot name FX separately cannot
-- pre-declare the refusal set its run will produce, which is the whole function
-- of freezing one (#2599).
ALTER TABLE strategy_preregistration_declarations
    ADD COLUMN IF NOT EXISTS declared_fx_unmodelled BOOLEAN NOT NULL DEFAULT TRUE;

COMMENT ON COLUMN strategy_preregistration_declarations.declared_fx_unmodelled IS
    'The FX stamp the run WILL carry, declared before it starts. Compared '
    'against the stored row''s actual stamp at hold-out write time. ⚠ Any row '
    'predating this column backfills to true and will then disagree with its '
    'own expected_structural_refusals — which is correct: it was frozen under a '
    'superseded structural-refusal policy and is refused on that ground anyway.';

COMMIT;
