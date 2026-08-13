-- #2598 step 4: record WHICH PATH priced a preflight's cost, not just the amount.
--
-- `strategy_entry_preflights.stressed_cost_amount` (sql/287:113) stores the number the
-- entry decision turned on and nothing stores where it came from.  Today there is
-- exactly one producer -- `strategy_paper_executor._costs` sums the broker's what-if
-- components and multiplies by the deployment's `cost_stress_multiplier` -- so the
-- provenance is currently recoverable by reading the code.  It stops being recoverable
-- the moment a second pricing path exists, and it is not recoverable AT ALL for a row
-- already written, because a stored amount cannot say which function produced it.
-- "Every trade path must be auditable" (.claude/CLAUDE.md) is the standing requirement;
-- this is the column that keeps it true through the next change rather than after it.
--
-- ⚠⚠ ONE VALUE IN THE CHECK, DELIBERATELY, AND IT IS NOT AN OVERSIGHT.
-- #2598's scope text names two (`broker_preflight`, `static_band_bound`), the second
-- being the banded static model declared as an execution-side bound.  This run measured
-- that model against the broker on a band-stratified census (60 targets, 15 per band,
-- 2026-08-13, `tests/fixtures/etoro_preflight_2598/band_census_2026-08-13.json`) and
-- the worst observation -- ETR at 381.5 bps against a 32.2 bps band -- is 1.55x the
-- MAXIMUM spread anywhere in that band's calibration snapshot (245.9 bps).  No
-- percentile of that snapshot bounds the broker's own charge, so `static_band_bound`
-- names a design the evidence argues against, and minting the vocabulary member now
-- would let a reader infer a second priced path exists.  It does not.  Adding it later
-- is one `ALTER ... DROP CONSTRAINT ... ADD CONSTRAINT`; un-implying a capability is
-- not.  Same shape as #2653, from the other side: there the fix was to keep a refusal
-- and pin it to its constraint, here it is not to declare a state nothing can produce.
--
-- Source rule: none applies.  This is our own audit vocabulary over our own decision
-- table, not a treatment of source data -- there is no SEC reg or eToro document that
-- governs what we call our pricing paths.  Stated explicitly because "no source rule
-- exists" is a finding that has to be recorded rather than a step to skip.
--
-- Full population at authoring time (dev, the only database):
--   select verdict, count(*) from strategy_entry_preflights group by verdict;  -- []
--   select count(*) from strategy_entry_preflights;                            -- 0
-- The table is EMPTY, so the allocated-row constraint below needs no backfill and can
-- be strict from the first row rather than NULLable-then-tightened.  (It is empty
-- because every preflight to date refuses at `cost_unit_undocumented`: eToro sends the
-- undocumented `value` field and never the documented monetary `amount`, which is the
-- 0/20 wall #2446 measured and #2598 exists to resolve.)
--
-- NULLable on a REJECTED row on purpose: a rejection that never reached the cost step
-- -- `instrument_ineligible`, `below_broker_minimum`, every gate before `_costs` --
-- priced nothing, and writing a basis there would record a pricing that did not happen.

ALTER TABLE strategy_entry_preflights
    ADD COLUMN IF NOT EXISTS cost_basis TEXT;

ALTER TABLE strategy_entry_preflights
    DROP CONSTRAINT IF EXISTS strategy_entry_preflights_cost_basis_vocabulary;
ALTER TABLE strategy_entry_preflights
    ADD CONSTRAINT strategy_entry_preflights_cost_basis_vocabulary
    CHECK (cost_basis IS NULL OR cost_basis IN ('broker_preflight'));

-- An allocated row DID price something -- `stressed_cost_amount` and
-- `net_expectancy_pct` are both non-NULL on that path -- so it must say what priced it.
ALTER TABLE strategy_entry_preflights
    DROP CONSTRAINT IF EXISTS strategy_entry_preflights_allocated_cost_basis;
ALTER TABLE strategy_entry_preflights
    ADD CONSTRAINT strategy_entry_preflights_allocated_cost_basis
    CHECK (verdict <> 'allocated' OR cost_basis IS NOT NULL);

COMMENT ON COLUMN strategy_entry_preflights.cost_basis IS
    'Which pricing path produced stressed_cost_amount. NOT NULL on allocated rows; '
    'NULL on a rejection that never reached the cost step. Vocabulary is pinned to '
    'app.services.strategy_paper_executor.COST_BASES by '
    'tests/test_2598_preflight_cost_basis.py.';
