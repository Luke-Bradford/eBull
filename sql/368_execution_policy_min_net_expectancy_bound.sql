-- #2859 -- bound `min_net_expectancy_pct`, the one numeric limit on
-- `strategy_execution_policies` that carried NO constraint of any kind.
--
-- Every numeric sibling on this table is bounded both in
-- `strategy_control_plane.configure_execution_policy` and by a CHECK here:
--
--     max_instrument_exposure_pct  > 0 AND <= 100
--     max_portfolio_exposure_pct   > 0 AND <= 100
--     max_drawdown_pct             > 0 AND <  100
--     cost_stress_multiplier       >= 1
--
-- `min_net_expectancy_pct` had neither.  It is read by
-- `app/services/strategy_paper_executor.py` as
-- `net_expectancy < intent.min_net_expectancy_pct` -> refuse, so a NEGATIVE
-- floor inverts the gate: a paper order whose stressed cost exceeds its
-- forecast expectancy is admitted, and the refusal row that would have
-- recorded the fact is never written.  The service-side bound lands in the
-- same change; this is the copy that survives a future caller which does not
-- go through it.
--
-- ⚠⚠ The bound is deliberately two-sided-by-construction, not a bare `>= 0`.
-- Measured against this Postgres, NOT reasoned from IEEE semantics:
--
--     select 'NaN'::numeric >= 0;              -- t
--     select 'NaN'::numeric = 'NaN'::numeric;  -- t
--     select 'Infinity'::numeric(12,8);        -- ERROR: numeric field overflow
--
-- so `>= 0` alone ADMITS NaN (Postgres sorts NaN above every non-NaN value),
-- while the two-sided sibling CHECKs above exclude it only as an accident of
-- their upper bound.  `<> 'NaN'` is the explicit exclusion: `NaN <> NaN` is
-- FALSE here, and a CHECK rejects on FALSE.  Infinity needs no clause -- the
-- NUMERIC(12,8) declaration already refuses it.
--
-- Verification of existing rows before adding the constraint:
--
--     select count(*) from strategy_execution_policies;                        -- 0
--     select count(*) from strategy_execution_policies
--      where min_net_expectancy_pct < 0
--         or min_net_expectancy_pct = 'NaN'::numeric;                          -- 0
--
-- ⚠ The NaN half of that query is written as `= 'NaN'::numeric` and NOT as the
-- IEEE self-inequality `col <> col`, which is the natural thing to reach for and
-- is WRONG here for the same reason this CHECK needs its own clause: Postgres
-- makes `NaN = NaN` TRUE, so `col <> col` is FALSE for a NaN row and the query
-- would report zero offending rows while NaNs sat in the table. Caught by a
-- second-opinion pass, after the correct semantics had already been measured
-- three lines above -- knowing the rule is not the same as applying it.
--
-- ⚠ No CHECK on `strategy_execution_policy_events`, matching that table's
-- existing shape: it is the append-only revision log and carries no numeric
-- CHECKs at all, because it must be able to record whatever the policy table
-- accepted rather than re-adjudicate it.

ALTER TABLE strategy_execution_policies
    DROP CONSTRAINT IF EXISTS strategy_execution_policies_min_net_expectancy_bounded;

ALTER TABLE strategy_execution_policies
    ADD CONSTRAINT strategy_execution_policies_min_net_expectancy_bounded
    CHECK (min_net_expectancy_pct >= 0 AND min_net_expectancy_pct <> 'NaN'::numeric);

COMMENT ON COLUMN strategy_execution_policies.min_net_expectancy_pct IS
    'Inclusive floor on stress-adjusted net expectancy, in percent. '
    'strategy_paper_executor refuses a fired signal on '
    '"net_expectancy < min_net_expectancy_pct", so a signal EQUAL to the floor is '
    'admitted -- the comparison is strict, and the floor is a minimum rather than '
    'a threshold to exceed. Non-negative and never NaN. Zero is admissible and '
    'means "refuse anything negative"; an operator wanting strictly-positive '
    'expectancy sets a positive floor.';
