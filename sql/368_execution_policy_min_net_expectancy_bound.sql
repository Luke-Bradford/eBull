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
--         or min_net_expectancy_pct <> min_net_expectancy_pct;                 -- 0
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
    'Minimum stress-adjusted net expectancy, in percent, a fired signal must clear '
    'before strategy_paper_executor will submit it. Non-negative and never NaN. '
    'Zero is admissible: it means "refuse anything negative", and an operator who '
    'wants strictly-positive expectancy sets a positive floor.';
