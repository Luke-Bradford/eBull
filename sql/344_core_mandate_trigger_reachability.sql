-- 344_core_mandate_trigger_reachability.sql
--
-- #2670.  sql/336's two band CHECKs each admit a mandate on which one of the
-- band's two triggers can NEVER fire -- the exact state the first CHECK's own
-- comment says it exists to prevent.  Both comparators are non-strict at
-- precisely the dead point.
--
-- `core_pct = 100 * core_mv / (core_mv + cash)` with both components
-- non-negative (leverage is barred; a negative core value is not a state this
-- two-holding sleeve has), so core_pct is bounded to [0,100].  The allocator's
-- triggers are strict (allocator spec Q2).  Therefore:
--
--   lower trigger  core_pct < target - band   reachable IFF  target - band > 0
--   upper trigger  core_pct > target + band   reachable IFF  target + band < 100
--
-- sql/336 shipped `>= 0` and (via the reserve CHECK at reserve = 0) `<= 100`.
-- Measured on dev, the only database: `t=20,b=20,r=0` and `t=60,b=40,r=0` both
-- INSERT today, and `evaluate_core_rebalance` returns `hold` on each at the
-- extreme state (core at 0% and at 100%) -- the trigger does not fire even
-- where it is arithmetically most able to.
--
-- `select count(*) from strategy_core_mandate_events` -> 0 rows, so both
-- constraints validate with nothing to repair.  Plain ADD, not NOT VALID:
-- there is no row for a deferred validation to spare.  On a database that DID
-- hold a degenerate row this would fail loudly, which is the correct outcome.
--
-- ⚠ `strategy_core_mandate_band_respects_reserve` is deliberately UNCHANGED and
-- stays non-strict.  `t=60,b=30,r=10` satisfies it at equality, is a legitimate
-- (pre-cost) mandate, and its upper trigger is reachable at 90 < 100.
-- Strictening it would reject that.  The unreachability at reserve = 0 comes
-- from the COMBINATION, so reachability gets its own named constraint and the
-- reserve keeps its own -- two properties, two names in
-- psycopg.errors.CheckViolation.diag.constraint_name.  Nor is the reserve CHECK
-- made redundant: `upper < 100` buys only POSITIVE worst-case cash, not cash of
-- at least a positive reserve.  The implication runs the other way (reserve > 0
-- implies upper < 100), so the new constraint bites only at reserve = 0.
--
-- ⚠ sql/336 is NOT edited: app/db/migrations.py:160-172 fails the entire run on
-- a content-hash drift for an already-applied file.  Its `--` comment therefore
-- still describes a guarantee it did not provide; this header and the re-issued
-- COMMENT ON below are the correction.
--
-- NUMERIC(8,4) makes the strict inequalities discrete: each bound needs 0.0001
-- of clearance, so the smallest feasible mandate is target 0.0002 / band 0.0001
-- and the largest feasible upper edge is 99.9999.  A representational
-- exclusion, stated rather than discovered.
--
-- Does NOT newly forbid `core_target_pct = 100`: the shipped reserve CHECK
-- already rejects it for any band > 0, since 100 - (100 + band) < 0 <= reserve.
--
-- Q1 of the allocator spec ("a reserve breach strictly implies an upper-band
-- breach") chains through the reserve CHECK, which does not change; the two new
-- bounds only tighten the set of mandates it quantifies over.
--
-- Spec: docs/proposals/ta/2026-08-13-core-cash-mandate.md, "Amendment (#2670)".

ALTER TABLE strategy_core_mandate_events
    DROP CONSTRAINT strategy_core_mandate_band_within_range;

ALTER TABLE strategy_core_mandate_events
    ADD CONSTRAINT strategy_core_mandate_band_within_range
    CHECK (core_target_pct - rebalance_band_pct > 0);

ALTER TABLE strategy_core_mandate_events
    ADD CONSTRAINT strategy_core_mandate_band_upper_reachable
    CHECK (core_target_pct + rebalance_band_pct < 100);

-- The policy version bump.  Changing the invariants is "a migration plus a new
-- version, never a redefinition of the old one" (item 1's spec, Source rule),
-- and this is squarely that.  0 stored rows does NOT excuse it: a version
-- denotes a RULE SET, not a row population, and `CoreMandate` is a public frozen
-- dataclass, so a v1 mandate can exist without ever having been stored (a test,
-- a cached input, a retried command) and goes from valid to invalid across this
-- change.  Leaving the stamp alone would make one version string mean two
-- different arithmetics -- the silent reinterpretation the stamp exists to
-- prevent.  v1 rows become unstorable, which is intended: a v1 row is one
-- written under looser arithmetic and there are none to preserve.
--
-- ⚠ This ABORTS the migration -- and therefore boot -- on any database that DID
-- configure a mandate under v1, EVEN IF that mandate satisfies the new band
-- bounds.  That is deliberate and the abort is the point, so the precondition is
-- stated as a named failure rather than left to surface as an opaque
-- CheckViolation on a constraint whose subject is a bookkeeping stamp.  Raised
-- at Codex checkpoint 2 (#2670).
--
-- The remedy Codex proposed -- permit both versions at rest so history stays
-- readable -- was measured and rejected.  `load_core_mandate` reads only the
-- LATEST revision, and `strategy_core_allocator._mandate_refusal` returns
-- `core_mandate_policy_unsupported` for any version but the current one.  So a
-- permitted v1 row is a state that is storable and that the table's ONLY
-- consumer refuses: a dead state, which is the class this milestone keeps
-- finding.  sql/311's legacy-readability carve-out does not transfer either --
-- sql/336's own header records that this table constrains from row one BECAUSE
-- it has no legacy rows, which is still true.
DO $$
DECLARE
    superseded bigint;
BEGIN
    SELECT count(*) INTO superseded
    FROM strategy_core_mandate_events
    WHERE policy_version <> 'core-mandate-v2';
    IF superseded > 0 THEN
        RAISE EXCEPTION
            'sql/344: % core mandate revision(s) carry a superseded policy_version', superseded
            USING HINT =
                'Each was written under looser band arithmetic (#2670). Decide per row whether '
                'it still expresses the operator intent under the new bounds, then either '
                'append a fresh revision under core-mandate-v2 and delete the superseded rows, '
                'or re-stamp them deliberately. Do NOT widen this CHECK to admit v1: the '
                'allocator refuses any version but the current one, so such a row is storable '
                'and unusable.';
    END IF;
END $$;

ALTER TABLE strategy_core_mandate_events
    DROP CONSTRAINT strategy_core_mandate_events_policy_version_check;

ALTER TABLE strategy_core_mandate_events
    ADD CONSTRAINT strategy_core_mandate_events_policy_version_check
    CHECK (policy_version = 'core-mandate-v2');

COMMENT ON COLUMN strategy_core_mandate_events.rebalance_band_pct IS
    'Allowed drift in PERCENTAGE POINTS of core weight, absolute (not relative to target). '
    'Bounded so BOTH triggers are REACHABLE, not merely in range (#2670): band < target and '
    'band < 100 - target, strictly. A band equal to either leaves that side''s comparator '
    'unable to become true for a non-negative sleeve, which makes a declared two-sided band '
    'silently one-sided -- and a one-sided intent is not expressible in a single symmetric '
    'column, so it must not be reachable by accident. Strictly positive: a zero band '
    'authorises a rebalance on any drift, and turnover is the first-order cost filter.';

COMMENT ON COLUMN strategy_core_mandate_events.policy_version IS
    'Stamps which arithmetic the row was written under so a later policy is detectable per '
    'row. core-mandate-v2 as of #2670, which made both band triggers reachable; v1 rows are '
    'no longer storable and none existed. A version string does not freeze a CHECK: changing '
    'the invariants is a migration plus a new version, never a redefinition of this one -- '
    'and 0 stored rows does not excuse the bump, because CoreMandate is publicly '
    'constructible and a never-stored v1 object changes validity across such a change.';
