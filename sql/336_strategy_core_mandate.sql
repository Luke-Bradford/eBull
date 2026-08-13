-- 336_strategy_core_mandate.sql
--
-- #2603 item 1.  The core/cash mandate: hold a benchmark instrument and cash to
-- operator-declared weights, rebalance only outside a declared band.  Event
-- shaped like every other capital authority here -- one row per operator
-- change, never a heartbeat, no in-place mutation.
--
-- This table AUTHORISES NOTHING.  No allocator, order intent, scheduler entry
-- or endpoint reads it; item 3 is its first consumer.  It is state, not a gate,
-- and must not be cited as one until something calls it.
--
-- Spec: docs/proposals/ta/2026-08-13-core-cash-mandate.md
--
-- Cash is DERIVED (100 - core_target_pct), not stored: a second column permits
-- a state that disagrees with the first, so the weights-sum rule is structural
-- rather than a constraint to chase.
--
-- Unlike sql/311, an enabled row is constrained from row one -- 311 declines
-- that only because it had legacy enabled events to keep readable, and this
-- table has none.

CREATE TABLE IF NOT EXISTS strategy_core_mandate_events (
    core_mandate_event_id   BIGSERIAL PRIMARY KEY,
    revision                BIGINT NOT NULL CHECK (revision >= 1),
    enabled                 BOOLEAN NOT NULL,
    base_currency           TEXT NOT NULL CHECK (base_currency = 'USD'),
    core_instrument_id      BIGINT REFERENCES instruments(instrument_id) ON DELETE RESTRICT,
    core_target_pct         NUMERIC(8,4) NOT NULL CHECK (core_target_pct >= 0 AND core_target_pct <= 100),
    liquidity_reserve_pct   NUMERIC(8,4) NOT NULL CHECK (liquidity_reserve_pct >= 0 AND liquidity_reserve_pct < 100),
    rebalance_band_pct      NUMERIC(8,4) NOT NULL CHECK (rebalance_band_pct > 0 AND rebalance_band_pct <= 100),
    min_rebalance_amount    NUMERIC(18,6) NOT NULL CHECK (min_rebalance_amount > 0),
    policy_version          TEXT NOT NULL CHECK (policy_version = 'core-mandate-v1'),
    changed_by              TEXT NOT NULL CHECK (char_length(changed_by) BETWEEN 1 AND 200),
    reason                  TEXT NOT NULL CHECK (char_length(reason) BETWEEN 1 AND 1000),
    changed_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (revision),
    CONSTRAINT strategy_core_mandate_enabled_has_instrument
        CHECK (NOT enabled OR core_instrument_id IS NOT NULL),
    -- The band must keep both triggers inside [0,100].  A band wider than the
    -- target leaves the lower trigger unreachable short of the core going to
    -- zero, which makes a declared two-sided band silently one-sided.
    CONSTRAINT strategy_core_mandate_band_within_range
        CHECK (core_target_pct - rebalance_band_pct >= 0),
    -- Worst-case cash under the band is 100 - (core_target + band).  Without
    -- this, a band authorises drifting straight through the liquidity reserve,
    -- and the reserve becomes a number the mandate states and the band
    -- contradicts.  It also bounds the band above:
    --   band <= 100 - core_target_pct - liquidity_reserve_pct.
    CONSTRAINT strategy_core_mandate_band_respects_reserve
        CHECK (100 - (core_target_pct + rebalance_band_pct) >= liquidity_reserve_pct)
);

COMMENT ON TABLE strategy_core_mandate_events IS
    'Append-only core/cash mandate revisions (#2603 item 1). Authorises nothing on its own: '
    'the allocator, the eligibility proof and the rebalance path are later slices.';
COMMENT ON COLUMN strategy_core_mandate_events.base_currency IS
    'Locked to USD. This CHECK is the explicit deferral of #2603 item 4 -- five other USD '
    'sites (sql/290:96, strategy_control_plane.py:313, strategy_paper_executor.py:374/:555/:593) '
    'must lift in the same change. Never a partial lift.';
COMMENT ON COLUMN strategy_core_mandate_events.core_target_pct IS
    'Target core weight. Cash is the complement (100 - core_target_pct) by construction; '
    'a two-holding mandate is what makes it exact.';
COMMENT ON COLUMN strategy_core_mandate_events.liquidity_reserve_pct IS
    'Minimum cash share of the CORE SLEEVE. Not strategy_paper_pool_events.cash_reserve_pct, '
    'which is a share of the effective pot for the active sleeve (sql/311:66) -- different '
    'denominator, different claimant. #2525 reconciles them when it fixes the sleeve boundary.';
COMMENT ON COLUMN strategy_core_mandate_events.rebalance_band_pct IS
    'Allowed drift in PERCENTAGE POINTS of core weight, absolute (not relative to target). '
    'Strictly positive: a zero band authorises a rebalance on any drift, and turnover is the '
    'first-order cost filter.';
COMMENT ON COLUMN strategy_core_mandate_events.min_rebalance_amount IS
    'Operator floor in base_currency. NOT the broker minimum: eToro min_position_amount varies '
    'by instrument and arm, so the effective floor at execution is max(declared, broker) and '
    'belongs to item 3.';
COMMENT ON COLUMN strategy_core_mandate_events.core_instrument_id IS
    'Deliberately NOT constrained to the validated US/USD universe: settled-decisions.md:901 '
    'permits a non-US core instrument whose eligibility proof passes.';
COMMENT ON COLUMN strategy_core_mandate_events.policy_version IS
    'Stamps which arithmetic the row was written under so a later policy is detectable per row. '
    'A version string does not freeze a CHECK: changing the invariants is a migration plus a new '
    'version, never a redefinition of this one.';
