-- 363_f0_reconciliation_comparand.sql
--
-- #2602 item 4 (second half) — make the F-0 reconciliation compare LIKE WITH LIKE,
-- so a tolerance can be declared against it.
-- Writers: app/services/account_equity_evidence.py (`record_account_equity_snapshot`),
--          app/services/portfolio_eod.py (`compute_eod_equity`, `_write_snapshot`).
-- Reader:  app/services/account_equity_evidence.py (`load_account_equity_evidence`).
-- Spec:    docs/proposals/ta/2026-08-22-f0-reconciliation-verdict.md
--
--
-- THE TWO SIDES WERE NEVER VALUING THE SAME POPULATION
-- ---------------------------------------------------------------------------
-- `load_account_equity_evidence` compared `broker_account_equity_snapshots.equity`
-- against `portfolio_eod_snapshots.total_value` and hard-coded `comparable = False`.
-- Measured on the dev DB 2026-08-22, on the 6 days where both rows exist:
--
--     official equity          2026-08-19    99,395.65 USD
--     official total_invested                104,060.06 USD
--     sum(broker_positions.amount)            64,529.06 USD
--     gap                                     39,531.00 USD  (39.8% of equity)
--
-- That gap is structural, not drift. `_parse_account_risk_snapshot` folds MIRRORS
-- and PENDING ORDERS into `total_invested`; `portfolio_eod._read_positions` values
-- DIRECT positions only. `BrokerInstrumentInvestment`'s docstring records the same
-- fact from the other end (#2704): of 38 reported instruments, 33 had no direct
-- position at all. The 2026-08-22 sandbox decision states it outright — "the
-- account is shared with non-engine holdings" (docs/settled-decisions.md).
--
-- So the comparand becomes `available_cash + direct_long_market_value`, and
-- `equity - comparand` is reported as a sized residual rather than folded into the
-- difference.
--
--
-- ⚠ NULL IS NEVER READ AS ZERO ON ANY COLUMN HERE
-- ---------------------------------------------------------------------------
-- Every column is nullable because the 12 official rows and 47 local rows that
-- already exist predate it and cannot be backfilled — neither the broker's
-- direct/mirror split nor the marks behind a past local total were retained.
-- Each absence therefore gets its OWN named refusal reason in the reader. A zero
-- default would have been the dangerous choice on precisely the two count columns
-- whose safety argument is "there are none of these": a defaulted
-- `official_direct_short_positions = 0` reads as "no shorts to worry about" on a
-- row that never looked.
--
--
-- ⚠ WHY THE COUNTS EXIST AT ALL, WHEN THE VALUES ARE WHAT IS COMPARED
-- ---------------------------------------------------------------------------
-- Comparing two sums lets one MISSING holding and one EXTRA holding of equal value
-- net to `reconciled`. The counts are the structural check the value comparison
-- cannot perform on itself: `portfolio_eod_snapshots.positions_total` must equal
-- `official_direct_long_positions + official_direct_short_positions`.

BEGIN;

SET LOCAL lock_timeout = '5s';

ALTER TABLE broker_account_equity_snapshots
    -- Sum of BrokerInstrumentInvestment.direct_long_market_value: the direct LONG
    -- holdings' market value, sum(amount + unrealizedPnL.pnL), mirrors and pending
    -- orders excluded. Denominated in the account currency, like every other money
    -- column on this table.
    ADD COLUMN IF NOT EXISTS official_direct_long_market_value NUMERIC,
    ADD COLUMN IF NOT EXISTS official_direct_long_positions INTEGER,
    -- ⚠ A direct SHORT contributes nothing to direct_long_market_value. A non-zero
    -- count means the comparand is incomplete, so the reader refuses rather than
    -- comparing an under-stated official side against a complete local one.
    ADD COLUMN IF NOT EXISTS official_direct_short_positions INTEGER,
    -- ⚠ Pending orders are SUBTRACTED from available_cash and added to
    -- total_invested by eToro's own formula. Our `cash_ledger` knows nothing about
    -- them, so any non-zero value makes the two CASH legs incomparable — a
    -- divergence that would otherwise present as a valuation error.
    ADD COLUMN IF NOT EXISTS official_pending_order_amount NUMERIC;

ALTER TABLE portfolio_eod_snapshots
    -- The declared reconciliation tolerance's positions leg, in this row's
    -- display_currency: sum over PRICED positions of units * 0.01, converted with
    -- the same rates that produced the position's value. One cent of price per unit
    -- held is the rounding of the stored mark and nothing more; see the spec's
    -- "Source rule" section for why no published rule governs this and what a
    -- widening would require.
    ADD COLUMN IF NOT EXISTS mark_rounding_tolerance NUMERIC;

-- ⚠ A tolerance is an allowance, so a negative one would WIDEN nothing and instead
-- make `abs(difference) <= tolerance` unsatisfiable — a silent permanent `diverged`
-- that looks like a finding. Refused at the write.
ALTER TABLE portfolio_eod_snapshots
    ADD CONSTRAINT portfolio_eod_snapshots_mark_rounding_tolerance_nonneg
    CHECK (mark_rounding_tolerance IS NULL OR mark_rounding_tolerance >= 0);

-- ⚠⚠ NO non-negativity CHECK on official_direct_long_market_value, deliberately.
-- It sums a SIGNED term (amount + unrealizedPnL.pnL), and `BrokerInstrumentInvestment`
-- records the decision explicitly: a negative value is "an extreme-but-legitimate
-- state rather than response drift", refused where the number is USED and not at
-- parse time. A CHECK here would fail the entire snapshot write on a state the
-- provider deliberately admits, losing the equity observation as well. The reader
-- refuses it instead, as `reconciliation_inputs_out_of_bounds`.
--
-- Counts have no such licence: they are cardinalities of a set.
ALTER TABLE broker_account_equity_snapshots
    ADD CONSTRAINT broker_account_equity_snapshots_direct_counts_nonneg
    CHECK (
        (official_direct_long_positions IS NULL OR official_direct_long_positions >= 0)
        AND (official_direct_short_positions IS NULL OR official_direct_short_positions >= 0)
    );

-- ⚠ eToro's formula accumulates pending order amounts additively and subtracts the
-- total from credit, so the stored figure is a magnitude. A negative one would
-- ADD to available cash.
ALTER TABLE broker_account_equity_snapshots
    ADD CONSTRAINT broker_account_equity_snapshots_pending_order_amount_nonneg
    CHECK (official_pending_order_amount IS NULL OR official_pending_order_amount >= 0);

COMMENT ON COLUMN broker_account_equity_snapshots.official_direct_long_market_value IS
    'Sum of BrokerInstrumentInvestment.direct_long_market_value -- direct LONG market '
    'value only, mirrors and pending orders excluded, in the account currency. With '
    'available_cash this forms the comparand the local EOD total is reconciled against; '
    'equity minus that comparand is the residual not represented in the local book. NULL '
    'on rows predating sql/363, which the reader refuses as '
    'official_direct_position_value_not_recorded. #2602 item 4.';

COMMENT ON COLUMN broker_account_equity_snapshots.official_direct_long_positions IS
    'Count of direct LONG positions behind official_direct_long_market_value. With the '
    'short count it must equal portfolio_eod_snapshots.positions_total; the counts catch '
    'a missing holding offsetting an extra one, which the value sums cannot. #2602 item 4.';

COMMENT ON COLUMN broker_account_equity_snapshots.official_direct_short_positions IS
    'Count of direct SHORT positions. NOT valued by official_direct_long_market_value, so '
    'any value other than 0 refuses the comparison. NULL (row predates sql/363) refuses '
    'too -- it is "never looked", not "none". #2602 item 4.';

COMMENT ON COLUMN broker_account_equity_snapshots.official_pending_order_amount IS
    'eToro subtracts this from credit to reach available_cash and adds it to '
    'total_invested. cash_ledger does not model pending orders, so any value other than 0 '
    'makes the cash legs incomparable and refuses. NULL refuses for the same reason the '
    'short count does. #2602 item 4.';

COMMENT ON COLUMN portfolio_eod_snapshots.mark_rounding_tolerance IS
    'Positions leg of the declared reconciliation tolerance, in display_currency: sum over '
    'PRICED positions of units * 0.01 converted at this row''s FX. Frozen under '
    'RECONCILIATION_RULE_VERSION = f0-reconcile-v1; widening it requires a measured '
    'justification and a new version, never an edit to the constant. NULL on rows '
    'predating sql/363. #2602 item 4.';

COMMIT;
