-- #2961: record, durably and BEFORE the broker verb, that a core ENTRY
-- submission is about to be entered.
--
-- The absence of the marker is the only client-side evidence that can prove a
-- broker never received a core authority.  Reference-keyed recovery cannot:
-- measured 2026-09-17 on the demo account, a v2-submitted order that FILLED
-- echoed our `referenceId` exactly and `orders:lookup?referenceId=` still
-- returned HTTP 404 (only `orderId` resolved it).  See issue #2961, comment
-- "The positive case is now observed, and it FAILS".
--
-- Shape follows #2979's landed close-side marker
-- (`strategy_position_operations.status = 'submitting'`), including the reason
-- the two writes must be SEPARATE commits: folding the marker into the
-- authority transaction would make every row read as "may have reached the
-- broker" and the separation would be vacuous.
--
-- ⚠ NULL is not "no marker yet" -- it is "this row was not written by the core
-- ENTRY path, or predates this column".  The alpha arm, manual orders and core
-- CLOSES (which carry their own marker in `strategy_position_operations`) all
-- leave it NULL, and a NULL row is NEVER terminalisable.  The discriminator
-- requires the affirmative 'authority_committed', so every pre-existing row
-- keeps exactly the behaviour it has today and no backfill is required.
--
-- ⚠ A new column rather than a new `state` value: `state` records what the
-- BROKER said, and "we are about to call" is not a broker answer.  Keeping them
-- apart also leaves `strategy_order_reconciliation_resolved_shape` and the
-- backlog partial index untouched.
ALTER TABLE strategy_order_reconciliation_state
    ADD COLUMN IF NOT EXISTS submission_phase TEXT;

ALTER TABLE strategy_order_reconciliation_state
    DROP CONSTRAINT IF EXISTS strategy_order_reconciliation_submission_phase_check;

ALTER TABLE strategy_order_reconciliation_state
    ADD CONSTRAINT strategy_order_reconciliation_submission_phase_check
    CHECK (
        submission_phase IS NULL
        OR submission_phase IN ('authority_committed', 'broker_verb_entered')
    );

COMMENT ON COLUMN strategy_order_reconciliation_state.submission_phase IS
    'Core ENTRY write-ordering marker (#2961). authority_committed = the durable '
    'authority exists and mark_core_submission_entered has NOT committed, so the '
    'broker verb was provably never entered. broker_verb_entered = it MAY have '
    'been entered; nothing client-side can say more. NULL = not a core entry row '
    '(alpha arm, manual order, or core close) and never terminalisable.';
