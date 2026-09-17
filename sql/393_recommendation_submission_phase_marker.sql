-- 393_recommendation_submission_phase_marker.sql
--
-- #2942 half 2, slice A. Record, durably and BEFORE the broker verb, that a
-- LIVE recommendation submission is about to be entered.
--
-- Half 1 (sql/375) made the pre-I/O intent row THE CLAIM: at most one
-- unresolved attempt per recommendation. That is correct and this does not
-- weaken it. What it left open is that a process death between the claim
-- commit and the broker call parks the claim for ever, on a recommendation the
-- broker never heard of -- `PriorSubmissionUnresolvedError` on every later
-- scheduler pass, with no client-side way to tell that case apart from an
-- attempt that really did reach the broker.
--
-- The absence of this marker is the only client-side evidence that can tell
-- them apart. Reference-keyed recovery cannot: measured 2026-09-17 on the demo
-- account, a v2-submitted order that FILLED echoed our `referenceId` exactly
-- and `orders:lookup?referenceId=` still returned HTTP 404 (only `orderId`
-- resolved it), and the cancel path returns an empty reference. See issue
-- #2961, comment "The positive case is now observed, and it FAILS", which
-- names this shape for #2942 half 2 by number.
--
-- Third adoption of the same shape: `strategy_position_operations.status =
-- 'submitting'` (#2979, closes) and
-- `strategy_order_reconciliation_state.submission_phase` (#2961, sql/392, core
-- entries). Including the reason the two writes must be SEPARATE commits --
-- folding the marker into the claim transaction would make every row read as
-- "may have reached the broker" and the separation would be vacuous.
--
-- ⚠ `broker_verb_entered` means "MAY subsequently have been entered". A marker
-- bounds the instant it commits, never the statement after it: the unattended
-- guard, the credential read, body construction and `ResilientClient`'s
-- throttle and shared-lock wait all sit on the unprovable side.
--
-- ⚠ NULL is not "no marker yet" -- it is "this row was not written by the live
-- recommendation submission path, or predates this column". Demo synthetic
-- fills, strategy-arm orders and manual rows all leave it NULL, and a NULL row
-- is NEVER terminalisable. The discriminator requires the affirmative
-- 'claim_committed', so every pre-existing row keeps exactly the behaviour it
-- has today and no backfill is required.
--
-- ⚠ Named `recommendation_submission_phase`, matching `recommendation_request_id`
-- on this same table, so it cannot be misread as #2961's `submission_phase` --
-- a different column, on a different table, for the core arm.
--
-- Full-population state before writing this file (dev DB, 2026-09-17):
--   select status, count(*) from orders group by 1;            -- ('filled', 1)
--   select count(*) filter (where recommendation_id is not null),
--          count(*) from orders;                               -- (0, 1)
--   select count(*) from orders where recommendation_id is not null
--     and status in ('submitted','pending','uncertain');       -- 0
-- No recommendation-origin order exists, so there is nothing to backfill and
-- no row changes meaning.
ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS recommendation_submission_phase TEXT;

ALTER TABLE orders
    DROP CONSTRAINT IF EXISTS orders_recommendation_submission_phase_check;

ALTER TABLE orders
    ADD CONSTRAINT orders_recommendation_submission_phase_check CHECK (
        recommendation_submission_phase IS NULL
        OR recommendation_submission_phase IN ('claim_committed', 'broker_verb_entered')
    );

COMMENT ON COLUMN orders.recommendation_submission_phase IS
    'Live recommendation-submission write-ordering marker (#2942 half 2). '
    'claim_committed = the durable claim exists and '
    'mark_recommendation_submission_entered has NOT committed, so the broker verb '
    'was provably never entered and the claim may be released. broker_verb_entered '
    '= it MAY have been entered; nothing client-side can say more. NULL = not a '
    'live recommendation submission (demo synthetic fill, strategy arm, manual '
    'order, or predates this column) and never terminalisable.';
