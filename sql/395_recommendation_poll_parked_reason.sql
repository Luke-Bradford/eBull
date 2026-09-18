-- 395_recommendation_poll_parked_reason.sql
--
-- #2942 half 2 slice B, review follow-up (PR #3168 WARNING). Bound the poller's
-- "record but do not resolve" verdicts.
--
-- Two of the poller's verdicts describe a PERMANENT property of the row, not a
-- transient failure, and both leave the order at status='pending' on purpose so
-- the submission claim stays held:
--
--   filled_unbooked  -- the broker filled it and we refuse to book a late fill
--                       (no persisted exit lot; see order_client
--                       `_record_unbooked_fill`). Re-asking cannot change the
--                       answer: the order is terminal at the broker.
--   ref_not_pollable -- `broker_order_ref` is not a positive integer, so
--                       `lookup_order(order_id=...)` cannot be called at all.
--
-- Without this column both re-select on every hourly fire, spending a shared
-- eToro read (or, for `ref_not_pollable`, a whole no-op tick) and appending a
-- fresh `decision_audit` row for ever.
--
-- ⚠ Parked is NOT resolved, and deliberately does not touch `orders.status`.
-- 'rejected'/'filled' would fall outside
-- `idx_orders_recommendation_open_attempt`'s predicate and RELEASE the claim --
-- which for `filled_unbooked` is the duplicate-order defect #2942 exists to
-- prevent, on an order that demonstrably executed. The row stays 'pending', the
-- claim stays held, and the recommendation stays 'execution_pending' where the
-- operator's own recommendations filter already surfaces it.
--
-- ⚠ Transient verdicts are deliberately NOT parked: `not_found` and
-- `lookup_error` must keep retrying (a transport failure is not a fact about the
-- order), and an unsettled partial fill (#2965) can still progress to Filled.
--
-- ⚠ NULL means "not parked", which is every pre-existing row and every ordinary
-- outstanding order. No backfill: dev holds zero recommendation-origin orders.
ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS recommendation_poll_parked_reason TEXT;

ALTER TABLE orders
    DROP CONSTRAINT IF EXISTS orders_recommendation_poll_parked_reason_check;

ALTER TABLE orders
    ADD CONSTRAINT orders_recommendation_poll_parked_reason_check CHECK (
        recommendation_poll_parked_reason IS NULL
        OR recommendation_poll_parked_reason IN ('filled_unbooked', 'ref_not_pollable')
    );

COMMENT ON COLUMN orders.recommendation_poll_parked_reason IS
    'Why the pending recommendation-order poller stopped asking about this row '
    '(#2942). Set only for verdicts that are a PERMANENT property of the row, '
    'never for a transient lookup failure. Parked is NOT resolved: the order '
    'stays status=''pending'' so the submission claim stays held, because a '
    'terminal status would release it — on filled_unbooked that would be the '
    'duplicate-order defect, on an order that demonstrably executed.';
