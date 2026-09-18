-- 398_orders_broker_environment.sql
--
-- #3189 finding 4, half (b). Record WHICH broker environment an order was
-- submitted to, so the pending-order poller can prove the row it is about to
-- look up belongs to the environment it is talking to.
--
-- The gap: `orders` carried no environment at all, while
-- `recommendation_order_reconcile` constructs its provider with the CURRENT
-- `settings.etoro_env` and is deliberately NOT demo-gated (refusing to look at
-- a live-environment order would leave exactly the row that matters most
-- wedged). The job's own docstring asserted "the order it is asking about was
-- placed on settings.etoro_env" — an assumption, not a recorded fact.
--
-- ⚠ Why the identity guard added in the same ticket does not cover this.
-- Broker order ids are namespaced PER ENVIRONMENT, so the same numeric id can
-- exist in both. A demo lookup of a live order id can therefore return a real,
-- well-formed demo order whose id — and possibly whose instrument — match, and
-- `identity_mismatch` never fires. The environment has to be compared BEFORE
-- the lookup, which is why it must be stored rather than inferred.
--
-- ⚠ Written at the pre-broker-call INTENT insert (`_persist_submitted_intent`),
-- not from the response: the environment is a property of the ATTEMPT, so it
-- belongs with the durable intent that survives a crash mid-call.
--
-- ⚠⚠ NULL means "no broker environment was recorded", and the poller REFUSES it
-- rather than treating it as a match. A NULL is not "probably ours": if the
-- deployment ever moved between demo and real, a colliding id terminalises the
-- wrong order and releases its claim, which is the failure this column exists to
-- stop. Measured before choosing that side, not assumed:
--   * no CURRENT path can write a pollable NULL row — `_persist_order`'s
--     synthetic-fill branch resolves to `filled` or `failed`, never `pending`,
--     and `_persist_submitted_intent` (the only writer of a pollable row) now
--     records the environment; and
--   * the dev corpus holds ZERO of them:
--       SELECT count(*) FROM orders
--        WHERE recommendation_id IS NOT NULL AND status = 'pending'
--          AND broker_order_ref IS NOT NULL AND broker_environment IS NULL;  -- 0
--     (there are zero recommendation-origin orders at all; the one row is
--     `execution_origin='manual'`).
-- So no backfill, and refusing NULL wedges nothing that exists. A pre-existing
-- row from another deployment surfaces as a DEGRADED reconciliation run needing
-- an operator, which is what it is.
ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS broker_environment TEXT;

ALTER TABLE orders
    DROP CONSTRAINT IF EXISTS orders_broker_environment_check;

ALTER TABLE orders
    ADD CONSTRAINT orders_broker_environment_check CHECK (
        broker_environment IS NULL
        OR broker_environment IN ('demo', 'real')
    );

COMMENT ON COLUMN orders.broker_environment IS
    'Which eToro environment this order was SUBMITTED to, written at the '
    'pre-broker-call intent insert (#3189). NULL means no broker environment '
    'was recorded: a pre-existing row, or a synthetic fill that never reached a '
    'broker (those are never pollable — they resolve to filled or failed). '
    'Broker order ids are namespaced per environment, so the poller compares '
    'this BEFORE looking an id up — a demo lookup of a live id can otherwise '
    'return a well-formed demo order whose id matches. NULL is REFUSED rather '
    'than treated as a match: an unknown environment is not evidence of the '
    'right one.';
