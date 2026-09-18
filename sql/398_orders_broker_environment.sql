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
-- ⚠ NULL means "no broker environment was recorded", and that is a real state,
-- not a missing value:
--   * every pre-existing row (no backfill — dev holds zero
--     recommendation-origin orders; `execution_origin='manual'` only), and
--   * every synthetic-fill row, which never reached a broker at all
--     (`_persist_order`'s branch: demo mode, or a live EXIT with no
--     `broker_positions` row, where the broker is not called).
-- The poller treats NULL as pollable, exactly as today. Refusing NULL would
-- wedge every outstanding order the moment this column landed — the #2942
-- defect reintroduced by its own fix.
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
    'broker. Broker order ids are namespaced per environment, so the poller '
    'compares this BEFORE looking an id up — a demo lookup of a live id can '
    'otherwise return a well-formed demo order whose id matches.';
