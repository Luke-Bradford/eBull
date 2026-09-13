-- 375_recommendation_order_identity.sql
--
-- #2942 half 1. The legacy recommendation executor committed a durable order
-- intent before broker I/O (#243) but gave it no durable REQUEST identity and
-- no claim, so an interrupted submission left the recommendation 'approved'
-- and the next scheduler pass created a second intent and a second economic
-- order under a freshly minted x-request-id.
--
-- Two things land here:
--   1. recommendation_request_id -- the UUID committed before I/O and sent as
--      x-request-id, never rotated. Mirrors strategy_request_id (285) rather
--      than reusing it: #2942 forbids relabelling recommendations as strategy
--      trades, and orders_strategy_request_origin_check would reject it.
--   2. idx_orders_recommendation_open_attempt -- THE CLAIM. At most one
--      unresolved attempt per recommendation, enforced by Postgres because the
--      failure mode is a restarted or concurrent second attempt that an
--      application-level SELECT cannot exclude.
--
-- Recommendation orders carry execution_origin = 'manual' (the column's CHECK
-- allows only 'manual'/'strategy', and order_client never sets it), so
-- recommendation origin is identified by recommendation_id IS NOT NULL.
--
-- Neither index is created guarded. A pre-existing duplicate unresolved
-- attempt is exactly the defect this ticket exists for and must fail loudly
-- rather than be absorbed. Verified empty before writing this file:
--   select recommendation_id, count(*) from orders
--    where recommendation_id is not null and status in ('submitted','pending')
--    group by 1 having count(*) > 1;          -- 0 rows
--   select count(*) filter (where recommendation_id is not null) from orders;  -- 0

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS recommendation_request_id UUID;

CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_recommendation_request_id
    ON orders (recommendation_request_id)
    WHERE recommendation_request_id IS NOT NULL;

ALTER TABLE orders
    DROP CONSTRAINT IF EXISTS orders_recommendation_request_origin_check;

ALTER TABLE orders
    ADD CONSTRAINT orders_recommendation_request_origin_check CHECK (
        recommendation_request_id IS NULL
        OR (recommendation_id IS NOT NULL AND execution_origin = 'manual')
    );

CREATE OR REPLACE FUNCTION prevent_recommendation_request_id_change()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.recommendation_request_id IS NOT NULL
       AND NEW.recommendation_request_id IS DISTINCT FROM OLD.recommendation_request_id THEN
        RAISE EXCEPTION 'recommendation_request_id is immutable once assigned';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_orders_recommendation_request_id_immutable ON orders;
CREATE TRIGGER trg_orders_recommendation_request_id_immutable
BEFORE UPDATE OF recommendation_request_id ON orders
FOR EACH ROW EXECUTE FUNCTION prevent_recommendation_request_id_change();

-- 'uncertain' is a new orders.status value: the broker call neither succeeded
-- nor was refused, so the attempt's fate is unknown and re-submission is
-- forbidden. orders.status has no CHECK constraint, and a census of every
-- reader (rg 'FROM orders|JOIN orders|UPDATE orders|INTO orders' over app and
-- frontend/src) found no non-strategy reader of the column -- reporting,
-- tax_ledger and return_attribution join orders THROUGH fills, and an
-- uncertain order has no fills row by construction.
CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_recommendation_open_attempt
    ON orders (recommendation_id)
    WHERE recommendation_id IS NOT NULL
      AND status IN ('submitted', 'pending', 'uncertain');

COMMENT ON COLUMN orders.recommendation_request_id IS
    'Immutable broker idempotency UUID for a recommendation-origin order. '
    'Committed before broker I/O and sent as x-request-id; never rotated after '
    'an uncertain response (#2942).';
