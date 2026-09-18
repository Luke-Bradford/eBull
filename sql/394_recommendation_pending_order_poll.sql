-- 394_recommendation_pending_order_poll.sql
--
-- #2942 half 2, slice B. The rotation key for the pending recommendation-order
-- poller.
--
-- `execute_order` writes orders.status='pending' when the broker acknowledges
-- without filling, and nothing has ever looked at that row again. The claim
-- index `idx_orders_recommendation_open_attempt` (sql/375) covers 'pending', so
-- such a row wedges its recommendation for ever -- including when the broker
-- rejected the order asynchronously a second later and no order exists at all.
--
-- ⚠ This column is a ROTATION key, not telemetry, and that is why it is written
-- on EVERY attempt path including the ones that change nothing. #2948 recorded
-- the failure it prevents: ordering a bounded backlog on keys that do not change
-- for a non-terminal row is an ABSORBING STATE, not a delay -- once the first
-- `limit` rows are stuck, the row at `limit + 1` is never visited again. NULL
-- sorts first, so a never-polled order is always due.
--
-- ⚠ NOT a health signal, deliberately. It says when we last ASKED, never what
-- the broker said; the answer lives in orders.status, orders.raw_payload_json
-- and the decision_audit row. Reading a recent timestamp here as "reconciled"
-- would be the #2942 defect in a new place.
--
-- Deliberately no cooldown/attempt-count companion columns: the job's cadence is
-- the rate limit (hourly, limit=20 -> at most 20 eToro reads/hour against a
-- shared 60/min budget), and a second retry-backoff mechanism would be machinery
-- with no measured load behind it.
--
-- Full-population state before writing this file (dev DB, 2026-09-18):
--   select status, execution_origin, count(*) from orders group by 1,2;
--                                                        -- ('filled','manual',1)
--   select count(*) filter (where recommendation_id is not null) from orders;  -- 0
--   select count(*) from orders
--     where recommendation_id is not null and status = 'pending';              -- 0
-- No recommendation-origin order exists, so nothing is backfilled and no
-- existing row changes meaning.
ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS recommendation_last_polled_at TIMESTAMPTZ;

COMMENT ON COLUMN orders.recommendation_last_polled_at IS
    'When the pending recommendation-order poller last ASKED the broker about '
    'this order (#2942 half 2). Rotation key for the bounded backlog scan -- '
    'written by every attempt path, including those that change nothing, so the '
    'scan cannot enter the #2948 absorbing state. NULL = never polled, and sorts '
    'first. Says nothing about what the broker answered.';
