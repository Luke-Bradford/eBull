-- Migration 012: link orders to the guard decision that approved them
--
-- 1. recommendation_id — the recommendation this order fulfils
-- 2. decision_id       — the decision_audit row that approved execution
-- 3. Index on recommendation_id for lookups

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS recommendation_id BIGINT
        REFERENCES trade_recommendations(recommendation_id);

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS decision_id BIGINT
        REFERENCES decision_audit(decision_id);

CREATE INDEX IF NOT EXISTS idx_orders_recommendation
    ON orders(recommendation_id);
