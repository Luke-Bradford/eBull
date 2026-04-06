-- Migration 010: execution guard support
--
-- 1. kill_switch — typed singleton table.
--    id = TRUE enforced by CHECK and PRIMARY KEY so only one row can exist.
--    Seeded immediately so the service can always find the row.
--    Missing row at runtime is treated as configuration corruption (fail closed).
--
-- 2. decision_audit.recommendation_id — FK back to the recommendation being
--    evaluated, nullable so existing rows and future non-recommendation audit
--    use-cases are not broken.
--
-- 3. idx_decision_audit_recommendation — supports lookups by recommendation.

CREATE TABLE IF NOT EXISTS kill_switch (
    id           BOOLEAN PRIMARY KEY DEFAULT TRUE,
    is_active    BOOLEAN NOT NULL DEFAULT FALSE,
    activated_at TIMESTAMPTZ,
    activated_by TEXT,
    reason       TEXT,
    CONSTRAINT kill_switch_single_row CHECK (id = TRUE)
);

-- Seed the single row; safe to re-run.
INSERT INTO kill_switch (id, is_active)
VALUES (TRUE, FALSE)
ON CONFLICT (id) DO NOTHING;

ALTER TABLE decision_audit
    ADD COLUMN IF NOT EXISTS recommendation_id BIGINT
        REFERENCES trade_recommendations(recommendation_id);

CREATE INDEX IF NOT EXISTS idx_decision_audit_recommendation
    ON decision_audit(recommendation_id);
