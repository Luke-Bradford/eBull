-- Migration 011: coverage_audit table for tracking tier changes and blocked promotions

CREATE TABLE IF NOT EXISTS coverage_audit (
    audit_id       BIGSERIAL PRIMARY KEY,
    instrument_id  BIGINT NOT NULL REFERENCES instruments(instrument_id),
    changed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    old_tier       SMALLINT NOT NULL CHECK (old_tier BETWEEN 1 AND 3),
    new_tier       SMALLINT NOT NULL CHECK (new_tier BETWEEN 1 AND 3),
    change_type    TEXT NOT NULL CHECK (change_type IN ('promotion', 'demotion', 'override', 'blocked_promotion')),
    rationale      TEXT NOT NULL,
    evidence_json  JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_coverage_audit_instrument
    ON coverage_audit(instrument_id, changed_at DESC);
