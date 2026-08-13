-- Migration 030: report snapshots
--
-- Stores periodic (weekly/monthly) performance report snapshots as JSONB.
-- One row per (report_type, period_start). Idempotent rerun replaces the snapshot.

CREATE TABLE IF NOT EXISTS report_snapshots (
    snapshot_id    BIGSERIAL PRIMARY KEY,
    report_type    TEXT NOT NULL CHECK (report_type IN ('weekly', 'monthly')),
    period_start   DATE NOT NULL,
    period_end     DATE NOT NULL,
    snapshot_json  JSONB NOT NULL,
    computed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_report_snapshots_type_period
    ON report_snapshots(report_type, period_start);
