-- Migration 045: position-alert episode persistence + operator read cursor
--
-- 1. position_alerts — one row per breach EPISODE (not per hourly evaluation).
--    opened_at = onset detection time. resolved_at = clearance detection time
--    (NULL while still breaching). alert_id is BIGSERIAL for strict-> cursor
--    semantics mirroring operators.alerts_last_seen_decision_id (#394 rationale).
-- 2. Partial unique index enforces at-most-one-open-episode per (instrument_id,
--    alert_type). The writer's INSERT path relies on this as the concurrency
--    backstop. Single-threaded scheduler (app/jobs/runtime.py max_instances=1
--    + per-job threading.Lock) makes overlap effectively impossible; this
--    constraint is the defensive second layer.
-- 3. idx_position_alerts_recent on alert_id DESC supports the strip scan in
--    GET /alerts/position-alerts (ORDER BY alert_id DESC + LIMIT 500).
--    No WHERE predicate: partial-index predicates must be IMMUTABLE in
--    PostgreSQL, and now()-based predicates use a STABLE function which
--    would be rejected.
-- 4. operators.alerts_last_seen_position_alert_id — parallel cursor to the
--    existing alerts_last_seen_decision_id column. NULL = never acknowledged.

CREATE TABLE IF NOT EXISTS position_alerts (
    alert_id      BIGSERIAL    PRIMARY KEY,
    instrument_id BIGINT       NOT NULL REFERENCES instruments(instrument_id),
    alert_type    TEXT         NOT NULL
                               CHECK (alert_type IN ('sl_breach', 'tp_breach', 'thesis_break')),
    opened_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    resolved_at   TIMESTAMPTZ  NULL,
    detail        TEXT         NOT NULL,
    current_bid   NUMERIC      NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_position_alerts_open
    ON position_alerts (instrument_id, alert_type)
    WHERE resolved_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_position_alerts_recent
    ON position_alerts (alert_id DESC);

ALTER TABLE operators
    ADD COLUMN IF NOT EXISTS alerts_last_seen_position_alert_id BIGINT;
