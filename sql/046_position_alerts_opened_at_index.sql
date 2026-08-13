-- Migration 046: btree index on position_alerts.opened_at
--
-- Codex pre-push review P2: GET /alerts/position-alerts, POST
-- /alerts/position-alerts/seen, and POST /alerts/position-alerts/dismiss-all
-- all filter on `opened_at >= now() - INTERVAL '7 days'`. Migration 045
-- only added an index on `alert_id DESC`, so once position_alerts
-- accumulates history (no pruning job in scope for #396) the time-window
-- predicate degrades to a full-table scan.
--
-- Plain btree on opened_at (no partial predicate) because partial-index
-- predicates must be IMMUTABLE — now()-based predicates use a STABLE
-- function and would be rejected. A full btree stays small while row
-- volume is low (transition-only episode model) and supports both the
-- 7-day window scan and ORDER BY opened_at if future queries need it.

CREATE INDEX IF NOT EXISTS idx_position_alerts_opened_at
    ON position_alerts (opened_at DESC);
