-- Migration 047: coverage.filings_status transition log + operator read cursor
--
-- 1. coverage_status_events — append-only row per filings_status transition.
--    event_id BIGSERIAL PK for strict-> cursor semantics. Advisory xact lock
--    in the trigger (below) serializes concurrent writers so commit order
--    matches event_id order — required because coverage.filings_status has
--    multiple writer paths (audit_all_instruments, audit_instrument,
--    _apply_backfill_outcome) that can overlap.
-- 2. Trigger logs ALL UPDATE transitions (including NULL->terminal first
--    audit). Endpoint filters to drops-from-analysable. Other slices
--    reserved for future audit UIs without schema change.
-- 3. INSERT path NOT covered — rows land via seed_coverage / bootstrap
--    with NULL or 'unknown'; first subsequent UPDATE fires the trigger.
--    Moot for drops scope (no INSERT writes 'analysable' directly).
-- 4. Dual partial indexes mirror sql/046_position_alerts_opened_at_index.sql
--    — one on event_id DESC for cursor walks, one on changed_at DESC for
--    the 7-day window filter. Same partial predicate on both.
--    No now()-based predicate (STABLE, not IMMUTABLE; Postgres rejects —
--    same rationale as sql/045_position_alerts.sql).
-- 5. operators.alerts_last_seen_coverage_event_id — parallel cursor to
--    existing alerts_last_seen_decision_id + alerts_last_seen_position_alert_id
--    columns. NULL = never acknowledged.

CREATE TABLE IF NOT EXISTS coverage_status_events (
    event_id      BIGSERIAL    PRIMARY KEY,
    instrument_id BIGINT       NOT NULL REFERENCES instruments(instrument_id),
    changed_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    old_status    TEXT         NULL,
    new_status    TEXT         NULL
);

CREATE INDEX IF NOT EXISTS idx_coverage_status_events_drops
    ON coverage_status_events (event_id DESC)
    WHERE old_status = 'analysable' AND new_status IS DISTINCT FROM 'analysable';

CREATE INDEX IF NOT EXISTS idx_coverage_status_events_drops_changed_at
    ON coverage_status_events (changed_at DESC)
    WHERE old_status = 'analysable' AND new_status IS DISTINCT FROM 'analysable';

CREATE OR REPLACE FUNCTION log_coverage_status_transition()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.filings_status IS DISTINCT FROM OLD.filings_status THEN
        -- Xact-scoped advisory lock serializes concurrent coverage writers so
        -- commit order matches event_id order (#396-style cursor safety).
        -- Idempotent within a single txn (bulk UPDATE takes it once per
        -- transitioning row — stacking is harmless; all refs release on commit).
        PERFORM pg_advisory_xact_lock(hashtext('coverage_status_events_writer'));
        INSERT INTO coverage_status_events (instrument_id, old_status, new_status)
        VALUES (NEW.instrument_id, OLD.filings_status, NEW.filings_status);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_coverage_filings_status_transition ON coverage;

CREATE TRIGGER trg_coverage_filings_status_transition
    AFTER UPDATE OF filings_status ON coverage
    FOR EACH ROW
    EXECUTE FUNCTION log_coverage_status_transition();

ALTER TABLE operators
    ADD COLUMN IF NOT EXISTS alerts_last_seen_coverage_event_id BIGINT;
