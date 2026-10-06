-- 476_kill_switch_drill.sql
--
-- #3614 item 4 slice 1 — the recorded kill-switch drill
-- (docs/specs/ops/2026-10-06-3614-kill-drill-time-to-flat.md § Schema).
--
-- One event per drill run, with its per-chokepoint outcomes, the engine book it
-- snapshotted, the outstanding authority it found and the close samples behind its
-- time-to-flat ESTIMATE. The event and its children commit in one transaction.
-- Append-only: rows are never updated.
--
-- The build-stamp columns default from the session settings sql/474 reads
-- (`app/db/build_stamp.py`); the table is new, so the default stamps no old row.

BEGIN;

CREATE TABLE IF NOT EXISTS kill_switch_drill_events (
    kill_switch_drill_event_id  BIGSERIAL PRIMARY KEY,
    started_at                  TIMESTAMPTZ NOT NULL,
    finished_at                 TIMESTAMPTZ NOT NULL,
    scheduled_for               TIMESTAMPTZ,
    trigger                     TEXT NOT NULL CHECK (trigger IN ('scheduled', 'manual')),
    job_run_id                  BIGINT REFERENCES job_runs(run_id) ON DELETE RESTRICT,
    actor                       TEXT NOT NULL CHECK (actor <> ''),
    run_token                   UUID NOT NULL UNIQUE,
    mode                        TEXT CHECK (mode IN ('sandbox', 'observe')),
    kill_active_at_start        BOOLEAN,
    kill_active_at_verify       BOOLEAN,
    observed_activated_by       TEXT,
    observed_activated_at       TIMESTAMPTZ,
    observe_outcome             TEXT CHECK (observe_outcome IN ('kill_changed_before_observe')),
    core_mandate_event_id       BIGINT,
    core_mandate_revision       INTEGER,
    core_instrument_id          BIGINT,
    entry_verdict               TEXT NOT NULL CHECK (entry_verdict IN ('passed', 'incomplete', 'failed', 'not_run')),
    book_verdict                TEXT NOT NULL CHECK (book_verdict IN ('ok', 'defects', 'not_run')),
    run_failure                 TEXT CHECK (run_failure IN ('kill_switch_row_missing', 'sandbox_committed', 'exception')),
    failure_detail              TEXT,
    snapshot_at                 TIMESTAMPTZ,
    unmapped_ownerships         INTEGER CHECK (unmapped_ownerships >= 0),
    multiply_mapped_ownerships  INTEGER CHECK (multiply_mapped_ownerships >= 0),
    positions_without_stop      INTEGER CHECK (positions_without_stop >= 0),
    estimated_time_to_flat_s    NUMERIC CHECK (estimated_time_to_flat_s >= 0),
    estimate_null_reason        TEXT CHECK (estimate_null_reason IN (
        'snapshot_unavailable', 'no_close_observed', 'session_unknown',
        'outstanding_authority', 'mapping_defect', 'unsupported_route'
    )),
    close_samples_n             INTEGER CHECK (close_samples_n >= 0),
    close_samples_not_applied   INTEGER CHECK (close_samples_not_applied >= 0),
    close_resolution_max_s      NUMERIC CHECK (close_resolution_max_s >= 0),
    operator_surface            TEXT NOT NULL CHECK (operator_surface IN ('api_only', 'ui')),
    code_commit                 TEXT DEFAULT NULLIF(current_setting('ebull.code_commit', true), ''),
    code_dirty                  BOOLEAN DEFAULT (
        CASE current_setting('ebull.code_dirty', true) WHEN 'true' THEN true WHEN 'false' THEN false END
    ),
    uv_lock_sha256              TEXT DEFAULT NULLIF(current_setting('ebull.uv_lock_sha256', true), ''),
    CHECK (finished_at >= started_at),
    CHECK ((trigger = 'scheduled') = (scheduled_for IS NOT NULL)),
    -- An estimate is a number or a stated reason, never both and never neither.
    CHECK ((estimated_time_to_flat_s IS NULL) <> (estimate_null_reason IS NULL)),
    CHECK ((run_failure IS NULL) = (failure_detail IS NULL)),
    CHECK (book_verdict = 'not_run' OR snapshot_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_kill_switch_drill_events_started
    ON kill_switch_drill_events (started_at DESC);

CREATE TABLE IF NOT EXISTS kill_switch_drill_chokepoints (
    event_id                BIGINT NOT NULL
        REFERENCES kill_switch_drill_events(kill_switch_drill_event_id) ON DELETE RESTRICT,
    chokepoint              TEXT NOT NULL CHECK (chokepoint IN ('C1', 'C2', 'C3')),
    outcome                 TEXT NOT NULL CHECK (outcome IN (
        'kill_refused', 'other_refusal', 'allowed', 'error', 'not_applicable'
    )),
    refusal_code            TEXT,
    failed_rules            TEXT[] NOT NULL DEFAULT '{}',
    drill_read_kill_active  BOOLEAN,
    error_detail            TEXT,
    evaluated_at            TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (event_id, chokepoint),
    CHECK ((outcome = 'error') = (error_detail IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS kill_switch_drill_positions (
    kill_switch_drill_position_id  BIGSERIAL PRIMARY KEY,
    event_id                       BIGINT NOT NULL
        REFERENCES kill_switch_drill_events(kill_switch_drill_event_id) ON DELETE RESTRICT,
    ownership_id                   BIGINT NOT NULL,
    broker_position_id             BIGINT,
    instrument_id                  BIGINT NOT NULL,
    broker_row_updated_at          TIMESTAMPTZ,
    stop_present                   BOOLEAN,
    target_present                 BOOLEAN,
    broker_environment             TEXT,
    opportunity_at                 TIMESTAMPTZ,
    session_unknown                BOOLEAN NOT NULL,
    CHECK (session_unknown = (opportunity_at IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_kill_switch_drill_positions_mapped
    ON kill_switch_drill_positions (event_id, ownership_id, broker_position_id)
    WHERE broker_position_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_kill_switch_drill_positions_unmapped
    ON kill_switch_drill_positions (event_id, ownership_id)
    WHERE broker_position_id IS NULL;

CREATE TABLE IF NOT EXISTS kill_switch_drill_authority (
    event_id  BIGINT NOT NULL
        REFERENCES kill_switch_drill_events(kill_switch_drill_event_id) ON DELETE RESTRICT,
    kind      TEXT NOT NULL CHECK (kind IN ('strategy_trade', 'order')),
    ref_id    BIGINT NOT NULL,
    state     TEXT NOT NULL,
    PRIMARY KEY (event_id, kind, ref_id)
);

CREATE TABLE IF NOT EXISTS kill_switch_drill_close_samples (
    event_id               BIGINT NOT NULL
        REFERENCES kill_switch_drill_events(kill_switch_drill_event_id) ON DELETE RESTRICT,
    position_operation_id  BIGINT NOT NULL,
    trigger_code           TEXT NOT NULL,
    status                 TEXT NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL,
    resolved_at            TIMESTAMPTZ,
    PRIMARY KEY (event_id, position_operation_id)
);

CREATE OR REPLACE FUNCTION kill_switch_drill_append_only()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END $$;

DROP TRIGGER IF EXISTS kill_switch_drill_events_append_only ON kill_switch_drill_events;
CREATE TRIGGER kill_switch_drill_events_append_only
    BEFORE UPDATE ON kill_switch_drill_events
    FOR EACH ROW EXECUTE FUNCTION kill_switch_drill_append_only();

DROP TRIGGER IF EXISTS kill_switch_drill_chokepoints_append_only ON kill_switch_drill_chokepoints;
CREATE TRIGGER kill_switch_drill_chokepoints_append_only
    BEFORE UPDATE ON kill_switch_drill_chokepoints
    FOR EACH ROW EXECUTE FUNCTION kill_switch_drill_append_only();

DROP TRIGGER IF EXISTS kill_switch_drill_positions_append_only ON kill_switch_drill_positions;
CREATE TRIGGER kill_switch_drill_positions_append_only
    BEFORE UPDATE ON kill_switch_drill_positions
    FOR EACH ROW EXECUTE FUNCTION kill_switch_drill_append_only();

DROP TRIGGER IF EXISTS kill_switch_drill_authority_append_only ON kill_switch_drill_authority;
CREATE TRIGGER kill_switch_drill_authority_append_only
    BEFORE UPDATE ON kill_switch_drill_authority
    FOR EACH ROW EXECUTE FUNCTION kill_switch_drill_append_only();

DROP TRIGGER IF EXISTS kill_switch_drill_close_samples_append_only ON kill_switch_drill_close_samples;
CREATE TRIGGER kill_switch_drill_close_samples_append_only
    BEFORE UPDATE ON kill_switch_drill_close_samples
    FOR EACH ROW EXECUTE FUNCTION kill_switch_drill_append_only();

COMMENT ON TABLE kill_switch_drill_events IS
    '#3614 one kill-switch drill run: mode, entry verdict over the C1-C3 chokepoints, book verdict, '
    'and an ESTIMATED time-to-flat (never gated). Append-only.';
COMMENT ON COLUMN kill_switch_drill_events.estimated_time_to_flat_s IS
    'An ESTIMATE in seconds from snapshot_at for an operator_close of every engine position; '
    'NULL with estimate_null_reason when it cannot be formed. 0 = no known engine exposure.';
COMMENT ON TABLE kill_switch_drill_chokepoints IS
    '#3614 one entry chokepoint''s outcome inside one drill. No rows when entry_verdict = not_run.';
COMMENT ON TABLE kill_switch_drill_positions IS
    '#3614 the engine positions one drill snapshotted, from cached broker_positions, not the broker.';

COMMIT;
