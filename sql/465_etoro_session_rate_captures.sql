-- 465_etoro_session_rate_captures.sql
--
-- #3545 slice 1 — eToro bid/ask for the cost model's calibration population, captured hourly inside the NYSE
-- regular session. Spec: docs/proposals/etl/2026-10-03-3545-session-rate-capture.md.
--
-- The daily perishables recorder (sql/426) captures rates once a day at 19:07 UTC, i.e. one clock hour —
-- the same bias the frozen cost model already names as its limit 1. These tables are separate from sql/426
-- on purpose: `ranking_pot_activation` reads the latest `complete` `etoro_perishable_snapshots` row for its
-- eligibility rows, and `ai_trial_readout` reads the latest `etoro_rate_observations` row by an instant;
-- both are hashed into live declarations, and intraday rows there would change what they read.
--
-- Clocks: `observed_at` = receipt of the response a row came from; `quote_at` is the provider's own clock;
-- a capture's knowledge time is `finished_at` (everything commits in ONE transaction at the end).
-- `instrument_id` carries no FK, as sql/426: the universe moves with `sync_universe`.
--
-- Raw bodies are kept for ERRORED requests only (`error_body`). An ok body is projected into
-- `etoro_session_rate_observations` field for field, as sql/426 projects it; the daily recorder keeps the
-- raw ok bodies of the same endpoint and client as the drift sample.
--
-- The only writer is the `etoro_session_rates_capture` job (app/services/session_rate_capture.py).
-- Every table refuses UPDATE (sql/424's `etoro_crowd_append_only`); DELETE stays possible for test cleanup.

CREATE TABLE IF NOT EXISTS etoro_session_rate_captures (
    capture_id            BIGSERIAL   PRIMARY KEY,
    started_at            TIMESTAMPTZ NOT NULL,
    finished_at           TIMESTAMPTZ NOT NULL CHECK (finished_at >= started_at),
    status                TEXT        NOT NULL CHECK (status IN ('complete', 'partial', 'failed')),
    recorder_version      TEXT        NOT NULL,
    universe_rule_version TEXT        NOT NULL,
    -- NULL only when the capture failed before its universe was read.
    universe_size         INTEGER     CHECK (universe_size >= 0),
    -- NULL only when the capture failed before it planned its requests.
    requests_expected     INTEGER     CHECK (requests_expected >= 0),
    requests_ok           INTEGER     NOT NULL CHECK (requests_ok >= 0),
    requests_errored      INTEGER     NOT NULL CHECK (requests_errored >= 0),
    -- Observation rows written; and of those, rows with bid > 0, ask > 0 and ask >= bid (a census, no filter).
    instruments_served    INTEGER     NOT NULL CHECK (instruments_served >= 0),
    instruments_quoted    INTEGER     NOT NULL CHECK (instruments_quoted >= 0 AND instruments_quoted <= instruments_served),
    error                 TEXT,
    CHECK ((status = 'failed') = (error IS NOT NULL)),
    CHECK (requests_expected IS NOT NULL OR status = 'failed'),
    CHECK (universe_size IS NOT NULL OR status = 'failed'),
    CHECK (requests_expected IS NULL OR requests_ok + requests_errored <= requests_expected),
    CHECK (status <> 'complete' OR requests_ok = requests_expected),
    CHECK (status <> 'partial' OR (requests_ok + requests_errored = requests_expected AND requests_errored > 0))
);

CREATE INDEX IF NOT EXISTS etoro_session_rate_captures_started_at_idx ON etoro_session_rate_captures (started_at);

CREATE TABLE IF NOT EXISTS etoro_session_rate_requests (
    capture_id     BIGINT      NOT NULL REFERENCES etoro_session_rate_captures(capture_id),
    seq            INTEGER     NOT NULL CHECK (seq >= 0),
    instrument_ids BIGINT[]    NOT NULL CHECK (cardinality(instrument_ids) >= 1),
    observed_at    TIMESTAMPTZ NOT NULL,
    outcome        TEXT        NOT NULL CHECK (outcome IN ('ok', 'error')),
    -- Any response received, a malformed 200 included; NULL only for a transport error.
    http_status    INTEGER,
    -- The body as served (or the transport error's text) on an errored request; NULL on ok.
    error_body     JSONB,
    CHECK (outcome = 'error' OR http_status BETWEEN 200 AND 299),
    CHECK ((outcome = 'error') = (error_body IS NOT NULL)),
    PRIMARY KEY (capture_id, seq)
);

CREATE TABLE IF NOT EXISTS etoro_session_rate_observations (
    capture_id          BIGINT      NOT NULL,
    instrument_id       BIGINT      NOT NULL CHECK (instrument_id > 0),
    request_seq         INTEGER     NOT NULL,
    observed_at         TIMESTAMPTZ NOT NULL,
    -- Published units as served; NULL when absent or not a number. No validity filter.
    quote_at            TIMESTAMPTZ,
    bid                 NUMERIC,
    ask                 NUMERIC,
    last_execution      NUMERIC,
    conversion_rate_bid NUMERIC,
    conversion_rate_ask NUMERIC,
    PRIMARY KEY (capture_id, instrument_id),
    FOREIGN KEY (capture_id, request_seq) REFERENCES etoro_session_rate_requests (capture_id, seq)
);

CREATE INDEX IF NOT EXISTS etoro_session_rate_observations_instrument_idx
    ON etoro_session_rate_observations (instrument_id, observed_at);

DROP TRIGGER IF EXISTS etoro_session_rate_captures_append_only ON etoro_session_rate_captures;
CREATE TRIGGER etoro_session_rate_captures_append_only
    BEFORE UPDATE ON etoro_session_rate_captures
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();

DROP TRIGGER IF EXISTS etoro_session_rate_requests_append_only ON etoro_session_rate_requests;
CREATE TRIGGER etoro_session_rate_requests_append_only
    BEFORE UPDATE ON etoro_session_rate_requests
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();

DROP TRIGGER IF EXISTS etoro_session_rate_observations_append_only ON etoro_session_rate_observations;
CREATE TRIGGER etoro_session_rate_observations_append_only
    BEFORE UPDATE ON etoro_session_rate_observations
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();
