-- 479_etoro_session_close_captures.sql
--
-- #3740 slice F, parent obligations 6 and 7 — the close eToro serves for a session, captured on that session.
--
-- eToro's candle history is back-adjusted at fetch time: after a split every served bar is on the new basis
-- (app/services/market_data.py `detect_adjustment_event`, which only logs the ratio). `price_daily` is
-- upserted in place, `price_daily_revision` (sql/387) records which bar was overwritten but not its prior
-- value, and raw eToro response dumps were retired in #471, so nothing stored today records the close as it
-- was served before a later re-basing. The step-3 spec names this capture as the
-- candidate for both obligations (docs/research/2026-10-10-3740-step3-vw-book.md, inputs 6 and 7): the
-- captured close is the session's unadjusted close if eToro's served bar is unadjusted on its own session, and
-- a split is dated by the change in the captured-to-adjusted close ratio between sessions. Whether the first
-- condition holds is slice F's evidence to establish; this table only starts the record.
--
-- One row per own-session bar observed by `daily_candle_refresh`: the bar dated the sweep's completed provider
-- session (`fresh_through`), written in the same transaction as the `price_daily` upsert. A bar first fetched
-- after its own session's sweep is not captured here. No de-duplication: a refetch in a later retry appends.
-- `observed_at` is `clock_timestamp()` at insert, after the response was received, so it bounds the fetch time
-- from above. The close is stored as served, zero-close sentinels included; no validity filter.
--
-- `instrument_id` carries no FK, as sql/465: the universe moves with `sync_universe`. UPDATE is refused
-- (sql/424's `etoro_crowd_append_only`); DELETE stays possible for test cleanup.

CREATE TABLE IF NOT EXISTS etoro_session_close_captures (
    capture_id    BIGSERIAL   PRIMARY KEY,
    instrument_id BIGINT      NOT NULL CHECK (instrument_id > 0),
    session_date  DATE        NOT NULL,
    close         NUMERIC     NOT NULL,
    observed_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS etoro_session_close_captures_instrument_session_idx
    ON etoro_session_close_captures (instrument_id, session_date);

DROP TRIGGER IF EXISTS etoro_session_close_captures_append_only ON etoro_session_close_captures;
CREATE TRIGGER etoro_session_close_captures_append_only
    BEFORE UPDATE ON etoro_session_close_captures
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();

COMMENT ON TABLE etoro_session_close_captures IS
    '#3740 slice F: the daily close eToro served for the sweep''s completed session, captured by '
    'daily_candle_refresh on that session, before any later back-adjustment. Candidate raw-close and '
    'split-dating record (step-3 obligations 6 and 7); unvalidated. Append-only (UPDATE refused).';
