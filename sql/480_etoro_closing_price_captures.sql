-- 480_etoro_closing_price_captures.sql
--
-- #3740 slice F, parent obligation 6 — daily raw capture of eToro's documented official closing prices,
-- `GET /api/v1/market-data/instruments/history/closing-price`. One parameterless response carries every
-- instrument: `officialClosingPrice` ("Most recent official closing price") and `closingPrices.daily`,
-- `.weekly`, `.monthly` ("Official closing price from the previous trading day / week / month", each with its
-- date). eToro serves no history beyond those, so a day not captured is lost.
--
-- Why it is the obligation-6 candidate rather than the candle close: the daily candle close in `price_daily`
-- is built from Bid (etoro-api skill, #2240) and, measured on 2026-10-10, equals this endpoint's official
-- close exactly for 3,468 of 10,979 instruments on 2026-10-09 (median −0.022%). The step-3 spec asks for an
-- authoritative unadjusted print; this endpoint documents itself as the official close. Whether that holds,
-- and how it meets a split, is slice F's evidence to establish; this table only starts the record.
--
-- The body is stored exactly as served with no parsing (as sql/478). `instrument_rows` is the length of the
-- top-level JSON array: a liveness check, NULL when the body is not a JSON array. A non-200 or a body with
-- no rows is stored, then the job fails.
--
-- UPDATE and DELETE are refused (sql/424's `etoro_crowd_append_only`), as sql/478; the test fixture empties tables
-- under session_replication_role = replica.

CREATE TABLE IF NOT EXISTS etoro_closing_price_captures (
    capture_id      BIGSERIAL   PRIMARY KEY,
    requested_at    TIMESTAMPTZ NOT NULL,
    observed_at     TIMESTAMPTZ NOT NULL,
    http_status     SMALLINT    NOT NULL CHECK (http_status BETWEEN 100 AND 599),
    response_sha256 TEXT        NOT NULL CHECK (response_sha256 ~ '^[0-9a-f]{64}$'),
    payload_bytes   INTEGER     NOT NULL CHECK (payload_bytes >= 0),
    payload_gzip    BYTEA       NOT NULL,
    instrument_rows INTEGER     CHECK (instrument_rows >= 0),
    CHECK (observed_at >= requested_at)
);

CREATE INDEX IF NOT EXISTS etoro_closing_price_captures_observed_idx
    ON etoro_closing_price_captures (observed_at DESC);

DROP TRIGGER IF EXISTS etoro_closing_price_captures_append_only ON etoro_closing_price_captures;
CREATE TRIGGER etoro_closing_price_captures_append_only
    BEFORE UPDATE OR DELETE ON etoro_closing_price_captures
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();

COMMENT ON TABLE etoro_closing_price_captures IS
    '#3740 slice F raw daily capture of eToro''s official closing prices for every instrument (latest, '
    'previous day, week and month-end): exact gzip body, status, fetch clocks. No parsing; eToro keeps no '
    'history. Candidate authoritative raw close (step-3 obligation 6); unvalidated. Append-only.';
