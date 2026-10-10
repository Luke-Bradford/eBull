-- 478_etoro_dividend_calendar_captures.sql
--
-- #3740 slice F — daily raw capture of eToro's public dividend calendar page
-- (`https://www.etoro.com/investing/dividend-calendar/`). eToro keeps no archive of it, so a day not
-- captured is lost. The bytes are stored exactly as served (after transfer decoding) with no parsing: the
-- page is the candidate ex-date source for a later prospective concordance against EDGAR amounts, and
-- every treatment decision belongs to that measurement, not to the capture.
--
-- One row per fetch, including refusals (a non-200 or a page without calendar rows is stored, then the
-- job fails). The HTML differs on every fetch (a per-request monitoring token), so there is no content
-- de-duplication. `ex_date_marker_count` counts `data-ex-dividend-date="` occurrences: a liveness check
-- only, not a parsed row count. Append-only.

BEGIN;

CREATE TABLE IF NOT EXISTS etoro_dividend_calendar_captures (
    capture_id            BIGSERIAL PRIMARY KEY,
    url                   TEXT NOT NULL CHECK (url <> ''),
    requested_at          TIMESTAMPTZ NOT NULL,
    observed_at           TIMESTAMPTZ NOT NULL,
    http_status           SMALLINT NOT NULL CHECK (http_status BETWEEN 100 AND 599),
    -- Response headers in served order as [name, value] pairs; Set-Cookie is not kept.
    response_headers      JSONB NOT NULL CHECK (jsonb_typeof(response_headers) = 'array'),
    response_sha256       TEXT NOT NULL CHECK (response_sha256 ~ '^[0-9a-f]{64}$'),
    payload_bytes         INTEGER NOT NULL CHECK (payload_bytes >= 0),
    payload_gzip          BYTEA NOT NULL,
    ex_date_marker_count  INTEGER NOT NULL CHECK (ex_date_marker_count >= 0),
    CHECK (observed_at >= requested_at)
);

CREATE INDEX IF NOT EXISTS idx_etoro_dividend_calendar_captures_observed
    ON etoro_dividend_calendar_captures (observed_at DESC);

CREATE OR REPLACE FUNCTION etoro_dividend_calendar_append_only()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END $$;

DROP TRIGGER IF EXISTS etoro_dividend_calendar_captures_append_only ON etoro_dividend_calendar_captures;
CREATE TRIGGER etoro_dividend_calendar_captures_append_only
    BEFORE UPDATE OR DELETE ON etoro_dividend_calendar_captures
    FOR EACH ROW EXECUTE FUNCTION etoro_dividend_calendar_append_only();

COMMENT ON TABLE etoro_dividend_calendar_captures IS
    '#3740 slice F raw daily capture of eToro''s public dividend calendar page: exact gzip body, status, '
    'headers, fetch clocks. No parsing; eToro marks the data indicative and keeps no history. Append-only.';

COMMIT;
