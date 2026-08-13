-- 055_instrument_business_summary.sql
--
-- 10-K Item 1 "Business" narrative extraction (#428). Replaces the
-- Yahoo ``longBusinessSummary`` blurb (~150 words, scraped) with the
-- authoritative multi-page description that every SEC 10-K carries
-- under Item 1. Free, official, bounded per issuer to ~quarterly
-- (10-K cadence + 10-K/A amendments).
--
-- Storage model: one row per instrument. The latest 10-K wins — we
-- don't keep a history because the UI never surfaces a diff view and
-- older descriptions are still recoverable from the source filing
-- via ``source_accession``. A side table of snapshots is a follow-up
-- if product ever wants an "X vs last year" panel.
--
-- Body capped at ~10 KB via NUMERIC column policy elsewhere — here a
-- TEXT field lets Postgres TOAST compress transparently. A soft cap
-- is enforced at the service layer (issue says ~4 KB first-slice
-- for render; we store slightly more headroom so future "more..."
-- expanders don't need a re-fetch).

CREATE TABLE IF NOT EXISTS instrument_business_summary (
    instrument_id       BIGINT      PRIMARY KEY
                            REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    -- Empty string = tombstone sentinel: the ingester recorded an
    -- attempt but parse missed / fetch failed. Readers treat empty
    -- as "no body available" so the UI still falls back to yfinance.
    body                TEXT        NOT NULL DEFAULT '',
    source_accession    TEXT        NOT NULL,
    fetched_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Bumped on every re-parse (mirrors dividend_events.last_parsed_at
    -- pattern from #434). Gates the 7-day TTL so a bad parse on one
    -- run doesn't pound SEC daily.
    last_parsed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_instrument_business_summary_source
    ON instrument_business_summary (source_accession);

COMMENT ON TABLE instrument_business_summary IS
    'Parsed 10-K Item 1 "Business" description per instrument. One '
    'row per instrument — latest 10-K wins. ``source_accession`` '
    'points back to the originating filing for audit; the raw text '
    'is reconstructable from the filing on demand.';

COMMENT ON COLUMN instrument_business_summary.body IS
    'Plain-text Item 1 narrative, HTML-stripped and whitespace-'
    'collapsed. Truncated at the parser layer to ~10 KB so oversized '
    'filings don''t bloat the row. Renderers slice to their own '
    'display budget. Empty string means "tombstone" — the ingester '
    'attempted this instrument but could not extract a body; readers '
    'surface that as None and fall through to yfinance fallback.';

COMMENT ON COLUMN instrument_business_summary.source_accession IS
    'SEC accession of the 10-K the body was extracted from. Forms '
    'the idempotency contract together with instrument_id — re-'
    'running the ingester on the same accession is a no-op unless '
    'the body changed.';

COMMENT ON COLUMN instrument_business_summary.last_parsed_at IS
    'Bumped on every ingester pass even when the upsert is a no-op, '
    'so the 7-day TTL guard skips recently-parsed instruments and '
    'keeps the SEC rate-limit budget intact.';
