-- 297_research_comparator_snapshots.sql
--
-- #2482 — immutable provenance for the recent eToro comparator frontier.
--
-- This is deliberately metadata-only apart from the bounded comparator bars
-- written by the explicit loader.  Indicators, returns and polling snapshots
-- do not belong here.  One snapshot row plus one member row per comparator is
-- enough to prove which mutable price_daily extraction produced a frozen
-- research series.

BEGIN;

CREATE TABLE IF NOT EXISTS research_comparator_snapshots (
    snapshot_id              TEXT PRIMARY KEY,
    provider                 TEXT NOT NULL,
    source_relation          TEXT NOT NULL,
    source_contract          TEXT NOT NULL,
    source_terms_url         TEXT NOT NULL,
    frozen_frontier          DATE NOT NULL,
    captured_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_row_count         INTEGER NOT NULL CHECK (source_row_count > 0),
    source_sha256            TEXT NOT NULL
        CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    session_calendar         TEXT NOT NULL,
    source_timezone          TEXT NOT NULL,
    ohlc_adjustment_basis    TEXT NOT NULL
        CHECK (ohlc_adjustment_basis IN ('split_adjusted', 'unadjusted', 'unknown')),
    dividend_adjustment_basis TEXT NOT NULL
        CHECK (dividend_adjustment_basis IN ('none', 'split_and_dividend_adjusted', 'unknown')),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE research_price_series
    ADD COLUMN IF NOT EXISTS comparator_snapshot_id TEXT
        REFERENCES research_comparator_snapshots(snapshot_id);

ALTER TABLE research_price_series
    DROP CONSTRAINT IF EXISTS research_price_series_comparator_not_tradable;
ALTER TABLE research_price_series
    ADD CONSTRAINT research_price_series_comparator_not_tradable
    CHECK (
        comparator_snapshot_id IS NULL
        OR (instrument_id IS NULL AND resolution_method IS NULL)
    );

CREATE INDEX IF NOT EXISTS idx_research_price_series_comparator_snapshot
    ON research_price_series (comparator_snapshot_id, vendor_symbol)
    WHERE comparator_snapshot_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS research_comparator_snapshot_members (
    snapshot_id          TEXT NOT NULL
        REFERENCES research_comparator_snapshots(snapshot_id) ON DELETE RESTRICT,
    vendor_symbol        TEXT NOT NULL,
    source_instrument_id BIGINT NOT NULL CHECK (source_instrument_id > 0),
    series_id            BIGINT NOT NULL UNIQUE
        REFERENCES research_price_series(series_id) ON DELETE RESTRICT,
    source_row_count     INTEGER NOT NULL CHECK (source_row_count > 0),
    source_sha256        TEXT NOT NULL
        CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    first_bar            DATE NOT NULL,
    last_bar             DATE NOT NULL,
    PRIMARY KEY (snapshot_id, vendor_symbol),
    CONSTRAINT research_comparator_snapshot_member_dates_ordered
        CHECK (last_bar >= first_bar)
);

COMMENT ON TABLE research_comparator_snapshots IS
    'One immutable, fingerprinted extraction of comparator-only bars. '
    'Stores provenance and adjustment semantics once per bounded snapshot; '
    'derived indicators are intentionally absent.';

COMMENT ON COLUMN research_price_series.comparator_snapshot_id IS
    'Non-NULL only for comparator-only frozen snapshots. The paired CHECK '
    'forces instrument_id/resolution_method to remain NULL so these series '
    'cannot enter the validated tradable universe.';

COMMENT ON TABLE research_comparator_snapshot_members IS
    'Per-symbol source mapping and fingerprint for one comparator snapshot. '
    'The source instrument id is audit evidence only, never a tradable-universe '
    'resolution on research_price_series.';

COMMIT;
