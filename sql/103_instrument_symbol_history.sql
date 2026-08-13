-- 103_instrument_symbol_history.sql
--
-- Symbol history per instrument with effective-date ranges (#794
-- schema piece, batched alongside #789 in the ownership-card Tier 0
-- PR).
--
-- Symbol is unstable across rebrands (FB → META), bankruptcy reorgs
-- (BBBY → BBBYQ at delisting), share-class shuffles (GOOG/GOOGL),
-- and ticker reassignment (a previously-delisted ticker reissued to
-- a different CIK). This table records the canonical symbol chain
-- per instrument so:
--
--   1. The Batch 7 symbol-change ingester can write a new row when a
--      ticker change happens, closing out the prior chain.
--   2. The Batch 7 frontend's ``HistoricalSymbolCallout`` can render
--      "Filed as BBBY before 2023 reorg" when the displayed accession
--      pre-dates the current symbol effective range.
--
-- Synthetic backfill from ``instrument_sec_profile.former_names`` is
-- NOT performed here. ``former_names`` is **company-name** history
-- (e.g. "Facebook, Inc." → "Meta Platforms, Inc."), not symbol
-- history. SEC publishes name changes per-CIK; symbol history has to
-- be reconstructed from EDGAR's per-accession ticker tagging or
-- from the instrument's exchange listing history. That ingester
-- ships in Batch 7. For Tier 0, only the current symbol is seeded —
-- one row per instrument with ``effective_to IS NULL``.
--
-- Symbol-clash guard: the PK is scoped per ``instrument_id``, so a
-- previously-used symbol later assigned to a different instrument is
-- recorded as a separate row keyed on the new instrument_id. The
-- two history chains never join.
--
-- Temporal invariants match instrument_cik_history (migration 102):
-- ordered ranges, single-current per instrument, no overlap.
--
-- _PLANNER_TABLES in tests/fixtures/ebull_test_db.py is updated in
-- the same PR per the prevention-log entry.

CREATE TABLE IF NOT EXISTS instrument_symbol_history (
    instrument_id   BIGINT NOT NULL
        REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    symbol          TEXT NOT NULL,
    effective_from  DATE NOT NULL,
    effective_to    DATE,
    source_event    TEXT NOT NULL
        CHECK (source_event IN
            ('imported', 'rebrand', 'delisting', 'relisting', 'manual')),
    PRIMARY KEY (instrument_id, symbol, effective_from),
    CONSTRAINT instrument_symbol_history_dates_ordered
        CHECK (effective_to IS NULL OR effective_to > effective_from)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_instrument_symbol_history_current
    ON instrument_symbol_history (instrument_id)
    WHERE effective_to IS NULL;

ALTER TABLE instrument_symbol_history
    ADD CONSTRAINT instrument_symbol_history_no_overlap
    EXCLUDE USING GIST (
        instrument_id WITH =,
        daterange(effective_from, effective_to, '[)') WITH &&
    );

CREATE INDEX IF NOT EXISTS idx_instrument_symbol_history_symbol
    ON instrument_symbol_history (symbol);

COMMENT ON TABLE instrument_symbol_history IS
    'Symbol history per instrument with effective-date ranges. '
    'Symbol-clash guard: every row is scoped to one instrument_id by '
    'PK, so a reused symbol on a different instrument is a separate '
    'chain. Synthetic backfill from former_names (which is name '
    'history, not symbol history) is intentionally NOT done here; '
    'real symbol-change ingest ships in Batch 7.';

COMMENT ON COLUMN instrument_symbol_history.source_event IS
    'How this row landed: imported (initial backfill), rebrand, '
    'delisting, relisting, manual. CHECK-constrained so a future '
    'parser regression cannot smuggle a sixth value.';
