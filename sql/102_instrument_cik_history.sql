-- 102_instrument_cik_history.sql
--
-- CIK chain per instrument with effective-date ranges (#794 schema
-- piece, batched alongside #789 in the ownership-card Tier 0 PR).
--
-- SEC's CIK is the stable identifier across rebrands, reorgs, and
-- ticker changes (FB → META, BBBY → BBBYQ at delisting). Symbol is
-- not stable. Today every filings table keys on ``instrument_id``,
-- not CIK, so the read side already survives a rename if the
-- ingester resolved the historical CIK to the same instrument_id at
-- write time. This table records the canonical CIK chain so:
--
--   1. The Batch 7 symbol-change ingester can write a new row when a
--      reorg happens, closing out the prior chain.
--   2. The ownership-rollup service has a stable hook
--      (``historical_ciks_for(instrument_id)``) that returns every
--      CIK ever associated with the instrument — useful for
--      cross-source dedup and for the diagnostic operator endpoint.
--
-- This migration only ships the schema + a one-row-per-instrument
-- backfill from ``instrument_sec_profile.cik`` (the current CIK).
-- Historical chains land in Batch 7 alongside the actual reorg
-- ingester. The BBBY ownership card is NOT yet production-trustworthy
-- after this migration — that's by design; the schema lands so the
-- Batch 7 ingester has somewhere to write.
--
-- Temporal invariants enforced at the DB layer:
--
--   * ``effective_to IS NULL OR effective_to > effective_from`` — no
--     inverted ranges, no zero-duration ranges. Codex spec review
--     caught the v1 spec missing this.
--   * UNIQUE INDEX on ``(instrument_id) WHERE effective_to IS NULL``
--     — only one "current" CIK per instrument. A reorg ingester that
--     forgets to close out the prior current row blows up loud at
--     the DB rather than silently producing two current chains.
--   * GIST EXCLUDE on ``daterange(effective_from, effective_to, '[)')``
--     — no two ranges for one instrument can overlap in time.
--     Half-open ``[from, to)`` so a chain ending on 2023-05-01 doesn't
--     conflict with the next chain starting on 2023-05-01. ``btree_gist``
--     is required to combine the equality on instrument_id with the
--     range overlap on the daterange.
--
-- _PLANNER_TABLES in tests/fixtures/ebull_test_db.py is updated in
-- the same PR per the prevention-log entry.

CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE IF NOT EXISTS instrument_cik_history (
    instrument_id   BIGINT NOT NULL
        REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    cik             TEXT NOT NULL,
    effective_from  DATE NOT NULL,
    effective_to    DATE,
    source_event    TEXT NOT NULL
        CHECK (source_event IN
            ('imported', 'rebrand', 'reorg', 'merger', 'spinoff', 'manual')),
    PRIMARY KEY (instrument_id, cik, effective_from),
    CONSTRAINT instrument_cik_history_dates_ordered
        CHECK (effective_to IS NULL OR effective_to > effective_from)
);

-- One "current" CIK per instrument.
CREATE UNIQUE INDEX IF NOT EXISTS uq_instrument_cik_history_current
    ON instrument_cik_history (instrument_id)
    WHERE effective_to IS NULL;

-- Non-overlapping date ranges per instrument. ``btree_gist`` lets us
-- combine ``instrument_id WITH =`` (btree-shaped equality) with the
-- daterange overlap operator ``&&`` in one EXCLUDE constraint.
ALTER TABLE instrument_cik_history
    ADD CONSTRAINT instrument_cik_history_no_overlap
    EXCLUDE USING GIST (
        instrument_id WITH =,
        daterange(effective_from, effective_to, '[)') WITH &&
    );

-- Reverse lookup: given a CIK, which instruments has it ever been
-- associated with? Used by the Batch 7 ingester to resolve a filing
-- under a historical CIK back to the current instrument_id.
CREATE INDEX IF NOT EXISTS idx_instrument_cik_history_cik
    ON instrument_cik_history (cik);

COMMENT ON TABLE instrument_cik_history IS
    'CIK chain per instrument with effective-date ranges. Reader path '
    'on the ownership card uses this to resolve filings under a '
    'historical CIK back to the current instrument_id (#794). '
    'effective_to NULL = current. EXCLUDE forbids overlapping ranges; '
    'partial UNIQUE INDEX forbids two "current" rows per instrument.';

COMMENT ON COLUMN instrument_cik_history.source_event IS
    'How this row landed: imported (initial backfill), rebrand, '
    'reorg, merger, spinoff, manual. CHECK-constrained so a future '
    'parser regression cannot smuggle a seventh value.';
