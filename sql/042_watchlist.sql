-- Watchlist (Phase 3.2 of the 2026-04-19 research-tool refocus).
--
-- Per-operator list of instruments the user is tracking. Separate from
-- positions (actively held) and from coverage (system-tiered for scoring).
-- Primary consumer: Dashboard watchlist section + "add to watchlist"
-- button on the instrument page.

CREATE TABLE IF NOT EXISTS watchlist (
    operator_id    UUID NOT NULL REFERENCES operators(operator_id) ON DELETE CASCADE,
    instrument_id  BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    added_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes          TEXT,

    PRIMARY KEY (operator_id, instrument_id)
);

-- Index for the dashboard's "watchlist for this operator" query
-- ordered by added_at DESC (newest first).
CREATE INDEX IF NOT EXISTS watchlist_operator_added_idx
    ON watchlist(operator_id, added_at DESC);
