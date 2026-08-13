-- 214_copy_mirror_closed_positions.sql
--
-- #1927 — copy-mirror closed-position history.
--
-- Archive for nested copy-mirror positions that a copied trader closed
-- while we were still copying them (evicted at step 3a of _sync_mirrors,
-- app/services/portfolio_sync.py). Before this table those rows were
-- hard-DELETEd with no history, so the copy view read as "buy-only":
-- you saw the current open book, never the exits.
--
-- This mirrors the SETTLED own-positions archive pattern
-- (broker_positions_closed, sql/194): the evidence copy of the last-seen
-- row is written immediately before the disappeared-DELETE sweep, in the
-- SAME transaction.
--
-- `LIKE ... INCLUDING DEFAULTS` (not INCLUDING ALL) deliberately: it
-- copies columns + defaults but NOT copy_mirror_positions' FK
-- (mirror_id REFERENCES copy_mirrors ON DELETE CASCADE). History must
-- survive even if a mirror row is ever hard-deleted — same choice as
-- broker_positions_closed. It also means TRUNCATE ... CASCADE over the
-- copy cluster does NOT reach this table (test fixtures truncate it
-- explicitly).
--
-- PK adds mirror_id (position_id is only unique per-mirror) and
-- closed_detected_at (a position may close -> re-copy -> close again),
-- mirroring broker_positions_closed's (position_id, closed_detected_at).

BEGIN;

CREATE TABLE IF NOT EXISTS copy_mirror_closed_positions (
    LIKE copy_mirror_positions INCLUDING DEFAULTS,
    closed_detected_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (mirror_id, position_id, closed_detected_at)
);

-- Read path: recent exits per mirror (get_mirror_detail).
CREATE INDEX IF NOT EXISTS copy_mirror_closed_positions_mirror_time_idx
    ON copy_mirror_closed_positions (mirror_id, closed_detected_at DESC);

COMMIT;
