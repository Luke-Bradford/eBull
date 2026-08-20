-- 350_portfolio_eod_mark_effective_date.sql
--
-- #2602 item 4 — give the local EOD valuation a MEASURED effective mark date.
-- Writer: app/services/portfolio_eod.py (`_read_positions`, `compute_eod_equity`,
--         `_write_snapshot`).
-- Reader: app/services/account_equity_evidence.py (`load_account_equity_evidence`).
--
--
-- WHAT WAS UNKNOWABLE, AND WHY IT NEED NOT HAVE BEEN
-- ---------------------------------------------------------------------------
-- `account_equity_evidence.py` appended `local_eod_effective_time_unknown` to
-- EVERY comparison that had a local snapshot, with the note "computed_at is when
-- the local job ran, not when its closing prices were effective". That was true
-- and it was permanent by construction: nothing recorded when the marks were
-- effective.
--
-- It was also avoidable. `portfolio_eod.py`'s per-position lookup already
-- ORDERs `price_daily` BY `price_date DESC LIMIT 1` and then projects only
-- `close` — the ordering key, which IS the effective date of that mark, was
-- fetched and discarded. `_resolve_snapshot_date` sets `snapshot_date` to
-- MAX(price_date) across held instruments, so `snapshot_date` is defined by the
-- MOST CURRENT instrument and every position whose latest bar is older is
-- carried forward into the total without a record of it.
--
--
-- ⚠ THE HEADER TAKES THE OLDEST MARK, NOT THE NEWEST
-- ---------------------------------------------------------------------------
-- "As of when is this total true" is bounded by the STALEST input, not the
-- freshest. `oldest_mark_date` is therefore MIN(mark_price_date) over the
-- positions that actually contributed to `positions_value` — i.e. the `priced`
-- ones. A `no_price` position has no mark to be stale, and a `no_fx` position
-- contributed nothing to the total, so neither can move the bound; both are
-- already reported by their own counters.
--
-- ⚠ Cash carries no mark date and is unaffected. A snapshot that is ALL cash
-- has no priced position, so `oldest_mark_date` is NULL with
-- `stale_mark_positions = 0` — which the reader must distinguish from "written
-- before this migration". It does, via `positions_priced`.
--
--
-- ⚠⚠ POPULATE FORWARD ONLY. THIS MIGRATION BACKFILLS NOTHING.
-- ---------------------------------------------------------------------------
-- The 43 existing `portfolio_eod_snapshots` rows stored `close_price` without
-- its date. The mark date could be re-derived by re-running the same
-- carry-forward lookup against `price_daily` today — and that would be a
-- reconstruction, not an observation: `price_daily` is amendable, so a bar that
-- has since been backfilled or corrected would hand a historical snapshot a mark
-- date it did not actually use. Those rows read NULL, permanently, and the
-- reader keeps reporting `local_eod_effective_time_unknown` for them, which is
-- the truthful state.

ALTER TABLE portfolio_eod_position_snapshots
    ADD COLUMN IF NOT EXISTS mark_price_date DATE;

COMMENT ON COLUMN portfolio_eod_position_snapshots.mark_price_date IS
    'price_daily.price_date of the close used as this position''s mark. NULL when '
    'price_status = ''no_price'' (there was no bar to use), or when the row predates '
    'sql/350. #2602 item 4.';

ALTER TABLE portfolio_eod_snapshots
    ADD COLUMN IF NOT EXISTS oldest_mark_date DATE,
    ADD COLUMN IF NOT EXISTS stale_mark_positions INTEGER NOT NULL DEFAULT 0;

COMMENT ON COLUMN portfolio_eod_snapshots.oldest_mark_date IS
    'MIN(mark_price_date) over the PRICED positions — the stalest input, which is what '
    'bounds "as of when is this total true". NULL when no position was priced (all cash, '
    'or every position unpriced) or when the row predates sql/350; positions_priced '
    'distinguishes those. #2602 item 4.';

COMMENT ON COLUMN portfolio_eod_snapshots.stale_mark_positions IS
    'Priced positions whose mark_price_date < snapshot_date, i.e. carried forward from an '
    'earlier bar. 0 on rows predating sql/350 is the column default and is NOT evidence '
    'of freshness — read oldest_mark_date IS NULL first. #2602 item 4.';

-- ⚠ A mark cannot be effective AFTER the session it prices. `snapshot_date` is
-- MAX(price_date) across held instruments, so a later mark date would mean the
-- per-position lookup disagreed with the date resolver — a writer defect, and
-- one that would silently make a snapshot look fresher than its inputs.
ALTER TABLE portfolio_eod_snapshots
    ADD CONSTRAINT portfolio_eod_snapshots_oldest_mark_not_future
    CHECK (oldest_mark_date IS NULL OR oldest_mark_date <= snapshot_date);

-- ⚠ Same rule per position. Stated separately because the header holds only the
-- MIN: a single position marked from the future would be invisible in the
-- header the moment any other position is staler.
ALTER TABLE portfolio_eod_position_snapshots
    ADD CONSTRAINT portfolio_eod_position_snapshots_mark_not_future
    CHECK (mark_price_date IS NULL OR mark_price_date <= snapshot_date);

-- ⚠ `stale_mark_positions` counts a SUBSET of the priced positions. Without
-- this a writer could report more carried-forward marks than it priced, which
-- would read as a coverage figure rather than as the arithmetic error it is.
ALTER TABLE portfolio_eod_snapshots
    ADD CONSTRAINT portfolio_eod_snapshots_stale_marks_within_priced
    CHECK (stale_mark_positions >= 0 AND stale_mark_positions <= positions_priced);
