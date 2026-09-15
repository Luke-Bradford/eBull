-- #2414 — record WHICH bar was overwritten, at the moment the writer overwrites it.
--
-- WHAT THIS IS
--
-- `price_daily` is upserted in place and has no audit column, so once
-- `_upsert_candles`'s ON CONFLICT DO UPDATE returns, nothing can establish that
-- a bar ever held a different number (app/services/market_data.py:1281-1284).
-- The existing telemetry -- `bars_revised`, the age histogram, and #2414's
-- `bars_revised_by_cause` -- is a run-level SUM, and an aggregate carries no
-- per-entity identity. This table adds the identity, and nothing else.
--
-- It is written INSIDE the same transaction as the bar write, unlike those
-- counters, which are deliberately merged AFTER commit (#1293) because a
-- counter cannot roll back. A row can, so it does, and the table therefore
-- cannot claim a revision `price_daily` does not have.
--
-- ⚠⚠ WHAT THIS IS NOT: "THE CORPUS MOVED".
--
-- It records exactly one mutation class -- an OHLCV overwrite. A decision's
-- consumed inputs also change when:
--
--   * a HISTORICAL BAR IS INSERTED (stale re-observation, interior gap fill).
--     `_upsert_candles` counts those as `inserted` and they produce NO row here,
--     yet they move every later recursive indicator, the segment calendar and
--     next-bar fill identity. This is a known, named gap on #2414, left out
--     because capturing it is a different decision with a different volume
--     profile -- and widening this table to carry it would make its name a lie.
--   * `price_bar_quarantine` verdicts or `price_series_break` resolution change
--     what `load_masked_bars` yields, with the OHLCV untouched.
--   * the rule sets change -- already covered by `input_rule_set_versions`.
--
-- ⚠⚠ AND IT DOES NOT LOCATE AFFECTED LEDGER ROWS.
--
-- The obvious join -- `strategy_signals s JOIN price_daily_revision r ON
-- r.instrument_id = s.instrument_id AND s.signal_bar_date >= r.price_date AND
-- s.created_at < r.revised_at` -- was specced, taken to checkpoint 1 and KILLED.
-- It is neither an upper nor a lower bound:
--
--   OVERCOUNTS: segmented evaluation restarts indicator state per segment;
--   `load_masked_bars` masks fields, so revising a masked value changes no
--   consumed input; a VOLUME-ONLY revision is indistinguishable here from a
--   close revision; `signal_bar_date >= price_date` does not establish the bar
--   existed when the signal ran; and not every stored strategy id has unbounded
--   memory (the Wilder-recursion argument covers ATR/ADX, not SMA windows).
--
--   UNDERCOUNTS: the regime is computed on the BENCHMARK, so a benchmark
--   revision reaches every regime-gated instrument and equality on
--   `instrument_id` excludes all of them; cross-sectional ranking evaluates
--   instruments together; `resolve_fills` reads the NEXT bar's open, which
--   `signal_bar_date >= price_date` excludes by construction.
--
-- ⚠ And `strategy_signals.created_at` is the ledger WRITE time, not the bar READ
-- time -- the scan loads and computes before opening its write transaction. Under
-- Read Committed a transaction timestamp does not encode which row versions a
-- reader observed, so NO wall-clock column on either table repairs this. It needs
-- a read-set record, which is the prefix-digest shape sql/386 already priced.
--
-- So: per-entity identity was the missing INPUT for #2414's supersession half.
-- It is not sufficient on its own, and this header is where that is written down.

CREATE TABLE IF NOT EXISTS price_daily_revision (
    revision_id   BIGSERIAL PRIMARY KEY,
    instrument_id BIGINT      NOT NULL REFERENCES instruments(instrument_id),
    price_date    DATE        NOT NULL,
    -- Transaction start time. ⚠ NOT a read-visibility oracle -- see above.
    revised_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- The closed vocabulary is `RevisionCause` (app/services/market_data.py).
    -- A test asserts the two are equal, so adding a member in Python without
    -- this migration fails a test rather than a production INSERT.
    -- ⚠ A BRANCH, NOT AN ECONOMIC CAUSE: one fetch can rewrite bars for more
    -- than one underlying reason at once, including inside a heal, so this is
    -- an UPPER BOUND on attribution. `adjustment_heal` does not mean "split".
    cause         TEXT        NOT NULL CHECK (cause IN (
                      'initial_backfill', 'stale_reobservation', 'incremental',
                      'adjustment_heal', 'force_backfill', 'unknown'))
);

-- APPEND-ONLY, and no UNIQUE (instrument_id, price_date): a bar can be
-- overwritten repeatedly and each overwrite is its own event. It is also
-- reachable for ONE call to revise the same date twice -- `_normalise_candles`
-- (app/providers/implementations/etoro.py:537) flattens every group's inner
-- array with no date dedup -- so a natural key would raise on a payload the
-- writer must tolerate.
--
-- ⚠ "Append-only" is a CONVENTION here. Nothing in this DDL forbids UPDATE or
-- DELETE, and no claim is made that anything does.
--
-- FK is NO ACTION, deliberately unlike `strategy_signals` (which CASCADEs) and
-- deliberately like `price_bar_quarantine`, its closest sibling in this layer.
-- An audit record of a write that happened should not be silently erased by a
-- later instrument delete; a delete blocking on this table is the right signal.

CREATE INDEX IF NOT EXISTS idx_price_daily_revision_instrument_date
    ON price_daily_revision (instrument_id, price_date);

COMMENT ON TABLE price_daily_revision IS
    'Append-only log of price_daily OHLCV OVERWRITES, one row per revised bar '
    '(#2414). ⚠ One mutation class only -- historical INSERTs, quarantine and '
    'segmentation changes move a decision''s consumed inputs and produce NO row '
    'here. ⚠ Does NOT locate affected strategy_signals rows: the instrument_id / '
    'signal_bar_date / created_at join over- AND under-counts, so it is neither '
    'bound. See this migration''s header for both enumerations.';

COMMENT ON COLUMN price_daily_revision.revised_at IS
    'Transaction start time of the write that overwrote the bar. ⚠ NOT a '
    'read-visibility oracle -- it cannot say which row version any reader saw, '
    'so comparing it to strategy_signals.created_at does not establish exposure.';

COMMENT ON COLUMN price_daily_revision.cause IS
    'The write BRANCH that produced the revision (RevisionCause). ⚠ An UPPER '
    'BOUND on attribution, never an economic cause: one fetch can rewrite bars '
    'for several underlying reasons at once. adjustment_heal != split.';
