-- #3189 finding 10 — the three remaining ways a stored fill can abort the whole
-- outcome batch. Same treatment as sql/296 (#2489) and sql/396 (#2414): the
-- existing bounded ledgers get new closed refusal codes; no rows, columns or
-- stores are added.
--
-- sql/396 converted ONE cause — "the fill date is still there and the open under
-- it moved". These are its siblings, and each is still a raise:
--
--   fill_bar_absent      `locate_fill_index` raises when the stored fill date is
--                        no longer in the loaded series.
--   signal_bar_absent    `_locate_signal_index` raises the same way for the
--                        signal date. Signal ledger only — the forecast path
--                        never locates a signal index.
--   fill_bar_open_absent the fill date survives but its open no longer loads.
--                        `fill_price_is_superseded` deliberately returns FALSE
--                        for a masked open ("None is not evidence the bar
--                        moved"), so flow reaches `resolve_outcome`, which
--                        raises "bar N has no open, so it cannot be a fill bar".
--
-- Why they wedge rather than merely fail: `_resolve_fill` is called inside three
-- nested loops (strategy -> instrument -> fill) with no handler, and the cursor
-- is written AFTER the whole strategy resolves. So one unresolvable row aborts
-- its strategy before the cursor advances — the next tick re-selects the same
-- fills and raises again — AND every alphabetically later strategy in the same
-- tick never runs. #3189 finding 7 was the same shape in the order poller.
--
-- ⚠ Recording is not silent re-reading. `locate_fill_index`'s guard exists so a
-- rebuilt corpus cannot be re-read as "whatever bar sits at that position now";
-- an `unresolved` row NAMES the disagreement instead, which is what sql/396
-- established for the sibling case.
--
-- ⚠ No row that resolves today can be relabelled: all three paths currently
-- RAISE, so the only rows this can change are ones that crash.
--
-- Live constraints inspected before authoring (2026-09-18):
--   strategy_outcomes_reason_check — window_truncated, series_break,
--     quarantined_bar, missing_bar_data, unorderable_exit_levels,
--     fill_price_superseded
--   strategy_opportunity_forecast_outcomes_reason_check — the same minus
--     window_truncated
-- Both unions are preserved.
--
-- ⚠ The forecast ledger's vocabulary stays the SMALLER one, per sql/396's own
-- rule. It gains `fill_bar_absent` and `fill_bar_open_absent` and NOT
-- `signal_bar_absent`: that path locates no signal index, so storing it would
-- assert a state the writer cannot reach.
--
-- Population at authoring time (dev, 2026-09-18). Every figure below is
-- reproduced by the query beside it, so a stale one is visible rather than
-- inherited:
--
--   -- fired signals (denominator): 59,135
--   SELECT count(*) FROM strategy_signals WHERE verdict = 'fired';
--
--   -- fill date the corpus no longer holds: 0
--   SELECT count(*) FROM strategy_signals s WHERE s.verdict = 'fired'
--     AND s.fill_bar_date IS NOT NULL AND NOT EXISTS (
--       SELECT 1 FROM price_daily p WHERE p.instrument_id = s.instrument_id
--         AND p.price_date = s.fill_bar_date);
--
--   -- signal date the corpus no longer holds: 0   (same shape, signal_bar_date)
--
--   -- fill bar whose open does not load: 0
--   SELECT count(*) FROM strategy_signals s WHERE s.verdict = 'fired'
--     AND EXISTS (SELECT 1 FROM price_daily p
--       WHERE p.instrument_id = s.instrument_id AND p.price_date = s.fill_bar_date
--         AND (p.open IS NULL OR p.open <= 0));
--
--   -- fill bar outside quarantine coverage: 0
--   SELECT count(*) FROM strategy_signals s WHERE s.verdict = 'fired'
--     AND s.fill_bar_date IS NOT NULL AND NOT EXISTS (
--       SELECT 1 FROM price_quarantine_coverage cov
--        WHERE cov.instrument_id = s.instrument_id
--          AND s.fill_bar_date BETWEEN cov.first_bar AND cov.last_bar);
--
--   -- bars whose open would mask, anywhere in the corpus: 154
--   SELECT count(*) FROM price_daily WHERE open IS NULL OR open <= 0;
--
-- So this is wedge PREVENTION, not an incident — though the class is not
-- hypothetical: #2414 measured its sibling (`fill_price_superseded`) at 221 of
-- 59,069, and 154 bars would mask an open, none of them currently the fill bar
-- of a fired signal.

BEGIN;

ALTER TABLE strategy_outcomes
    DROP CONSTRAINT strategy_outcomes_reason_check;

ALTER TABLE strategy_outcomes
    ADD CONSTRAINT strategy_outcomes_reason_check CHECK (
        reason IS NULL OR reason IN (
            'window_truncated',
            'series_break',
            'quarantined_bar',
            'missing_bar_data',
            'unorderable_exit_levels',
            'fill_price_superseded',
            'fill_bar_absent',
            'signal_bar_absent',
            'fill_bar_open_absent'
        )
    );

ALTER TABLE strategy_opportunity_forecast_outcomes
    DROP CONSTRAINT strategy_opportunity_forecast_outcomes_reason_check;

ALTER TABLE strategy_opportunity_forecast_outcomes
    ADD CONSTRAINT strategy_opportunity_forecast_outcomes_reason_check CHECK (
        reason IS NULL OR reason IN (
            'series_break',
            'quarantined_bar',
            'missing_bar_data',
            'unorderable_exit_levels',
            'fill_price_superseded',
            'fill_bar_absent',
            'fill_bar_open_absent'
        )
    );

COMMIT;
