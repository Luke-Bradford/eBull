-- #2414 — a stored fill price the corpus no longer holds must not abort the
-- forward outcome batch. Same treatment as sql/296 (#2489, "one unorderable S-4
-- bracket must not abort the evidence or forward outcome batch"): the existing
-- bounded ledgers get one new closed refusal code; no rows, columns or stores
-- are added.
--
-- `strategy_signals.fill_price` is a stored copy of `price_daily.open` at the
-- fill bar, and `resolve_outcome` already refuses to reinterpret a decision
-- against a corpus that has since moved. Nothing caught that refusal, so it
-- escaped `_resolve_fill` and failed the whole job run. It is now a recorded
-- outcome.
--
-- Live constraints inspected before authoring (2026-09-18):
--   strategy_outcomes_reason_check                    — window_truncated,
--     series_break, quarantined_bar, missing_bar_data, unorderable_exit_levels
--   strategy_opportunity_forecast_outcomes' inline reason CHECK — series_break,
--     quarantined_bar, missing_bar_data, unorderable_exit_levels
-- Both unions are preserved; each appends only fill_price_superseded.
--
-- ⚠ The forecast ledger's vocabulary is DELIBERATELY the smaller one and stays
-- that way. `window_truncated` is not storable there: an immature horizon
-- returns None and is retried rather than written (sql/315). Widening it to the
-- full set here would assert a state that path cannot reach.

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
            'fill_price_superseded'
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
            'fill_price_superseded'
        )
    );

COMMIT;
